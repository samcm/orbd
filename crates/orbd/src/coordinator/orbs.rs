//! `orbd.control.v1.Orbs` on the admin socket, and the `orbd orb` and `orbd status` CLI:
//! the coordinator's orbs and why one is stuck, deleting one whose thread is gone (Amp
//! never deletes the orbs of archived threads), and pausing or deleting one Amp no
//! longer drives.

use super::core::{
    BACKEND_CALL_TIMEOUT, Msg, OperatorJob, Policy, shortfalls, state_label, wake_demand,
};
use super::db::Db;
use super::store::{self, CmdHistory};
use super::types::{Req, ReqStatus, Status as ReqResult};
use crate::backend::{
    Availability, ComputeBackend, OrbDiskUse, PlacementCandidate, PlacementRequest,
};
use crate::model::{
    ActionResult, AmpOrbId, BackendId, CommandFailure, ExecutionTarget, FailureCode, Location,
    OrbId, OrbRecord, OrbState, Resources, StableOrbState,
};
use orbd_proto::control::v1 as pb;
use orbd_proto::control::v1::dispatch_event::Event;
use orbd_proto::control::v1::orbs_server::Orbs;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

pub struct OrbsService {
    pub(super) core: mpsc::Sender<Msg>,
    pub(super) db: Db,
    pub(super) backends: HashMap<BackendId, Arc<dyn ComputeBackend>>,
    pub(super) policy: Policy,
}

/// Commands read per orb for its failures and its open command.
const RECENT_COMMANDS: u32 = 50;
const MAX_FAILURES: usize = 5;

fn internal(e: anyhow::Error) -> Status {
    Status::internal(format!("{e:#}"))
}

/// serde's snake_case name of a unit enum value.
fn snake<T: serde::Serialize>(v: &T) -> String {
    match serde_json::to_value(v) {
        Ok(serde_json::Value::String(s)) => s,
        _ => String::new(),
    }
}

/// A live orb: its record, when it was created, and whether a delete of it started.
type Live = (OrbRecord, u64, bool);

/// What `ListOrbs` and `GetOrb` read from the database for some orbs.
struct Snapshot {
    /// Per orb: its latest commands (latest first) and when it last spawned or paused.
    activity: HashMap<OrbId, (Vec<CmdHistory>, Option<u64>)>,
    requests: Vec<Req>,
    pools: HashMap<(String, String), store::PoolUse>,
    /// What each orb's own running reservation holds, left out of its wake check as
    /// admission leaves it out.
    own: HashMap<OrbId, Resources>,
}

#[tonic::async_trait]
impl Orbs for OrbsService {
    async fn list_orbs(
        &self,
        _: Request<pb::ListOrbsRequest>,
    ) -> Result<Response<pb::ListOrbsResponse>, Status> {
        let orbs = self.live().await?;
        let snap = self.snapshot(&orbs, RECENT_COMMANDS).await?;
        let orbs = self.infos(&snap, &orbs).await;
        Ok(Response::new(pb::ListOrbsResponse { orbs }))
    }

    async fn get_orb(
        &self,
        request: Request<pb::GetOrbRequest>,
    ) -> Result<Response<pb::GetOrbResponse>, Status> {
        let r = request.into_inner();
        let live = self.live().await?;
        let one = resolve(&live, &r.orb)?.clone();
        let history = match r.history {
            0 => 20,
            n => n.min(500),
        };
        let snap = self
            .snapshot(std::slice::from_ref(&one), history.max(RECENT_COMMANDS))
            .await?;
        let info = self
            .infos(&snap, std::slice::from_ref(&one))
            .await
            .pop()
            .expect("one orb");
        let orb = one.0;
        let commands = snap
            .activity
            .get(&orb.id)
            .map(|(c, _)| c.iter().take(history as usize).map(command_pb).collect())
            .unwrap_or_default();
        let requests = snap
            .requests
            .iter()
            .rev()
            .filter(|q| {
                q.amp_endpoint == orb.identity.amp_endpoint
                    && q.amp_orb_id == orb.identity.amp_orb_id
            })
            .map(|q| {
                let res = q.result.as_ref();
                pb::OrbRequest {
                    action: q.action.name().into(),
                    status: q.status.as_str().into(),
                    start_unix_ms: q.start_ms,
                    result: res.map(|r| r.status.label().to_owned()).unwrap_or_default(),
                    message: res.map(|r| r.message.clone()).unwrap_or_default(),
                    operation_id: q.operation_id.map(|o| o.to_string()).unwrap_or_default(),
                }
            })
            .collect();
        let image_ref = match &orb.spec.initial_disk {
            crate::model::InitialDiskSource::Image { image_ref }
            | crate::model::InitialDiskSource::ProjectSnapshot { image_ref, .. } => {
                image_ref.clone()
            }
        };
        Ok(Response::new(pb::GetOrbResponse {
            orb: Some(info),
            amp_endpoint: orb.identity.amp_endpoint.to_string(),
            spec_digest: orb.spec_digest.0.clone(),
            image_ref,
            generation: orb.observation.generation,
            commands,
            requests,
        }))
    }

    async fn delete_orb(
        &self,
        request: Request<pb::DeleteOrbRequest>,
    ) -> Result<Response<pb::DeleteOrbResponse>, Status> {
        let r = request.into_inner();
        let min_asleep = (!r.force).then(|| Duration::from_secs(r.min_asleep_secs));
        let message = self
            .job(&r.amp_orb_id, OperatorJob::Delete { min_asleep })
            .await?;
        Ok(Response::new(pb::DeleteOrbResponse { message }))
    }

    async fn pause_orb(
        &self,
        request: Request<pb::PauseOrbRequest>,
    ) -> Result<Response<pb::PauseOrbResponse>, Status> {
        let r = request.into_inner();
        let message = self.job(&r.amp_orb_id, OperatorJob::Pause).await?;
        Ok(Response::new(pb::PauseOrbResponse { message }))
    }
}

/// `query` as an Amp orb id: `T-<uuid>` and a bare uuid become `amp-<uuid>`. None when
/// nothing follows the prefix.
fn amp_id(query: &str) -> Option<String> {
    let q = query.trim();
    let rest = q
        .strip_prefix("T-")
        .or_else(|| q.strip_prefix("amp-"))
        .unwrap_or(q);
    (!rest.is_empty()).then(|| format!("amp-{rest}"))
}

/// The live orb `query` names exactly: its internal id, Amp id or thread id.
fn resolve_exact<'a>(orbs: &'a [Live], query: &str) -> Option<&'a Live> {
    let q = query.trim();
    orbs.iter()
        .find(|(o, _, _)| o.id.to_string() == q)
        .or_else(|| {
            let id = amp_id(q)?;
            orbs.iter().find(|(o, _, _)| o.identity.amp_orb_id.0 == id)
        })
}

/// Finds the one live orb `query` names: exactly (see [`resolve_exact`]), or by a
/// unique prefix of its Amp id.
fn resolve<'a>(orbs: &'a [Live], query: &str) -> Result<&'a Live, Status> {
    if let Some(o) = resolve_exact(orbs, query) {
        return Ok(o);
    }
    let Some(prefix) = amp_id(query) else {
        return Err(Status::invalid_argument(format!(
            "{query:?} names no orb: give an Amp orb id, a prefix of one, or a thread id"
        )));
    };
    let matches: Vec<_> = orbs
        .iter()
        .filter(|(o, _, _)| o.identity.amp_orb_id.0.starts_with(&prefix))
        .collect();
    match matches.as_slice() {
        [one] => Ok(one),
        [] => Err(Status::not_found(format!(
            "no orb matches {query:?} (deleted orbs are not kept)"
        ))),
        many => Err(Status::invalid_argument(format!(
            "{query:?} matches {} orbs, give more of the id: {}",
            many.len(),
            many.iter()
                .take(10)
                .map(|(o, _, _)| o.identity.amp_orb_id.0.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

fn failure_pb(action: &str, f: &CommandFailure, at: u64, op: String) -> pb::OrbFailure {
    pb::OrbFailure {
        action: action.into(),
        code: pb::FailureCode::from(f.code) as i32,
        message: f.message.clone(),
        unix_ms: at,
        operation_id: op,
    }
}

fn command_pb(c: &CmdHistory) -> pb::OrbCommand {
    let at = c.terminal_ms.unwrap_or(c.created_ms);
    let (result, failure) = match c.outcome.as_ref().map(|o| &o.result) {
        Some(Ok(ActionResult::Spawned { start, .. })) => {
            (format!("spawned ({})", snake(start)), None)
        }
        Some(Ok(ActionResult::Paused {
            checkpoint: Some(_),
        })) => ("paused (memory checkpoint)".into(), None),
        Some(Ok(ActionResult::Paused { checkpoint: None })) => ("paused (disk only)".into(), None),
        Some(Ok(ActionResult::Deleted)) => ("deleted".into(), None),
        Some(Err(f)) => (
            "failed".into(),
            Some(failure_pb(
                c.action.name(),
                f,
                at,
                c.operation_id.to_string(),
            )),
        ),
        None => (String::new(), None),
    };
    pb::OrbCommand {
        operation_id: c.operation_id.to_string(),
        action: c.action.name().into(),
        generation: c.generation,
        status: c.status.clone(),
        created_unix_ms: c.created_ms,
        accepted_unix_ms: c.accepted_ms.unwrap_or(0),
        finished_unix_ms: c.terminal_ms.unwrap_or(0),
        result,
        failure,
    }
}

/// An outcome: (time, action, failure or None for a success).
type Outcome = (u64, &'static str, Option<pb::OrbFailure>);

/// Outcomes of one orb's commands and of its Amp requests that never became a command,
/// latest first. A request is dated when it started (finished ones are kept for about
/// an hour, and the coordinator does not record when one finished).
fn outcomes(
    orb: &OrbRecord,
    created: u64,
    commands: &[CmdHistory],
    requests: &[Req],
) -> Vec<Outcome> {
    let mut out: Vec<Outcome> = commands
        .iter()
        .filter(|c| c.status == "terminal")
        .map(|c| {
            (
                c.terminal_ms.unwrap_or(c.created_ms),
                c.action.name(),
                command_pb(c).failure,
            )
        })
        .collect();
    for q in requests {
        if q.amp_endpoint != orb.identity.amp_endpoint
            || q.amp_orb_id != orb.identity.amp_orb_id
            || q.start_ms < created
            || q.operation_id.is_some()
            || q.status != ReqStatus::Finished
        {
            continue;
        }
        let Some(r) = &q.result else { continue };
        let failure = (r.status != ReqResult::Succeeded).then(|| {
            match r.outcome.as_ref().map(|o| &o.result) {
                Some(Err(f)) => failure_pb(q.action.name(), f, q.start_ms, String::new()),
                _ => {
                    let code = match r.status {
                        ReqResult::Unavailable => FailureCode::BackendUnavailable,
                        ReqResult::Invalid => FailureCode::InvalidRequest,
                        ReqResult::DeadlineExceeded => FailureCode::DeadlineExceeded,
                        _ => FailureCode::BackendFailure,
                    };
                    let f = CommandFailure {
                        code,
                        message: r.message.clone(),
                    };
                    failure_pb(q.action.name(), &f, q.start_ms, String::new())
                }
            }
        });
        out.push((q.start_ms, q.action.name(), failure));
    }
    out.sort_by_key(|(t, _, _)| std::cmp::Reverse(*t));
    out
}

/// Failures of one action back to that action's last success, latest first: failed
/// spawns if there are any (a pause that succeeds, e.g. Amp giving up on a wake, or one
/// that fails, does not hide them), else those of the action that failed last.
fn failing(outcomes: &[Outcome]) -> Vec<pb::OrbFailure> {
    let streak = |action: &str| -> Vec<pb::OrbFailure> {
        outcomes
            .iter()
            .filter(|(_, a, _)| *a == action)
            .map_while(|(_, _, f)| f.clone())
            .collect()
    };
    let spawns = streak("spawn");
    if !spawns.is_empty() {
        return spawns;
    }
    outcomes
        .iter()
        .filter(|(_, a, f)| f.is_some() && *a != "spawn")
        .map(|(_, a, _)| streak(a))
        .find(|s| !s.is_empty())
        .unwrap_or_default()
}

fn blocker(code: FailureCode, message: String) -> pb::CommandFailure {
    pb::CommandFailure {
        code: pb::FailureCode::from(code) as i32,
        message,
    }
}

impl OrbsService {
    async fn live(&self) -> Result<Vec<Live>, Status> {
        self.db
            .call(|c| store::live_orbs_created(c))
            .await
            .map_err(internal)
    }

    /// Reads what `infos` needs for `orbs`: per orb, with its indexes.
    async fn snapshot(&self, orbs: &[Live], commands: u32) -> Result<Snapshot, Status> {
        let ids: Vec<OrbId> = orbs.iter().map(|(o, _, _)| o.id).collect();
        let backends: Vec<String> = self.backends.keys().map(|b| b.to_string()).collect();
        self.db
            .call(move |c| {
                let mut pools = HashMap::new();
                for b in backends {
                    for (pool, used) in store::pools(c, &b)? {
                        pools.insert((b.clone(), pool), used);
                    }
                }
                let mut activity = HashMap::new();
                for id in ids {
                    let cmds = store::orb_commands(c, id, commands)?;
                    activity.insert(id, (cmds, store::last_active(c, id)?));
                }
                Ok(Snapshot {
                    activity,
                    requests: store::all_requests(c)?,
                    pools,
                    own: store::running_reservations(c)?,
                })
            })
            .await
            .map_err(internal)
    }

    /// `OrbInfo` of each of `orbs`, with a check against its host now, as admission
    /// would make it, for each orb that does not run.
    async fn infos(&self, snap: &Snapshot, orbs: &[Live]) -> Vec<pb::OrbInfo> {
        let mut disk: HashMap<Location, HashMap<OrbId, OrbDiskUse>> = HashMap::new();
        let mut releases: HashMap<Location, HashMap<OrbId, crate::backend::WakeRelease>> =
            HashMap::new();
        let mut hosts: HashMap<Location, Result<PlacementCandidate, pb::CommandFailure>> =
            HashMap::new();
        let mut out = Vec::with_capacity(orbs.len());
        for (orb, created, tombstoned) in orbs {
            let backend = self.backends.get(orb.location.backend_id());
            if !disk.contains_key(&orb.location) {
                let d = backend
                    .and_then(|b| b.orb_disk(&orb.location))
                    .unwrap_or_default();
                disk.insert(orb.location.clone(), d);
            }
            if let Some(backend) = backend
                && !hosts.contains_key(&orb.location)
            {
                let h = self.host(backend, orb).await;
                hosts.insert(orb.location.clone(), h);
            }
            let host = hosts.get(&orb.location);
            let (commands, last_success) = snap
                .activity
                .get(&orb.id)
                .map(|(c, l)| (c.as_slice(), *l))
                .unwrap_or_default();
            let open_command = commands
                .iter()
                .find(|c| c.status != "terminal")
                .map(command_pb);
            let state = &orb.observation.state;
            let stable = state.last_known();
            let starting = open_command.as_ref().is_some_and(|c| c.action == "spawn");
            let mut wake_blockers = Vec::new();
            if !matches!(stable, StableOrbState::Running | StableOrbState::Deleted)
                && !tombstoned
                && !starting
            {
                match (backend, host) {
                    (None, _) => wake_blockers.push(blocker(
                        FailureCode::BackendUnavailable,
                        format!("backend {} is not configured", orb.location.backend_id()),
                    )),
                    (_, None) => {}
                    (_, Some(Err(e))) => wake_blockers.push(e.clone()),
                    (_, Some(Ok(cand))) => {
                        let key = (
                            cand.location.backend_id().to_string(),
                            cand.capacity_pool.clone(),
                        );
                        let mut used = snap.pools.get(&key).copied().unwrap_or_default();
                        // As admission does: the orb's own reservation is not in its way.
                        if let Some(own) = snap.own.get(&orb.id) {
                            used.held.vcpus = used.held.vcpus.saturating_sub(own.vcpus);
                            used.held.memory_bytes =
                                used.held.memory_bytes.saturating_sub(own.memory_bytes);
                            used.held.disk_bytes =
                                used.held.disk_bytes.saturating_sub(own.disk_bytes);
                            used.running = used.running.saturating_sub(1);
                        }
                        // Net of what its wake frees, as admission does.
                        let release = releases
                            .entry(orb.location.clone())
                            .or_insert_with(|| {
                                backend
                                    .map(|b| b.wake_releases(&orb.location))
                                    .unwrap_or_default()
                            })
                            .get(&orb.id)
                            .copied();
                        let need = wake_demand(
                            &self.policy,
                            &orb.spec.template,
                            &orb.observation.state,
                            release,
                        );
                        wake_blockers.extend(
                            shortfalls(cand, &used, need)
                                .into_iter()
                                .map(|(code, msg)| blocker(code, msg)),
                        );
                    }
                }
            }
            let mut failures = failing(&outcomes(orb, *created, commands, &snap.requests));
            let failures_since_success = failures.len() as u32;
            failures.truncate(MAX_FAILURES);
            let queued_requests = snap
                .requests
                .iter()
                .filter(|q| {
                    q.status == ReqStatus::Queued
                        && q.amp_endpoint == orb.identity.amp_endpoint
                        && q.amp_orb_id == orb.identity.amp_orb_id
                })
                .count() as u32;
            let tmpl = &orb.spec.template;
            let (agent_id, backend_id) = match &orb.location {
                Location::Fleet { agent_id, .. } => (agent_id.to_string(), String::new()),
                Location::External { backend_id, .. } => (String::new(), backend_id.to_string()),
            };
            out.push(pb::OrbInfo {
                amp_orb_id: orb.identity.amp_orb_id.to_string(),
                offering_id: orb.offering_id.to_string(),
                agent_id,
                state: if *tombstoned {
                    "deleting".into()
                } else {
                    state_label(state)
                },
                observed_unix_ms: orb.observation.observed_at_unix_ms,
                orb_id: orb.id.to_string(),
                arch: tmpl.requirements.arch.as_str().into(),
                runtime: match &tmpl.target {
                    ExecutionTarget::Fleet { runtime } => runtime.as_str().into(),
                    ExecutionTarget::Provider { profile } => profile.to_string(),
                },
                resources: Some(tmpl.resources.into()),
                created_unix_ms: *created,
                disk: disk[&orb.location].get(&orb.id).map(|d| pb::DiskUse {
                    allocated_bytes: d.allocated_bytes,
                    checkpoint_bytes: d.checkpoint_bytes,
                    archived: d.archived,
                }),
                wake_blockers,
                failures,
                failures_since_success,
                open_command,
                queued_requests,
                last_active_unix_ms: last_success.unwrap_or(*created),
                fault: match state {
                    OrbState::Faulted { reason, .. } => reason.clone(),
                    OrbState::Known { .. } => String::new(),
                },
                backend_id,
                overflow_agent: matches!(host, Some(Ok(c)) if c.overflow),
            });
        }
        out
    }

    /// The orb's host as a placement candidate, or why it cannot take the orb now.
    async fn host(
        &self,
        backend: &Arc<dyn ComputeBackend>,
        orb: &OrbRecord,
    ) -> Result<PlacementCandidate, pb::CommandFailure> {
        let unavailable = |m: String| blocker(FailureCode::BackendUnavailable, m);
        let tmpl = &orb.spec.template;
        let req = PlacementRequest {
            target: tmpl.target.clone(),
            resources: tmpl.resources,
            existing: Some(orb.location.clone()),
            project_snapshot: None,
        };
        let cands = match tokio::time::timeout(BACKEND_CALL_TIMEOUT, backend.candidates(req)).await
        {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(unavailable(format!("its host could not be checked: {e}"))),
            Err(_) => {
                return Err(unavailable(
                    "its host could not be checked: timed out".into(),
                ));
            }
        };
        let host = match &orb.location {
            Location::Fleet { agent_id, .. } => agent_id.to_string(),
            Location::External { backend_id, .. } => backend_id.to_string(),
        };
        match cands.into_iter().find(|c| c.location == orb.location) {
            None => Err(unavailable(format!("{host} is not a known host"))),
            Some(c) => match &c.availability {
                Availability::Available => Ok(c),
                Availability::Draining => Err(unavailable(format!("{host} is shutting down"))),
                Availability::Unavailable { reason } => {
                    Err(unavailable(format!("{host} is unavailable: {reason}")))
                }
            },
        }
    }

    /// Runs `job` on the orb named exactly by `query` (see [`resolve_exact`]) through
    /// the coordinator and waits for its result message.
    async fn job(&self, query: &str, job: OperatorJob) -> Result<String, Status> {
        let live = self.live().await?;
        let id = match resolve_exact(&live, query) {
            Some((o, _, _)) => o.identity.amp_orb_id.0.clone(),
            None => amp_id(query).ok_or_else(|| {
                Status::invalid_argument(format!(
                    "{query:?} names no orb: give its Amp orb id or thread id"
                ))
            })?,
        };
        let amp_orb_id = AmpOrbId::parse(&id)
            .map_err(|_| Status::invalid_argument(format!("{query:?} is not an orb id")))?;
        let (tx, mut rx) = mpsc::channel(64);
        self.core
            .send(Msg::OperatorJob {
                amp_orb_id,
                job,
                tx,
            })
            .await
            .map_err(|_| Status::unavailable("coordinator is shutting down"))?;
        while let Some(ev) = rx.recv().await {
            if let Some(Event::Result(res)) = ev?.event {
                use pb::DispatchStatus as D;
                return match D::try_from(res.status) {
                    Ok(D::Succeeded) => Ok(res.message),
                    Ok(D::Unavailable) => Err(Status::unavailable(res.message)),
                    Ok(D::Invalid) => Err(Status::invalid_argument(res.message)),
                    Ok(D::Failed) => Err(Status::aborted(res.message)),
                    _ => Err(Status::unknown(format!(
                        "{} (the outcome is not known: it may have happened)",
                        res.message
                    ))),
                };
            }
        }
        Err(Status::unavailable(
            "the coordinator stopped before the job finished",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(action: &str) -> Option<pb::OrbFailure> {
        Some(pb::OrbFailure {
            action: action.into(),
            ..Default::default()
        })
    }

    #[test]
    fn failed_spawns_end_only_with_a_spawn() {
        // Latest first: a pause succeeded after two failed wakes, before them a wake.
        let o = vec![
            (5, "pause", None),
            (4, "spawn", f("spawn")),
            (3, "spawn", f("spawn")),
            (2, "spawn", None),
            (1, "spawn", f("spawn")),
        ];
        assert_eq!(failing(&o).len(), 2);
        let woke = [vec![(6, "spawn", None)], o.clone()].concat();
        assert!(failing(&woke).is_empty());
        // A failed pause after them does not hide them either.
        let paused = [vec![(6, "pause", f("pause"))], o].concat();
        let got = failing(&paused);
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|f| f.action == "spawn"));
        // With no failed spawn: the action that failed last.
        let pauses = [
            (3, "pause", f("pause")),
            (2, "delete", f("delete")),
            (1, "pause", None),
        ];
        assert_eq!(failing(&pauses)[0].action, "pause");
        assert!(failing(&[(1, "pause", None)]).is_empty());
    }

    #[test]
    fn ids_resolve_exactly_or_by_unique_prefix() {
        assert_eq!(amp_id("T-01a1-x").as_deref(), Some("amp-01a1-x"));
        assert_eq!(amp_id(" 01a1-x ").as_deref(), Some("amp-01a1-x"));
        assert_eq!(amp_id("amp-01a1").as_deref(), Some("amp-01a1"));
        for empty in ["T-", "amp-", "", "  "] {
            assert_eq!(amp_id(empty), None, "{empty:?}");
        }
        let none: Vec<Live> = vec![];
        assert_eq!(
            resolve(&none, "T-").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            resolve(&none, "amp-1").unwrap_err().code(),
            tonic::Code::NotFound
        );
    }
}
