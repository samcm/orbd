//! The coordinator actor. One task owns all mutable coordinator state and processes
//! messages serially (dispatch requests, backend events, timer ticks). Admission,
//! reservations and per-orb serialization are therefore race-free by construction, and
//! are additionally enforced by SQLite constraints.

use super::db::Db;
use super::logs::{self, LogStore};
use super::store::{self, Applied, CreatePlan};
use super::types::*;
use crate::backend::{
    Availability, BackendEvent, CancelRequest, CommandDelivery, CommandSecrets, ComputeBackend,
    OperationStatus, PlacementCandidate, PlacementRequest, ReconcileQuery,
};
use crate::model::*;
use crate::secrets::{PermittedGuestEnv, SecretKey};
use anyhow::Result;
use orbd_proto::control::v1 as pb;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::Status as GrpcStatus;

type EventTx = mpsc::Sender<Result<pb::DispatchEvent, GrpcStatus>>;

const RESUBMIT_EVERY: Duration = Duration::from_secs(5);
const CANCEL_EVERY: Duration = Duration::from_secs(5);
const RECONCILE_EVERY: Duration = Duration::from_secs(10);
const BACKEND_CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// A validated dispatch request (see `service`).
pub struct ValidatedRequest {
    pub invocation_id: InvocationId,
    pub offering_id: OfferingId,
    pub amp_endpoint: AmpEndpointId,
    pub amp_orb_id: AmpOrbId,
    pub action: ReqAction,
    pub env: PermittedGuestEnv,
    pub fingerprint: Option<String>,
    pub remaining_budget_ms: u64,
}

pub struct Exec {
    pub req: ValidatedRequest,
    pub resume_after_log_seq: u64,
    pub tx: EventTx,
}

pub enum Msg {
    Execute(Box<Exec>),
    Backend(BackendEvent),
    Reconciled {
        key: CommandKey,
        status: OperationStatus,
    },
    /// `orbd orb delete`: delete the orb if it has been asleep, with no job, for
    /// `min_asleep`. Refusals arrive on `tx` as an error, results as dispatch events.
    DeleteAsleep {
        amp_orb_id: AmpOrbId,
        min_asleep: Duration,
        tx: EventTx,
    },
}

pub struct Core {
    pub db: Db,
    pub key: Arc<SecretKey>,
    pub epoch: u64,
    pub offerings: HashMap<OfferingId, Offering>,
    pub backends: HashMap<BackendId, Arc<dyn ComputeBackend>>,
    pub self_tx: mpsc::Sender<Msg>,
    subs: HashMap<InvocationId, EventTx>,
    logs: LogStore,
    last_submit: HashMap<OperationId, Instant>,
    last_cancel: HashMap<OperationId, Instant>,
    last_reconcile: HashMap<OperationId, Instant>,
    /// Orbs an idle reclaim was attempted for, and when (retried at most hourly).
    last_reclaim: HashMap<OrbId, Instant>,
    policy: Policy,
    ticks: u64,
}

/// Placement and lifecycle policy from the coordinator config.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    /// Disk each running orb may still grow by, held free on its host.
    pub running_orb_disk_headroom: u64,
    /// Orbs asleep longer than this are deleted. None disables.
    pub idle_orb_ttl: Option<Duration>,
}

enum Step {
    /// Request finished without a command; look at the next queued request.
    Finished,
    /// A command now occupies the orb.
    Started,
}

type Refusal = CommandFailure;

fn refuse(code: FailureCode, message: impl Into<String>) -> Refusal {
    CommandFailure {
        code,
        message: message.into(),
    }
}

fn event(e: pb::dispatch_event::Event) -> pb::DispatchEvent {
    pb::DispatchEvent { event: Some(e) }
}

impl Core {
    pub fn new(
        db: Db,
        key: Arc<SecretKey>,
        epoch: u64,
        offerings: HashMap<OfferingId, Offering>,
        backends: HashMap<BackendId, Arc<dyn ComputeBackend>>,
        self_tx: mpsc::Sender<Msg>,
        policy: Policy,
    ) -> Self {
        Self {
            db,
            key,
            epoch,
            offerings,
            backends,
            self_tx,
            subs: HashMap::new(),
            logs: LogStore::default(),
            last_submit: HashMap::new(),
            last_cancel: HashMap::new(),
            last_reconcile: HashMap::new(),
            last_reclaim: HashMap::new(),
            policy,
            ticks: 0,
        }
    }

    /// Recovery: re-submit every non-terminal command (same operation id, generation and
    /// absolute deadlines), reconcile uncertain ones, and re-evaluate every queue.
    pub async fn recover(&mut self) -> Result<()> {
        let open = self.db.call(|c| store::open_commands(c)).await?;
        tracing::info!(commands = open.len(), "recovering non-terminal commands");
        for (cmd, _) in &open {
            self.submit_command(cmd.operation_id).await;
            if cmd.status != CmdStatus::Accepted || cmd.submitted_ms.is_some() {
                self.start_reconcile(cmd.operation_id);
            }
        }
        for (ep, orb) in self.db.call(|c| store::queue_keys(c)).await? {
            self.evaluate(&ep, &orb).await;
        }
        Ok(())
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<Msg>, shutdown: CancellationToken) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                m = rx.recv() => match m {
                    Some(m) => self.handle(m).await,
                    None => break,
                },
                _ = tick.tick() => self.tick().await,
            }
        }
    }

    async fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Execute(e) => {
                let tx = e.tx.clone();
                if let Err(err) = self.execute(*e).await {
                    tracing::error!(error = %format!("{err:#}"), "dispatch request failed");
                    let _ = tx
                        .send(Err(GrpcStatus::unavailable("coordinator internal error")))
                        .await;
                }
            }
            Msg::Backend(ev) => {
                if let Err(err) = self.backend_event(ev).await {
                    tracing::error!(error = %format!("{err:#}"), "applying backend event failed");
                }
            }
            Msg::Reconciled { key, status } => {
                if let Err(err) = self.reconciled(key, status).await {
                    tracing::error!(error = %format!("{err:#}"), "applying reconcile result failed");
                }
            }
            Msg::DeleteAsleep {
                amp_orb_id,
                min_asleep,
                tx,
            } => {
                if let Err(err) = self.delete_asleep(amp_orb_id, min_asleep, tx.clone()).await {
                    tracing::error!(error = %format!("{err:#}"), "orb delete request failed");
                    let _ = tx
                        .send(Err(GrpcStatus::unavailable("coordinator internal error")))
                        .await;
                }
            }
        }
    }

    // ---- dispatch requests ------------------------------------------------------------

    async fn execute(&mut self, e: Exec) -> Result<()> {
        let Exec {
            req: v,
            resume_after_log_seq,
            tx,
        } = e;
        let inv = v.invocation_id;
        if let Some(existing) = self.db.call(move |c| store::get_request(c, inv)).await? {
            return self.reattach(existing, resume_after_log_seq, tx).await;
        }
        let now = unix_ms_now();
        let used = DISPATCH_BUDGET_MS.saturating_sub(v.remaining_budget_ms.min(DISPATCH_BUDGET_MS));
        let start = now.saturating_sub(used);
        let mut req = Req {
            invocation_id: inv,
            offering_id: v.offering_id.clone(),
            amp_endpoint: v.amp_endpoint.clone(),
            amp_orb_id: v.amp_orb_id.clone(),
            action: v.action.clone(),
            fingerprint: v.fingerprint.clone(),
            seq: 0,
            start_ms: start,
            work_deadline_ms: start + WORK_BUDGET_MS,
            cleanup_deadline_ms: start + CLEANUP_BUDGET_MS,
            dispatch_deadline_ms: start + DISPATCH_BUDGET_MS,
            status: ReqStatus::Queued,
            operation_id: None,
            result: None,
        };

        // Identical spawn already running on this orb: attach, never extend its deadline.
        if let (ReqAction::Spawn { .. }, Some(fp)) = (&req.action, &req.fingerprint)
            && now < req.work_deadline_ms
        {
            let (ep, aid) = (req.amp_endpoint.clone(), req.amp_orb_id.clone());
            let active = self
                .db
                .call(
                    move |c| match store::get_orb_by_identity(c, ep.as_str(), aid.as_str())? {
                        Some(orb) => store::open_command(c, orb.id),
                        None => Ok(None),
                    },
                )
                .await?;
            if let Some(cmd) = active
                && cmd.action == Action::Spawn
                && cmd.fingerprint.as_deref() == Some(fp.as_str())
            {
                req.status = ReqStatus::Active;
                req.operation_id = Some(cmd.operation_id);
                let r = req.clone();
                self.db
                    .call(move |c| store::insert_request(c, &r, None))
                    .await?;
                self.subs.insert(inv, tx);
                self.announce_accepted(inv, cmd.operation_id, cmd.generation, true, 0);
                return Ok(());
            }
        }

        let sealed = matches!(req.action, ReqAction::Spawn { .. }).then(|| {
            (
                inv.to_string(),
                self.key.seal(&v.env.to_json(), inv.to_string().as_bytes()),
            )
        });
        let r = req.clone();
        self.db
            .call(move |c| store::insert_request(c, &r, sealed))
            .await?;
        crate::crash_point!("coordinator::request_persisted");
        self.subs.insert(inv, tx);
        self.evaluate(req.amp_endpoint.as_str(), req.amp_orb_id.as_str())
            .await;
        if let Some(r) = self.db.call(move |c| store::get_request(c, inv)).await?
            && r.status == ReqStatus::Queued
        {
            let pos = self.db.call(move |c| store::queue_position(c, &r)).await?;
            self.send_event(
                inv,
                event(pb::dispatch_event::Event::Queued(pb::DispatchQueued {
                    position: pos,
                })),
            );
        }
        Ok(())
    }

    async fn reattach(&mut self, req: Req, after: u64, tx: EventTx) -> Result<()> {
        let inv = req.invocation_id;
        match req.status {
            ReqStatus::Finished => {
                if let Some(op) = req.operation_id {
                    for chunk in self.logs.replay(op, after) {
                        if tx
                            .send(Ok(event(pb::dispatch_event::Event::Log(chunk))))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                }
                let res = req.result.unwrap_or_else(|| {
                    StoredResult::plain(Status::OutcomeUnknown, "request finished without a result")
                });
                let _ = tx
                    .send(Ok(event(pb::dispatch_event::Event::Result(res.to_pb()))))
                    .await;
            }
            ReqStatus::Queued => {
                let pos = self
                    .db
                    .call(move |c| store::queue_position(c, &req))
                    .await?;
                self.subs.insert(inv, tx);
                self.send_event(
                    inv,
                    event(pb::dispatch_event::Event::Queued(pb::DispatchQueued {
                        position: pos,
                    })),
                );
            }
            ReqStatus::Active => {
                let Some(op) = req.operation_id else {
                    return Ok(());
                };
                let cmd = self.db.call(move |c| store::get_command(c, op)).await?;
                self.subs.insert(inv, tx);
                if let Some(cmd) = cmd {
                    self.announce_accepted(inv, op, cmd.generation, false, after);
                }
            }
        }
        Ok(())
    }

    fn announce_accepted(
        &mut self,
        inv: InvocationId,
        op: OperationId,
        generation: u64,
        attached: bool,
        after: u64,
    ) {
        self.send_event(
            inv,
            event(pb::dispatch_event::Event::Accepted(pb::DispatchAccepted {
                operation_id: op.to_string(),
                generation,
                attached,
            })),
        );
        for chunk in self.logs.replay(op, after) {
            self.send_event(inv, event(pb::dispatch_event::Event::Log(chunk)));
        }
    }

    /// Best-effort delivery to a connected client. Disconnected clients are dropped;
    /// accepted work never depends on them.
    fn send_event(&mut self, inv: InvocationId, ev: pb::DispatchEvent) {
        if let Some(tx) = self.subs.get(&inv)
            && let Err(mpsc::error::TrySendError::Closed(_)) = tx.try_send(Ok(ev))
        {
            self.subs.remove(&inv);
        }
    }

    async fn notify_result(&mut self, inv: InvocationId, action: &str, result: &StoredResult) {
        metrics::counter!(
            "orbd_dispatch_requests_total",
            "action" => action.to_owned(),
            "status" => result.status.label()
        )
        .increment(1);
        if let Some(tx) = self.subs.remove(&inv) {
            let ev = Ok(event(pb::dispatch_event::Event::Result(result.to_pb())));
            // The result must not be lost to a momentarily full buffer.
            tokio::spawn(async move {
                let _ = tx.send(ev).await;
            });
        }
    }

    async fn finish_req(&mut self, req: &Req, result: StoredResult) -> Result<()> {
        let inv = req.invocation_id;
        let r = result.clone();
        let changed = self
            .db
            .call(move |c| store::finish_request(c, inv, &r))
            .await?;
        if changed {
            self.notify_result(inv, req.action.name(), &result).await;
        }
        Ok(())
    }

    // ---- queue evaluation -------------------------------------------------------------

    /// Starts, attaches or finishes queued requests for one orb identity, FIFO. Infra
    /// errors leave the queue untouched; the next tick retries.
    async fn evaluate(&mut self, ep: &str, orb: &str) {
        loop {
            match self.evaluate_head(ep, orb).await {
                Ok(true) => continue,
                Ok(false) => break,
                Err(e) => {
                    tracing::error!(error = %format!("{e:#}"), "queue evaluation failed");
                    break;
                }
            }
        }
    }

    /// Returns true when another head should be examined.
    async fn evaluate_head(&mut self, ep: &str, orb: &str) -> Result<bool> {
        let (e, o) = (ep.to_owned(), orb.to_owned());
        let (queued, active) = self
            .db
            .call(move |c| {
                let q = store::queued_for(c, &e, &o)?;
                let active = match store::get_orb_by_identity(c, &e, &o)? {
                    Some(orb) => store::open_command(c, orb.id)?,
                    None => None,
                };
                Ok((q, active))
            })
            .await?;
        let Some(head) = queued.into_iter().next() else {
            return Ok(false);
        };
        if unix_ms_now() >= head.work_deadline_ms {
            self.finish_req(
                &head,
                StoredResult {
                    status: Status::DeadlineExceeded,
                    operation_id: None,
                    outcome: None,
                    outcome_known: true,
                    message: "deadline exceeded while queued; nothing was started".into(),
                },
            )
            .await?;
            return Ok(true);
        }
        if let Some(cmd) = active {
            if matches!(head.action, ReqAction::Spawn { .. })
                && cmd.action == Action::Spawn
                && head.fingerprint.is_some()
                && head.fingerprint == cmd.fingerprint
            {
                let (inv, op) = (head.invocation_id, cmd.operation_id);
                self.db
                    .call(move |c| store::attach_request(c, inv, op))
                    .await?;
                self.announce_accepted(inv, op, cmd.generation, true, 0);
                return Ok(true);
            }
            return Ok(false);
        }
        match self.start(&head).await? {
            Step::Finished => Ok(true),
            Step::Started => Ok(false),
        }
    }

    async fn finish_plain(&mut self, req: &Req, status: Status, msg: &str) -> Result<Step> {
        self.finish_req(req, StoredResult::plain(status, msg))
            .await?;
        Ok(Step::Finished)
    }

    async fn finish_refused(
        &mut self,
        req: &Req,
        orb: Option<&OrbRecord>,
        r: Refusal,
    ) -> Result<Step> {
        let observed = orb
            .map(|o| o.observation.state.clone())
            .unwrap_or(OrbState::known(StableOrbState::Uncreated));
        let outcome = synthetic_outcome(observed, r);
        let code = outcome.result.as_ref().err().map(|f| f.code);
        let mut res =
            StoredResult::from_outcome(req.action.name(), OperationId::generate(), &outcome);
        res.operation_id = None;
        res.status = match code {
            Some(FailureCode::BackendUnavailable) => Status::Unavailable,
            Some(FailureCode::InvalidRequest) => Status::Invalid,
            Some(FailureCode::DeadlineExceeded) => Status::DeadlineExceeded,
            _ => Status::Failed,
        };
        self.finish_req(req, res).await?;
        Ok(Step::Finished)
    }

    /// Decides what the head request does now that its orb has no open command.
    async fn start(&mut self, head: &Req) -> Result<Step> {
        let (ep, aid) = (head.amp_endpoint.clone(), head.amp_orb_id.clone());
        let (mut orb, tomb) = self
            .db
            .call(move |c| {
                let orb = store::get_orb_by_identity(c, ep.as_str(), aid.as_str())?;
                let tomb = match &orb {
                    Some(o) => store::is_tombstoned(c, o.id)?,
                    None => false,
                };
                Ok((orb, tomb))
            })
            .await?;
        let Some(offering) = self.offerings.get(&head.offering_id).cloned() else {
            return self
                .finish_plain(head, Status::Invalid, "unknown offering")
                .await;
        };
        if let Some(o) = &orb
            && o.offering_id != head.offering_id
        {
            return self
                .finish_refused(
                    head,
                    orb.as_ref(),
                    refuse(
                        FailureCode::SpecMismatch,
                        "orb exists under a different offering",
                    ),
                )
                .await;
        }
        let state = orb.as_ref().map(|o| &o.observation.state);
        let stopped = |s: &OrbState| {
            matches!(
                s,
                OrbState::Known {
                    state: StableOrbState::Sleeping { .. }
                        | StableOrbState::DiskOnlyStopped { .. }
                        | StableOrbState::Uncreated
                        | StableOrbState::Deleted
                }
            )
        };
        match &head.action {
            ReqAction::Delete => {
                if orb.is_none() {
                    return self
                        .finish_plain(head, Status::Succeeded, "delete: orb does not exist")
                        .await;
                }
                if state.is_some_and(|s| is_known(s, &StableOrbState::Deleted)) {
                    return self
                        .finish_plain(head, Status::Succeeded, "delete: orb already deleted")
                        .await;
                }
            }
            ReqAction::Pause { snapshot_id } => {
                if orb.is_none() {
                    return self
                        .finish_plain(head, Status::Succeeded, "pause: orb does not exist")
                        .await;
                }
                // A requested project snapshot still has to be published, so only a
                // plain pause of a stopped orb is a no-op.
                if snapshot_id.is_none() && state.is_some_and(stopped) {
                    return self
                        .finish_plain(head, Status::Succeeded, "pause: orb is already stopped")
                        .await;
                }
            }
            ReqAction::Spawn { .. } => {
                let deleted = state.is_some_and(|s| is_known(s, &StableOrbState::Deleted));
                if deleted {
                    // Amp resets an orb by deleting it and spawning the same ORB_ID again.
                    // The deleted incarnation never runs again; the spawn creates a new
                    // one (new internal id, fresh disk, fresh placement).
                    let old = orb.take().expect("deleted orb exists");
                    tracing::info!(orb = %old.id, amp_orb_id = %head.amp_orb_id, "spawn after delete: creating a new incarnation");
                    self.db.call(move |c| store::retire_orb(c, old.id)).await?;
                } else if tomb {
                    return self
                        .finish_refused(
                            head,
                            orb.as_ref(),
                            refuse(
                                FailureCode::OrbDeleted,
                                "orb is being deleted and its deletion has not completed",
                            ),
                        )
                        .await;
                }
            }
        }

        let plan = match self.plan(head, &offering, orb.as_ref()).await? {
            Ok(p) => p,
            Err(r) => return self.finish_refused(head, orb.as_ref(), r).await,
        };
        let op = plan.operation_id;
        let p = plan;
        let (generation, orb_id) = (p.generation, p.orb_id);
        self.db.call(move |c| store::create_command(c, &p)).await?;
        crate::crash_point!("coordinator::command_committed");
        self.announce_accepted(head.invocation_id, op, generation, false, 0);
        tracing::info!(operation = %op, orb = %orb_id, generation, action = head.action.name(), "command created");
        self.submit_command(op).await;
        Ok(Step::Started)
    }

    async fn candidates(
        &self,
        backend: &Arc<dyn ComputeBackend>,
        req: PlacementRequest,
    ) -> Option<Vec<PlacementCandidate>> {
        match tokio::time::timeout(BACKEND_CALL_TIMEOUT, backend.candidates(req)).await {
            Ok(Ok(v)) => Some(v),
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "placement candidates failed");
                None
            }
            Err(_) => {
                tracing::warn!("placement candidates timed out");
                None
            }
        }
    }

    async fn pool_used(
        &self,
        cand: &PlacementCandidate,
        exclude: Option<OrbId>,
    ) -> Result<store::PoolUse> {
        let (b, p) = (
            cand.location.backend_id().to_string(),
            cand.capacity_pool.clone(),
        );
        self.db
            .call(move |c| store::pool_used(c, &b, &p, exclude))
            .await
    }

    /// What a running orb of `spec` reserves on its host. Disk is headroom, not the disk
    /// size: room to grow (at most to its size) plus, for memory-preserving sleep, its
    /// next memory checkpoint. What it has already written is in the host's free space.
    fn demand(&self, spec: &SpecTemplate) -> Resources {
        let r = spec.resources;
        let checkpoint = match spec.pause_policy {
            PausePolicy::MemoryPreserving { .. } => r.memory_bytes,
            PausePolicy::DiskOnly => 0,
        };
        Resources {
            disk_bytes: r
                .disk_bytes
                .min(self.policy.running_orb_disk_headroom)
                .saturating_add(checkpoint),
            ..r
        }
    }

    /// Admission: choose/verify the location and build the atomic creation plan.
    async fn plan(
        &mut self,
        head: &Req,
        offering: &Offering,
        orb: Option<&OrbRecord>,
    ) -> Result<std::result::Result<CreatePlan, Refusal>> {
        let op = OperationId::generate();
        let digest_fp = head.fingerprint.clone();
        let mut plan = CreatePlan {
            invocation_id: head.invocation_id,
            new_orb: None,
            orb_id: OrbId::generate(),
            prev_generation: 0,
            generation: 1,
            operation_id: op,
            action: Action::Spawn,
            command_digest: String::new(),
            fingerprint: digest_fp,
            work_deadline_ms: head.work_deadline_ms,
            cleanup_deadline_ms: head.cleanup_deadline_ms,
            reservation: None,
            tombstone: None,
            sealed: None,
        };
        plan.action = match &head.action {
            ReqAction::Spawn { .. } => Action::Spawn,
            ReqAction::Pause { snapshot_id } => Action::Pause {
                publish_snapshot: snapshot_id
                    .as_deref()
                    .map(ProjectSnapshotKey::from_external),
            },
            ReqAction::Delete => Action::Delete,
        };
        if matches!(head.action, ReqAction::Delete) {
            plan.tombstone = Some((head.amp_endpoint.to_string(), head.amp_orb_id.to_string()));
        }

        let Some(backend) = self.backends.get(&offering.backend_id).cloned() else {
            return Ok(Err(refuse(
                FailureCode::BackendUnavailable,
                "backend not configured",
            )));
        };

        match orb {
            Some(orb) => {
                // Existing orb: never re-place.
                let template = &orb.spec.template;
                let cands = self
                    .candidates(
                        &backend,
                        PlacementRequest {
                            target: template.target.clone(),
                            resources: template.resources,
                            existing: Some(orb.location.clone()),
                            project_snapshot: None,
                        },
                    )
                    .await;
                let cand = cands.and_then(|v| {
                    v.into_iter().find(|c| {
                        c.location == orb.location && c.availability == Availability::Available
                    })
                });
                let Some(cand) = cand else {
                    return Ok(Err(refuse(
                        FailureCode::BackendUnavailable,
                        "the orb's host is unavailable",
                    )));
                };
                if matches!(head.action, ReqAction::Spawn { .. }) {
                    let demand = self.demand(template);
                    // A running orb already holds its reservation; re-spawning it (a
                    // bootstrap retry) needs nothing new.
                    let running =
                        matches!(orb.observation.state.last_known(), StableOrbState::Running);
                    if !running {
                        let used = self.pool_used(&cand, Some(orb.id)).await?;
                        let short = shortfalls(&cand, &used, demand);
                        if let Some(first) = short.first() {
                            return Ok(Err(refuse(
                                first.0,
                                format!(
                                    "the orb's host lacks capacity to run it: {}",
                                    describe(&short)
                                ),
                            )));
                        }
                    }
                    plan.reservation = Some((
                        cand.location.backend_id().to_string(),
                        cand.capacity_pool.clone(),
                        demand,
                    ));
                }
                plan.orb_id = orb.id;
                plan.prev_generation = orb.last_issued_generation;
                plan.generation = orb.last_issued_generation + 1;
                plan.command_digest = command_digest(&plan, &orb.spec_digest);
                if let Some(s) = self.seal_for(head, op).await? {
                    plan.sealed = Some(s);
                } else if matches!(head.action, ReqAction::Spawn { .. }) {
                    return Ok(Err(refuse(
                        FailureCode::SecretUnavailable,
                        "queued secrets unavailable",
                    )));
                }
                Ok(Ok(plan))
            }
            None => {
                let ReqAction::Spawn {
                    image_ref,
                    snapshot_id,
                } = &head.action
                else {
                    // pause/delete of an unknown orb was handled as a no-op earlier
                    return Ok(Err(refuse(
                        FailureCode::InvalidRequest,
                        "orb does not exist",
                    )));
                };
                let snap_key = snapshot_id
                    .as_deref()
                    .map(ProjectSnapshotKey::from_external);
                let cands = self
                    .candidates(
                        &backend,
                        PlacementRequest {
                            target: offering.spec.target.clone(),
                            resources: offering.spec.resources,
                            existing: None,
                            project_snapshot: snap_key.clone(),
                        },
                    )
                    .await;
                let available: Vec<PlacementCandidate> = cands
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|c| {
                        c.availability == Availability::Available
                            && c.capabilities.check(&offering.spec).is_empty()
                    })
                    .collect();
                if available.is_empty() {
                    return Ok(Err(refuse(
                        FailureCode::BackendUnavailable,
                        "no available host can run this offering",
                    )));
                }
                let demand = self.demand(&offering.spec);
                let mut fitting: Vec<(PlacementCandidate, u64)> = Vec::new();
                let mut refusals: Vec<(String, Vec<(FailureCode, String)>)> = Vec::new();
                for cand in available {
                    let used = self.pool_used(&cand, None).await?;
                    let short = shortfalls(&cand, &used, demand);
                    if short.is_empty() {
                        let free = cand
                            .capacity_ceiling
                            .memory_bytes
                            .saturating_sub(used.held.memory_bytes);
                        fitting.push((cand, free));
                    } else {
                        refusals.push((cand.capacity_pool.clone(), short));
                    }
                }
                if fitting.is_empty() {
                    let hosts: Vec<String> = refusals
                        .iter()
                        .map(|(pool, short)| format!("{pool}: {}", describe(short)))
                        .collect();
                    return Ok(Err(refuse(
                        limiting(&refusals),
                        format!(
                            "no host has capacity for this orb ({} vCPUs, {} memory, {} disk \
                             headroom); {}",
                            demand.vcpus,
                            gib(demand.memory_bytes),
                            gib(demand.disk_bytes),
                            hosts.join("; ")
                        ),
                    )));
                }
                // Prefer a host holding the project snapshot (v1: snapshots are
                // host-local); otherwise fall back to the base image for this new orb.
                if snap_key.is_some() && fitting.iter().any(|(c, _)| c.has_project_snapshot) {
                    fitting.retain(|(c, _)| c.has_project_snapshot);
                }
                let (chosen, _) = fitting
                    .into_iter()
                    .max_by(|(a, fa), (b, fb)| {
                        fa.cmp(fb).then_with(|| {
                            let (ja, jb) = (js(&a.location), js(&b.location));
                            jb.cmp(&ja)
                        })
                    })
                    .expect("non-empty");
                let initial_disk = match &snap_key {
                    Some(k) if chosen.has_project_snapshot => InitialDiskSource::ProjectSnapshot {
                        snapshot: ProjectSnapshotRef { key: k.clone() },
                        image_ref: image_ref.clone(),
                    },
                    _ => InitialDiskSource::Image {
                        image_ref: image_ref.clone(),
                    },
                };
                let spec = OrbSpec {
                    template: offering.spec.clone(),
                    initial_disk,
                };
                let spec_digest = spec.digest();
                let orb_id = OrbId::generate();
                plan.orb_id = orb_id;
                plan.generation = 1;
                plan.new_orb = Some(OrbRecord {
                    id: orb_id,
                    identity: OrbIdentity {
                        amp_endpoint: head.amp_endpoint.clone(),
                        amp_orb_id: head.amp_orb_id.clone(),
                    },
                    offering_id: offering.id.clone(),
                    spec,
                    spec_digest: spec_digest.clone(),
                    location: chosen.location.clone(),
                    last_issued_generation: 1,
                    observation: VersionedObservation {
                        state: OrbState::known(StableOrbState::Uncreated),
                        generation: 0,
                        observed_at_unix_ms: unix_ms_now(),
                    },
                });
                plan.reservation = Some((
                    chosen.location.backend_id().to_string(),
                    chosen.capacity_pool.clone(),
                    demand,
                ));
                plan.command_digest = command_digest(&plan, &spec_digest);
                match self.seal_for(head, op).await? {
                    Some(s) => plan.sealed = Some(s),
                    None => {
                        return Ok(Err(refuse(
                            FailureCode::SecretUnavailable,
                            "queued secrets unavailable",
                        )));
                    }
                }
                Ok(Ok(plan))
            }
        }
    }

    /// Re-seals a queued request's guest env under the new operation id.
    async fn seal_for(&self, head: &Req, op: OperationId) -> Result<Option<Vec<u8>>> {
        if !matches!(head.action, ReqAction::Spawn { .. }) {
            return Ok(None);
        }
        let owner = head.invocation_id.to_string();
        let o = owner.clone();
        let Some(sealed) = self.db.call(move |c| store::get_payload(c, &o)).await? else {
            return Ok(None);
        };
        let Ok(plain) = self.key.open(&sealed, owner.as_bytes()) else {
            return Ok(None);
        };
        Ok(Some(self.key.seal(&plain, op.to_string().as_bytes())))
    }

    // ---- delivery ---------------------------------------------------------------------

    async fn build_delivery(&self, op: OperationId) -> Result<Option<(CommandDelivery, CmdRow)>> {
        let now = unix_ms_now();
        let Some((cmd, orb, payload)) = self
            .db
            .call(move |c| {
                let Some(cmd) = store::get_command(c, op)? else {
                    return Ok(None);
                };
                let orb = store::get_orb(c, cmd.orb_id)?;
                let payload = store::get_payload(c, &op.to_string())?;
                Ok(orb.map(|o| (cmd, o, payload)))
            })
            .await?
        else {
            return Ok(None);
        };
        if cmd.status == CmdStatus::Terminal {
            return Ok(None);
        }
        let secrets = if cmd.action == Action::Spawn {
            let opened = payload
                .and_then(|p| self.key.open(&p, op.to_string().as_bytes()).ok())
                .and_then(|plain| PermittedGuestEnv::from_json(&plain).ok());
            match opened {
                Some(env) => CommandSecrets::Bootstrap(env),
                None => {
                    tracing::error!(operation = %op, "sealed guest env is unavailable; not delivering");
                    return Ok(None);
                }
            }
        } else {
            CommandSecrets::None
        };
        let delivery = CommandDelivery {
            command: Command {
                operation_id: op,
                orb_id: orb.id,
                generation: cmd.generation,
                spec_digest: orb.spec_digest.clone(),
                command_digest: CommandDigest(cmd.command_digest.clone()),
                action: cmd.action.clone(),
            },
            controller_epoch: self.epoch,
            budget: ExecutionBudget {
                work_remaining_ms: cmd.work_deadline_ms.saturating_sub(now),
                cleanup_remaining_ms: cmd.cleanup_deadline_ms.saturating_sub(now),
            },
            orb: OrbDefinition {
                id: orb.id,
                identity: orb.identity.clone(),
                offering_id: orb.offering_id.clone(),
                spec: orb.spec.clone(),
                spec_digest: orb.spec_digest.clone(),
            },
            location: orb.location.clone(),
            secrets,
        };
        Ok(Some((delivery, cmd)))
    }

    /// Delivery attempt. `submitted_ms` is made durable first, so "never submitted" is
    /// provable after a crash. An `Err` from the backend proves nothing: we retry.
    async fn submit_command(&mut self, op: OperationId) {
        let built = match self.build_delivery(op).await {
            Ok(Some(b)) => b,
            Ok(None) => return,
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "building delivery failed");
                return;
            }
        };
        let (delivery, _cmd) = built;
        let Some(backend) = self.backends.get(delivery.location.backend_id()).cloned() else {
            tracing::error!("command location references an unconfigured backend");
            return;
        };
        let now = unix_ms_now();
        if let Err(e) = self
            .db
            .call(move |c| store::mark_submitted(c, op, now))
            .await
        {
            tracing::error!(error = %format!("{e:#}"), "recording submission failed");
            return;
        }
        crate::crash_point!("coordinator::before_submit");
        self.last_submit.insert(op, Instant::now());
        tokio::spawn(async move {
            if let Err(e) = backend.submit(delivery).await {
                tracing::warn!(operation = %op, error = %e, "submit failed; will retry");
            }
        });
    }

    fn send_cancel(&mut self, op: OperationId, key: CommandKey, location: Location) {
        let Some(backend) = self.backends.get(location.backend_id()).cloned() else {
            return;
        };
        self.last_cancel.insert(op, Instant::now());
        tokio::spawn(async move {
            let r = backend
                .cancel(CancelRequest {
                    key,
                    location,
                    reason: "work deadline reached".into(),
                })
                .await;
            if let Err(e) = r {
                tracing::warn!(operation = %key.operation_id, error = %e, "cancel failed; will retry");
            }
        });
    }

    fn start_reconcile(&mut self, op: OperationId) {
        self.last_reconcile.insert(op, Instant::now());
        let db = self.db.clone();
        let backends = self.backends.clone();
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            let Ok(Some((cmd, orb))) = db
                .call(move |c| {
                    let Some(cmd) = store::get_command(c, op)? else {
                        return Ok(None);
                    };
                    Ok(store::get_orb(c, cmd.orb_id)?.map(|o| (cmd, o)))
                })
                .await
            else {
                return;
            };
            let Some(backend) = backends.get(orb.location.backend_id()) else {
                return;
            };
            let key = CommandKey {
                operation_id: op,
                orb_id: orb.id,
                generation: cmd.generation,
            };
            let query = ReconcileQuery {
                location: orb.location.clone(),
                operations: vec![key],
                orbs: vec![orb.id],
                page_token: None,
            };
            match tokio::time::timeout(Duration::from_secs(10), backend.reconcile(query)).await {
                Ok(Ok(page)) => {
                    if let Some((_, status)) = page.operations.into_iter().find(|(k, _)| *k == key)
                    {
                        let _ = tx.send(Msg::Reconciled { key, status }).await;
                    }
                }
                Ok(Err(e)) => tracing::debug!(operation = %op, error = %e, "reconcile unavailable"),
                Err(_) => tracing::debug!(operation = %op, "reconcile timed out"),
            }
        });
    }

    // ---- backend events ---------------------------------------------------------------

    async fn backend_event(&mut self, ev: BackendEvent) -> Result<()> {
        match ev {
            BackendEvent::Accepted { key } => {
                let (op, now) = (key.operation_id, unix_ms_now());
                self.db
                    .call(move |c| store::mark_accepted(c, op, now))
                    .await?;
                self.fan_out(key.operation_id, |op| {
                    pb::dispatch_event::Event::Progress(pb::DispatchProgress {
                        operation_id: op.to_string(),
                        phase: "accepted".into(),
                        message: "accepted by executor".into(),
                    })
                })
                .await;
            }
            BackendEvent::Rejected { key, failure } => {
                let orb = self.db.call(move |c| store::get_orb(c, key.orb_id)).await?;
                let observed = orb
                    .map(|o| o.observation.state)
                    .unwrap_or(OrbState::known(StableOrbState::Uncreated));
                let outcome = synthetic_outcome(observed, failure);
                self.complete(key, outcome, true).await?;
            }
            BackendEvent::Progress {
                key,
                phase,
                message,
            } => {
                self.fan_out(key.operation_id, move |op| {
                    pb::dispatch_event::Event::Progress(pb::DispatchProgress {
                        operation_id: op.to_string(),
                        phase: phase.clone(),
                        message: message.clone(),
                    })
                })
                .await;
            }
            BackendEvent::Log { key, chunk } => {
                let op = key.operation_id;
                let dropped = chunk.dropped_before;
                let pbc = logs::to_pb(op, &chunk, dropped);
                if self.logs.push(op, chunk) {
                    self.fan_out(op, move |_| pb::dispatch_event::Event::Log(pbc.clone()))
                        .await;
                }
            }
            BackendEvent::Completed { key, outcome } => {
                self.complete(key, outcome, true).await?;
            }
            BackendEvent::ReconcileNeeded { key } => self.start_reconcile(key.operation_id),
            BackendEvent::AvailabilityChanged {
                location,
                availability,
            } => {
                tracing::info!(?location, ?availability, "backend availability changed");
                if availability == Availability::Available {
                    let open = self.db.call(|c| store::open_commands(c)).await?;
                    for (cmd, orb) in open {
                        if orb.location == location {
                            self.submit_command(cmd.operation_id).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Fan an event out to every connected invocation attached to `op`.
    async fn fan_out(
        &mut self,
        op: OperationId,
        make: impl Fn(OperationId) -> pb::dispatch_event::Event,
    ) {
        if self.subs.is_empty() {
            return;
        }
        let invs: Vec<InvocationId> = self.subs.keys().copied().collect();
        let ops = self
            .db
            .call(move |c| {
                let mut out = Vec::new();
                for inv in invs {
                    if let Some(r) = store::get_request(c, inv)?
                        && r.operation_id == Some(op)
                        && r.status == ReqStatus::Active
                    {
                        out.push(inv);
                    }
                }
                Ok(out)
            })
            .await
            .unwrap_or_default();
        for inv in ops {
            self.send_event(inv, event(make(op)));
        }
    }

    /// Applies a terminal outcome durably, then acknowledges and wakes the queue.
    async fn complete(
        &mut self,
        key: CommandKey,
        outcome: CommandOutcome,
        ack: bool,
    ) -> Result<()> {
        let applied = self
            .db
            .call({
                let outcome = outcome.clone();
                move |c| store::apply_completion(c, key, &outcome, unix_ms_now())
            })
            .await?;
        crate::crash_point!("coordinator::result_committed");
        let location = match applied {
            Applied::Unknown => {
                tracing::warn!(operation = %key.operation_id, "completion for unknown command ignored");
                return Ok(());
            }
            Applied::AlreadyTerminal => self
                .db
                .call(move |c| store::get_orb(c, key.orb_id))
                .await?
                .map(|o| o.location),
            Applied::Done(done) => {
                let store::Done {
                    cmd,
                    orb,
                    finished,
                    results,
                } = *done;
                for (inv, res) in finished.into_iter().zip(results) {
                    self.notify_result(inv, cmd.action.name(), &res).await;
                }
                self.last_submit.remove(&key.operation_id);
                self.last_cancel.remove(&key.operation_id);
                self.last_reconcile.remove(&key.operation_id);
                let (ep, aid) = (
                    orb.identity.amp_endpoint.to_string(),
                    orb.identity.amp_orb_id.to_string(),
                );
                if ack {
                    self.ack(&orb.location, key);
                }
                self.evaluate(&ep, &aid).await;
                return Ok(());
            }
        };
        if ack && let Some(loc) = location {
            self.ack(&loc, key);
        }
        Ok(())
    }

    fn ack(&self, location: &Location, key: CommandKey) {
        let Some(backend) = self.backends.get(location.backend_id()).cloned() else {
            return;
        };
        let location = location.clone();
        tokio::spawn(async move {
            if let Err(e) = backend.acknowledge_completion(&location, key).await {
                tracing::warn!(operation = %key.operation_id, error = %e, "ack failed; completion will be replayed");
            }
        });
    }

    async fn reconciled(&mut self, key: CommandKey, status: OperationStatus) -> Result<()> {
        let op = key.operation_id;
        let Some(cmd) = self.db.call(move |c| store::get_command(c, op)).await? else {
            return Ok(());
        };
        if cmd.status == CmdStatus::Terminal {
            return Ok(());
        }
        match status {
            OperationStatus::Completed(outcome) => self.complete(key, outcome, true).await?,
            OperationStatus::Running => {
                let now = unix_ms_now();
                self.db
                    .call(move |c| store::mark_accepted(c, op, now))
                    .await?;
            }
            OperationStatus::CancelledBeforeAccept => {
                let orb = self.db.call(move |c| store::get_orb(c, key.orb_id)).await?;
                let observed = orb
                    .map(|o| o.observation.state)
                    .unwrap_or(OrbState::known(StableOrbState::Uncreated));
                let outcome = synthetic_outcome(
                    observed,
                    refuse(
                        FailureCode::Cancelled,
                        "cancelled before the executor accepted it",
                    ),
                );
                self.complete(key, outcome, false).await?;
            }
            OperationStatus::NotAccepted => {
                // A delivery may still be in flight, so this alone is not terminal.
                // Past the work deadline, cancel (writes the executor-side tombstone);
                // the next reconcile then reports CancelledBeforeAccept.
                if unix_ms_now() >= cmd.work_deadline_ms {
                    if let Some(orb) = self.db.call(move |c| store::get_orb(c, key.orb_id)).await? {
                        self.send_cancel(op, key, orb.location);
                    }
                } else {
                    self.submit_command(op).await;
                }
            }
            OperationStatus::Unknown => {}
        }
        Ok(())
    }

    // ---- timers -----------------------------------------------------------------------

    async fn tick(&mut self) {
        self.ticks += 1;
        if let Err(e) = self.tick_inner().await {
            tracing::error!(error = %format!("{e:#}"), "tick failed");
        }
    }

    async fn tick_inner(&mut self) -> Result<()> {
        let now = unix_ms_now();

        // 1. Queued requests past their work deadline are finished, never delivered.
        for req in self.db.call(move |c| store::expired_queued(c, now)).await? {
            self.finish_req(
                &req,
                StoredResult {
                    status: Status::DeadlineExceeded,
                    operation_id: None,
                    outcome: None,
                    outcome_known: true,
                    message: "deadline exceeded while queued; nothing was started".into(),
                },
            )
            .await?;
        }
        for (ep, orb) in self.db.call(|c| store::queue_keys(c)).await? {
            self.evaluate(&ep, &orb).await;
        }

        // 2. Open commands: cancel at the work deadline, resubmit unconfirmed ones,
        //    reconcile uncertain ones.
        let open = self.db.call(|c| store::open_commands(c)).await?;
        for (cmd, orb) in &open {
            let op = cmd.operation_id;
            let key = CommandKey {
                operation_id: op,
                orb_id: orb.id,
                generation: cmd.generation,
            };
            if now >= cmd.work_deadline_ms {
                if cmd.submitted_ms.is_none() {
                    // Provably never delivered: no side effect can exist.
                    let outcome = synthetic_outcome(
                        orb.observation.state.clone(),
                        refuse(
                            FailureCode::DeadlineExceeded,
                            "deadline exceeded before submission",
                        ),
                    );
                    self.complete(key, outcome, false).await?;
                    continue;
                }
                if cmd.cancel_requested_ms.is_none() {
                    self.db
                        .call(move |c| store::mark_cancel_requested(c, op, now))
                        .await?;
                }
                if self
                    .last_cancel
                    .get(&op)
                    .is_none_or(|t| t.elapsed() >= CANCEL_EVERY)
                {
                    self.send_cancel(op, key, orb.location.clone());
                }
            } else if cmd.status == CmdStatus::Pending
                && self
                    .last_submit
                    .get(&op)
                    .is_none_or(|t| t.elapsed() >= RESUBMIT_EVERY)
            {
                self.submit_command(op).await;
            }
            let uncertain = cmd.status == CmdStatus::Unknown
                || (now >= cmd.cleanup_deadline_ms && cmd.submitted_ms.is_some());
            if uncertain
                && self
                    .last_reconcile
                    .get(&op)
                    .is_none_or(|t| t.elapsed() >= RECONCILE_EVERY)
            {
                self.start_reconcile(op);
            }
        }

        // 3. Dispatch deadline: answer the client, but keep the command slot occupied.
        for req in self
            .db
            .call(move |c| store::active_past_dispatch(c, now))
            .await?
        {
            let Some(op) = req.operation_id else { continue };
            let Some(cmd) = self.db.call(move |c| store::get_command(c, op)).await? else {
                continue;
            };
            let result = match (&cmd.status, &cmd.outcome) {
                (CmdStatus::Terminal, Some(o)) => {
                    StoredResult::from_outcome(req.action.name(), op, o)
                }
                _ if cmd.submitted_ms.is_none() => StoredResult {
                    status: Status::DeadlineExceeded,
                    operation_id: Some(op),
                    outcome: None,
                    outcome_known: true,
                    message: "deadline exceeded before the command was submitted".into(),
                },
                _ => {
                    self.db.call(move |c| store::mark_unknown(c, op)).await?;
                    StoredResult {
                        status: Status::DeadlineExceeded,
                        operation_id: Some(op),
                        outcome: None,
                        outcome_known: false,
                        message: format!(
                            "{} outcome unknown at the deadline; reconciliation continues",
                            req.action.name()
                        ),
                    }
                }
            };
            self.finish_req(&req, result).await?;
        }

        // 4. Housekeeping and metrics.
        if self.ticks.is_multiple_of(600) {
            let cutoff = now.saturating_sub(3_600_000);
            self.db.call(move |c| store::gc(c, cutoff)).await?;
        }
        if self.ticks.is_multiple_of(60) {
            self.reclaim_idle(now).await?;
        }
        let (queued, active, pools) = self
            .db
            .call(|c| {
                Ok((
                    store::count_where(c, "SELECT COUNT(*) FROM requests WHERE status = 'queued'")?,
                    store::count_where(
                        c,
                        "SELECT COUNT(*) FROM commands WHERE status <> 'terminal'",
                    )?,
                    store::pool_gauges(c)?,
                ))
            })
            .await?;
        metrics::gauge!("orbd_queue_depth").set(queued as f64);
        metrics::gauge!("orbd_active_commands").set(active as f64);
        for (pool, u) in pools {
            metrics::gauge!("orbd_reserved_memory_bytes", "pool" => pool.clone())
                .set(u.held.memory_bytes as f64);
            metrics::gauge!("orbd_reserved_disk_bytes", "pool" => pool.clone())
                .set(u.held.disk_bytes as f64);
            metrics::gauge!("orbd_orbs", "pool" => pool.clone()).set(u.orbs as f64);
            metrics::gauge!("orbd_running_orbs", "pool" => pool).set(u.running as f64);
        }
        self.subs.retain(|_, tx| !tx.is_closed());
        Ok(())
    }

    /// Deletes orbs asleep longer than the idle TTL through the ordinary delete path.
    /// Amp sends no delete when a thread is archived, only a pause, so without this
    /// abandoned orbs keep their disks forever. A later spawn of the same orb creates a
    /// fresh one.
    async fn reclaim_idle(&mut self, now: u64) -> Result<()> {
        let Some(ttl) = self.policy.idle_orb_ttl else {
            return Ok(());
        };
        let cutoff = now.saturating_sub(ttl.as_millis() as u64);
        self.last_reclaim
            .retain(|_, t| t.elapsed() < Duration::from_secs(3600));
        let idle = self.db.call(move |c| store::idle_orbs(c, cutoff)).await?;
        for orb in idle {
            if self.last_reclaim.contains_key(&orb.id)
                || !self.offerings.contains_key(&orb.offering_id)
            {
                continue;
            }
            self.last_reclaim.insert(orb.id, Instant::now());
            let asleep_days = now.saturating_sub(orb.observation.observed_at_unix_ms) / 86_400_000;
            tracing::info!(
                orb = %orb.id,
                amp_orb_id = %orb.identity.amp_orb_id,
                location = ?orb.location,
                asleep_days,
                ttl_days = ttl.as_secs() / 86_400,
                "deleting orb asleep past the idle TTL"
            );
            metrics::counter!("orbd_idle_orbs_reclaimed_total").increment(1);
            self.queue_delete(&orb, now, None).await?;
        }
        Ok(())
    }

    /// Deletes one orb on an operator's request (`orbd orb delete`, e.g. from a job
    /// that finds archived threads), through the ordinary delete path. Only an orb
    /// that has been asleep, with no job, for `min_asleep` qualifies, so an orb Amp is
    /// using is never deleted; checking and queueing here in the actor leaves no gap
    /// for a spawn to slip in between.
    async fn delete_asleep(
        &mut self,
        amp_orb_id: AmpOrbId,
        min_asleep: Duration,
        tx: EventTx,
    ) -> Result<()> {
        let now = unix_ms_now();
        let cutoff = now.saturating_sub(min_asleep.as_millis() as u64);
        let id = amp_orb_id.clone();
        let (orb, idle) = self
            .db
            .call(move |c| {
                let Some(orb) = store::live_orbs(c)?
                    .into_iter()
                    .find(|o| o.identity.amp_orb_id == id)
                else {
                    return Ok((None, false));
                };
                let idle = store::idle_orbs(c, cutoff)?.iter().any(|o| o.id == orb.id);
                Ok((Some(orb), idle))
            })
            .await?;
        let refusal = match &orb {
            None => Some(GrpcStatus::not_found(format!("no orb {amp_orb_id}"))),
            Some(o) if !idle => Some(GrpcStatus::failed_precondition(format!(
                "{amp_orb_id} is {}; it must be asleep, with no job, for {} min",
                state_label(&o.observation.state),
                min_asleep.as_secs() / 60
            ))),
            Some(o) if !self.offerings.contains_key(&o.offering_id) => {
                Some(GrpcStatus::failed_precondition(format!(
                    "{amp_orb_id}'s offering {} is no longer configured",
                    o.offering_id
                )))
            }
            Some(_) => None,
        };
        if let Some(status) = refusal {
            let _ = tx.send(Err(status)).await;
            return Ok(());
        }
        let orb = orb.expect("checked above");
        tracing::info!(
            orb = %orb.id,
            amp_orb_id = %orb.identity.amp_orb_id,
            location = ?orb.location,
            asleep_mins = now.saturating_sub(orb.observation.observed_at_unix_ms) / 60_000,
            "deleting orb on request"
        );
        self.queue_delete(&orb, now, Some(tx)).await
    }

    /// Queues a Delete request for `orb`, as if Amp had sent it, and starts it if the
    /// orb is free. With `tx`, the caller follows it like a dispatch.
    async fn queue_delete(&mut self, orb: &OrbRecord, now: u64, tx: Option<EventTx>) -> Result<()> {
        let req = Req {
            invocation_id: InvocationId::generate(),
            offering_id: orb.offering_id.clone(),
            amp_endpoint: orb.identity.amp_endpoint.clone(),
            amp_orb_id: orb.identity.amp_orb_id.clone(),
            action: ReqAction::Delete,
            fingerprint: None,
            seq: 0,
            start_ms: now,
            work_deadline_ms: now + WORK_BUDGET_MS,
            cleanup_deadline_ms: now + CLEANUP_BUDGET_MS,
            dispatch_deadline_ms: now + DISPATCH_BUDGET_MS,
            status: ReqStatus::Queued,
            operation_id: None,
            result: None,
        };
        let inv = req.invocation_id;
        self.db
            .call(move |c| store::insert_request(c, &req, None))
            .await?;
        if let Some(tx) = tx {
            self.subs.insert(inv, tx);
        }
        self.evaluate(
            orb.identity.amp_endpoint.as_str(),
            orb.identity.amp_orb_id.as_str(),
        )
        .await;
        Ok(())
    }
}

/// An orb's state in words: running, asleep, stopped, uncreated, deleted, or faulted
/// (with its last known state).
pub(crate) fn state_label(s: &OrbState) -> String {
    let stable = |s: &StableOrbState| match s {
        StableOrbState::Uncreated => "uncreated",
        StableOrbState::Running => "running",
        StableOrbState::Sleeping { .. } => "asleep",
        StableOrbState::DiskOnlyStopped { .. } => "stopped",
        StableOrbState::Deleted => "deleted",
    };
    match s {
        OrbState::Known { state } => stable(state).into(),
        OrbState::Faulted { last_known, .. } => format!("faulted ({})", stable(last_known)),
    }
}

fn js<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("serializable")
}

fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

/// Every resource `cand` lacks for `demand` after the reservations in `used`, with the
/// numbers. Disk is judged by what the host reports, not by orb disk sizes (sparse): the
/// filesystem's free space less its reserve, and the agent's disk limit less what orbd
/// has allocated, must each still hold the headroom of every running orb plus this one.
fn shortfalls(
    cand: &PlacementCandidate,
    used: &store::PoolUse,
    demand: Resources,
) -> Vec<(FailureCode, String)> {
    let cap = cand.capacity_ceiling;
    let held = used.held;
    let running = used.running;
    let mut out = Vec::new();
    let free_mem = cap.memory_bytes.saturating_sub(held.memory_bytes);
    if demand.memory_bytes > free_mem {
        out.push((
            FailureCode::InsufficientMemory,
            format!(
                "memory: needs {}, {} free of {} (running orbs: {running})",
                gib(demand.memory_bytes),
                gib(free_mem),
                gib(cap.memory_bytes)
            ),
        ));
    }
    let free_cpu = cap.vcpus.saturating_sub(held.vcpus);
    if demand.vcpus > free_cpu {
        out.push((
            FailureCode::QuotaExceeded,
            format!(
                "vCPUs: needs {}, {free_cpu} free of {} (running orbs: {running})",
                demand.vcpus, cap.vcpus
            ),
        ));
    }
    match cand.disk {
        None => out.push((
            FailureCode::BackendUnavailable,
            "disk: the agent has not reported its disk yet".into(),
        )),
        Some(d) => {
            let reserve = d.reserve_bytes;
            let fs_room = d
                .free_bytes
                .saturating_sub(reserve)
                .saturating_sub(held.disk_bytes);
            let cap_room = cap
                .disk_bytes
                .saturating_sub(d.allocated_bytes)
                .saturating_sub(held.disk_bytes);
            if demand.disk_bytes > fs_room.min(cap_room) {
                let detail = if fs_room <= cap_room {
                    let of = if d.total_bytes > 0 {
                        format!(" of {}", gib(d.total_bytes))
                    } else {
                        String::new()
                    };
                    format!(
                        "disk: needs {} headroom, {} usable (filesystem {} free{of}, {} \
                         reserve, {} held by {running} running)",
                        gib(demand.disk_bytes),
                        gib(fs_room),
                        gib(d.free_bytes),
                        gib(reserve),
                        gib(held.disk_bytes)
                    )
                } else {
                    format!(
                        "disk: needs {} headroom, {} usable (agent disk limit {}, {} \
                         allocated, {} held by {running} running)",
                        gib(demand.disk_bytes),
                        gib(cap_room),
                        gib(cap.disk_bytes),
                        gib(d.allocated_bytes),
                        gib(held.disk_bytes)
                    )
                };
                out.push((FailureCode::InsufficientDisk, detail));
            }
        }
    }
    out
}

fn describe(short: &[(FailureCode, String)]) -> String {
    short
        .iter()
        .map(|(_, s)| s.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The refusal code when no host fits: the resource most hosts lack (memory before
/// disk before vCPUs on a tie). Hosts that only lack a disk report are unavailable,
/// not full.
fn limiting(refusals: &[(String, Vec<(FailureCode, String)>)]) -> FailureCode {
    const ORDER: [FailureCode; 3] = [
        FailureCode::InsufficientMemory,
        FailureCode::InsufficientDisk,
        FailureCode::QuotaExceeded,
    ];
    let count = |code: FailureCode| {
        refusals
            .iter()
            .filter(|(_, s)| s.iter().any(|(c, _)| *c == code))
            .count()
    };
    ORDER
        .into_iter()
        .filter(|c| count(*c) > 0)
        .max_by_key(|c| {
            (
                count(*c),
                std::cmp::Reverse(ORDER.iter().position(|o| o == c)),
            )
        })
        .unwrap_or(FailureCode::BackendUnavailable)
}

/// Digest over the immutable command contents, including the identity (keyed
/// fingerprint) of the secret payload.
fn command_digest(plan: &CreatePlan, spec_digest: &SpecDigest) -> String {
    let body = serde_json::to_vec(&(
        plan.operation_id,
        plan.orb_id,
        plan.generation,
        &spec_digest.0,
        &plan.action,
        &plan.fingerprint,
    ))
    .expect("serializable");
    hex::encode(Sha256::digest(body))
}
