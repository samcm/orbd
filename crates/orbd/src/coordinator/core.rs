//! The coordinator actor. One task owns all mutable coordinator state and processes
//! messages serially (dispatch requests, backend events, timer ticks). Admission,
//! reservations and per-orb serialization are therefore race-free by construction, and
//! are additionally enforced by SQLite constraints.

use super::db::Db;
use super::logs::{self, LogStore};
use super::store::{self, Applied, CreatePlan};
use super::types::*;
use super::wake::{Hold, Hook, Waker};
use crate::backend::{
    Availability, BackendEvent, CancelRequest, CommandDelivery, CommandSecrets, ComputeBackend,
    OperationStatus, PlacementCandidate, PlacementRequest, ReconcileQuery,
};
use crate::config::coordinator::FLEET_BACKEND;
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
pub(super) const BACKEND_CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// A validated dispatch request (see `service`).
pub struct ValidatedRequest {
    pub invocation_id: InvocationId,
    /// The offering a new orb is placed as. An existing orb keeps its own.
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
    /// Orb states a host reported when it became available (see `reconcile_location`).
    OrbsReported {
        location: Location,
        orbs: Vec<(OrbId, u64, OrbState)>,
    },
    /// `orbd orb pause|delete`: a job for one orb, queued as if Amp had sent it.
    /// Refusals arrive on `tx` as an error, results as dispatch events.
    OperatorJob {
        amp_orb_id: AmpOrbId,
        job: OperatorJob,
        tx: EventTx,
    },
    HookFinished {
        hook: Hook,
        result: std::result::Result<(), String>,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum OperatorJob {
    Pause,
    /// Only an orb asleep, with no job, for `min_asleep`; `None` (`--force`) deletes
    /// it whatever it is doing.
    Delete {
        min_asleep: Option<Duration>,
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
    waker: Option<Waker>,
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
    Waiting,
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
            waker: None,
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

    pub fn with_waker(mut self, waker: Option<Waker>) -> Self {
        self.waker = waker;
        self
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
            Msg::OrbsReported { location, orbs } => {
                if let Err(err) = self.orbs_reported(location, orbs).await {
                    tracing::error!(error = %format!("{err:#}"), "applying reported orb states failed");
                }
            }
            Msg::OperatorJob {
                amp_orb_id,
                job,
                tx,
            } => {
                if let Err(err) = self.operator_job(amp_orb_id, job, tx.clone()).await {
                    tracing::error!(error = %format!("{err:#}"), "operator orb job failed");
                    let _ = tx
                        .send(Err(GrpcStatus::unavailable("coordinator internal error")))
                        .await;
                }
            }
            Msg::HookFinished { hook, result } => {
                if let Some(w) = &mut self.waker {
                    w.finished(hook, result);
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
            Step::Started | Step::Waiting => Ok(false),
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

        // An existing orb keeps the offering it was created as, whatever the job names
        // (a pause has no orb type; a thread's orb type or provider may have changed).
        // Only a new orb needs the job's offering.
        let mut offering = match &orb {
            Some(o) => {
                if o.offering_id != head.offering_id {
                    tracing::debug!(orb = %o.id, offering = %o.offering_id, requested = %head.offering_id, "orb keeps its offering");
                }
                None
            }
            None => match self.offerings.get(&head.offering_id).cloned() {
                Some(o) => Some(o),
                None => {
                    return self
                        .finish_plain(head, Status::Invalid, "unknown offering")
                        .await;
                }
            },
        };
        let spawn = matches!(head.action, ReqAction::Spawn { .. });
        // During and after the sleep command, the agent may still look available.
        if spawn
            && self.waker.as_ref().is_some_and(|w| w.holding())
            && self.on_fleet(orb.as_ref(), offering.as_ref(), false)
            && let Some(step) = self.hold(head, orb.as_ref(), None).await?
        {
            return Ok(step);
        }
        let mut snapshot_report = None;
        // An orb that never started has nothing on its host (its agent said so), so
        // nothing ties it there once that host will not take it: its spawn then places a
        // new incarnation like a new orb, away from every host that refused it. At once
        // when its agent refused it for a reason of its own the coordinator cannot see
        // (its image not allowed there), or kept refusing it for want of room; otherwise
        // only when the coordinator finds its host cannot take it now: no room, unable
        // to run it, or gone well beyond a restart ([`HOST_GONE`]) or removed. Never when
        // it was asked to wait (an image being prepared).
        let unstarted = match (&orb, spawn) {
            (Some(o), true) => {
                let (id, generation) = (o.id, o.last_issued_generation);
                self.db
                    .call(move |c| store::unstarted(c, id, generation))
                    .await?
            }
            _ => None,
        };
        let mut replaced: Option<(OrbRecord, String)> = None;
        let mut planned = None;
        if let (Some(u), Some(o)) = (&unstarted, &orb)
            && let Some(why) = refused_for_good(u)
            && let Some((p, o2)) = self
                .replace_unstarted(head, o, &mut snapshot_report)
                .await?
        {
            replaced = Some((o.clone(), why));
            offering = Some(o2);
            planned = Some(Ok(p));
        }
        let planned = match planned {
            Some(p) => p,
            None => {
                let p = self
                    .plan(
                        head,
                        offering.as_ref(),
                        orb.as_ref(),
                        &mut snapshot_report,
                        &[],
                    )
                    .await?;
                match (&p, &unstarted, &orb) {
                    (Err(r), Some(_), Some(o)) if self.host_cannot_take(r, &o.location) => {
                        match self
                            .replace_unstarted(head, o, &mut snapshot_report)
                            .await?
                        {
                            Some((np, o2)) => {
                                replaced = Some((o.clone(), r.message.clone()));
                                offering = Some(o2);
                                Ok(np)
                            }
                            None => p,
                        }
                    }
                    _ => p,
                }
            }
        };
        if replaced.is_some() {
            orb = None;
        }
        let wakes =
            spawn && self.waker.is_some() && self.on_fleet(orb.as_ref(), offering.as_ref(), true);
        let plan = match planned {
            Ok(p) => p,
            Err(r) if wakes && r.code == FailureCode::BackendUnavailable => {
                return match self.hold(head, orb.as_ref(), Some(r.clone())).await? {
                    Some(step) => Ok(step),
                    None => self.finish_refused(head, orb.as_ref(), r).await,
                };
            }
            Err(r) => return self.finish_refused(head, orb.as_ref(), r).await,
        };
        if let Some(w) = &mut self.waker {
            w.forget(head.invocation_id);
        }
        let op = plan.operation_id;
        let p = plan;
        let (generation, orb_id) = (p.generation, p.orb_id);
        self.db.call(move |c| store::create_command(c, &p)).await?;
        crate::crash_point!("coordinator::command_committed");
        if let Some(r) = snapshot_report {
            r.report(&head.amp_orb_id);
        }
        if let Some((old, why)) = replaced {
            tracing::info!(orb = %old.id, new_orb = %orb_id, amp_orb_id = %head.amp_orb_id, location = ?old.location, reason = %why, "spawn of an orb that never started: placing it anew");
            metrics::counter!("orbd_unstarted_orbs_replaced_total").increment(1);
            self.cleanup_retired(Some(old.id)).await?;
        }
        self.announce_accepted(head.invocation_id, op, generation, false, 0);
        tracing::info!(operation = %op, orb = %orb_id, generation, action = head.action.name(), "command created");
        self.submit_command(op).await;
        Ok(Step::Started)
    }

    /// Plans a new incarnation of `old`, an orb that never started, like a new orb
    /// (the job's orb type, else the one it was created as), away from every host that
    /// refused this Amp orb lately ([`REFUSALS_KEPT`]) if another fits; the plan
    /// retires `old` in the same transaction (see [`CreatePlan::retire`]). None, leaving
    /// `old` as it is, when no host takes it, when only hosts that refused it do, or
    /// when it was placed anew [`MAX_REPLACEMENTS`] times lately already.
    async fn replace_unstarted(
        &mut self,
        head: &Req,
        old: &OrbRecord,
        snapshot_report: &mut Option<SnapshotReport>,
    ) -> Result<Option<(CreatePlan, Offering)>> {
        let Some(offering) = self
            .offerings
            .get(&head.offering_id)
            .or_else(|| self.offerings.get(&old.offering_id))
            .cloned()
        else {
            return Ok(None);
        };
        let (ep, amp) = (
            old.identity.amp_endpoint.to_string(),
            old.identity.amp_orb_id.to_string(),
        );
        let since = unix_ms_now().saturating_sub(REFUSALS_KEPT.as_millis() as u64);
        let mut avoid = self
            .db
            .call(move |c| store::refused_hosts(c, &ep, &amp, since))
            .await?;
        if avoid.len() >= MAX_REPLACEMENTS {
            tracing::debug!(orb = %old.id, amp_orb_id = %old.identity.amp_orb_id, "placed anew too often lately; staying where it is");
            return Ok(None);
        }
        avoid.push(old.location.clone());
        let mut report = None;
        let Ok(mut p) = self
            .plan(head, Some(&offering), None, &mut report, &avoid)
            .await?
        else {
            return Ok(None);
        };
        if p.new_orb
            .as_ref()
            .is_none_or(|n| avoid.iter().any(|a| same_host(a, &n.location)))
        {
            return Ok(None);
        }
        p.retire = Some((old.id, old.last_issued_generation));
        *snapshot_report = report;
        Ok(Some((p, offering)))
    }

    /// Whether the coordinator's refusal `r` of an orb at `location` says that host
    /// cannot take it now: no room or unable to run it, or gone (disconnected) well
    /// beyond a restart ([`HOST_GONE`]) or removed. Not while it restarts or drains.
    fn host_cannot_take(&self, r: &Refusal, location: &Location) -> bool {
        match r.code {
            FailureCode::InsufficientMemory
            | FailureCode::InsufficientDisk
            | FailureCode::QuotaExceeded
            | FailureCode::UnsupportedCapability => true,
            FailureCode::BackendUnavailable => self
                .backends
                .get(location.backend_id())
                .and_then(|b| b.host_absence(location))
                .is_some_and(|gone| gone >= HOST_GONE),
            _ => false,
        }
    }

    /// Tells the old host of each orb that never started and was placed anew (retired,
    /// its delete not done; `only` that one) to delete what it recorded of it: an
    /// agent's row and receipt, or a Boat sandbox whose create response was lost. Only
    /// while that host is available, so no command piles up for one that is gone; again
    /// after [`CLEANUP_EVERY`], doubling, at most [`CLEANUP_ATTEMPTS`] times. A host that
    /// answers it cannot run the orb created nothing of it: done
    /// ([`store::retired_undeleted`]).
    async fn cleanup_retired(&mut self, only: Option<OrbId>) -> Result<()> {
        let orbs = self.db.call(|c| store::retired_undeleted(c)).await?;
        let now = unix_ms_now();
        for orb in orbs {
            if only.is_some_and(|id| id != orb.id)
                || !cleanup_due(orb.attempts, orb.last_attempt_ms, now)
            {
                continue;
            }
            let Some(backend) = self.backends.get(orb.location.backend_id()).cloned() else {
                continue;
            };
            let cands = self
                .candidates(
                    &backend,
                    PlacementRequest {
                        target: orb.target.clone(),
                        resources: orb.resources,
                        existing: Some(orb.location.clone()),
                        project_snapshot: None,
                    },
                )
                .await;
            let available = cands.is_some_and(|v| {
                v.iter().any(|c| {
                    c.location == orb.location && c.availability == Availability::Available
                })
            });
            if !available {
                continue;
            }
            let mut plan = CreatePlan {
                invocation_id: InvocationId::generate(),
                new_orb: None,
                orb_id: orb.id,
                prev_generation: orb.last_issued_generation,
                generation: orb.last_issued_generation + 1,
                operation_id: OperationId::generate(),
                action: Action::Delete,
                command_digest: String::new(),
                fingerprint: None,
                work_deadline_ms: now + WORK_BUDGET_MS,
                cleanup_deadline_ms: now + CLEANUP_BUDGET_MS,
                reservation: None,
                tombstone: None,
                sealed: None,
                retire: None,
            };
            plan.command_digest = command_digest(&plan, &orb.spec_digest);
            let op = plan.operation_id;
            tracing::info!(orb = %orb.id, amp_orb_id = %orb.amp_orb_id, location = ?orb.location, attempt = orb.attempts + 1, "deleting what its old host recorded of an orb that never started");
            self.db
                .call(move |c| store::create_command(c, &plan))
                .await?;
            self.submit_command(op).await;
        }
        Ok(())
    }

    /// `spill`: a new orb also counts when a spill target is on the agents.
    fn on_fleet(&self, orb: Option<&OrbRecord>, offering: Option<&Offering>, spill: bool) -> bool {
        let fleet = |o: &Offering| o.backend_id.as_str() == FLEET_BACKEND;
        match (orb, offering) {
            (Some(o), _) => matches!(o.location, Location::Fleet { .. }),
            (None, Some(o)) => {
                fleet(o)
                    || spill
                        && o.spill_to
                            .iter()
                            .filter_map(|id| self.offerings.get(id))
                            .any(fleet)
            }
            (None, None) => false,
        }
    }

    /// `refusal`: placement found no host. None: carry on as without `wake:`.
    async fn hold(
        &mut self,
        head: &Req,
        orb: Option<&OrbRecord>,
        refusal: Option<Refusal>,
    ) -> Result<Option<Step>> {
        let Some(w) = &mut self.waker else {
            return Ok(None);
        };
        let inv = head.invocation_id;
        let (hold, note) = w.hold(inv, refusal.is_some());
        if let Some(message) = note {
            self.send_event(
                inv,
                event(pb::dispatch_event::Event::Progress(pb::DispatchProgress {
                    operation_id: String::new(),
                    phase: "waking".into(),
                    message,
                })),
            );
        }
        match (hold, refusal) {
            (Hold::Wait, _) => Ok(Some(Step::Waiting)),
            (Hold::GiveUp(why), Some(mut r)) => {
                r.message = format!("{}; {why}", r.message);
                Ok(Some(self.finish_refused(head, orb, r).await?))
            }
            _ => Ok(None),
        }
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
        demand(&self.policy, spec)
    }

    /// Admission: choose/verify the location and build the atomic creation plan.
    /// `snapshot_report`: for a new orb that names a project snapshot, where it went as
    /// far as the snapshot goes, to report once the command is committed.
    /// `avoid`: for a new orb, hosts to place it away from if another fits.
    async fn plan(
        &mut self,
        head: &Req,
        offering: Option<&Offering>,
        orb: Option<&OrbRecord>,
        snapshot_report: &mut Option<SnapshotReport>,
        avoid: &[Location],
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
            retire: None,
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

        match orb {
            Some(orb) => {
                // An existing orb lives on its own backend, which differs from the
                // offering's when the orb was spilled.
                let Some(backend) = self.backends.get(orb.location.backend_id()).cloned() else {
                    return Ok(Err(refuse(
                        FailureCode::BackendUnavailable,
                        "backend not configured",
                    )));
                };
                // Existing orb: never re-placed (one that never started is retired and
                // placed anew by `start` when its host refuses it here).
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
                // An orb with restricted egress never starts where it would run open
                // (e.g. its agent was downgraded); its agent refuses it as well.
                if matches!(head.action, ReqAction::Spawn { .. })
                    && template.requirements.egress.is_some()
                    && cand.capabilities.egress_policy != crate::model::Support::Supported
                {
                    return Ok(Err(refuse(
                        FailureCode::UnsupportedCapability,
                        "the orb's host does not enforce restricted egress, which its orb type \
                         has (egress)",
                    )));
                }
                if matches!(head.action, ReqAction::Spawn { .. }) {
                    let demand = self.demand(template);
                    // A running orb already holds its reservation; re-spawning it (a
                    // bootstrap retry) needs nothing new.
                    let running =
                        matches!(orb.observation.state.last_known(), StableOrbState::Running);
                    if !running {
                        // Checked net of what its wake frees (`wake_demand`); once
                        // running it reserves its whole demand, that room being then
                        // free space held for its next checkpoint.
                        let release = backend.wake_releases(&orb.location).remove(&orb.id);
                        let need =
                            wake_demand(&self.policy, template, &orb.observation.state, release);
                        let used = self.pool_used(&cand, Some(orb.id)).await?;
                        let short = shortfalls(&cand, &used, need);
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
                let offering = offering.expect("a new orb's offering was looked up");
                let snap_key = snapshot_id
                    .as_deref()
                    .map(ProjectSnapshotKey::from_external);
                // The offering's own hosts first, then its spill_to offerings in order.
                let mut held = false;
                let mut kept_room = false;
                let mut placed = self
                    .place_new(
                        offering,
                        snap_key.as_ref(),
                        &mut held,
                        &mut kept_room,
                        avoid,
                    )
                    .await?;
                if let Err(first) = &placed {
                    let mut tried = Vec::new();
                    for id in &offering.spill_to {
                        let Some(target) = self.offerings.get(id).cloned() else {
                            continue;
                        };
                        match self
                            .place_new(&target, snap_key.as_ref(), &mut held, &mut kept_room, avoid)
                            .await?
                        {
                            Ok(p) => {
                                tracing::info!(
                                    offering = %offering.id,
                                    spill_to = %target.id,
                                    amp_orb_id = %head.amp_orb_id,
                                    reason = %first.message,
                                    "spilling a new orb"
                                );
                                metrics::counter!("orbd_spills_total", "offering" => offering.id.to_string(), "to" => target.id.to_string()).increment(1);
                                placed = Ok(p);
                                break;
                            }
                            Err(r) => tried.push(format!("{}: {}", target.id, r.message)),
                        }
                    }
                    if let Err(first) = &mut placed
                        && !tried.is_empty()
                    {
                        first.message = format!("{}; spill: {}", first.message, tried.join("; "));
                    }
                }
                let (chosen, demand, template) = match placed {
                    Ok(p) => p,
                    Err(r) => return Ok(Err(r)),
                };
                let snapshot_placement = snap_key.as_ref().map(|k| SnapshotReport {
                    placement: SnapshotPlacement::of(
                        chosen.has_project_snapshot,
                        chosen.capabilities.project_snapshots != SnapshotCapability::Unsupported,
                        held,
                        kept_room,
                    ),
                    snapshot: k.as_str().get(..12).unwrap_or(k.as_str()).to_owned(),
                    host: chosen.capacity_pool.clone(),
                });
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
                    template,
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
                *snapshot_report = snapshot_placement;
                Ok(Ok(plan))
            }
        }
    }

    /// Places a new orb of `offering` on one of its backend's hosts: the chosen
    /// candidate, what the orb reserves there, and the spec template it runs with.
    /// `held` becomes true if any of the backend's hosts holds `snap_key`, available or
    /// not, able to run the offering or not. `kept_room` is set to whether the chosen
    /// host lacks `snap_key` only because the hosts holding it are keeping room to wake
    /// their own orbs ([`choose`]).
    async fn place_new(
        &self,
        offering: &Offering,
        snap_key: Option<&ProjectSnapshotKey>,
        held: &mut bool,
        kept_room: &mut bool,
        avoid: &[Location],
    ) -> Result<std::result::Result<(PlacementCandidate, Resources, SpecTemplate), Refusal>> {
        let Some(backend) = self.backends.get(&offering.backend_id).cloned() else {
            return Ok(Err(refuse(
                FailureCode::BackendUnavailable,
                "backend not configured",
            )));
        };
        let cands = self
            .candidates(
                &backend,
                PlacementRequest {
                    target: offering.spec.target.clone(),
                    resources: offering.spec.resources,
                    existing: None,
                    project_snapshot: snap_key.cloned(),
                },
            )
            .await;
        let Some(cands) = cands else {
            return Ok(Err(refuse(
                FailureCode::BackendUnavailable,
                "no available host can run this offering (the backend did not list its hosts)",
            )));
        };
        *held |= cands.iter().any(|c| c.has_project_snapshot);
        let available: Vec<PlacementCandidate> = cands
            .iter()
            .filter(|c| {
                c.availability == Availability::Available
                    && c.capabilities.check(&offering.spec).is_empty()
            })
            .cloned()
            .collect();
        if available.is_empty() {
            return Ok(Err(refuse(
                FailureCode::BackendUnavailable,
                no_host_message(&offering.spec, &cands),
            )));
        }
        let demand = self.demand(&offering.spec);
        let mut fitting: Vec<Fit> = Vec::new();
        let mut refusals: Vec<(String, Vec<(FailureCode, String)>)> = Vec::new();
        for cand in available {
            let used = self.pool_used(&cand, None).await?;
            let short = shortfalls(&cand, &used, demand);
            if short.is_empty() {
                let room = self.wake_room(&backend, &cand).await?;
                let leaves_wake_room = shortfalls(&cand, &used, plus(demand, room)).is_empty();
                let score = if leaves_wake_room {
                    let kept = store::PoolUse {
                        held: plus(used.held, room),
                        ..used
                    };
                    room_score(&cand, &kept)
                } else {
                    room_score(&cand, &used)
                };
                let avoided = avoid.iter().any(|a| same_host(a, &cand.location));
                fitting.push(Fit {
                    cand,
                    score,
                    leaves_wake_room,
                    avoided,
                });
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
        let (chosen, passed_over) = choose(fitting, snap_key.is_some());
        *kept_room = passed_over;
        Ok(Ok((chosen, demand, offering.spec.clone())))
    }

    /// The room `cand` keeps for waking the orbs recently asleep or stopped on it (they
    /// cannot wake anywhere else): what the largest of their wakes needs, resource by
    /// resource ([`wake_demand`], with what the host reports each wake frees), computed
    /// by the database. Not for orbs being deleted, faulted, retired, asleep longer than
    /// [`WAKE_ROOM_RECENT`], or archived (their agent already keeps the largest archived
    /// disk's restore out of the free space it reports).
    async fn wake_room(
        &self,
        backend: &Arc<dyn ComputeBackend>,
        cand: &PlacementCandidate,
    ) -> Result<Resources> {
        let (b, p) = (
            cand.location.backend_id().to_string(),
            cand.capacity_pool.clone(),
        );
        let releases: Vec<(OrbId, crate::backend::WakeRelease)> =
            backend.wake_releases(&cand.location).into_iter().collect();
        let archived: Vec<OrbId> = backend
            .orb_disk(&cand.location)
            .unwrap_or_default()
            .into_iter()
            .filter(|(_, u)| u.archived)
            .map(|(id, _)| id)
            .collect();
        let headroom = self.policy.running_orb_disk_headroom;
        let since = unix_ms_now().saturating_sub(WAKE_ROOM_RECENT.as_millis() as u64);
        self.db
            .call(move |c| store::wake_room(c, &b, &p, headroom, since, &releases, &archived))
            .await
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
                egress: current_egress(&self.offerings, &orb),
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

    /// Asks a host that just became available for every orb it holds. Its VMs may have
    /// died while it was away (a host reboot or freeze), when it could not report them
    /// (`BackendEvent::OrbsObserved`); without this such an orb stays "running", holding
    /// its compute, until its next command.
    fn reconcile_location(&self, location: Location) {
        let Some(backend) = self.backends.get(location.backend_id()).cloned() else {
            return;
        };
        let tx = self.self_tx.clone();
        tokio::spawn(async move {
            let mut page_token = None;
            loop {
                let query = ReconcileQuery {
                    location: location.clone(),
                    operations: vec![],
                    orbs: vec![],
                    page_token: page_token.take(),
                };
                let page =
                    match tokio::time::timeout(Duration::from_secs(10), backend.reconcile(query))
                        .await
                    {
                        Ok(Ok(p)) => p,
                        Ok(Err(e)) => {
                            tracing::debug!(?location, error = %e, "orb reconcile unavailable");
                            return;
                        }
                        Err(_) => {
                            tracing::debug!(?location, "orb reconcile timed out");
                            return;
                        }
                    };
                let next = page.next_page_token;
                let msg = Msg::OrbsReported {
                    location: location.clone(),
                    orbs: page.orbs,
                };
                if tx.send(msg).await.is_err() {
                    return;
                }
                match next {
                    Some(t) if !t.is_empty() => page_token = Some(t),
                    _ => return,
                }
            }
        });
    }

    async fn orbs_reported(
        &mut self,
        location: Location,
        orbs: Vec<(OrbId, u64, OrbState)>,
    ) -> Result<()> {
        for (orb, generation, state) in orbs {
            let (loc, st, now) = (location.clone(), state.clone(), unix_ms_now());
            let replaced = self
                .db
                .call(move |c| store::observe_stopped(c, orb, &loc, generation, &st, now))
                .await?;
            match replaced {
                Some(OrbState::Known {
                    state: StableOrbState::Sleeping { .. },
                }) => {
                    tracing::info!(%orb, ?state, "asleep orb lost its memory checkpoint on its host; it cold-boots on its next wake");
                }
                Some(_) => {
                    tracing::warn!(%orb, ?state, "orb recorded as running has stopped on its host");
                    metrics::counter!("orbd_orbs_found_stopped_total").increment(1);
                }
                None => {}
            }
        }
        Ok(())
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
            BackendEvent::Removed { location } => {
                // Its host will never run, or answer for, what it was sent: those commands
                // end failed, the orbs as last recorded, so a spawn of one that never
                // started is placed anew on its next try.
                let open = self.db.call(|c| store::open_commands(c)).await?;
                for (cmd, orb) in open {
                    if orb.location != location {
                        continue;
                    }
                    let key = CommandKey {
                        operation_id: cmd.operation_id,
                        orb_id: orb.id,
                        generation: cmd.generation,
                    };
                    tracing::info!(operation = %cmd.operation_id, orb = %orb.id, ?location, "ending a command of a removed host");
                    let outcome = synthetic_outcome(
                        orb.observation.state.clone(),
                        refuse(
                            FailureCode::BackendUnavailable,
                            "its host was removed; it never answered",
                        ),
                    );
                    self.complete(key, outcome, false).await?;
                }
            }
            BackendEvent::AvailabilityChanged {
                location,
                availability,
            } => {
                tracing::info!(?location, ?availability, "backend availability changed");
                if let (Location::Fleet { agent_id, .. }, Some(w)) = (&location, &mut self.waker) {
                    if availability == Availability::Available {
                        w.agent_connected(agent_id.to_string());
                    } else {
                        w.agent_disconnected(&agent_id.to_string());
                    }
                }
                if availability == Availability::Available {
                    let open = self.db.call(|c| store::open_commands(c)).await?;
                    for (cmd, orb) in open {
                        if orb.location == location {
                            self.submit_command(cmd.operation_id).await;
                        }
                    }
                    self.reconcile_location(location);
                }
            }
            BackendEvent::OrbsObserved { location, orbs } => {
                self.orbs_reported(location, orbs).await?;
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
        let fleet_jobs = open
            .iter()
            .any(|(_, orb)| matches!(orb.location, Location::Fleet { .. }));
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
            let before = now.saturating_sub(RETIRED_KEPT.as_millis() as u64);
            let pruned = self
                .db
                .call(move |c| store::prune_retired_unstarted(c, before, CLEANUP_ATTEMPTS))
                .await?;
            if pruned > 0 {
                tracing::info!(
                    pruned,
                    "removed the rows of incarnations retired for a new placement"
                );
            }
        }
        if self.ticks.is_multiple_of(60) {
            self.reclaim_idle(now).await?;
            self.cleanup_retired(None).await?;
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
        if let Some(w) = &mut self.waker {
            let fleet = format!("{FLEET_BACKEND}/");
            let running = pools
                .iter()
                .any(|(pool, u)| pool.starts_with(&fleet) && u.running > 0);
            w.tick(fleet_jobs || running);
        }
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
            if self.last_reclaim.contains_key(&orb.id) {
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
            self.queue_job(&orb, ReqAction::Delete, now, None).await?;
        }
        Ok(())
    }

    /// Pauses or deletes one orb on an operator's request (`orbd orb pause|delete`),
    /// through the ordinary path, queued behind any job Amp has for it.
    ///
    /// A delete with `min_asleep` (e.g. from a job that finds archived threads) takes
    /// only an orb [`store::idle_orbs`] judges idle with a cutoff `min_asleep` ago:
    /// last seen asleep or stopped before the cutoff, no open command, and no request
    /// queued, running, or started since the cutoff (finished requests are pruned
    /// after about an hour). An orb Amp is using is never deleted; checking and
    /// queueing here in the actor leaves no gap for a spawn to slip in between.
    ///
    /// Pause and a forced delete are for orbs Amp no longer drives, e.g. one left
    /// running when its provider's token stopped being served: nothing else would
    /// ever stop it.
    async fn operator_job(
        &mut self,
        amp_orb_id: AmpOrbId,
        job: OperatorJob,
        tx: EventTx,
    ) -> Result<()> {
        let now = unix_ms_now();
        let min_asleep = match job {
            OperatorJob::Delete { min_asleep } => min_asleep,
            OperatorJob::Pause => None,
        };
        let cutoff = min_asleep.map(|m| now.saturating_sub(m.as_millis() as u64));
        let mut endpoints: Vec<AmpEndpointId> = self
            .offerings
            .values()
            .map(|o| o.amp_endpoint.clone())
            .collect();
        endpoints.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        endpoints.dedup();
        // A forced delete also takes an orb whose earlier delete did not finish (it has a
        // tombstone, so Amp's spawns are refused, but it is not deleted): the case it is for.
        let force = matches!(job, OperatorJob::Delete { min_asleep: None });
        let id = amp_orb_id.clone();
        let (orbs, idle) = self
            .db
            .call(move |c| {
                let mut orbs = Vec::new();
                for ep in &endpoints {
                    if let Some(o) = store::get_orb_by_identity(c, ep.as_str(), id.as_str())?
                        && (!store::is_tombstoned(c, o.id)?
                            || force
                                && !matches!(
                                    o.observation.state,
                                    OrbState::Known {
                                        state: StableOrbState::Deleted
                                    }
                                ))
                    {
                        orbs.push(o);
                    }
                }
                let idle: Vec<OrbId> = match cutoff {
                    Some(cutoff) => store::idle_orbs(c, cutoff)?.iter().map(|o| o.id).collect(),
                    None => Vec::new(),
                };
                Ok((orbs, idle))
            })
            .await?;
        let mins = min_asleep.unwrap_or_default().as_secs() / 60;
        let refusal = match orbs.as_slice() {
            [] => Some(GrpcStatus::not_found(format!("no orb {amp_orb_id}"))),
            [_, _, ..] => Some(GrpcStatus::failed_precondition(format!(
                "{amp_orb_id} names orbs under {} Amp endpoints",
                orbs.len()
            ))),
            [o] if cutoff.is_some() && !idle.contains(&o.id) => {
                Some(GrpcStatus::failed_precondition(format!(
                    "{amp_orb_id} is {} (recorded {} min ago); it must have been last seen \
                     asleep or stopped at least {mins} min ago, with no job from Amp open or \
                     started since (finished jobs are kept for about an hour)",
                    state_label(&o.observation.state),
                    now.saturating_sub(o.observation.observed_at_unix_ms) / 60_000
                )))
            }
            [_] => None,
        };
        if let Some(status) = refusal {
            let _ = tx.send(Err(status)).await;
            return Ok(());
        }
        let orb = orbs.into_iter().next().expect("checked above");
        let action = match job {
            OperatorJob::Pause => ReqAction::Pause { snapshot_id: None },
            OperatorJob::Delete { .. } => ReqAction::Delete,
        };
        tracing::info!(
            orb = %orb.id,
            amp_orb_id = %orb.identity.amp_orb_id,
            location = ?orb.location,
            state = %state_label(&orb.observation.state),
            state_age_mins = now.saturating_sub(orb.observation.observed_at_unix_ms) / 60_000,
            ?job,
            "{} orb on request",
            match action {
                ReqAction::Delete => "deleting",
                _ => "pausing",
            }
        );
        self.queue_job(&orb, action, now, Some(tx)).await
    }

    /// Queues a pause or delete request for `orb`, as if Amp had sent it, and starts it
    /// if the orb is free. With `tx`, the caller follows it like a dispatch.
    async fn queue_job(
        &mut self,
        orb: &OrbRecord,
        action: ReqAction,
        now: u64,
        tx: Option<EventTx>,
    ) -> Result<()> {
        let req = Req {
            invocation_id: InvocationId::generate(),
            offering_id: orb.offering_id.clone(),
            amp_endpoint: orb.identity.amp_endpoint.clone(),
            amp_orb_id: orb.identity.amp_orb_id.clone(),
            action,
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

/// How much room a fitting host has left, for choosing among fitting hosts (higher is
/// better): the scarcer of its free memory (capacity less running orbs) and its usable
/// disk (filesystem free space less the reserve and running orbs' headroom), each as a
/// fraction of the host's own (in millionths). By memory alone, the host with the most
/// free memory took every new orb however full its disk was, and orbs cannot move
/// between hosts; this way a host whose disk is filling up takes fewer, while memory
/// still decides between hosts whose disks have room. Shares, not bytes: hosts fill
/// evenly, rather than the biggest taking orbs until it is as empty as the others.
pub(super) fn room_score(cand: &PlacementCandidate, used: &store::PoolUse) -> u64 {
    const PARTS: u128 = 1_000_000;
    let frac = |part: u64, whole: u64| match whole {
        0 => PARTS as u64,
        w => (u128::from(part.min(w)) * PARTS / u128::from(w)) as u64,
    };
    let cap = cand.capacity_ceiling;
    let memory = frac(
        cap.memory_bytes.saturating_sub(used.held.memory_bytes),
        cap.memory_bytes,
    );
    // The filesystem's room, and the room under the agent's disk limit (`shortfalls`
    // checks both), each as a share of its whole.
    let disk = match cand.disk {
        Some(d) if d.total_bytes > 0 => {
            let fs = frac(
                d.free_bytes
                    .saturating_sub(d.reserve_bytes)
                    .saturating_sub(used.held.disk_bytes),
                d.total_bytes,
            );
            let limit = match cap.disk_bytes {
                0 => PARTS as u64,
                c => frac(
                    c.saturating_sub(d.allocated_bytes)
                        .saturating_sub(used.held.disk_bytes),
                    c,
                ),
            };
            fs.min(limit)
        }
        _ => PARTS as u64,
    };
    memory.min(disk)
}

/// What a running orb of `spec` reserves on its host (see `Core::demand`).
pub(super) fn demand(policy: &Policy, spec: &SpecTemplate) -> Resources {
    let r = spec.resources;
    let checkpoint = match spec.pause_policy {
        PausePolicy::MemoryPreserving { .. } => r.memory_bytes,
        PausePolicy::DiskOnly => 0,
    };
    Resources {
        disk_bytes: r
            .disk_bytes
            .min(policy.running_orb_disk_headroom)
            .saturating_add(checkpoint),
        ..r
    }
}

/// What waking an orb of `spec` in `state` needs admitted on its host: its [`demand`],
/// less what its host reported the wake frees now (`release`, see
/// [`crate::backend::WakeRelease`]), up to the checkpoint headroom. That is room the
/// host already counts as taken (a sleep held in RAM, whose agent commits the orb's
/// memory size for its checkpoint; a checkpoint on disk where a restore reads it into
/// memory and it is deleted right after), and what the orb's next checkpoint takes
/// again: charging it as new headroom too would refuse wakes the host can take. Credit
/// only on that positive knowledge, for a wake from the reported checkpoint generation:
/// a checkpoint a restored VMM keeps mapped until the next sleep is written (Firecracker),
/// or one not measured since the orb went to sleep, is reported by no host; an orb
/// stopped without a checkpoint, faulted or never started gets none.
pub(super) fn wake_demand(
    policy: &Policy,
    spec: &SpecTemplate,
    state: &OrbState,
    release: Option<crate::backend::WakeRelease>,
) -> Resources {
    let mut need = demand(policy, spec);
    let checkpoint = match spec.pause_policy {
        PausePolicy::MemoryPreserving { .. } => spec.resources.memory_bytes,
        PausePolicy::DiskOnly => 0,
    };
    if let OrbState::Known {
        state: StableOrbState::Sleeping { checkpoint: cp },
    } = state
        && let Some(r) = release
        && r.checkpoint_generation == cp.generation
    {
        need.disk_bytes = need.disk_bytes.saturating_sub(r.bytes.min(checkpoint));
    }
    need
}

/// How long a host must have been gone (disconnected) before an orb that never started
/// there is placed anew elsewhere: well beyond an agent's restart or upgrade.
const HOST_GONE: Duration = Duration::from_secs(30 * 60);
/// How long the hosts that refused an Amp orb before it started are avoided for it.
const REFUSALS_KEPT: Duration = Duration::from_secs(24 * 3600);
/// At most this many new placements of one Amp orb that never started per
/// [`REFUSALS_KEPT`]; after that it stays where it is.
const MAX_REPLACEMENTS: usize = 4;
/// How many refusals in a row for want of room by its agent move an orb that never
/// started, while the coordinator finds room for it there.
const CAPACITY_REFUSALS: u32 = 3;
/// How long the rows of an incarnation retired for a new placement are kept once
/// nothing is left to do for it.
const RETIRED_KEPT: Duration = Duration::from_secs(7 * 24 * 3600);

/// How long after a delete of a retired orb its old host is told again, while it has
/// not deleted it; doubling with each try.
const CLEANUP_EVERY: Duration = Duration::from_secs(600);
/// How many deletes a retired orb's old host is sent at most (the last one about
/// 21 hours after the first that went unanswered).
const CLEANUP_ATTEMPTS: u32 = 8;

/// Whether a retired orb's old host is due another delete: the first at once, then
/// [`CLEANUP_EVERY`] after the last, doubling, at most [`CLEANUP_ATTEMPTS`] in all.
fn cleanup_due(attempts: u32, last_attempt_ms: Option<u64>, now_ms: u64) -> bool {
    if attempts >= CLEANUP_ATTEMPTS {
        return false;
    }
    let Some(last) = last_attempt_ms else {
        return true;
    };
    let doublings = attempts.saturating_sub(1).min(16);
    let wait = (CLEANUP_EVERY.as_millis() as u64).saturating_mul(1 << doublings);
    now_ms >= last.saturating_add(wait)
}

/// Whether `a` and `b` are the same host: the same agent, or the same external backend
/// and scope (one capacity pool; its allocations differ per orb).
fn same_host(a: &Location, b: &Location) -> bool {
    match (a, b) {
        (
            Location::Fleet {
                backend_id: ba,
                agent_id: aa,
            },
            Location::Fleet {
                backend_id: bb,
                agent_id: ab,
            },
        ) => ba == bb && aa == ab,
        (
            Location::External {
                backend_id: ba,
                scope: sa,
                ..
            },
            Location::External {
                backend_id: bb,
                scope: sb,
                ..
            },
        ) => ba == bb && sa == sb,
        _ => false,
    }
}

/// How long ago an orb may have gone to sleep or stopped and still have its wake room
/// kept on its host ([`choose`]): orbs asleep longer rarely wake, and keeping room for
/// them would keep new orbs off their hosts for as long as they are kept.
pub(super) const WAKE_ROOM_RECENT: Duration = Duration::from_secs(24 * 3600);

fn plus(a: Resources, b: Resources) -> Resources {
    Resources {
        vcpus: a.vcpus.saturating_add(b.vcpus),
        memory_bytes: a.memory_bytes.saturating_add(b.memory_bytes),
        disk_bytes: a.disk_bytes.saturating_add(b.disk_bytes),
    }
}

/// A host that fits a new orb, as [`choose`] ranks it.
#[derive(Clone, Debug)]
pub(super) struct Fit {
    pub cand: PlacementCandidate,
    /// [`room_score`]; for a host that keeps its wake room, with that room counted as
    /// taken, so hosts are ranked by what they have left beyond their own orbs' wakes.
    pub score: u64,
    /// Whether the host still fits the largest wake of the orbs recently asleep or
    /// stopped on it (the room `wake_room` gives) once it takes the new orb.
    pub leaves_wake_room: bool,
    /// The host this orb is placed away from: it refused the orb (see
    /// `Core::replace_unstarted`).
    pub avoided: bool,
}

/// Chooses among the hosts that fit a new orb (`fitting`, not empty), narrowing tier by
/// tier, each only when some host passes it:
///
/// 1. An always-on host over an overflow one (not always on), as an orb stays where it
///    was created.
/// 2. Not a host to avoid (one that refused this orb): only among hosts of the same
///    kind, so a refusal never moves an orb onto an overflow host.
/// 3. One that still leaves room to wake its own orbs: they cannot wake anywhere else,
///    while a new orb can go anywhere. Ahead of the project snapshot: missing it costs a
///    new orb a slower first boot; a host filled up for it leaves its sleeping orbs
///    unable to wake.
/// 4. One holding the project snapshot the orb names (`wants_snapshot`).
/// 5. The most room ([`Fit::score`]); ties by location.
///
/// Returns the host, and whether tier 3 passed over a host holding the snapshot for
/// one without it (a miss counted as `wake_room`).
pub(super) fn choose(mut fitting: Vec<Fit>, wants_snapshot: bool) -> (PlacementCandidate, bool) {
    if fitting.iter().any(|f| !f.cand.overflow) {
        fitting.retain(|f| !f.cand.overflow);
    }
    if fitting.iter().any(|f| !f.avoided) {
        fitting.retain(|f| !f.avoided);
    }
    let mut passed_over_holder = false;
    if fitting.iter().any(|f| f.leaves_wake_room) {
        passed_over_holder = wants_snapshot
            && fitting
                .iter()
                .any(|f| !f.leaves_wake_room && f.cand.has_project_snapshot);
        fitting.retain(|f| f.leaves_wake_room);
    }
    // v1: snapshots are host-local; a new orb elsewhere starts from the base image.
    if wants_snapshot && fitting.iter().any(|f| f.cand.has_project_snapshot) {
        fitting.retain(|f| f.cand.has_project_snapshot);
    }
    let chosen = fitting
        .into_iter()
        .max_by(|a, b| {
            a.score.cmp(&b.score).then_with(|| {
                let (ja, jb) = (js(&a.cand.location), js(&b.cand.location));
                jb.cmp(&ja)
            })
        })
        .expect("non-empty");
    let kept_room = passed_over_holder && !chosen.cand.has_project_snapshot;
    (chosen.cand, kept_room)
}

/// Every resource `cand` lacks for `demand` after the reservations in `used`, with the
/// numbers. Memory must also fit what the host really has available, less its reserve:
/// a host's capacity may overcommit memory, and its agent rechecks this at admission. Disk is judged by what the host reports, not by orb disk sizes (sparse): the
/// filesystem's free space less its reserve, and the agent's disk limit less what orbd
/// has allocated, must each still hold the headroom of every running orb plus this one.
pub(super) fn shortfalls(
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
    if let Some(m) = cand.memory {
        let room = m.available_bytes.saturating_sub(m.reserve_bytes);
        if demand.memory_bytes > room && demand.memory_bytes <= free_mem {
            out.push((
                FailureCode::InsufficientMemory,
                format!(
                    "memory: needs {}, {} usable on the host ({} available, {} reserve)",
                    gib(demand.memory_bytes),
                    gib(room),
                    gib(m.available_bytes),
                    gib(m.reserve_bytes)
                ),
            ));
        }
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

/// Why no host can take a new orb of `spec`, given its runtime's hosts (`cands`), none
/// of them both available and able to run it: the hosts that could run it but are not
/// available, then what the others lack (once each), or that there are none at all.
fn no_host_message(spec: &SpecTemplate, cands: &[PlacementCandidate]) -> String {
    let mut reasons: Vec<String> = Vec::new();
    if cands.is_empty()
        && let ExecutionTarget::Fleet { runtime } = &spec.target
    {
        reasons.push(match runtime {
            RuntimeKind::Docker => "no agent runs docker orbs (agents run them only with \
                ORBD_VM_ONLY=false)"
                .into(),
            r => format!("no agent runs {} orbs", r.as_str()),
        });
    }
    let mut lacking: Vec<CapabilityMismatch> = Vec::new();
    for c in cands {
        let m = c.capabilities.check(spec);
        if m.is_empty() {
            match &c.availability {
                Availability::Unavailable { reason } => {
                    reasons.push(format!("{}: {reason}", c.capacity_pool))
                }
                Availability::Draining => reasons.push(format!("{}: draining", c.capacity_pool)),
                Availability::Available => {}
            }
        }
        for x in m {
            if !lacking.contains(&x) {
                lacking.push(x);
            }
        }
    }
    // A host with nested KVM that is not available is named above instead.
    let nests = cands
        .iter()
        .any(|c| c.capabilities.nested_kvm == Support::Supported);
    let (mut want, mut have) = (None, Vec::new());
    for m in &lacking {
        match m {
            CapabilityMismatch::NestedKvm if nests => {}
            CapabilityMismatch::NestedKvm => reasons.push(
                "its type has kvm, which needs ORBD_NESTED_KVM=true on a host whose KVM nests"
                    .into(),
            ),
            CapabilityMismatch::Arch(w, h) => {
                want = Some(*w);
                if !have.contains(h) {
                    have.push(*h);
                }
            }
            other => reasons.push(other.to_string()),
        }
    }
    if let Some(w) = want {
        reasons.push(format!("{} hosts cannot run {w} orbs", have.join(" and ")));
    }
    let mut message = "no available host can run this offering".to_owned();
    if !reasons.is_empty() {
        message = format!("{message} ({})", reasons.join("; "));
    }
    message
}

/// Why an orb that never started should move at once for how its agent refused it, if
/// it should: for a reason of that host's own the coordinator cannot see (its image not
/// allowed or no runtime for it there), or for want of room [`CAPACITY_REFUSALS`] times
/// in a row (once may pass: room admitted lately frees up within minutes). Never when
/// asked to wait and retry there (an image being prepared, a deadline, a cancel).
fn refused_for_good(u: &store::Unstarted) -> Option<String> {
    match u.last_failure {
        Some(FailureCode::UnsupportedCapability) => {
            Some("its agent cannot run it (UnsupportedCapability)".into())
        }
        Some(code) if u.capacity_refusals >= CAPACITY_REFUSALS => Some(format!(
            "its agent refused it for want of room {} times ({code:?})",
            u.capacity_refusals
        )),
        _ => None,
    }
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

/// A restricted orb's policy as its orb type (the offering it records, wherever it was
/// placed or spilled) has it now, which its agent applies from the orb's next launch on.
/// None for an unrestricted orb, and for one whose orb type is gone (renamed) or no
/// longer has `egress`: its agent then keeps the policy it last applied.
fn current_egress(
    offerings: &HashMap<OfferingId, Offering>,
    orb: &OrbRecord,
) -> Option<crate::model::EgressPolicy> {
    orb.spec.template.requirements.egress.as_ref()?;
    offerings
        .get(&orb.offering_id)?
        .spec
        .requirements
        .egress
        .clone()
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

/// Where a new orb that names a project snapshot (`ORB_SNAPSHOT_ID`) was placed, as far
/// as the snapshot goes. A host the coordinator believes holds it may still start the
/// orb from its image (it deleted the snapshot, and the coordinator has not heard yet,
/// or passes it over for another lineage): the agent counts those in
/// `orbd_project_snapshot_fallbacks_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SnapshotPlacement {
    /// On a host that holds it.
    Hit,
    /// On a host of a backend without project snapshots (e.g. Boat).
    Unsupported,
    /// Elsewhere: the hosts holding it could not take the orb (full, unavailable,
    /// overflow, or unable to run its offering).
    Elsewhere,
    /// Elsewhere: a host holding it could take the orb, but only by using the room it
    /// keeps to wake its own orbs ([`choose`]).
    WakeRoom,
    /// No host of the backends tried holds it.
    Absent,
}

impl SnapshotPlacement {
    /// `kept_room`: a host holding it was passed over to keep its wake room.
    fn of(chosen_holds: bool, chosen_supports: bool, held: bool, kept_room: bool) -> Self {
        match (chosen_holds, chosen_supports, held, kept_room) {
            (true, ..) => Self::Hit,
            (false, false, ..) => Self::Unsupported,
            (false, true, _, true) => Self::WakeRoom,
            (false, true, true, false) => Self::Elsewhere,
            (false, true, false, false) => Self::Absent,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Unsupported => "unsupported",
            Self::Elsewhere => "elsewhere",
            Self::WakeRoom => "wake_room",
            Self::Absent => "absent",
        }
    }
}

/// A new orb's [`SnapshotPlacement`], reported once its command is committed.
#[derive(Debug)]
struct SnapshotReport {
    placement: SnapshotPlacement,
    /// The start of the snapshot's (storage) key.
    snapshot: String,
    host: String,
}

impl SnapshotReport {
    /// Counts the placement, and logs a miss a host could have avoided.
    fn report(&self, amp_orb_id: &impl std::fmt::Display) {
        let result = self.placement.label();
        metrics::counter!("orbd_project_snapshot_placements_total", "result" => result)
            .increment(1);
        if matches!(
            self.placement,
            SnapshotPlacement::Elsewhere | SnapshotPlacement::WakeRoom | SnapshotPlacement::Absent
        ) {
            tracing::info!(
                %amp_orb_id,
                snapshot = %self.snapshot,
                result,
                host = %self.host,
                "project snapshot miss: the new orb starts from its image"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AgentId, BackendId, DiskReport, MemoryReport, RuntimeKind};

    /// A placement on a host holding the snapshot is a hit; on a backend without
    /// snapshots it is unsupported; elsewhere it is a miss, told apart by whether any
    /// host held it.
    #[test]
    fn snapshot_placements_say_why_they_missed() {
        use SnapshotPlacement as P;
        assert_eq!(P::of(true, true, true, false), P::Hit);
        assert_eq!(P::of(true, true, true, true), P::Hit);
        assert_eq!(P::of(false, false, true, false), P::Unsupported);
        assert_eq!(P::of(false, false, false, false), P::Unsupported);
        assert_eq!(P::of(false, true, true, false), P::Elsewhere);
        assert_eq!(P::of(false, true, true, true), P::WakeRoom);
        assert_eq!(P::of(false, true, false, false), P::Absent);
        let all = [P::Hit, P::Unsupported, P::Elsewhere, P::WakeRoom, P::Absent];
        let labels: Vec<_> = all.map(P::label).into();
        assert_eq!(
            labels,
            ["hit", "unsupported", "elsewhere", "wake_room", "absent"]
        );
    }

    fn policy() -> Policy {
        Policy {
            running_orb_disk_headroom: 10 * GIB,
            idle_orb_ttl: None,
        }
    }

    /// A 100 GiB-disk, 32 GiB orb that keeps its memory when it sleeps.
    fn big_spec() -> SpecTemplate {
        SpecTemplate {
            target: ExecutionTarget::Fleet {
                runtime: RuntimeKind::Firecracker,
            },
            resources: Resources {
                vcpus: 8,
                memory_bytes: 32 * GIB,
                disk_bytes: 100 * GIB,
            },
            requirements: CapabilityRequirements {
                release_compute_when_paused: true,
                nested_kvm: NestedKvmRequirement::NotRequired,
                project_snapshots: SnapshotRequirement::NotRequired,
                arch: Arch::Amd64,
                egress: None,
            },
            pause_policy: PausePolicy::MemoryPreserving {
                on_failure: CheckpointFailurePolicy::ColdStop,
            },
        }
    }

    fn sleeping() -> OrbState {
        OrbState::known(StableOrbState::Sleeping {
            checkpoint: SleepCheckpointRef { generation: 9 },
        })
    }

    fn release(generation: u64, gib: u64) -> Option<crate::backend::WakeRelease> {
        Some(crate::backend::WakeRelease {
            checkpoint_generation: generation,
            bytes: gib * GIB,
        })
    }

    /// A wake is credited only what its host reports the wake frees, for the checkpoint
    /// generation it sleeps on, up to its checkpoint headroom: a sleep held in RAM (its
    /// memory size, committed by its agent) or a checkpoint a restore reads into memory
    /// (what it measured, sparse). Nothing reported (a Firecracker checkpoint on disk,
    /// which the restored VMM keeps mapped until its next sleep is written; or one not
    /// measured yet), a report for another generation, or any other state: the whole
    /// demand.
    #[test]
    fn a_wake_is_credited_only_what_its_host_reports_it_frees() {
        let (p, spec) = (policy(), big_spec());
        let full = demand(&p, &spec);
        assert_eq!(
            full.disk_bytes,
            42 * GIB,
            "10 GiB growth + 32 GiB checkpoint"
        );
        let wake = |state: &OrbState, r| wake_demand(&p, &spec, state, r);
        // Held in RAM (32 GiB committed), or a fully written QEMU checkpoint.
        assert_eq!(wake(&sleeping(), release(9, 32)).disk_bytes, 10 * GIB);
        // A sparse QEMU checkpoint frees only what it takes.
        assert_eq!(wake(&sleeping(), release(9, 6)).disk_bytes, 36 * GIB);
        assert_eq!(wake(&sleeping(), release(9, 64)).disk_bytes, 10 * GIB);
        // Nothing reported: a Firecracker checkpoint on disk, or not measured yet.
        assert_eq!(wake(&sleeping(), None), full);
        // A report of an earlier sleep's checkpoint.
        assert_eq!(wake(&sleeping(), release(8, 32)), full);
        // Memory and vCPUs are what they are.
        assert_eq!(
            wake(&sleeping(), release(9, 32)).memory_bytes,
            full.memory_bytes
        );
        assert_eq!(wake(&sleeping(), release(9, 32)).vcpus, full.vcpus);
        for state in [
            OrbState::known(StableOrbState::DiskOnlyStopped {
                reason: "checkpoint dropped".into(),
            }),
            OrbState::known(StableOrbState::Uncreated),
            OrbState::known(StableOrbState::Running),
            OrbState::Faulted {
                last_known: StableOrbState::Sleeping {
                    checkpoint: SleepCheckpointRef { generation: 9 },
                },
                reason: "inspect failed".into(),
            },
        ] {
            assert_eq!(wake(&state, release(9, 32)), full, "{state:?}");
        }
        let mut disk_only = big_spec();
        disk_only.pause_policy = PausePolicy::DiskOnly;
        assert_eq!(
            wake_demand(&p, &disk_only, &sleeping(), release(9, 32)).disk_bytes,
            10 * GIB
        );
    }

    /// An orb that never started moves at once when its agent cannot run it there, or
    /// refused it for want of room [`CAPACITY_REFUSALS`] times in a row; not after one
    /// such refusal (room admitted lately frees up), nor when asked to wait and retry
    /// (its image being prepared there), nor for anything else.
    #[test]
    fn unstarted_orbs_move_at_once_only_for_lasting_refusals() {
        use FailureCode as F;
        let u = |last: F, capacity_refusals: u32| store::Unstarted {
            last_failure: Some(last),
            capacity_refusals,
        };
        assert!(refused_for_good(&u(F::UnsupportedCapability, 0)).is_some());
        for code in [F::InsufficientMemory, F::InsufficientDisk, F::QuotaExceeded] {
            assert!(refused_for_good(&u(code, 1)).is_none(), "{code:?} once");
            assert!(refused_for_good(&u(code, CAPACITY_REFUSALS - 1)).is_none());
            assert!(
                refused_for_good(&u(code, CAPACITY_REFUSALS)).is_some(),
                "{code:?}"
            );
        }
        for code in [
            F::ImageNotReady,
            F::DeadlineExceeded,
            F::Cancelled,
            F::BackendFailure,
            F::BackendUnavailable,
            F::GuestBootstrapFailed,
            F::SecretUnavailable,
            F::StaleGeneration,
            F::InvalidRequest,
        ] {
            assert!(refused_for_good(&u(code, 0)).is_none(), "{code:?}");
        }
        assert!(refused_for_good(&store::Unstarted::default()).is_none());
    }

    /// A retired orb's old host gets a delete at once, then again only after a wait
    /// that doubles from [`CLEANUP_EVERY`], and no more than [`CLEANUP_ATTEMPTS`] in all.
    #[test]
    fn cleanup_deletes_back_off_and_stop() {
        let every = CLEANUP_EVERY.as_millis() as u64;
        assert!(cleanup_due(0, None, 0));
        assert!(!cleanup_due(1, Some(1_000), 1_000 + every - 1));
        assert!(cleanup_due(1, Some(1_000), 1_000 + every));
        assert!(!cleanup_due(2, Some(1_000), 1_000 + 2 * every - 1));
        assert!(cleanup_due(2, Some(1_000), 1_000 + 2 * every));
        assert!(cleanup_due(3, Some(0), 4 * every));
        assert!(!cleanup_due(CLEANUP_ATTEMPTS, Some(0), u64::MAX));
    }

    /// The same host is the same agent, or the same external backend and scope, whatever
    /// allocation each orb there got.
    #[test]
    fn same_host_ignores_allocations() {
        let agent = |id: &str| Location::Fleet {
            backend_id: BackendId::new("fleet"),
            agent_id: AgentId::new(id.to_owned()),
        };
        let boat = |scope: &str| Location::External {
            backend_id: BackendId::new("boat"),
            scope: BackendScopeId::new(scope),
            allocation_id: AllocationId::generate(),
        };
        assert!(same_host(&agent("a"), &agent("a")));
        assert!(!same_host(&agent("a"), &agent("b")));
        assert!(
            same_host(&boat("s"), &boat("s")),
            "fresh allocations, same pool"
        );
        assert!(!same_host(&boat("s"), &boat("t")));
        assert!(!same_host(&agent("a"), &boat("s")));
    }

    /// Which runtimes free a checkpoint at the wake that restores it, and so may report
    /// it as freed: QEMU reads it into the guest's memory; Firecracker maps it for the
    /// life of the restored VM.
    #[test]
    fn only_runtimes_that_read_checkpoints_into_memory_free_them_at_the_wake() {
        use crate::runtime::Runtime;
        let dir = tempfile::tempdir().unwrap();
        let fake = |keeps: bool| {
            crate::runtime::fake::FakeRuntime::new(
                dir.path().to_path_buf(),
                &crate::config::agent::FakeRuntimeConfig {
                    restore_keeps_checkpoint: keeps,
                    ..Default::default()
                },
            )
        };
        const {
            assert!(crate::runtime::qemu::QemuRuntime::RESTORE_FREES_CHECKPOINT);
            assert!(!crate::runtime::firecracker::FirecrackerRuntime::RESTORE_FREES_CHECKPOINT);
        }
        assert!(fake(false).restore_frees_checkpoint(), "like QEMU");
        assert!(!fake(true).restore_frees_checkpoint(), "like Firecracker");
    }

    fn host(
        id: &str,
        free_gib: u64,
        held: Resources,
        running: u32,
    ) -> (PlacementCandidate, store::PoolUse) {
        let cand = PlacementCandidate {
            location: Location::Fleet {
                backend_id: BackendId::new("fleet"),
                agent_id: AgentId::new(id.to_owned()),
            },
            availability: Availability::Available,
            capabilities: crate::backend::fleet::capabilities::ceiling(RuntimeKind::Firecracker),
            capacity_pool: id.into(),
            capacity_ceiling: Resources {
                vcpus: 32,
                memory_bytes: 64 * GIB,
                disk_bytes: 1000 * GIB,
            },
            demand: Resources::default(),
            disk: Some(DiskReport {
                free_bytes: free_gib * GIB,
                total_bytes: 1000 * GIB,
                allocated_bytes: 600 * GIB,
                reserve_bytes: 50 * GIB,
            }),
            memory: None,
            has_project_snapshot: false,
            overflow: false,
        };
        let used = store::PoolUse {
            held,
            running,
            orbs: running,
        };
        (cand, used)
    }

    /// A host with 150 GiB free, a 50 GiB reserve and five running orbs holding 14 GiB
    /// each has 30 GiB usable: a 32 GiB orb asleep there, held in RAM, was refused its
    /// 42 GiB of headroom, though its wake frees as much as its next sleep takes; net of
    /// that it needs 10.
    #[test]
    fn a_sleeping_orb_wakes_where_only_its_growth_fits() {
        let (p, spec) = (policy(), big_spec());
        let held = Resources {
            vcpus: 10,
            memory_bytes: 20 * GIB,
            disk_bytes: 70 * GIB,
        };
        let (cand, used) = host("h", 150, held, 5);
        let short = shortfalls(&cand, &used, demand(&p, &spec));
        assert_eq!(short.len(), 1, "{short:?}");
        assert_eq!(short[0].0, FailureCode::InsufficientDisk);
        assert!(
            short[0]
                .1
                .contains("needs 42.0 GiB headroom, 30.0 GiB usable"),
            "{}",
            short[0].1
        );
        let need = wake_demand(&p, &spec, &sleeping(), release(9, 32));
        assert!(shortfalls(&cand, &used, need).is_empty());
        // Unreported (a Firecracker checkpoint on disk), it needs room for the next one.
        let need = wake_demand(&p, &spec, &sleeping(), None);
        assert!(!shortfalls(&cand, &used, need).is_empty());
    }

    fn fit(id: &str, score: u64, leaves_wake_room: bool, holds: bool, overflow: bool) -> Fit {
        let (mut cand, _) = host(id, 500, Resources::default(), 0);
        cand.has_project_snapshot = holds;
        cand.overflow = overflow;
        Fit {
            cand,
            score,
            leaves_wake_room,
            avoided: false,
        }
    }

    fn pool(c: &PlacementCandidate) -> &str {
        c.capacity_pool.as_str()
    }

    /// Hosts that fit a new orb are narrowed by tier: always-on before overflow, then
    /// those that leave room to wake their own orbs, then those holding the project
    /// snapshot, then the most room. Each tier applies only when a host passes it: no
    /// host leaving wake room still places the orb. Passing over a host holding the
    /// snapshot to keep its wake room is reported.
    #[test]
    fn new_orbs_leave_room_for_wakes_before_seeking_their_snapshot() {
        // The snapshot's host would fill up: the orb goes elsewhere, a wake_room miss.
        let (c, kept) = choose(
            vec![
                fit("holder", 900, false, true, false),
                fit("other", 100, true, false, false),
            ],
            true,
        );
        assert_eq!((pool(&c), kept), ("other", true));
        // Both keep their wake room: the snapshot decides, then room.
        let (c, kept) = choose(
            vec![
                fit("holder", 100, true, true, false),
                fit("other", 900, true, false, false),
            ],
            true,
        );
        assert_eq!((pool(&c), kept), ("holder", false));
        // No host keeps its wake room: placed anyway, by snapshot then room.
        let (c, kept) = choose(
            vec![
                fit("holder", 100, false, true, false),
                fit("other", 900, false, false, false),
            ],
            true,
        );
        assert_eq!((pool(&c), kept), ("holder", false));
        let (c, _) = choose(
            vec![
                fit("a", 100, false, false, false),
                fit("b", 900, false, false, false),
            ],
            false,
        );
        assert_eq!(pool(&c), "b");
        // Without a snapshot, wake room then room decide; nothing is reported.
        let (c, kept) = choose(
            vec![
                fit("roomy", 900, false, false, false),
                fit("keeps", 100, true, false, false),
            ],
            false,
        );
        assert_eq!((pool(&c), kept), ("keeps", false));
        // An overflow host is used only when no always-on host fits, wake room or not.
        let (c, _) = choose(
            vec![
                fit("spare", 900, true, true, true),
                fit("main", 100, false, false, false),
            ],
            true,
        );
        assert_eq!(pool(&c), "main");
        // A host passed over for wake room that does not hold the snapshot is no miss.
        let (c, kept) = choose(
            vec![
                fit("full", 900, false, false, false),
                fit("holder", 100, true, true, false),
            ],
            true,
        );
        assert_eq!((pool(&c), kept), ("holder", false));
        // A host that refused the orb is avoided ahead of wake room, snapshot and room,
        // among hosts of its kind: never for an overflow host, and kept when no other
        // fits (the caller then leaves the orb where it is).
        let avoid = |mut f: Fit| {
            f.avoided = true;
            f
        };
        let (c, _) = choose(
            vec![
                avoid(fit("refused", 900, true, true, false)),
                fit("other", 100, false, false, false),
            ],
            true,
        );
        assert_eq!(pool(&c), "other");
        let (c, _) = choose(
            vec![
                avoid(fit("refused", 900, true, true, false)),
                fit("spare", 900, true, false, true),
            ],
            true,
        );
        assert_eq!(pool(&c), "refused", "not onto an overflow host");
        let (c, _) = choose(vec![avoid(fit("refused", 900, true, true, false))], true);
        assert_eq!(pool(&c), "refused");
    }

    /// Among fitting hosts the one with more room wins, room being the scarcer of free
    /// memory and usable disk, each as a fraction of the host's: a host with more free
    /// memory but a filling disk loses to one whose disk has room.
    #[test]
    fn placement_prefers_the_host_with_more_room_in_its_scarcer_resource() {
        let gib = crate::model::GIB;
        let score = |mem_used_gib: u64, disk_free_gib: u64, disk_total_gib: u64| {
            let cand = PlacementCandidate {
                location: Location::Fleet {
                    backend_id: BackendId::new("fleet"),
                    agent_id: AgentId::new("h".to_owned()),
                },
                availability: Availability::Available,
                capabilities: crate::backend::fleet::capabilities::ceiling(
                    RuntimeKind::Firecracker,
                ),
                capacity_pool: "h".into(),
                capacity_ceiling: Resources {
                    vcpus: 32,
                    memory_bytes: 54 * gib,
                    disk_bytes: 888 * gib,
                },
                demand: Resources::default(),
                disk: Some(DiskReport {
                    free_bytes: disk_free_gib * gib,
                    total_bytes: disk_total_gib * gib,
                    allocated_bytes: 0,
                    reserve_bytes: 44 * gib,
                }),
                memory: None,
                has_project_snapshot: false,
                overflow: false,
            };
            let used = store::PoolUse {
                held: Resources {
                    vcpus: 0,
                    memory_bytes: mem_used_gib * gib,
                    disk_bytes: 0,
                },
                running: 1,
                orbs: 1,
            };
            room_score(&cand, &used)
        };
        let host = |mem_used_gib: u64, disk_free_gib: u64| score(mem_used_gib, disk_free_gib, 888);
        // More free memory on the second host, but its disk is filling up.
        let (one, two) = (host(40, 329), host(20, 214));
        assert!(one > two, "{one} vs {two}");
        // With disks alike, memory decides.
        assert!(host(20, 329) > host(40, 329));
        // A host that has not reported its disk total is judged by memory alone.
        assert_eq!(score(20, 0, 0), score(20, 888, 888));
        // Disk the scarcer resource: it decides even against much more free memory.
        assert!(score(40, 600, 888) > score(10, 120, 888));
    }

    /// A new orb no host can take says why: hosts that could run it but are not
    /// available, by name; what the others lack, once each; nested KVM's switch only when
    /// no host has it at all; and a runtime no agent offers.
    #[test]
    fn no_host_message_says_why() {
        let spec = |kvm: bool, arch: Arch| SpecTemplate {
            target: ExecutionTarget::Fleet {
                runtime: RuntimeKind::Qemu,
            },
            resources: Resources::default(),
            requirements: CapabilityRequirements {
                release_compute_when_paused: true,
                nested_kvm: if kvm {
                    NestedKvmRequirement::GuestDevice
                } else {
                    NestedKvmRequirement::NotRequired
                },
                project_snapshots: SnapshotRequirement::NotRequired,
                arch,
                egress: None,
            },
            pause_policy: PausePolicy::DiskOnly,
        };
        let host = |id: &str, availability: Availability, nests: bool, arch: Arch| {
            let mut capabilities = crate::backend::fleet::capabilities::ceiling(RuntimeKind::Qemu);
            capabilities.nested_kvm = if nests {
                Support::Supported
            } else {
                Support::Unsupported
            };
            capabilities.arch = Some(arch);
            PlacementCandidate {
                location: Location::Fleet {
                    backend_id: BackendId::new("fleet"),
                    agent_id: AgentId::new(id.to_owned()),
                },
                availability,
                capabilities,
                capacity_pool: id.into(),
                capacity_ceiling: Resources::default(),
                demand: Resources::default(),
                disk: None,
                memory: None,
                has_project_snapshot: false,
                overflow: false,
            }
        };
        let kvm = spec(true, Arch::Amd64);
        let plain = host("a", Availability::Available, false, Arch::Amd64);
        assert_eq!(
            no_host_message(&kvm, std::slice::from_ref(&plain)),
            "no available host can run this offering (its type has kvm, which needs \
             ORBD_NESTED_KVM=true on a host whose KVM nests)"
        );
        // A host with nested KVM that is draining or gone is named instead.
        for (availability, why) in [
            (Availability::Draining, "draining"),
            (
                Availability::Unavailable {
                    reason: "agent has not connected".into(),
                },
                "agent has not connected",
            ),
        ] {
            let nesting = host("b", availability, true, Arch::Amd64);
            assert_eq!(
                no_host_message(&kvm, &[plain.clone(), nesting]),
                format!("no available host can run this offering (b: {why})")
            );
        }
        let mac = host("m", Availability::Available, false, Arch::Arm64);
        assert_eq!(
            no_host_message(&spec(false, Arch::Amd64), &[mac.clone(), mac]),
            "no available host can run this offering (arm64 hosts cannot run amd64 orbs)"
        );
        assert_eq!(
            no_host_message(&kvm, &[]),
            "no available host can run this offering (no agent runs qemu orbs)"
        );
        let mut docker = spec(false, Arch::Amd64);
        docker.target = ExecutionTarget::Fleet {
            runtime: RuntimeKind::Docker,
        };
        assert!(no_host_message(&docker, &[]).contains("ORBD_VM_ONLY=false"));
    }

    /// The current policy of a restricted orb's type goes with its commands: an edited
    /// one, the type's own for an orb spilled to another place, none for a type that
    /// lost `egress` or is gone (the agent keeps what it applied last) and none for an
    /// unrestricted orb, whatever its type has now.
    #[test]
    fn current_egress_follows_the_orb_type() {
        let yaml = |egress: &str| {
            format!(
                "config_version: v2\npublic_url: https://c.example:7443\n\
                 provider: {{ token_file: /t/p }}\n\
                 orb_types:\n  - {{ id: t, vcpus: 2, memory_mib: 2048, disk_gib: 4, \
                 arch: [amd64, arm64]{egress} }}\n  - {{ id: u, vcpus: 2, memory_mib: 2048, disk_gib: 4 }}\n"
            )
        };
        let offerings = |egress: &str| -> HashMap<OfferingId, Offering> {
            let mut c: crate::config::coordinator::CoordinatorConfig =
                serde_saphyr::from_str(&yaml(egress)).unwrap();
            c.normalize().unwrap();
            c.validate().unwrap();
            c.offerings
                .iter()
                .map(|o| (o.id.clone(), o.to_offering().unwrap()))
                .collect()
        };
        let created = offerings(", egress: { domains: [a.example] }");
        let orb = |offering: &str, spec_from: &str| {
            let template = created[&OfferingId::new(spec_from)].spec.clone();
            let spec = OrbSpec {
                template,
                initial_disk: InitialDiskSource::Image {
                    image_ref: "i".into(),
                },
            };
            OrbRecord {
                id: OrbId::generate(),
                identity: OrbIdentity {
                    amp_endpoint: AmpEndpointId::new("amp"),
                    amp_orb_id: AmpOrbId::new("amp-1"),
                },
                offering_id: OfferingId::new(offering),
                spec_digest: spec.digest(),
                spec,
                location: Location::Fleet {
                    backend_id: BackendId::new("fleet"),
                    agent_id: AgentId::new("a".to_owned()),
                },
                last_issued_generation: 1,
                observation: VersionedObservation {
                    state: OrbState::known(StableOrbState::Running),
                    generation: 1,
                    observed_at_unix_ms: 0,
                },
            }
        };
        let domains = |p: Option<crate::model::EgressPolicy>| p.map(|p| p.parts().0);
        let restricted = orb("t", "t");
        // Spilled to t@arm64: the orb records its type, t.
        let spilled = orb("t", "t@arm64");
        let edited = offerings(", egress: { domains: [a.example, b.example] }");
        for o in [&restricted, &spilled] {
            assert_eq!(
                domains(current_egress(&created, o)),
                Some(vec!["a.example".to_owned()])
            );
            assert_eq!(
                domains(current_egress(&edited, o)),
                Some(vec!["a.example".to_owned(), "b.example".to_owned()])
            );
            assert_eq!(current_egress(&offerings(""), o), None, "egress removed");
        }
        let mut renamed = restricted.clone();
        renamed.offering_id = OfferingId::new("gone");
        assert_eq!(current_egress(&edited, &renamed), None);
        // An unrestricted orb of a type that gained egress stays unrestricted.
        let plain = orb("t", "u");
        assert_eq!(current_egress(&edited, &plain), None);
    }

    #[test]
    fn memory_fits_overcommitted_capacity_only_while_the_host_has_it() {
        let cand = |available_gib: u64| PlacementCandidate {
            location: Location::Fleet {
                backend_id: BackendId::new("fleet"),
                agent_id: AgentId::new("n100".to_owned()),
            },
            availability: Availability::Available,
            capabilities: crate::backend::fleet::capabilities::ceiling(RuntimeKind::Qemu),
            capacity_pool: "n100".into(),
            capacity_ceiling: Resources {
                vcpus: 80,
                memory_bytes: 88 * GIB,
                disk_bytes: 1000 * GIB,
            },
            demand: Resources::default(),
            disk: Some(DiskReport {
                free_bytes: 900 * GIB,
                total_bytes: 1800 * GIB,
                allocated_bytes: 100 * GIB,
                reserve_bytes: 90 * GIB,
            }),
            memory: Some(MemoryReport {
                available_bytes: available_gib * GIB,
                reserve_bytes: 8 * GIB,
            }),
            has_project_snapshot: false,
            overflow: false,
        };
        // Three running orbs reserve 34 GiB and 17 vCPUs nominally, as on 2026-10-30.
        let used = store::PoolUse {
            held: Resources {
                vcpus: 17,
                memory_bytes: 34 * GIB,
                disk_bytes: 30 * GIB,
            },
            running: 3,
            orbs: 18,
        };
        let demand = Resources {
            vcpus: 8,
            memory_bytes: 16 * GIB,
            disk_bytes: 10 * GIB,
        };
        // 34 GiB available - 8 GiB reserve = 26 GiB: a 16 GiB orb fits.
        assert!(shortfalls(&cand(34), &used, demand).is_empty());
        // 23 GiB - 8 GiB = 15 GiB: it does not, although capacity has room.
        let short = shortfalls(&cand(23), &used, demand);
        assert_eq!(short.len(), 1, "{short:?}");
        assert_eq!(short[0].0, FailureCode::InsufficientMemory);
        assert!(short[0].1.contains("usable on the host"), "{}", short[0].1);
    }
}
