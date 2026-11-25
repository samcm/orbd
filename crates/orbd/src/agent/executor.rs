//! Per-orb executors. Accepted operations belong to the executor, not the connection.
//!
//! One actor task per orb pulls operation ids from a queue and runs them strictly one
//! at a time. Every step inspects the runtime first and records a durable intent
//! (`Progress`) before acting, so after an agent restart the same code re-runs an
//! unfinished operation and does not repeat an effect that already happened:
//!
//! * Spawn decides from `inspect`: Absent → new disk (only if the orb was never created
//!   or this very operation recorded the create intent), Stopped with checkpoint →
//!   memory restore, Stopped without → cold boot, Running → keep the start kind this
//!   operation recorded (it started the orb before the crash) or AlreadyRunning.
//! * Pause records its intent (checkpoint generation / cold stop) before acting; an
//!   already-stopped orb is reported from `inspect` plus that intent.
//! * Delete is state-based and idempotent.
//!
//! Between operations, a liveness check on the same actor notices a VMM that ended on
//! its own and reports the orb stopped (see [`watch`]).
//!
//! Deadlines are pinned at acceptance as unix ms; here they become `Instant`s. Past the
//! work deadline only cleanup runs, until the cleanup deadline.

use super::db::{self, PausePlan, Progress, Receipt, StartPlan};
use super::{Shared, session};
use crate::model::{
    Action, ActionResult, CheckpointFailurePolicy, CommandFailure, CommandOutcome, ExecutionTarget,
    FailureCode, InitialDiskSource, OperationId, OrbDefinition, OrbId, OrbState, PausePolicy,
    ProcessDisposition, ProjectSnapshotKey, SnapshotDisposition, StableOrbState, StartKind,
    unix_ms_now,
};
use crate::runtime::{
    BootstrapRequest, GuestError, LogStream, OpContext, Runtime, RuntimeError, RuntimeObservation,
    StartSource,
};
use crate::secrets::PermittedGuestEnv;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// How long a newly admitted orb's memory counts against the host on top of what the
/// host reports: a cold boot or a restore takes a while to show up as used.
const RECENT_ADMIT: Duration = Duration::from_secs(120);
/// How often orbs holding a reservation are checked for a VMM that ended on its own.
const LIVENESS_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) fn runtime_for(sh: &Shared, orb: &OrbDefinition) -> Option<Arc<dyn Runtime>> {
    match &orb.spec.template.target {
        ExecutionTarget::Fleet { runtime } => sh.runtimes.get(runtime).cloned(),
        ExecutionTarget::Provider { .. } => None,
    }
}

fn instant_at(unix_ms: u64) -> Instant {
    let now_ms = unix_ms_now();
    let now = Instant::now();
    if unix_ms >= now_ms {
        now + Duration::from_millis(unix_ms - now_ms)
    } else {
        now.checked_sub(Duration::from_millis(now_ms - unix_ms))
            .unwrap_or(now)
    }
}

/// One unit of work for an orb's actor.
pub(crate) enum Work {
    Op(OperationId),
    /// Agent maintenance (drain checkpoint, restore after restart), serialized with the
    /// orb's operations so it never races a spawn, pause or delete.
    Maintenance(
        Box<OrbDefinition>,
        super::maintenance::Task,
        tokio::sync::oneshot::Sender<super::maintenance::Outcome>,
    ),
    /// Liveness check of an orb holding a reservation (see [`watch`]).
    Check(Box<OrbDefinition>),
}

/// Startup: rebuild reservations from what is actually running, requeue every
/// accepted-but-not-terminal receipt in acceptance order, then restore orbs the last
/// drain checkpointed (unless their own operations or later commands supersede that).
pub(crate) async fn recover(sh: &Arc<Shared>) -> anyhow::Result<()> {
    let defs = sh.db.call(|c| db::list_orb_definitions(c)).await?;
    // Orbs whose type has kvm, which an agent with nested KVM off (the default since
    // 0.4) never starts or wakes: say how many there are.
    let kvm: Vec<OrbDefinition> = match sh.config.agent.refuse_nested_kvm {
        true => defs
            .iter()
            .filter(|d| crate::runtime::needs_nested_kvm(d))
            .cloned()
            .collect(),
        false => Vec::new(),
    };
    if !kvm.is_empty() {
        let live = sh
            .db
            .call(move |c| {
                let mut live = std::collections::HashSet::new();
                for d in &kvm {
                    if db::get_orb(c, d.id)?.is_some_and(|r| !r.is_deleted()) {
                        live.insert(d.id);
                    }
                }
                Ok(live)
            })
            .await?;
        let n = super::session::kvm_orbs_held(&sh.config.agent, &defs, |d| live.contains(&d.id));
        if n > 0 {
            tracing::warn!(
                orbs = n,
                "{n} orb(s) here have a type with kvm, which this agent won't start or wake \
                 (nested KVM is off by default since 0.4): run it with ORBD_NESTED_KVM=true \
                 to run them, or delete them"
            );
        }
    }
    for d in defs {
        let Some(rt) = runtime_for(sh, &d) else {
            // Running without the runtime would leave the orb's VMM unrecovered.
            let id = d.id;
            let row = sh.db.call(move |c| db::get_orb(c, id)).await?;
            if let ExecutionTarget::Fleet { runtime } = &d.spec.template.target
                && row.is_some_and(|r| !r.is_deleted())
            {
                anyhow::bail!(
                    "orb {id} runs on {}, which this agent does not enable; enable that runtime \
                     again (or delete the orb from an agent that has it)",
                    runtime.as_str()
                );
            }
            continue;
        };
        let observed = rt.inspect(d.id).await;
        // A VMM that ended while the agent was down (host reboot, container stop
        // without a drain) took its processes with it: record that, not "running".
        record_stopped(
            sh,
            d.id,
            &observed,
            "the VM stopped while the agent was down; processes lost",
            true,
        )
        .await?;
        record_lost_sleep(sh, d.id, &observed).await?;
        // An orb we cannot inspect is assumed to hold its resources.
        let running = !matches!(
            observed,
            Ok(RuntimeObservation::Absent | RuntimeObservation::Stopped { .. })
        );
        if running {
            sh.reserved
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(d.id, d.spec.template.resources);
        }
    }
    let pending = sh.db.call(|c| db::list_unfinished(c)).await?;
    tracing::info!(count = pending.len(), "requeueing unfinished operations");
    let busy: std::collections::HashSet<OrbId> = pending.iter().map(|r| r.orb.id).collect();
    for r in pending {
        sh.token_for(r.operation_id);
        enqueue(sh, r.orb.id, r.operation_id);
    }
    super::maintenance::restore_all(sh, &busy).await
}

/// At startup: an orb recorded asleep on a checkpoint the host no longer has (a sleep
/// held in RAM whose VMM went with the agent, without a drain) is recorded as the host
/// has it, so it is reported and handled as such.
async fn record_lost_sleep(
    sh: &Shared,
    id: OrbId,
    observed: &Result<RuntimeObservation, RuntimeError>,
) -> anyhow::Result<()> {
    let Ok(RuntimeObservation::Stopped { checkpoint }) = observed else {
        return Ok(());
    };
    let Some(row) = sh.db.call(move |c| db::get_orb(c, id)).await? else {
        return Ok(());
    };
    let Some(OrbState::Known {
        state: StableOrbState::Sleeping {
            checkpoint: recorded,
        },
    }) = &row.last_state
    else {
        return Ok(());
    };
    if checkpoint.as_ref() == Some(recorded) {
        return Ok(());
    }
    let state = OrbState::known(match checkpoint {
        Some(cp) => StableOrbState::Sleeping {
            checkpoint: cp.clone(),
        },
        None => StableOrbState::DiskOnlyStopped {
            reason: "its memory checkpoint is gone (asleep in RAM when the agent stopped \
                     without a drain); processes lost"
                .into(),
        },
    });
    tracing::warn!(orb = %id, now = ?state, "orb was recorded asleep on a checkpoint the host no longer has");
    metrics::counter!("orbd_agent_held_flushes_total", "reason" => "lost").increment(1);
    sh.db
        .call(move |c| db::set_last_state(c, id, &state))
        .await?;
    Ok(())
}

/// Records that an orb last known running was observed stopped, its VMM gone without a
/// command. Returns the orb as recorded, or None if it was not recorded as running or
/// was not observed stopped. `faulted_too` also takes an orb faulted while running (at
/// startup); the liveness sweep takes only a plain running orb, as the coordinator does.
async fn record_stopped(
    sh: &Shared,
    id: OrbId,
    observed: &Result<RuntimeObservation, RuntimeError>,
    reason: &str,
    faulted_too: bool,
) -> anyhow::Result<Option<db::OrbRow>> {
    let Ok(RuntimeObservation::Stopped { checkpoint }) = observed else {
        return Ok(None);
    };
    let Some(mut row) = sh.db.call(move |c| db::get_orb(c, id)).await? else {
        return Ok(None);
    };
    let running = match &row.last_state {
        Some(OrbState::Known {
            state: StableOrbState::Running,
        }) => true,
        Some(s @ OrbState::Faulted { .. }) => {
            faulted_too && matches!(s.last_known(), StableOrbState::Running)
        }
        _ => false,
    };
    if !running {
        return Ok(None);
    }
    let state = OrbState::known(match checkpoint {
        Some(cp) => StableOrbState::Sleeping {
            checkpoint: cp.clone(),
        },
        None => StableOrbState::DiskOnlyStopped {
            reason: reason.into(),
        },
    });
    tracing::warn!(orb = %id, state = ?state, "orb is no longer running");
    let st = state.clone();
    sh.db.call(move |c| db::set_last_state(c, id, &st)).await?;
    row.last_state = Some(state);
    Ok(Some(row))
}

/// While the agent runs, checks every [`LIVENESS_INTERVAL`] that orbs holding a
/// reservation still have their VMM, so one that dies on its own (killed, crashed) is
/// reported stopped and its compute freed within seconds. The check runs on the orb's
/// actor, never alongside its operations; an orb with an operation pending is left to
/// that operation, which reports what it finds.
pub(crate) async fn watch(sh: Arc<Shared>) {
    loop {
        tokio::select! {
            _ = sh.shutdown.cancelled() => return,
            _ = sh.draining.cancelled() => return,
            _ = tokio::time::sleep(LIVENESS_INTERVAL) => {}
        }
        // Orbs whose VMM process plainly still runs need nothing more; only the rest
        // (gone, or a runtime that cannot tell) cost a DB read and an `inspect`.
        let held: std::collections::HashSet<OrbId> = sh
            .reserved
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .copied()
            .filter(|id| !sh.runtimes.values().any(|r| r.vmm_alive(*id) == Some(true)))
            .collect();
        if held.is_empty() {
            continue;
        }
        let found = sh
            .db
            .call(|c| Ok((db::list_orb_definitions(c)?, db::list_unfinished(c)?)))
            .await;
        let (defs, pending) = match found {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "liveness check: cannot list orbs");
                continue;
            }
        };
        let busy: std::collections::HashSet<OrbId> = pending.iter().map(|r| r.orb.id).collect();
        for d in defs {
            if held.contains(&d.id)
                && !busy.contains(&d.id)
                && sh
                    .liveness_queued
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(d.id)
            {
                enqueue_work(&sh, d.id, Work::Check(Box::new(d)));
            }
        }
    }
}

/// Reports an orb whose VMM has ended on its own as stopped (see [`watch`]).
async fn check_alive(sh: &Arc<Shared>, orb: &OrbDefinition) -> anyhow::Result<()> {
    let Some(rt) = runtime_for(sh, orb) else {
        return Ok(());
    };
    let observed = rt.inspect(orb.id).await;
    let Some(row) = record_stopped(
        sh,
        orb.id,
        &observed,
        "the VM stopped on its own; processes lost",
        false,
    )
    .await?
    else {
        return Ok(());
    };
    sh.reserved
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&orb.id);
    sh.send_control(session::orb_report_event(&row));
    Ok(())
}

pub(crate) fn enqueue(sh: &Arc<Shared>, orb: OrbId, op: OperationId) {
    enqueue_work(sh, orb, Work::Op(op));
}

pub(crate) fn enqueue_work(sh: &Arc<Shared>, orb: OrbId, work: Work) {
    // A command goes ahead of an archive of the orb's disk (see `archive`).
    if matches!(work, Work::Op(_))
        && let Some(t) = sh
            .archiving
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&orb)
    {
        t.cancel();
    }
    let tx = {
        let mut actors = sh.actors.lock().unwrap_or_else(|e| e.into_inner());
        actors
            .entry(orb)
            .or_insert_with(|| {
                let (tx, rx) = mpsc::unbounded_channel();
                sh.tasks.spawn(actor(sh.clone(), rx));
                tx
            })
            .clone()
    };
    let _ = tx.send(work);
}

async fn actor(sh: Arc<Shared>, mut rx: mpsc::UnboundedReceiver<Work>) {
    loop {
        let work = tokio::select! {
            _ = sh.shutdown.cancelled() => return,
            w = rx.recv() => match w { Some(w) => w, None => return },
        };
        let op = match work {
            // Draining: queued operations stay accepted and run after the restart.
            Work::Op(_) if sh.draining.is_cancelled() => continue,
            Work::Op(op) => op,
            Work::Maintenance(orb, task, done) => {
                let _ = done.send(super::maintenance::run(&sh, &orb, task).await);
                continue;
            }
            Work::Check(orb) => {
                // Cleared however the check ends (a panic included), so the orb is
                // checked again on a later sweep.
                struct Unqueue<'a>(&'a Shared, OrbId);
                impl Drop for Unqueue<'_> {
                    fn drop(&mut self) {
                        self.0
                            .liveness_queued
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&self.1);
                    }
                }
                let _unqueue = Unqueue(&sh, orb.id);
                if !sh.draining.is_cancelled()
                    && let Err(e) = check_alive(&sh, &orb).await
                {
                    tracing::warn!(orb = %orb.id, error = %format!("{e:#}"), "liveness check failed");
                }
                continue;
            }
        };
        metrics::gauge!("orbd_agent_running_executors").increment(1.0);
        let r = run_op(&sh, op).await;
        metrics::gauge!("orbd_agent_running_executors").decrement(1.0);
        if let Err(e) = r {
            tracing::error!(operation_id = %op, error = %e, "operation left unfinished; it is retried after restart");
        }
    }
}

// --- results ----------------------------------------------------------------

struct Finished {
    result: Result<ActionResult, CommandFailure>,
    processes: ProcessDisposition,
    snapshot: SnapshotDisposition,
    /// Observed state if the action already knows it; otherwise inspected afterwards.
    observed: Option<OrbState>,
}

impl Finished {
    fn ok(result: ActionResult, processes: ProcessDisposition) -> Self {
        Self {
            result: Ok(result),
            processes,
            snapshot: SnapshotDisposition::NotRequested,
            observed: None,
        }
    }

    fn fail(code: FailureCode, message: impl Into<String>, processes: ProcessDisposition) -> Self {
        Self {
            result: Err(CommandFailure {
                code,
                message: message.into(),
            }),
            processes,
            snapshot: SnapshotDisposition::NotRequested,
            observed: None,
        }
    }
}

enum Flow {
    /// Cancelled or past the work deadline.
    Interrupted,
    Failed(CommandFailure),
    /// The agent itself broke (database); the operation stays accepted.
    Fatal(anyhow::Error),
}

impl From<RuntimeError> for Flow {
    fn from(e: RuntimeError) -> Self {
        use FailureCode as F;
        let (code, msg) = match &e {
            RuntimeError::Cancelled | RuntimeError::DeadlineExceeded => return Flow::Interrupted,
            // Both mean: retry the spawn shortly.
            RuntimeError::ImageNotReady(_) | RuntimeError::DiskNotReady(_) => {
                (F::ImageNotReady, e.to_string())
            }
            RuntimeError::CheckpointIncompatible(_) => (F::CheckpointIncompatible, e.to_string()),
            RuntimeError::CheckpointFailed(_) => (F::CheckpointFailed, e.to_string()),
            RuntimeError::InsufficientDisk(_) => (F::InsufficientDisk, e.to_string()),
            RuntimeError::Unsupported(_) => (F::UnsupportedCapability, e.to_string()),
            RuntimeError::NotFound(_) | RuntimeError::Other(_) => {
                (F::BackendFailure, e.to_string())
            }
        };
        Flow::Failed(CommandFailure { code, message: msg })
    }
}

impl From<anyhow::Error> for Flow {
    fn from(e: anyhow::Error) -> Self {
        Flow::Fatal(e)
    }
}

// --- operation runner -------------------------------------------------------

struct Run {
    sh: Arc<Shared>,
    rt: Arc<dyn Runtime>,
    receipt: Receipt,
    ctx: OpContext,
    progress: Progress,
}

async fn run_op(sh: &Arc<Shared>, op_id: OperationId) -> anyhow::Result<()> {
    let Some(receipt) = sh.db.call(move |c| db::get_receipt(c, op_id)).await? else {
        return Ok(());
    };
    if receipt.terminal {
        return Ok(());
    }
    let action_name = receipt.command.action.name();
    let token = sh.token_for(op_id);
    let work_dl = instant_at(receipt.work_deadline_unix_ms);
    let cleanup_dl = instant_at(
        receipt
            .cleanup_deadline_unix_ms
            .max(receipt.work_deadline_unix_ms),
    );

    let Some(rt) = runtime_for(sh, &receipt.orb) else {
        let fin = Finished::fail(
            FailureCode::UnsupportedCapability,
            "this agent has no runtime for the orb's execution target",
            ProcessDisposition::Unknown,
        );
        let observed = OrbState::Faulted {
            last_known: StableOrbState::Uncreated,
            reason: "no runtime".into(),
        };
        return finalize(sh, &receipt, fin, observed, action_name).await;
    };

    let work_cancel = CancellationToken::new();
    let ctx = OpContext {
        operation_id: op_id,
        generation: receipt.command.generation,
        work_deadline: work_dl,
        cleanup_deadline: cleanup_dl,
        cancel: work_cancel.clone(),
        log: super::logs::sink(sh.logs.clone(), op_id),
    };
    let watcher = tokio::spawn({
        let (sh, token, wc) = (sh.clone(), token.clone(), work_cancel.clone());
        async move {
            tokio::select! {
                _ = token.cancelled() => {}
                _ = sh.shutdown.cancelled() => {}
                _ = sh.draining.cancelled() => {}
                _ = tokio::time::sleep_until(work_dl.into()) => {}
            }
            wc.cancel();
        }
    });

    let mut run = Run {
        sh: sh.clone(),
        rt,
        progress: receipt.progress.clone(),
        receipt: receipt.clone(),
        ctx,
    };
    run.log(&format!("{action_name}: starting"));

    let already_late = token.is_cancelled()
        || sh.shutdown.is_cancelled()
        || sh.draining.is_cancelled()
        || Instant::now() >= work_dl;
    let res: Option<Result<Finished, Flow>> = if already_late {
        Some(Err(Flow::Interrupted))
    } else {
        // Runtimes must honour ctx.cancel; if one hangs, give it until the cleanup
        // deadline and then abandon the call.
        let hard_stop = async {
            work_cancel.cancelled().await;
            tokio::time::sleep_until(cleanup_dl.into()).await;
        };
        tokio::select! {
            r = run.action() => Some(r),
            _ = hard_stop => None,
        }
    };
    watcher.abort();

    let (fin, observed) = match res {
        Some(Ok(fin)) => {
            let observed = match fin.observed.clone() {
                Some(o) => o,
                None => run.observe().await,
            };
            (fin, observed)
        }
        Some(Err(Flow::Failed(f))) => {
            let fin = Finished {
                result: Err(f),
                processes: ProcessDisposition::Unknown,
                snapshot: run.unpublished("operation failed"),
                observed: None,
            };
            let observed = run.observe().await;
            (fin, observed)
        }
        Some(Err(Flow::Fatal(e))) => return Err(e),
        Some(Err(Flow::Interrupted)) | None => {
            if sh.shutdown.is_cancelled() || sh.draining.is_cancelled() {
                return Ok(()); // stays accepted; resumed after restart
            }
            let (code, why) = if token.is_cancelled() {
                (FailureCode::Cancelled, "cancelled")
            } else {
                (FailureCode::DeadlineExceeded, "work deadline exceeded")
            };
            let observed = if res.is_none() {
                run.faulted("runtime call did not return by the cleanup deadline")
                    .await
            } else {
                run.cleanup().await;
                run.observe().await
            };
            let fin = Finished {
                result: Err(CommandFailure {
                    code,
                    message: why.into(),
                }),
                processes: ProcessDisposition::Unknown,
                snapshot: run.unpublished(why),
                observed: None,
            };
            (fin, observed)
        }
    };
    finalize(sh, &receipt, fin, observed, action_name).await
}

/// Commits the terminal receipt, then announces it.
async fn finalize(
    sh: &Arc<Shared>,
    receipt: &Receipt,
    fin: Finished,
    observed: OrbState,
    action_name: &'static str,
) -> anyhow::Result<()> {
    let op = receipt.operation_id;
    let orb = receipt.orb.id;
    let outcome = CommandOutcome {
        result: fin.result,
        observed,
        processes: fin.processes,
        snapshot: fin.snapshot,
    };
    {
        let mut r = sh.reserved.lock().unwrap_or_else(|e| e.into_inner());
        if matches!(outcome.observed.last_known(), StableOrbState::Running) {
            r.insert(orb, receipt.orb.spec.template.resources);
        } else {
            r.remove(&orb);
            // Admitted but not running: it takes none of the host's memory.
            sh.recent_admits
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .retain(|(_, _, o)| *o != orb);
        }
    }
    let o2 = outcome.clone();
    sh.db
        .call(move |c| db::complete(c, op, orb, &o2, unix_ms_now()))
        .await?;
    crate::crash_point!("agent::terminal_committed");
    super::refresh_wake_release(sh, &receipt.orb, &outcome.observed).await;
    let result_label = match &outcome.result {
        Ok(_) => "ok".to_owned(),
        Err(f) => format!("{:?}", f.code).to_lowercase(),
    };
    metrics::counter!("orbd_agent_operations_total", "action" => action_name, "result" => result_label)
        .increment(1);
    tracing::info!(operation_id = %op, action = action_name, ok = outcome.result.is_ok(), "operation terminal");
    sh.send_control(session::completed_event(&receipt.command.key(), &outcome));
    sh.tokens
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&op);
    sh.logs.forget(op);
    Ok(())
}

impl Run {
    fn log(&self, msg: &str) {
        let _ = self
            .ctx
            .log
            .try_send((LogStream::Stderr, format!("{msg}\n").into()));
    }

    fn orb(&self) -> &OrbDefinition {
        &self.receipt.orb
    }

    fn check(&self) -> Result<(), Flow> {
        if self.ctx.cancel.is_cancelled() {
            Err(Flow::Interrupted)
        } else {
            Ok(())
        }
    }

    async fn row(&self) -> Result<db::OrbRow, Flow> {
        let id = self.orb().id;
        self.sh
            .db
            .call(move |c| db::get_orb(c, id))
            .await?
            .ok_or_else(|| Flow::Fatal(anyhow::anyhow!("orb row missing")))
    }

    async fn save(&self) -> Result<(), Flow> {
        let (op, p) = (self.receipt.operation_id, self.progress.clone());
        self.sh
            .db
            .call(move |c| db::save_progress(c, op, &p))
            .await?;
        Ok(())
    }

    fn unpublished(&self, why: &str) -> SnapshotDisposition {
        match &self.receipt.command.action {
            Action::Pause {
                publish_snapshot: Some(_),
            } => SnapshotDisposition::NotPublished { reason: why.into() },
            _ => SnapshotDisposition::NotRequested,
        }
    }

    async fn last_known(&self) -> StableOrbState {
        self.row()
            .await
            .ok()
            .and_then(|r| r.last_state)
            .map(|s| s.last_known().clone())
            .unwrap_or(StableOrbState::Uncreated)
    }

    async fn faulted(&self, reason: &str) -> OrbState {
        OrbState::Faulted {
            last_known: self.last_known().await,
            reason: reason.into(),
        }
    }

    /// The observed state, from what the runtime sees on the host now.
    async fn observe(&self) -> OrbState {
        let row = self.row().await.ok();
        match self.rt.inspect(self.orb().id).await {
            Ok(RuntimeObservation::Running) => OrbState::known(StableOrbState::Running),
            Ok(RuntimeObservation::Stopped {
                checkpoint: Some(checkpoint),
            }) => OrbState::known(StableOrbState::Sleeping { checkpoint }),
            Ok(RuntimeObservation::Stopped { checkpoint: None }) => {
                OrbState::known(StableOrbState::DiskOnlyStopped {
                    reason: "stopped without a memory checkpoint".into(),
                })
            }
            Ok(RuntimeObservation::Absent) => match &row {
                Some(r) if r.is_deleted() => OrbState::known(StableOrbState::Deleted),
                Some(r) if r.created => self.faulted("disk is missing").await,
                _ => OrbState::known(StableOrbState::Uncreated),
            },
            Ok(RuntimeObservation::Inconsistent { reason }) => self.faulted(&reason).await,
            Err(e) => self.faulted(&format!("inspect failed: {e}")).await,
        }
    }

    /// Best-effort cleanup after cancellation/deadline, bounded by the cleanup deadline.
    async fn cleanup(&self) {
        let Action::Pause { .. } = self.receipt.command.action else {
            return;
        };
        let PausePolicy::MemoryPreserving {
            on_failure: CheckpointFailurePolicy::ColdStop,
        } = self.orb().spec.template.pause_policy
        else {
            return;
        };
        let cctx = OpContext {
            cancel: CancellationToken::new(),
            work_deadline: self.ctx.cleanup_deadline,
            ..self.ctx.clone()
        };
        let fut = async {
            if let Ok(RuntimeObservation::Running) = self.rt.inspect(self.orb().id).await {
                self.log("pause interrupted: cold-stopping the orb (processes are lost)");
                if let Err(e) = self.rt.cold_stop(self.orb(), &cctx).await {
                    tracing::warn!(error = %e, "cleanup cold stop failed");
                }
            }
        };
        let _ = tokio::time::timeout_at(self.ctx.cleanup_deadline.into(), fut).await;
    }

    async fn action(&mut self) -> Result<Finished, Flow> {
        match self.receipt.command.action.clone() {
            Action::Spawn => self.spawn().await,
            Action::Pause { publish_snapshot } => self.pause(publish_snapshot).await,
            Action::Delete => self.delete().await,
        }
    }

    fn fault_result(&self, last_known: StableOrbState, reason: &str) -> Finished {
        let mut f = Finished::fail(
            FailureCode::BackendFailure,
            format!("orb is faulted: {reason}"),
            ProcessDisposition::Unknown,
        );
        f.observed = Some(OrbState::Faulted {
            last_known,
            reason: reason.into(),
        });
        f
    }

    /// Local admission: running orbs' vCPUs and memory must never exceed capacity, and
    /// nothing starts on a filesystem below its reserve. The coordinator admits disk by
    /// headroom; orb disk sizes are sparse and are not summed here.
    ///
    /// Memory is also admitted by what the host really has: capacity may overcommit
    /// (guests rarely touch all of theirs, and free-page reporting hands freed pages
    /// back), so an orb starts only if the host's available memory, less the reserve and
    /// less what orbs admitted in the last [`RECENT_ADMIT`] may still take, holds it.
    fn admit(&self) -> Result<(), Flow> {
        self.admit_as(false)
    }

    /// [`admit`](Self::admit) for an orb whose memory is in RAM already (a sleep held
    /// in RAM): the host's available memory does not need to hold it again.
    fn admit_as(&self, resident: bool) -> Result<(), Flow> {
        // Feature `failpoints`: `agent::admit=return` refuses for want of memory, as a
        // host short of it right now would.
        fail::fail_point!("agent::admit", |_| Err(Flow::Failed(CommandFailure {
            code: FailureCode::InsufficientMemory,
            message: "agent capacity exceeded (memory: injected)".into(),
        })));
        let need = self.orb().spec.template.resources;
        let mut reserved = self.sh.reserved.lock().unwrap_or_else(|e| e.into_inner());
        if reserved.contains_key(&self.orb().id) {
            return Ok(());
        }
        let used = reserved
            .values()
            .fold(crate::model::Resources::default(), |a, r| {
                a.checked_add(*r).unwrap_or(a)
            });
        let total = used.checked_add(need);
        let cap = self.sh.capacity;
        // Committed writes (held sleeps' checkpoints, restores, room for one) are not
        // free.
        let free = self.sh.disk_room();
        let reserve = self.sh.disk_reserve;
        let mem_short = match resident {
            true => None,
            false => self.memory_short(need.memory_bytes),
        };
        // Sleeps held in RAM (other than this orb's) count against capacity; not for a
        // wake from RAM, which takes no memory the host does not already show as used
        // (the next admission writes holds if it must).
        let held = match resident {
            true => 0,
            false => self.sh.held_resident_except(self.orb().id),
        };
        let (code, what) = match total {
            None => (FailureCode::InsufficientMemory, "resources overflow".into()),
            Some(t)
                if t.memory_bytes.saturating_add(held) > cap.memory_bytes
                    || t.vcpus > cap.vcpus =>
            {
                (FailureCode::InsufficientMemory, "memory/cpu".into())
            }
            Some(_) if mem_short.is_some() => (
                FailureCode::InsufficientMemory,
                mem_short.unwrap_or_default(),
            ),
            Some(_) if free < reserve => (
                FailureCode::InsufficientDisk,
                format!(
                    "disk: {} MiB free, below the {} MiB reserve",
                    free / crate::model::MIB,
                    reserve / crate::model::MIB
                ),
            ),
            Some(_) => {
                reserved.insert(self.orb().id, need);
                // A wake from RAM uses memory the host already shows as used.
                if !resident {
                    self.sh
                        .recent_admits
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push((Instant::now(), need.memory_bytes, self.orb().id));
                }
                return Ok(());
            }
        };
        Err(Flow::Failed(CommandFailure {
            code,
            message: format!("agent capacity exceeded ({what})"),
        }))
    }

    /// Why the host cannot hold `need` bytes more right now, or None if it can.
    fn memory_short(&self, need: u64) -> Option<String> {
        self.memory_short_freed(need, 0)
    }

    /// [`memory_short`](Self::memory_short) were `freed` bytes more available.
    fn memory_short_freed(&self, need: u64, freed: u64) -> Option<String> {
        let available = self.sh.memory_available()?.saturating_add(freed);
        let recent: u64 = {
            let mut r = self
                .sh
                .recent_admits
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            r.retain(|(t, _, _)| t.elapsed() < RECENT_ADMIT);
            r.iter().map(|(_, b, _)| b).sum()
        };
        let room = available
            .saturating_sub(self.sh.memory_reserve)
            .saturating_sub(recent);
        let gib = |b: u64| format!("{:.1} GiB", b as f64 / crate::model::GIB as f64);
        (need > room).then(|| {
            format!(
                "memory: needs {}, {} usable on the host ({} available, {} reserve, {} for \
                 orbs just started)",
                gib(need),
                gib(room),
                gib(available),
                gib(self.sh.memory_reserve),
                gib(recent)
            )
        })
    }

    /// Writes sleeps held in RAM (oldest first) while the host lacks the memory this
    /// orb needs (see `hold::make_room`).
    async fn make_room(&self) -> Result<(), Flow> {
        let need = self.orb().spec.template.resources;
        let short = |freed: u64| {
            self.memory_short_freed(need.memory_bytes, freed).is_some()
                || self.capacity_short(need, freed)
        };
        if !short(0) {
            return Ok(());
        }
        let written = tokio::select! {
            biased;
            _ = self.ctx.cancel.cancelled() => return Err(Flow::Interrupted),
            _ = tokio::time::sleep_until(self.ctx.work_deadline.into()) => return Err(Flow::Interrupted),
            n = super::hold::make_room(&self.sh, self.orb().id, short) => n,
        };
        if written > 0 {
            self.log(&format!(
                "wrote {written} sleep(s) held in RAM to disk to make room"
            ));
        }
        Ok(())
    }

    /// Whether running orbs and sleeps held in RAM (less `freed` of them) leave no
    /// capacity for `need`.
    fn capacity_short(&self, need: crate::model::Resources, freed: u64) -> bool {
        let used = self.sh.reserved_total();
        let held = self
            .sh
            .held_resident_except(self.orb().id)
            .saturating_sub(freed);
        used.memory_bytes
            .saturating_add(need.memory_bytes)
            .saturating_add(held)
            > self.sh.capacity.memory_bytes
    }

    /// A spawn that would fail for its image anyway does not write held sleeps first.
    fn image_not_ready(&self) -> Result<(), Flow> {
        let (Some(store), InitialDiskSource::Image { image_ref }) =
            (&self.sh.images, &self.orb().spec.initial_disk)
        else {
            return Ok(());
        };
        match store.get(image_ref) {
            Err(crate::image::ImageError::NotReady(m)) => Err(Flow::Failed(CommandFailure {
                code: FailureCode::ImageNotReady,
                message: format!("image not ready: {m}"),
            })),
            _ => Ok(()),
        }
    }

    /// Whether this pause may hold the orb's sleep in RAM: holds are on, the agent is
    /// not stopping, and the host would still fit another orb of this size without
    /// writing one (so admissions rarely wait on a write).
    fn may_hold(&self) -> Option<super::hold::Reservation<'_>> {
        // A pause that publishes a project snapshot holds too: the disk is copied from
        // the paused VM (see `clone_disk`).
        if self.sh.config.agent.sleep_hold.is_none()
            || self.sh.draining.is_cancelled()
            || !self.rt.holds_sleeps()
        {
            return None;
        }
        super::hold::reserve(&self.sh, self.orb().spec.template.resources.memory_bytes)
    }

    // --- spawn --------------------------------------------------------------

    async fn spawn(&mut self) -> Result<Finished, Flow> {
        let orb_id = self.orb().id;
        let row = self.row().await?;
        if row.is_deleted() {
            let mut f = Finished::fail(
                FailureCode::OrbDeleted,
                "orb was deleted",
                ProcessDisposition::NotApplicable,
            );
            f.observed = Some(OrbState::known(StableOrbState::Deleted));
            return Ok(f);
        }
        let env = match self
            .sh
            .key
            .open(
                &self.receipt.sealed_env,
                self.receipt.operation_id.to_string().as_bytes(),
            )
            .ok()
            .and_then(|b| PermittedGuestEnv::from_json(&b).ok())
        {
            Some(e) => e,
            None => {
                return Ok(Finished::fail(
                    FailureCode::SecretUnavailable,
                    "sealed guest environment cannot be opened",
                    ProcessDisposition::NotApplicable,
                ));
            }
        };
        let last_known = row
            .last_state
            .as_ref()
            .map(|s| s.last_known().clone())
            .unwrap_or(StableOrbState::Uncreated);

        let planned = self.progress.start.clone();
        let mut resumed_in_place = false;
        let kind = match self.rt.inspect(orb_id).await? {
            RuntimeObservation::Running => {
                if let Some(plan) = planned {
                    // This operation started the orb before a restart.
                    if let (StartKind::MemoryRestore, Some(cp)) = (plan.kind, &plan.checkpoint) {
                        self.rt.discard_checkpoint(orb_id, cp).await?;
                    }
                    plan.kind
                } else {
                    StartKind::AlreadyRunning
                }
            }
            RuntimeObservation::Absent => {
                let planned_new = matches!(&planned, Some(p) if p.kind == StartKind::NewDisk);
                if row.created && !planned_new {
                    // A missing disk is a fault, never permission to recreate.
                    let reason = "disk is missing";
                    self.mark_fault(&last_known, reason);
                    return Ok(self.fault_result(last_known, reason));
                }
                // Before anything is resolved or pulled, whatever the coordinator sent.
                let (InitialDiskSource::Image { image_ref }
                | InitialDiskSource::ProjectSnapshot { image_ref, .. }) =
                    &self.orb().spec.initial_disk;
                if let Err(why) = self.sh.config.agent.image_allow.check(image_ref) {
                    tracing::warn!(orb_id = %orb_id, image_ref, "spawn refused: {why}");
                    if planned_new {
                        self.sh
                            .db
                            .call(move |c| db::clear_created(c, orb_id))
                            .await?;
                    }
                    return Ok(Finished::fail(
                        FailureCode::UnsupportedCapability,
                        why,
                        ProcessDisposition::NotApplicable,
                    ));
                }
                self.image_not_ready()?;
                self.make_room().await?;
                self.admit()?;
                self.check()?;
                if !planned_new {
                    self.progress.start = Some(StartPlan {
                        kind: StartKind::NewDisk,
                        checkpoint: None,
                    });
                    let (op, p) = (self.receipt.operation_id, self.progress.clone());
                    self.sh
                        .db
                        .call(move |c| db::begin_create(c, op, orb_id, &p))
                        .await?;
                }
                self.log("creating disk and booting");
                let mut result = self
                    .rt
                    .start(
                        self.orb(),
                        StartSource::NewDisk(self.orb().spec.initial_disk.clone()),
                        &self.ctx,
                    )
                    .await;
                // The coordinator placed this orb here for a project snapshot the host
                // no longer has: start from the spawn's image instead of failing forever.
                let mut fell_back = false;
                if let (
                    Err(RuntimeError::NotFound(why)),
                    InitialDiskSource::ProjectSnapshot { image_ref, .. },
                ) = (&result, &self.orb().spec.initial_disk)
                    && matches!(
                        self.rt.inspect(orb_id).await,
                        Ok(RuntimeObservation::Absent)
                    )
                {
                    tracing::warn!(orb_id = %orb_id, %why, image_ref, "project snapshot missing; creating the disk from the image");
                    fell_back = true;
                    self.log(&format!(
                        "{why}; creating the disk from {image_ref} instead"
                    ));
                    let src = StartSource::NewDisk(InitialDiskSource::Image {
                        image_ref: image_ref.clone(),
                    });
                    result = self.rt.start(self.orb(), src, &self.ctx).await;
                }
                if let Err(e) = result {
                    let absent = matches!(
                        self.rt.inspect(orb_id).await,
                        Ok(RuntimeObservation::Absent)
                    );
                    if absent {
                        self.sh
                            .db
                            .call(move |c| db::clear_created(c, orb_id))
                            .await?;
                    } else if fell_back {
                        // Its disk exists: a later start does not come this way again.
                        crate::runtime::vm::count_snapshot_fallback("missing");
                    }
                    return Err(e.into());
                }
                if fell_back {
                    crate::runtime::vm::count_snapshot_fallback("missing");
                }
                crate::crash_point!("agent::spawn::after_start");
                StartKind::NewDisk
            }
            RuntimeObservation::Stopped {
                checkpoint: Some(cp),
            } => {
                // Held in RAM: its memory is there already, and it resumes in place.
                let held = super::hold::is_held(&*self.rt, orb_id);
                resumed_in_place = held;
                if !held {
                    self.make_room().await?;
                }
                self.admit_as(held)?;
                self.progress.start = Some(StartPlan {
                    kind: StartKind::MemoryRestore,
                    checkpoint: Some(cp.clone()),
                });
                self.save().await?;
                self.check()?;
                self.log("restoring from memory checkpoint");
                self.rt
                    .start(
                        self.orb(),
                        StartSource::MemoryCheckpoint(cp.clone()),
                        &self.ctx,
                    )
                    .await?;
                crate::crash_point!("agent::spawn::after_start");
                self.rt.discard_checkpoint(orb_id, &cp).await?;
                StartKind::MemoryRestore
            }
            RuntimeObservation::Stopped { checkpoint: None } => {
                if let Some(a) = self.rt.archived(orb_id).filter(|a| !a.restoring) {
                    // A restore writes the disk back: it must fit above the reserve and
                    // what held sleeps' checkpoints take once written.
                    let (free, _) = self.sh.disk_stats();
                    let room = free
                        .saturating_sub(self.sh.disk_reserve)
                        .saturating_sub(self.sh.holds_committed());
                    if room < a.allocated_bytes {
                        return Err(Flow::Failed(CommandFailure {
                            code: FailureCode::InsufficientDisk,
                            message: format!(
                                "agent capacity exceeded (disk: restoring the orb's archived \
                                 disk needs {} MiB, {} MiB free above the reserve and held \
                                 sleeps)",
                                a.allocated_bytes / crate::model::MIB,
                                room / crate::model::MIB
                            ),
                        }));
                    }
                }
                self.make_room().await?;
                self.admit()?;
                self.progress.start = Some(StartPlan {
                    kind: StartKind::ExistingDiskColdBoot,
                    checkpoint: None,
                });
                self.save().await?;
                self.check()?;
                self.log("cold-booting existing disk");
                self.rt
                    .start(self.orb(), StartSource::ExistingDiskColdBoot, &self.ctx)
                    .await?;
                crate::crash_point!("agent::spawn::after_start");
                StartKind::ExistingDiskColdBoot
            }
            RuntimeObservation::Inconsistent { reason } => {
                self.mark_fault(&last_known, &reason);
                return Ok(self.fault_result(last_known, &reason));
            }
        };
        let from = match kind {
            StartKind::MemoryRestore if resumed_in_place => Some("ram"),
            StartKind::MemoryRestore => Some("checkpoint"),
            StartKind::ExistingDiskColdBoot => Some("disk"),
            _ => None,
        };
        if let Some(from) = from {
            metrics::counter!("orbd_agent_wakes_total", "from" => from).increment(1);
        }
        let (cold_boot, processes) = match kind {
            StartKind::NewDisk => (true, ProcessDisposition::NotApplicable),
            StartKind::ExistingDiskColdBoot => (true, ProcessDisposition::Lost),
            StartKind::MemoryRestore | StartKind::AlreadyRunning => {
                (false, ProcessDisposition::Preserved)
            }
        };
        self.progress.start.get_or_insert(StartPlan {
            kind,
            checkpoint: None,
        });

        if !self.progress.bootstrapped {
            self.check()?;
            self.log("bootstrapping guest");
            let generation = self.receipt.command.generation;
            let req = BootstrapRequest {
                generation,
                env,
                cold_boot,
            };
            match self.rt.guest().bootstrap(self.orb(), req, &self.ctx).await {
                Ok(()) => {}
                Err(GuestError::Cancelled | GuestError::DeadlineExceeded) => {
                    return Err(Flow::Interrupted);
                }
                Err(e) => {
                    return Ok(Finished::fail(
                        FailureCode::GuestBootstrapFailed,
                        e.to_string(),
                        processes,
                    ));
                }
            }
            crate::crash_point!("agent::spawn::after_bootstrap");
            self.progress.bootstrapped = true;
            self.save().await?;
        }
        let mut f = Finished::ok(
            ActionResult::Spawned {
                start: kind,
                bootstrap_generation: self.receipt.command.generation,
            },
            processes,
        );
        f.observed = Some(OrbState::known(StableOrbState::Running));
        Ok(f)
    }

    fn mark_fault(&self, last_known: &StableOrbState, reason: &str) {
        tracing::error!(orb_id = %self.orb().id, last_known = ?last_known, reason, "orb faulted");
    }

    // --- pause --------------------------------------------------------------

    async fn pause(&mut self, snapshot_key: Option<ProjectSnapshotKey>) -> Result<Finished, Flow> {
        let orb_id = self.orb().id;
        let row = self.row().await?;
        let last_known = row
            .last_state
            .as_ref()
            .map(|s| s.last_known().clone())
            .unwrap_or(StableOrbState::Uncreated);
        let mut fin = match self.rt.inspect(orb_id).await? {
            RuntimeObservation::Absent if row.created && !row.is_deleted() => {
                let reason = "disk is missing";
                self.mark_fault(&last_known, reason);
                return Ok(self.fault_result(last_known, reason));
            }
            RuntimeObservation::Absent => Finished::ok(
                ActionResult::Paused { checkpoint: None },
                ProcessDisposition::NotApplicable,
            ),
            RuntimeObservation::Inconsistent { reason } => {
                self.mark_fault(&last_known, &reason);
                return Ok(self.fault_result(last_known, &reason));
            }
            RuntimeObservation::Stopped { checkpoint } => {
                if let Some(reason) = self.progress.checkpoint_failed.clone() {
                    Finished::fail(
                        FailureCode::CheckpointFailed,
                        reason,
                        ProcessDisposition::Lost,
                    )
                } else {
                    let processes = if checkpoint.is_some() {
                        ProcessDisposition::Preserved
                    } else if self.progress.pause == Some(PausePlan::ColdStop) {
                        ProcessDisposition::Lost
                    } else {
                        ProcessDisposition::NotApplicable
                    };
                    Finished::ok(ActionResult::Paused { checkpoint }, processes)
                }
            }
            RuntimeObservation::Running => self.stop_running().await?,
        };

        if fin.result.is_ok()
            && let Some(key) = snapshot_key
        {
            self.check()?;
            // An orb asleep in RAM stays held: its disk is copied from the paused VM.
            let held = super::hold::is_held(&*self.rt, self.orb().id);
            self.log("publishing project snapshot");
            match self.rt.clone_disk(self.orb(), &key, &self.ctx).await {
                Ok(snapshot) => {
                    let k = snapshot.key.clone();
                    self.sh
                        .db
                        .call(move |c| db::record_project_snapshot(c, &k, unix_ms_now()))
                        .await?;
                    self.sh.snapshot_published(snapshot.key.as_str());
                    crate::crash_point!("agent::pause::after_snapshot");
                    fin.snapshot = SnapshotDisposition::Published { snapshot };
                }
                Err(RuntimeError::Cancelled | RuntimeError::DeadlineExceeded) => {
                    return Err(Flow::Interrupted);
                }
                Err(e) => {
                    fin.snapshot = SnapshotDisposition::NotPublished {
                        reason: e.to_string(),
                    };
                    fin.result = Err(CommandFailure {
                        code: FailureCode::SnapshotFailed,
                        message: e.to_string(),
                    });
                }
            }
            // A held VM that ended meanwhile took the orb's processes with it.
            if held
                && let RuntimeObservation::Stopped { checkpoint: None } =
                    self.rt.inspect(self.orb().id).await?
            {
                if fin.result.is_ok() {
                    fin.result = Ok(ActionResult::Paused { checkpoint: None });
                }
                fin.processes = ProcessDisposition::Lost;
            }
        }
        Ok(fin)
    }

    async fn stop_running(&mut self) -> Result<Finished, Flow> {
        let orb_id = self.orb().id;
        let op = self.receipt.operation_id;
        match self.orb().spec.template.pause_policy {
            PausePolicy::DiskOnly => {
                self.progress.pause = Some(PausePlan::ColdStop);
                self.save().await?;
                self.check()?;
                self.log("stopping orb (processes are lost)");
                self.rt.cold_stop(self.orb(), &self.ctx).await?;
                crate::crash_point!("agent::pause::after_stop");
                Ok(Finished::ok(
                    ActionResult::Paused { checkpoint: None },
                    ProcessDisposition::Lost,
                ))
            }
            PausePolicy::MemoryPreserving { on_failure } => {
                if self.progress.checkpoint_failed.is_none() {
                    let generation = match self.progress.pause {
                        Some(PausePlan::Checkpoint { generation }) => generation,
                        _ => {
                            let mut p = self.progress.clone();
                            let g = self
                                .sh
                                .db
                                .call(move |c| {
                                    db::allocate_checkpoint_generation(c, op, orb_id, &mut p)
                                        .map(|g| (g, p))
                                })
                                .await?;
                            self.progress = g.1;
                            g.0
                        }
                    };
                    self.check()?;
                    // Held until the pause is done: concurrent pauses count it.
                    let reservation = self.may_hold();
                    let hold = reservation.is_some();
                    self.log("going to sleep");
                    let started = Instant::now();
                    let saved = self
                        .rt
                        .checkpoint_and_stop(self.orb(), generation, hold, &self.ctx)
                        .await;
                    drop(reservation);
                    if saved.is_ok() && !super::hold::is_held(&*self.rt, orb_id) {
                        let memory = self.orb().spec.template.resources.memory_bytes;
                        super::hold::note_save(&self.sh, memory, started.elapsed());
                    }
                    match saved {
                        Ok(cp) => {
                            crate::crash_point!("agent::pause::after_stop");
                            return Ok(Finished::ok(
                                ActionResult::Paused {
                                    checkpoint: Some(cp),
                                },
                                ProcessDisposition::Preserved,
                            ));
                        }
                        Err(RuntimeError::Cancelled | RuntimeError::DeadlineExceeded) => {
                            return Err(Flow::Interrupted);
                        }
                        Err(e) => {
                            self.progress.checkpoint_failed = Some(e.to_string());
                            self.save().await?;
                        }
                    }
                }
                let reason = self.progress.checkpoint_failed.clone().unwrap_or_default();
                match on_failure {
                    CheckpointFailurePolicy::ColdStop => {
                        // Cleanup after a failed save must run even though the work
                        // budget may be nearly spent: it is bounded by the cleanup deadline.
                        let cctx = OpContext {
                            cancel: CancellationToken::new(),
                            work_deadline: self.ctx.cleanup_deadline,
                            ..self.ctx.clone()
                        };
                        self.log("checkpoint failed: cold-stopping the orb (processes are lost)");
                        self.rt.cold_stop(self.orb(), &cctx).await?;
                        crate::crash_point!("agent::pause::after_stop");
                        Ok(Finished::fail(
                            FailureCode::CheckpointFailed,
                            reason,
                            ProcessDisposition::Lost,
                        ))
                    }
                    CheckpointFailurePolicy::KeepRunning => Ok(Finished::fail(
                        FailureCode::CheckpointFailed,
                        reason,
                        ProcessDisposition::Preserved,
                    )),
                }
            }
        }
    }

    // --- delete -------------------------------------------------------------

    async fn delete(&mut self) -> Result<Finished, Flow> {
        let orb_id = self.orb().id;
        if let RuntimeObservation::Running = self.rt.inspect(orb_id).await? {
            self.check()?;
            self.log("stopping orb");
            self.rt.cold_stop(self.orb(), &self.ctx).await?;
            crate::crash_point!("agent::delete::after_stop");
        }
        self.check()?;
        self.log("deleting orb");
        self.rt.delete(orb_id, &self.ctx).await?;
        self.sh.orb_deleted.notify_one();
        crate::crash_point!("agent::delete::after_delete");
        let mut f = Finished::ok(ActionResult::Deleted, ProcessDisposition::NotApplicable);
        f.observed = Some(OrbState::known(StableOrbState::Deleted));
        Ok(f)
    }
}
