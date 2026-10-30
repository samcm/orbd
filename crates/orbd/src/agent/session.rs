//! AgentControl client: connect with backoff forever, Hello/Welcome, heartbeats, delivery
//! handling (durable decisions before replying), cancel, reconcile, ack and replay of
//! unacknowledged completions. Accepted operations never depend on this connection.

use super::db::{self, CancelDecision, Decision, NewReceipt, Receipt};
use super::{SessionHandle, Shared, executor};
use crate::model::wire;
use crate::model::{
    CommandKey, CommandOutcome, FailureCode, OperationId, OrbDefinition, OrbId, OrbState,
    StableOrbState, unix_ms_now,
};
use crate::secrets::PermittedGuestEnv;
use orbd_proto::CONTROL_PROTOCOL_VERSION;
use orbd_proto::control::v1 as pb;
use orbd_proto::control::v1::agent_control_client::AgentControlClient;
use secrecy::SecretString;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

// --- event builders ---------------------------------------------------------

fn ev(e: pb::agent_event::Event) -> pb::AgentEvent {
    pb::AgentEvent { event: Some(e) }
}

pub(super) fn completed_event(key: &CommandKey, outcome: &CommandOutcome) -> pb::AgentEvent {
    ev(pb::agent_event::Event::Completed(pb::CommandCompleted {
        key: Some((*key).into()),
        outcome: Some(outcome.into()),
    }))
}

fn accepted_event(key: pb::CommandKey) -> pb::AgentEvent {
    ev(pb::agent_event::Event::Accepted(pb::CommandAccepted {
        key: Some(key),
    }))
}

fn rejected_event(key: pb::CommandKey, code: FailureCode, message: String) -> pb::AgentEvent {
    ev(pb::agent_event::Event::Rejected(pb::CommandRejected {
        key: Some(key),
        failure: Some(pb::CommandFailure {
            code: pb::FailureCode::from(code) as i32,
            message,
        }),
    }))
}

// --- connection loop --------------------------------------------------------

pub(super) async fn run_loop(sh: &Arc<Shared>) {
    let mut attempt: u32 = 0;
    let mut first = true;
    while !sh.shutdown.is_cancelled() {
        if !first {
            metrics::counter!("orbd_agent_reconnects_total").increment(1);
        }
        first = false;
        match connect_once(sh, &mut attempt).await {
            Ok(()) => {}
            Err(e) => tracing::warn!(error = %format!("{e:#}"), "control session ended"),
        }
        if sh.shutdown.is_cancelled() {
            break;
        }
        // Exponential backoff with jitter, forever.
        let base = 100u64.saturating_mul(1 << attempt.min(7)).min(10_000);
        let jitter = rand::random::<u64>() % (base / 2 + 1);
        attempt = attempt.saturating_add(1);
        tokio::select! {
            _ = sh.shutdown.cancelled() => {}
            _ = tokio::time::sleep(Duration::from_millis(base / 2 + jitter)) => {}
        }
    }
}

async fn hello(sh: &Shared) -> anyhow::Result<pb::AgentEvent> {
    let epoch = sh.db.call(|c| db::highest_epoch(c)).await?;
    let project_snapshot_keys = sh.db.call(|c| db::list_project_snapshots(c)).await?;
    let mut runtimes: Vec<_> = sh.runtimes.values().map(|r| r.capabilities()).collect();
    runtimes.sort_by_key(|c| c.kind.as_str());
    Ok(ev(pb::agent_event::Event::Hello(pb::Hello {
        agent_id: sh.identity.agent_id.0.clone(),
        boot_id: sh.boot_id.clone(),
        protocol_versions: vec![CONTROL_PROTOCOL_VERSION],
        runtimes: runtimes
            .into_iter()
            .map(|c| pb::RuntimeInfo {
                kind: pb::RuntimeKind::from(c.kind) as i32,
                version: c.version,
                memory_preserving_sleep: c.memory_preserving_sleep,
                nested_kvm: c.nested_kvm,
                nested_state_qualified: c.nested_state_qualified,
                project_snapshots: c.project_snapshots,
            })
            .collect(),
        capacity: Some(sh.capacity.into()),
        last_controller_epoch: epoch,
        project_snapshot_keys,
    })))
}

pub(super) fn heartbeat(sh: &Shared) -> pb::AgentEvent {
    let (free, total) = sh.disk_stats();
    ev(pb::agent_event::Event::Heartbeat(pb::Heartbeat {
        capacity: Some(sh.capacity.into()),
        reserved: Some(sh.reserved_total().into()),
        disk_free_bytes: free,
        memory_pressure_permille: 0,
        draining: sh.draining.is_cancelled(),
        disk_total_bytes: total,
        disk_allocated_bytes: sh.disk_allocated.load(std::sync::atomic::Ordering::Relaxed),
        disk_reserve_bytes: sh.disk_reserve,
        memory_available_bytes: sh.memory_available().unwrap_or(0),
        memory_reserve_bytes: sh.memory_reserve,
    }))
}

async fn connect_once(sh: &Arc<Shared>, attempt: &mut u32) -> anyhow::Result<()> {
    let channel = tokio::select! {
        _ = sh.shutdown.cancelled() => return Ok(()),
        c = sh.identity.mtls_channel() => c?,
    };
    let mut client = AgentControlClient::new(channel);
    let (out_tx, out_rx) = mpsc::channel::<pb::AgentEvent>(4);
    out_tx.send(hello(sh).await?).await?;
    let resp = tokio::select! {
        _ = sh.shutdown.cancelled() => return Ok(()),
        r = client.session(tonic::Request::new(ReceiverStream::new(out_rx))) => r?,
    };
    let mut inbound = resp.into_inner();

    let welcome = match tokio::time::timeout(Duration::from_secs(10), inbound.message()).await {
        Err(_) => anyhow::bail!("no Welcome within 10s"),
        Ok(m) => match m?.and_then(|m| m.event) {
            Some(pb::coordinator_event::Event::Welcome(w)) => w,
            Some(_) => anyhow::bail!("first coordinator message was not Welcome"),
            None => anyhow::bail!("session closed before Welcome"),
        },
    };
    if welcome.protocol_version != CONTROL_PROTOCOL_VERSION {
        anyhow::bail!("unsupported protocol version {}", welcome.protocol_version);
    }
    let epoch = welcome.controller_epoch;
    if !sh.db.call(move |c| db::observe_epoch(c, epoch)).await? {
        anyhow::bail!("coordinator epoch {epoch} is stale");
    }
    *attempt = 0;
    tracing::info!(session_id = %welcome.session_id, epoch, "control session established");

    let kill = sh.shutdown.child_token();
    let (ctrl_tx, ctrl_rx) = mpsc::channel::<pb::AgentEvent>(1024);
    *sh.session.lock().unwrap_or_else(|e| e.into_inner()) = Some(SessionHandle {
        ctrl: ctrl_tx,
        kill: kill.clone(),
    });
    let pump = tokio::spawn(pump(sh.clone(), ctrl_rx, out_tx, kill.clone()));

    // Registered before replaying, so a completion committed meanwhile is either in the
    // replay query or sent live (possibly both; the coordinator tolerates duplicates).
    let r = session_loop(sh, &mut inbound, welcome.heartbeat_interval_ms, &kill).await;

    *sh.session.lock().unwrap_or_else(|e| e.into_inner()) = None;
    kill.cancel();
    pump.abort();
    r
}

/// Control messages first; logs only when no control message is waiting.
async fn pump(
    sh: Arc<Shared>,
    mut ctrl: mpsc::Receiver<pb::AgentEvent>,
    out: mpsc::Sender<pb::AgentEvent>,
    kill: CancellationToken,
) {
    loop {
        let next = if let Ok(e) = ctrl.try_recv() {
            e
        } else if let Some(chunk) = sh.logs.pop() {
            ev(pb::agent_event::Event::Log(chunk))
        } else {
            tokio::select! {
                biased;
                e = ctrl.recv() => match e { Some(e) => e, None => break },
                _ = sh.logs.wait() => continue,
                _ = kill.cancelled() => break,
            }
        };
        if out.send(next).await.is_err() {
            break;
        }
    }
    kill.cancel();
}

async fn session_loop(
    sh: &Arc<Shared>,
    inbound: &mut tonic::Streaming<pb::CoordinatorEvent>,
    heartbeat_ms: u64,
    kill: &CancellationToken,
) -> anyhow::Result<()> {
    for r in sh.db.call(|c| db::list_unacked(c)).await? {
        sh.send_control(completed_event(&r.command.key(), &outcome_of(&r)));
    }
    let mut tick = tokio::time::interval(Duration::from_millis(heartbeat_ms.max(50)));
    loop {
        tokio::select! {
            _ = kill.cancelled() => return Ok(()),
            _ = tick.tick() => sh.send_control(heartbeat(sh)),
            m = inbound.message() => {
                let Some(m) = m? else { return Ok(()) };
                handle_event(sh, m).await?;
            }
        }
    }
}

fn outcome_of(r: &Receipt) -> CommandOutcome {
    r.outcome.clone().expect("terminal receipt has an outcome")
}

async fn handle_event(sh: &Arc<Shared>, m: pb::CoordinatorEvent) -> anyhow::Result<()> {
    use pb::coordinator_event::Event as E;
    match m.event {
        Some(E::Welcome(_)) | None => {}
        Some(E::Command(d)) => handle_delivery(sh, d).await?,
        Some(E::Cancel(c)) => handle_cancel(sh, c).await?,
        Some(E::Reconcile(r)) => handle_reconcile(sh, r).await?,
        Some(E::Ack(a)) => {
            if let Some(key) = a.key.as_ref().and_then(|k| CommandKey::try_from(k).ok()) {
                let op = key.operation_id;
                sh.db.call(move |c| db::ack(c, op)).await?;
            }
        }
    }
    Ok(())
}

// --- delivery ---------------------------------------------------------------

struct Parsed {
    command: crate::model::Command,
    orb: OrbDefinition,
    env: PermittedGuestEnv,
}

fn parse_delivery(sh: &Shared, d: &pb::CommandDelivery) -> Result<Parsed, (FailureCode, String)> {
    let invalid = |m: String| (FailureCode::InvalidRequest, m);
    let pc = d
        .command
        .as_ref()
        .ok_or_else(|| invalid("no command".into()))?;
    let command = crate::model::Command::try_from(pc).map_err(|e| invalid(e.to_string()))?;
    let orb = OrbDefinition::try_from(d.orb.as_ref().ok_or_else(|| invalid("no orb".into()))?)
        .map_err(|e| invalid(e.to_string()))?;
    if orb.id != command.orb_id {
        return Err(invalid(
            "command and orb definition disagree on orb id".into(),
        ));
    }
    if orb.spec.digest() != orb.spec_digest || orb.spec_digest != command.spec_digest {
        return Err((
            FailureCode::SpecMismatch,
            "spec digest does not match the orb spec".into(),
        ));
    }
    if executor::runtime_for(sh, &orb).is_none() {
        return Err((
            FailureCode::UnsupportedCapability,
            "this agent has no runtime for the orb's execution target".into(),
        ));
    }
    let env = PermittedGuestEnv(
        d.secrets
            .as_ref()
            .map(|s| {
                s.guest_env
                    .iter()
                    .map(|e| (e.name.clone(), SecretString::from(e.value.clone())))
                    .collect()
            })
            .unwrap_or_default(),
    );
    env.validate().map_err(invalid)?;
    Ok(Parsed { command, orb, env })
}

async fn handle_delivery(sh: &Arc<Shared>, d: pb::CommandDelivery) -> anyhow::Result<()> {
    let raw_key = d
        .command
        .as_ref()
        .map(|c| pb::CommandKey {
            operation_id: c.operation_id.clone(),
            orb_id: c.orb_id.clone(),
            generation: c.generation,
        })
        .unwrap_or_default();
    if sh.draining.is_cancelled() {
        // Shutting down: known operations replay, new ones wait for the next start.
        let known = match CommandKey::try_from(&raw_key) {
            Ok(k) => {
                let op = k.operation_id;
                sh.db.call(move |c| db::get_receipt(c, op)).await?
            }
            Err(_) => None,
        };
        match known {
            Some(r) => {
                sh.send_control(accepted_event(raw_key));
                if r.terminal {
                    sh.send_control(completed_event(&r.command.key(), &outcome_of(&r)));
                }
            }
            None => sh.send_control(rejected_event(
                raw_key,
                FailureCode::BackendUnavailable,
                "agent is shutting down".into(),
            )),
        }
        return Ok(());
    }
    let p = match parse_delivery(sh, &d) {
        Ok(p) => p,
        Err((code, msg)) => {
            sh.send_control(rejected_event(raw_key, code, msg));
            return Ok(());
        }
    };
    let key = p.command.key();
    let now = unix_ms_now();
    let work = now.saturating_add(d.work_remaining_ms);
    let cleanup = now.saturating_add(d.cleanup_remaining_ms).max(work);
    let sealed_env = sh
        .key
        .seal(&p.env.to_json(), key.operation_id.to_string().as_bytes());
    let new = NewReceipt {
        command: p.command,
        orb: p.orb,
        sealed_env,
        work_deadline_unix_ms: work,
        cleanup_deadline_unix_ms: cleanup,
        now_unix_ms: now,
    };
    let orb_id = key.orb_id;
    let epoch = d.controller_epoch;
    match sh
        .db
        .call(move |c| db::decide_delivery(c, epoch, new))
        .await?
    {
        Decision::Rejected(f) => {
            sh.send_control(rejected_event(key.into(), f.code, f.message));
        }
        Decision::Replay(r) => {
            sh.send_control(accepted_event(key.into()));
            if r.terminal {
                sh.send_control(completed_event(&r.command.key(), &outcome_of(&r)));
            }
        }
        Decision::Accepted => {
            crate::crash_point!("agent::accepted_committed");
            sh.token_for(key.operation_id);
            sh.send_control(accepted_event(key.into()));
            executor::enqueue(sh, orb_id, key.operation_id);
        }
    }
    Ok(())
}

// --- cancel -----------------------------------------------------------------

async fn handle_cancel(sh: &Arc<Shared>, c: pb::CancelCommand) -> anyhow::Result<()> {
    let raw = c.key.clone().unwrap_or_default();
    let Ok(key) = CommandKey::try_from(&raw) else {
        tracing::warn!("ignoring CancelCommand with a malformed key");
        return Ok(());
    };
    let (op, orb) = (key.operation_id, key.orb_id);
    let decision = sh
        .db
        .call(move |c| db::cancel_operation(c, op, orb, unix_ms_now()))
        .await?;
    use pb::CancelStatus as S;
    let status = match decision {
        CancelDecision::RecordedBeforeAccept => S::RecordedBeforeAccept,
        CancelDecision::Cancelling => {
            sh.token_for(op).cancel();
            S::Cancelling
        }
        CancelDecision::AlreadyTerminal => S::AlreadyTerminal,
    };
    sh.send_control(ev(pb::agent_event::Event::CancelResult(pb::CancelResult {
        key: Some(raw),
        status: status as i32,
    })));
    if matches!(decision, CancelDecision::AlreadyTerminal)
        && let Some(r) = sh.db.call(move |c| db::get_receipt(c, op)).await?
    {
        sh.send_control(completed_event(&r.command.key(), &outcome_of(&r)));
    }
    Ok(())
}

// --- reconcile --------------------------------------------------------------

const RECONCILE_PAGE: usize = 500;

fn orb_state_pb(s: Option<&OrbState>) -> pb::OrbState {
    match s {
        Some(s) => s.into(),
        None => (&OrbState::known(StableOrbState::Uncreated)).into(),
    }
}

async fn handle_reconcile(sh: &Arc<Shared>, r: pb::ReconcileRequest) -> anyhow::Result<()> {
    let ops: Vec<OperationId> = r
        .operation_ids
        .iter()
        .filter_map(|s| wire::operation_id(s).ok())
        .collect();
    let orbs: Vec<OrbId> = r
        .orb_ids
        .iter()
        .filter_map(|s| wire::orb_id(s).ok())
        .collect();
    let all = r.all_orbs;
    let page = r.page_token.clone();

    #[allow(clippy::type_complexity)]
    let (op_reports, orb_rows, next): (Vec<pb::OperationReport>, Vec<db::OrbRow>, String) = sh
        .db
        .call(move |c| {
            let mut reports = Vec::new();
            for op in ops {
                let nil_orb = uuid::Uuid::nil().to_string();
                let report = if let Some(rc) = db::get_receipt(c, op)? {
                    let key: pb::CommandKey = rc.command.key().into();
                    if rc.terminal {
                        pb::OperationReport {
                            key: Some(key),
                            status: pb::OperationStatus::Completed as i32,
                            outcome: rc.outcome.as_ref().map(Into::into),
                        }
                    } else {
                        pb::OperationReport {
                            key: Some(key),
                            status: pb::OperationStatus::Running as i32,
                            outcome: None,
                        }
                    }
                } else if let Some(orb) = db::tombstone_orb(c, op)? {
                    pb::OperationReport {
                        key: Some(pb::CommandKey {
                            operation_id: op.to_string(),
                            orb_id: orb.to_string(),
                            generation: 0,
                        }),
                        status: pb::OperationStatus::CancelledBeforeAccept as i32,
                        outcome: None,
                    }
                } else {
                    pb::OperationReport {
                        key: Some(pb::CommandKey {
                            operation_id: op.to_string(),
                            orb_id: nil_orb,
                            generation: 0,
                        }),
                        status: pb::OperationStatus::NotAccepted as i32,
                        outcome: None,
                    }
                };
                reports.push(report);
            }
            let mut rows = Vec::new();
            for orb in orbs {
                if let Some(row) = db::get_orb(c, orb)? {
                    rows.push(row);
                }
            }
            let mut next = String::new();
            if all {
                let listed = db::list_orbs(c, &page, RECONCILE_PAGE + 1)?;
                if listed.len() > RECONCILE_PAGE {
                    next = listed[RECONCILE_PAGE - 1].orb_id.to_string();
                }
                rows.extend(listed.into_iter().take(RECONCILE_PAGE));
            }
            Ok((reports, rows, next))
        })
        .await?;
    sh.send_control(ev(pb::agent_event::Event::Reconcile(
        pb::ReconcileResponse {
            correlation_id: r.correlation_id,
            operations: op_reports,
            orbs: orb_rows
                .iter()
                .map(|row| pb::OrbObservation {
                    orb_id: row.orb_id.to_string(),
                    generation: row.highest_generation,
                    state: Some(orb_state_pb(row.last_state.as_ref())),
                })
                .collect(),
            next_page_token: next,
        },
    )));
    Ok(())
}
