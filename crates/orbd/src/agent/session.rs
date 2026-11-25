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

/// Reports an orb's state outside any command (its VMM ended on its own): a reconcile
/// response no request asked for, with an empty correlation id.
pub(super) fn orb_report_event(row: &db::OrbRow) -> pb::AgentEvent {
    ev(pb::agent_event::Event::Reconcile(pb::ReconcileResponse {
        correlation_id: String::new(),
        operations: vec![],
        orbs: vec![orb_observation(row)],
        next_page_token: String::new(),
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
            Err(e) => tracing::warn!(
                coordinator = %sh.identity.coordinator_url,
                error = %format!("{e:#}"),
                "control session ended"
            ),
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
                nested_kvm: c.nested_kvm && !sh.config.agent.refuse_nested_kvm,
                nested_state_qualified: c.nested_state_qualified,
                project_snapshots: c.project_snapshots,
                arch: pb::Arch::from(c.arch) as i32,
                egress_policy: c.egress_policy,
            })
            .collect(),
        capacity: Some(sh.capacity.into()),
        project_snapshot_keys,
        overflow: sh.config.agent.overflow,
    })))
}

pub(super) fn heartbeat(sh: &Shared) -> pb::AgentEvent {
    let (_, total) = sh.disk_stats();
    // Committed writes (held sleeps' checkpoints, restores) are not free to place on.
    let free = sh.disk_room();
    // Held sleeps' memory is available only while their checkpoints fit on disk.
    let held = match free >= sh.disk_reserve {
        true => sh.memory_held(),
        false => 0,
    };
    ev(pb::agent_event::Event::Heartbeat(pb::Heartbeat {
        capacity: Some(sh.capacity.into()),
        reserved: Some(sh.reserved_total().into()),
        disk_free_bytes: free,
        draining: sh.draining.is_cancelled(),
        disk_total_bytes: total,
        // Held checkpoints are as good as allocated (see `disk_committed`).
        disk_allocated_bytes: sh
            .disk_allocated
            .load(std::sync::atomic::Ordering::Relaxed)
            .saturating_add(sh.disk_committed()),
        disk_reserve_bytes: sh.disk_reserve,
        // Sleeps held in RAM give their memory up as soon as an orb needs it.
        memory_available_bytes: sh.memory_available().map_or(0, |a| a.saturating_add(held)),
        memory_reserve_bytes: sh.memory_reserve,
        orb_disk: sh.orb_disk.lock().unwrap().clone(),
        wake_releases: wake_releases(sh),
        // Sent once: a session that ends before the coordinator saw it starts with a
        // Hello listing what the agent holds.
        deleted_project_snapshot_keys: std::mem::take(
            &mut *sh
                .deleted_snapshots
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        ),
    }))
}

/// What waking each orb asleep here would free on disk now: the checkpoint room a
/// sleep held in RAM has committed (its memory size, as `disk_committed` counts it),
/// then the checkpoints on disk a wake frees, as last measured. Not a held sleep whose
/// write is queued or under way (`flush_queued`, which every write of a hold goes
/// through until it ends): its checkpoint is about to be on disk, where the restore of
/// a runtime that keeps it mapped (Firecracker) frees nothing.
fn wake_releases(sh: &Shared) -> Vec<pb::WakeRelease> {
    let writing = sh
        .flush_queued
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    let mut out: Vec<pb::WakeRelease> = sh
        .runtimes
        .values()
        .flat_map(|r| r.held())
        .filter(|h| !writing.contains(&h.orb))
        .map(|h| pb::WakeRelease {
            orb_id: h.orb.to_string(),
            checkpoint_generation: h.checkpoint.generation,
            bytes: h.memory_bytes,
        })
        .collect();
    let disk = sh.disk_wake_releases.lock().unwrap().clone();
    let held: Vec<String> = sh
        .runtimes
        .values()
        .flat_map(|r| r.held())
        .map(|h| h.orb.to_string())
        .collect();
    for r in disk {
        if !held.contains(&r.orb_id) && !out.iter().any(|h| h.orb_id == r.orb_id) {
            out.push(r);
        }
    }
    out
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
    let Some(runtime) = executor::runtime_for(sh, &orb) else {
        return Err((
            FailureCode::UnsupportedCapability,
            "this agent has no runtime for the orb's execution target".into(),
        ));
    };
    if let Some(why) = refused_by_policy(&sh.config.agent, &runtime.capabilities(), &command, &orb)
    {
        return Err((FailureCode::UnsupportedCapability, why.into()));
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

/// Why this agent's own settings or its orb's runtime (`caps`) refuse `command`,
/// whatever the coordinator decided (it may be wrong or compromised). Pauses and deletes
/// are never refused, so orbs created before a setting changed can still be put to sleep
/// and removed.
fn refused_by_policy(
    agent: &crate::config::agent::AgentSection,
    caps: &crate::runtime::RuntimeCapabilities,
    command: &crate::model::Command,
    orb: &OrbDefinition,
) -> Option<&'static str> {
    let spawn = matches!(command.action, crate::model::Action::Spawn);
    if spawn && agent.refuse_nested_kvm && crate::runtime::needs_nested_kvm(orb) {
        return Some(crate::runtime::NESTED_KVM_REFUSED);
    }
    // Restricted egress fails closed: never started where it would run open.
    if spawn && orb.egress_policy().is_some() && !caps.egress_policy {
        return Some(crate::runtime::EGRESS_REFUSED);
    }
    None
}

/// How many of the orbs here (`defs`, those `live` says are not deleted) have a type
/// with `kvm` that this agent never starts or wakes (nested KVM off, the default since
/// 0.4) until it runs with `ORBD_NESTED_KVM=true`.
pub(crate) fn kvm_orbs_held(
    agent: &crate::config::agent::AgentSection,
    defs: &[OrbDefinition],
    live: impl Fn(&OrbDefinition) -> bool,
) -> usize {
    if !agent.refuse_nested_kvm {
        return 0;
    }
    defs.iter()
        .filter(|d| crate::runtime::needs_nested_kvm(d) && live(d))
        .count()
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

fn orb_observation(row: &db::OrbRow) -> pb::OrbObservation {
    pb::OrbObservation {
        orb_id: row.orb_id.to_string(),
        generation: row.highest_generation,
        state: Some(orb_state_pb(row.last_state.as_ref())),
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
            orbs: orb_rows.iter().map(orb_observation).collect(),
            next_page_token: next,
        },
    )));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::agent::AgentSection;
    use crate::model::*;

    fn orb(nested_kvm: NestedKvmRequirement) -> OrbDefinition {
        let spec = OrbSpec {
            template: SpecTemplate {
                target: ExecutionTarget::Fleet {
                    runtime: RuntimeKind::Qemu,
                },
                resources: Resources {
                    vcpus: 2,
                    memory_bytes: 2 * GIB,
                    disk_bytes: 4 * GIB,
                },
                requirements: CapabilityRequirements {
                    release_compute_when_paused: true,
                    nested_kvm,
                    project_snapshots: SnapshotRequirement::NotRequired,
                    arch: Arch::Amd64,
                    egress: None,
                },
                pause_policy: PausePolicy::MemoryPreserving {
                    on_failure: CheckpointFailurePolicy::ColdStop,
                },
            },
            initial_disk: InitialDiskSource::Image {
                image_ref: "image".into(),
            },
        };
        OrbDefinition {
            id: OrbId::generate(),
            identity: OrbIdentity {
                amp_endpoint: AmpEndpointId::new("amp"),
                amp_orb_id: AmpOrbId::new("amp-1"),
            },
            offering_id: OfferingId::new("kvm"),
            spec_digest: spec.digest(),
            spec,
            egress: None,
        }
    }

    fn caps(egress_policy: bool) -> crate::runtime::RuntimeCapabilities {
        crate::runtime::RuntimeCapabilities {
            kind: RuntimeKind::Firecracker,
            version: "1".into(),
            memory_preserving_sleep: true,
            nested_kvm: false,
            nested_state_qualified: false,
            project_snapshots: true,
            arch: Arch::Amd64,
            egress_policy,
        }
    }

    fn command(orb: &OrbDefinition, action: Action) -> Command {
        Command {
            operation_id: OperationId::generate(),
            orb_id: orb.id,
            generation: 1,
            spec_digest: orb.spec_digest.clone(),
            command_digest: CommandDigest("d".into()),
            action,
        }
    }

    /// An agent with nested KVM off counts the live orbs it will not start (its startup
    /// warning); one with it on, or with no kvm orbs, counts none.
    #[test]
    fn kvm_orbs_held_counts_live_kvm_orbs_when_nested_kvm_is_off() {
        let off = AgentSection::new("/s".into());
        let on = AgentSection {
            refuse_nested_kvm: false,
            ..AgentSection::new("/s".into())
        };
        let kvm = orb(NestedKvmRequirement::GuestDevice);
        let preserved = orb(NestedKvmRequirement::GuestDeviceAndPreservedNestedState);
        let deleted = orb(NestedKvmRequirement::GuestDevice);
        let plain = orb(NestedKvmRequirement::NotRequired);
        let defs = [kvm, preserved, deleted.clone(), plain];
        let live = |d: &OrbDefinition| d.id != deleted.id;
        assert_eq!(kvm_orbs_held(&off, &defs, live), 2);
        assert_eq!(kvm_orbs_held(&on, &defs, live), 0);
        assert_eq!(kvm_orbs_held(&off, &defs[3..], live), 0);
    }

    /// Without ORBD_NESTED_KVM=true (the default) an orb with /dev/kvm is never started,
    /// whatever the coordinator sends, but can still be paused and deleted.
    #[test]
    fn nested_kvm_off_refuses_spawning_kvm_orbs_only() {
        let off = AgentSection::new("/s".into());
        let on = AgentSection {
            refuse_nested_kvm: false,
            ..AgentSection::new("/s".into())
        };
        let kvm = orb(NestedKvmRequirement::GuestDevice);
        let preserved = orb(NestedKvmRequirement::GuestDeviceAndPreservedNestedState);
        let plain = orb(NestedKvmRequirement::NotRequired);
        let spawn = |o: &OrbDefinition| command(o, Action::Spawn);
        assert_eq!(
            refused_by_policy(&off, &caps(true), &spawn(&kvm), &kvm),
            Some(crate::runtime::NESTED_KVM_REFUSED)
        );
        assert!(refused_by_policy(&off, &caps(true), &spawn(&preserved), &preserved).is_some());
        assert_eq!(
            refused_by_policy(&off, &caps(true), &spawn(&plain), &plain),
            None
        );
        assert_eq!(
            refused_by_policy(&on, &caps(true), &spawn(&kvm), &kvm),
            None
        );
        for action in [
            Action::Delete,
            Action::Pause {
                publish_snapshot: None,
            },
        ] {
            assert_eq!(
                refused_by_policy(&off, &caps(true), &command(&kvm, action.clone()), &kvm),
                None,
                "{action:?}"
            );
        }
    }

    /// An orb whose type restricts egress never starts on a runtime that cannot enforce
    /// it, whatever the coordinator sends (by the spec it was created with, also when the
    /// coordinator drops the current policy), but can still be paused and deleted. An
    /// unrestricted spec stays unrestricted whatever policy comes with it.
    #[test]
    fn restricted_egress_is_refused_where_it_cannot_be_enforced() {
        let agent = AgentSection::new("/s".into());
        let policy: EgressPolicy =
            serde_json::from_str(r#"{"domains":["ampcode.com"],"allow":[]}"#).unwrap();
        let mut restricted = orb(NestedKvmRequirement::NotRequired);
        restricted.spec.template.requirements.egress = Some(policy.clone());
        restricted.spec_digest = restricted.spec.digest();
        let spawn = command(&restricted, Action::Spawn);
        assert_eq!(
            refused_by_policy(&agent, &caps(false), &spawn, &restricted),
            Some(crate::runtime::EGRESS_REFUSED)
        );
        assert_eq!(
            refused_by_policy(&agent, &caps(true), &spawn, &restricted),
            None
        );
        for action in [
            Action::Delete,
            Action::Pause {
                publish_snapshot: None,
            },
        ] {
            let c = command(&restricted, action);
            assert_eq!(
                refused_by_policy(&agent, &caps(false), &c, &restricted),
                None
            );
        }
        assert_eq!(restricted.egress_policy(), Some(&policy));
        // The orb type's current policy replaces the recorded one...
        let edited: EgressPolicy =
            serde_json::from_str(r#"{"domains":["ampcode.com","github.com"]}"#).unwrap();
        restricted.egress = Some(edited.clone());
        assert_eq!(restricted.egress_policy(), Some(&edited));
        // ...but never makes an unrestricted orb restricted, or the reverse.
        let mut plain = orb(NestedKvmRequirement::NotRequired);
        plain.egress = Some(edited);
        assert_eq!(plain.egress_policy(), None);
        let p = command(&plain, Action::Spawn);
        assert_eq!(refused_by_policy(&agent, &caps(false), &p, &plain), None);
        // The spec's policy is part of its digest; an unrestricted spec's digest is as
        // before the field existed.
        let mut json = serde_json::to_value(&plain.spec).unwrap();
        assert!(json.to_string().find("egress").is_none(), "{json}");
        json["template"]["requirements"]["egress"] = serde_json::Value::Null;
        let back: OrbSpec = serde_json::from_value(json).unwrap();
        assert_eq!(back.digest(), plain.spec.digest());
        assert_ne!(restricted.spec.digest(), plain.spec.digest());
    }
}
