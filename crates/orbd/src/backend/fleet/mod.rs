//! Agent-fleet compute backend: agents dial the coordinator over mTLS gRPC
//! (`orbd.control.v1.AgentControl`) and run orbs with their local runtimes.
//! The gRPC protocol is this backend's private transport.
//!
//! Identity: the coordinator's built-in CA ([`crate::pki`]) issues every agent
//! certificate. Agents enroll once with a single-use join token and renew over mTLS.
//! The registry in the coordinator database decides who may connect: a session needs a
//! certificate from the CA naming an active agent *and* the key recorded at enrollment.
//! `orbd agent remove` revokes for good; the agent's orbs stay recorded on it.
//!
//! Session model: one live session per registered agent id. A new `Hello` replaces the
//! old session. All control traffic to an agent goes through that session's own
//! unbounded queue (control messages are small and are never queued behind anything;
//! there is no outbound log traffic). Inbound, control events and log chunks reach the
//! core through separate channels merged with control priority; the log channel is
//! bounded and drops (and counts) on overflow, so logs can never block control handling.
//!
//! Registry state is kept for disconnected agents too: they remain candidates with
//! `Availability::Unavailable` (a disconnect is not evidence their orbs stopped).

pub mod admin;
pub(crate) mod capabilities;
mod registry;
mod server;

use super::{
    Availability, BackendError, BackendEvent, BackendHandle, CancelRequest, CapabilityError,
    CommandDelivery, CommandSecrets, ComputeBackend, LogChunk, OperationStatus, PlacementCandidate,
    PlacementRequest, ReconcilePage, ReconcileQuery,
};
use crate::config::coordinator::{CoordinatorConfig, FLEET_BACKEND};
use crate::coordinator::db::Db;
use crate::model::wire;
use crate::model::{
    AgentId, BackendCapabilities, BackendId, CommandKey, ExecutionTarget, Location, OperationId,
    Resources,
};
use crate::pki::Ca;
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::{PollNext, select_with_strategy};
use orbd_proto::control::v1 as pb;
use secrecy::ExposeSecret;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Capacity of the inbound log channel towards the core.
const LOG_CHANNEL_CAPACITY: usize = 1024;
/// How long `reconcile` waits for the agent's matching response.
const RECONCILE_TIMEOUT: Duration = Duration::from_secs(15);
/// Missed heartbeat intervals before an agent is marked Unavailable.
const MISSED_HEARTBEATS: u32 = 3;

pub struct FleetBackend {
    inner: Arc<Inner>,
}

struct Inner {
    id: BackendId,
    controller_epoch: u64,
    heartbeat_interval: Duration,
    public_url: String,
    ca: Ca,
    db: Db,
    state: Mutex<State>,
    control_tx: mpsc::UnboundedSender<BackendEvent>,
    log_tx: mpsc::Sender<BackendEvent>,
    shutdown: CancellationToken,
}

#[derive(Default)]
struct State {
    /// Every registered agent, removed ones included.
    agents: BTreeMap<AgentId, AgentSlot>,
    /// Operation id -> key, learned from submits and agent messages; needed because
    /// `LogChunk` carries only the operation id. Removed on acknowledge/reject.
    op_keys: HashMap<OperationId, CommandKey>,
}

struct AgentSlot {
    /// Registry record (mirrors the `agents` row).
    key_sha256: String,
    cert_not_after_ms: u64,
    enrolled_ms: u64,
    removed: bool,
    session: Option<Session>,
    hello: Option<pb::Hello>,
    heartbeat: Option<(pb::Heartbeat, Instant)>,
    unavailable_reason: String,
    /// Project snapshot keys the agent holds: from its Hello, plus later publications.
    snapshots: HashSet<String>,
}

struct Session {
    id: Uuid,
    /// Announced by heartbeat; a new session (agent restart) starts not draining.
    draining: bool,
    tx: mpsc::UnboundedSender<pb::CoordinatorEvent>,
    cancel: CancellationToken,
    pending_reconcile: HashMap<String, oneshot::Sender<pb::ReconcileResponse>>,
}

impl AgentSlot {
    fn new(row: registry::AgentRow) -> Self {
        Self {
            key_sha256: row.key_sha256,
            cert_not_after_ms: row.cert_not_after_ms,
            enrolled_ms: row.enrolled_ms,
            removed: row.removed,
            session: None,
            hello: None,
            heartbeat: None,
            unavailable_reason: String::new(),
            snapshots: HashSet::new(),
        }
    }
}

impl FleetBackend {
    /// Loads the registry and starts the AgentControl listener. The returned admin
    /// service is for the local admin socket.
    pub async fn start(
        config: &CoordinatorConfig,
        ca: Ca,
        db: Db,
        controller_epoch: u64,
        shutdown: CancellationToken,
    ) -> anyhow::Result<(BackendHandle, admin::AdminService)> {
        let mut state = State::default();
        for row in db.call(|c| registry::load(c)).await? {
            state
                .agents
                .insert(AgentId::new(row.id.clone()), AgentSlot::new(row));
        }
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (log_tx, log_rx) = mpsc::channel(LOG_CHANNEL_CAPACITY);
        let inner = Arc::new(Inner {
            id: BackendId::new(FLEET_BACKEND),
            controller_epoch,
            heartbeat_interval: Duration::from_millis(config.heartbeat_interval_ms),
            public_url: config.public_url.clone(),
            ca,
            db,
            state: Mutex::new(state),
            control_tx,
            log_tx,
            shutdown: shutdown.clone(),
        });
        server::serve(inner.clone(), config).await?;
        let admin = admin::AdminService(inner.clone());
        metrics::gauge!("orbd_fleet_connected_agents").set(0.0);

        // Control events always win over logs.
        let events = select_with_strategy(
            UnboundedReceiverStream::new(control_rx),
            ReceiverStream::new(log_rx),
            |_: &mut PollNext| PollNext::Left,
        )
        .boxed();
        let backend: Arc<dyn ComputeBackend> = Arc::new(FleetBackend { inner });
        Ok((BackendHandle { backend, events }, admin))
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn location(&self, agent: &AgentId) -> Location {
        Location::Fleet {
            backend_id: self.id.clone(),
            agent_id: agent.clone(),
        }
    }

    fn emit(&self, ev: BackendEvent) {
        // The receiver only disappears when the core is gone.
        let _ = self.control_tx.send(ev);
    }

    fn agent_of(&self, location: &Location) -> Result<AgentId, BackendError> {
        match location {
            Location::Fleet {
                backend_id,
                agent_id,
            } if backend_id == &self.id && self.lock().agents.contains_key(agent_id) => {
                Ok(agent_id.clone())
            }
            other => Err(BackendError::Invalid(format!(
                "location {other:?} does not belong to fleet backend {}",
                self.id
            ))),
        }
    }

    fn send_to(&self, agent: &AgentId, event: pb::CoordinatorEvent) -> Result<(), BackendError> {
        let st = self.lock();
        let session = st
            .agents
            .get(agent)
            .and_then(|s| s.session.as_ref())
            .ok_or_else(|| BackendError::Unavailable(format!("agent {agent} not connected")))?;
        session
            .tx
            .send(event)
            .map_err(|_| BackendError::Unavailable(format!("agent {agent} session closed")))
    }

    fn update_connected_gauge(st: &State) {
        let n = st.agents.values().filter(|s| s.session.is_some()).count();
        metrics::gauge!("orbd_fleet_connected_agents").set(n as f64);
    }

    /// Installs a new session for `agent`, replacing any older one, if `key_sha256` is
    /// the active agent's registered key. Welcome is queued before `Available` is
    /// emitted.
    fn open_session(
        &self,
        agent: &AgentId,
        key_sha256: &str,
        hello: pb::Hello,
    ) -> Option<(
        Uuid,
        CancellationToken,
        mpsc::UnboundedReceiver<pb::CoordinatorEvent>,
    )> {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = Uuid::new_v4();
        let cancel = self.shutdown.child_token();
        let mut st = self.lock();
        let slot = st
            .agents
            .get_mut(agent)
            .filter(|s| !s.removed && s.key_sha256 == key_sha256)?;
        let replaced = slot.session.take();
        if let Some(old) = &replaced {
            // Cancels the old reader; dropping `old` closes its outbound stream.
            old.cancel.cancel();
            metrics::counter!("orbd_fleet_sessions_replaced_total").increment(1);
        }
        drop(replaced);
        slot.snapshots = hello.project_snapshot_keys.iter().cloned().collect();
        slot.hello = Some(hello);
        slot.unavailable_reason.clear();
        let _ = tx.send(pb::CoordinatorEvent {
            event: Some(pb::coordinator_event::Event::Welcome(pb::Welcome {
                session_id: id.to_string(),
                controller_epoch: self.controller_epoch,
                protocol_version: orbd_proto::CONTROL_PROTOCOL_VERSION,
                heartbeat_interval_ms: self.heartbeat_interval.as_millis() as u64,
            })),
        });
        slot.session = Some(Session {
            id,
            draining: false,
            tx,
            cancel: cancel.clone(),
            pending_reconcile: HashMap::new(),
        });
        metrics::counter!("orbd_fleet_sessions_opened_total").increment(1);
        self.emit(BackendEvent::AvailabilityChanged {
            location: self.location(agent),
            availability: Availability::Available,
        });
        Self::update_connected_gauge(&st);
        Some((id, cancel, rx))
    }

    /// Revokes `agent` (durably first) and ends its live session.
    async fn remove(&self, agent: &AgentId) -> anyhow::Result<bool> {
        let id = agent.as_str().to_owned();
        if !self.db.call(move |c| registry::remove(c, &id)).await? {
            return Ok(false);
        }
        let mut st = self.lock();
        if let Some(slot) = st.agents.get_mut(agent) {
            slot.removed = true;
            if let Some(old) = slot.session.take() {
                old.cancel.cancel();
                slot.unavailable_reason = "agent removed".into();
                self.emit(BackendEvent::AvailabilityChanged {
                    location: self.location(agent),
                    availability: Availability::Unavailable {
                        reason: slot.unavailable_reason.clone(),
                    },
                });
            }
        }
        Self::update_connected_gauge(&st);
        tracing::info!(%agent, "agent removed");
        Ok(true)
    }

    /// Ends session `sid` if it is still the current one.
    fn end_session(&self, agent: &AgentId, sid: Uuid, reason: String) {
        let mut st = self.lock();
        let Some(slot) = st.agents.get_mut(agent) else {
            return;
        };
        if slot.session.as_ref().map(|s| s.id) != Some(sid) {
            return;
        }
        slot.session = None;
        let age = slot
            .heartbeat
            .as_ref()
            .map(|(_, at)| format!(", last heartbeat {}s ago", at.elapsed().as_secs()))
            .unwrap_or_default();
        slot.unavailable_reason = format!("{reason}{age}");
        let reason = slot.unavailable_reason.clone();
        tracing::info!(%agent, %reason, "agent session ended");
        self.emit(BackendEvent::AvailabilityChanged {
            location: self.location(agent),
            availability: Availability::Unavailable { reason },
        });
        Self::update_connected_gauge(&st);
    }

    /// Handles one inbound message. Returns false if the session is stale (caller stops).
    /// Runs entirely under the registry lock so a replaced session can never emit.
    fn handle_event(&self, agent: &AgentId, sid: Uuid, event: pb::AgentEvent) -> bool {
        let mut st = self.lock();
        let current = st
            .agents
            .get(agent)
            .and_then(|s| s.session.as_ref())
            .map(|s| s.id);
        if current != Some(sid) {
            return false;
        }
        let Some(ev) = event.event else {
            tracing::warn!(%agent, "dropping agent message with no event");
            metrics::counter!("orbd_fleet_malformed_messages_total").increment(1);
            return true;
        };
        metrics::counter!("orbd_fleet_messages_total", "type" => event_type(&ev)).increment(1);
        match self.map_event(&mut st, agent, ev) {
            Ok(()) => {}
            Err(e) => {
                tracing::warn!(%agent, error = %e, "dropping malformed agent message");
                metrics::counter!("orbd_fleet_malformed_messages_total").increment(1);
            }
        }
        true
    }

    fn map_event(
        &self,
        st: &mut State,
        agent: &AgentId,
        ev: pb::agent_event::Event,
    ) -> Result<(), wire::WireError> {
        use pb::agent_event::Event as E;
        let key_of = |k: &Option<pb::CommandKey>| -> Result<CommandKey, wire::WireError> {
            CommandKey::try_from(
                k.as_ref()
                    .ok_or_else(|| wire::WireError("missing command key".into()))?,
            )
        };
        match ev {
            E::Hello(_) => tracing::warn!(%agent, "ignoring repeated Hello on live session"),
            E::Heartbeat(hb) => {
                if let Some(slot) = st.agents.get_mut(agent) {
                    if let Some(s) = slot.session.as_mut()
                        && hb.draining
                        && !s.draining
                    {
                        s.draining = true;
                        tracing::info!(%agent, "agent is draining");
                        self.emit(BackendEvent::AvailabilityChanged {
                            location: self.location(agent),
                            availability: Availability::Draining,
                        });
                    }
                    let d = disk_report(&hb);
                    let a = agent.to_string();
                    metrics::gauge!("orbd_agent_disk_free_bytes", "agent" => a.clone())
                        .set(d.free_bytes as f64);
                    metrics::gauge!("orbd_agent_disk_total_bytes", "agent" => a.clone())
                        .set(d.total_bytes as f64);
                    metrics::gauge!("orbd_agent_disk_allocated_bytes", "agent" => a)
                        .set(d.allocated_bytes as f64);
                    slot.heartbeat = Some((hb, Instant::now()));
                }
            }
            E::Accepted(a) => {
                let key = key_of(&a.key)?;
                st.op_keys.insert(key.operation_id, key);
                self.emit(BackendEvent::Accepted { key });
            }
            E::Rejected(r) => {
                let key = key_of(&r.key)?;
                let failure = r
                    .failure
                    .as_ref()
                    .ok_or_else(|| wire::WireError("rejection without failure".into()))?
                    .try_into()?;
                st.op_keys.remove(&key.operation_id);
                self.emit(BackendEvent::Rejected { key, failure });
            }
            E::Progress(p) => {
                let key = key_of(&p.key)?;
                st.op_keys.insert(key.operation_id, key);
                self.emit(BackendEvent::Progress {
                    key,
                    phase: p.phase,
                    message: p.message,
                });
            }
            E::Log(l) => {
                let op = wire::operation_id(&l.operation_id)?;
                let stream = match pb::LogStream::try_from(l.stream) {
                    Ok(pb::LogStream::Stdout) => super::LogStream::Stdout,
                    Ok(pb::LogStream::Stderr) => super::LogStream::Stderr,
                    _ => return Err(wire::WireError(format!("log stream {}", l.stream))),
                };
                let Some(key) = st.op_keys.get(&op).copied() else {
                    metrics::counter!("orbd_fleet_logs_dropped_total", "reason" => "unknown_op")
                        .increment(1);
                    return Ok(());
                };
                let ev = BackendEvent::Log {
                    key,
                    chunk: LogChunk {
                        seq: l.seq,
                        stream,
                        data: l.data.into(),
                        dropped_before: l.dropped_before,
                    },
                };
                if self.log_tx.try_send(ev).is_err() {
                    metrics::counter!("orbd_fleet_logs_dropped_total", "reason" => "overflow")
                        .increment(1);
                }
            }
            E::Completed(c) => {
                let key = key_of(&c.key)?;
                let outcome: crate::model::CommandOutcome = c
                    .outcome
                    .as_ref()
                    .ok_or_else(|| wire::WireError("completion without outcome".into()))?
                    .try_into()?;
                if let crate::model::SnapshotDisposition::Published { snapshot } = &outcome.snapshot
                    && let Some(slot) = st.agents.get_mut(agent)
                {
                    slot.snapshots.insert(snapshot.key.0.clone());
                }
                st.op_keys.insert(key.operation_id, key);
                self.emit(BackendEvent::Completed { key, outcome });
            }
            E::CancelResult(c) => {
                let key = key_of(&c.key)?;
                match pb::CancelStatus::try_from(c.status) {
                    // The agent replays Completed; nothing more to do.
                    Ok(pb::CancelStatus::AlreadyTerminal) | Ok(pb::CancelStatus::Cancelling) => {}
                    // Tombstone written: reconciliation will show CancelledBeforeAccept.
                    Ok(pb::CancelStatus::RecordedBeforeAccept) => {
                        self.emit(BackendEvent::ReconcileNeeded { key });
                    }
                    _ => return Err(wire::WireError(format!("cancel status {}", c.status))),
                }
            }
            E::Reconcile(r) => {
                let waiter = st
                    .agents
                    .get_mut(agent)
                    .and_then(|s| s.session.as_mut())
                    .and_then(|s| s.pending_reconcile.remove(&r.correlation_id));
                match waiter {
                    Some(w) => {
                        let _ = w.send(r);
                    }
                    None => tracing::debug!(%agent, "reconcile response with no waiter"),
                }
            }
        }
        Ok(())
    }
}

fn disk_report(hb: &pb::Heartbeat) -> crate::model::DiskReport {
    crate::model::DiskReport {
        free_bytes: hb.disk_free_bytes,
        total_bytes: hb.disk_total_bytes,
        allocated_bytes: hb.disk_allocated_bytes,
        // An agent that reports no filesystem size predates the reserve.
        reserve_bytes: if hb.disk_total_bytes == 0 {
            crate::model::disk_reserve(0)
        } else {
            hb.disk_reserve_bytes
        },
    }
}

fn event_type(ev: &pb::agent_event::Event) -> &'static str {
    use pb::agent_event::Event as E;
    match ev {
        E::Hello(_) => "hello",
        E::Heartbeat(_) => "heartbeat",
        E::Accepted(_) => "accepted",
        E::Rejected(_) => "rejected",
        E::Progress(_) => "progress",
        E::Log(_) => "log",
        E::Completed(_) => "completed",
        E::CancelResult(_) => "cancel_result",
        E::Reconcile(_) => "reconcile",
    }
}

fn to_bytes_err(e: wire::WireError) -> BackendError {
    BackendError::Invalid(e.to_string())
}

#[async_trait]
impl ComputeBackend for FleetBackend {
    fn id(&self) -> &BackendId {
        &self.inner.id
    }

    fn capabilities(
        &self,
        target: &ExecutionTarget,
    ) -> Result<BackendCapabilities, CapabilityError> {
        capabilities::for_target(target)
    }

    async fn candidates(
        &self,
        request: PlacementRequest,
    ) -> Result<Vec<PlacementCandidate>, BackendError> {
        let ExecutionTarget::Fleet { runtime } = request.target else {
            return Err(BackendError::Invalid(
                "fleet backend serves only fleet execution targets".into(),
            ));
        };
        let restrict = match &request.existing {
            None => None,
            Some(loc) => match self.inner.agent_of(loc) {
                Ok(a) => Some(a),
                // Another backend's or unknown agent's location: nothing here.
                Err(_) => return Ok(Vec::new()),
            },
        };
        let wire_kind: i32 = pb::RuntimeKind::from(runtime) as i32;
        let st = self.inner.lock();
        let mut out = Vec::new();
        for (agent, slot) in &st.agents {
            if restrict.as_ref().is_some_and(|r| r != agent) {
                continue;
            }
            // A removed agent's orbs stay there, unavailable; it gets no new ones.
            if slot.removed && restrict.is_none() {
                continue;
            }
            let info = slot
                .hello
                .as_ref()
                .and_then(|h| h.runtimes.iter().find(|r| r.kind == wire_kind));
            let (capabilities, ceiling) = match (info, slot.hello.as_ref()) {
                (Some(info), Some(hello)) => (
                    capabilities::effective(runtime, info),
                    hello.capacity.map(Resources::from).unwrap_or_default(),
                ),
                // An existing orb's location stays visible (as Unavailable) even if the
                // agent has not reconnected since a coordinator restart.
                _ if restrict.is_some() => (capabilities::ceiling(runtime), Resources::default()),
                _ => continue,
            };
            let availability = if slot.removed {
                Availability::Unavailable {
                    reason: "agent removed".into(),
                }
            } else if let Some(s) = slot.session.as_ref() {
                if s.draining {
                    Availability::Draining
                } else {
                    Availability::Available
                }
            } else if slot.unavailable_reason.is_empty() {
                Availability::Unavailable {
                    reason: "agent has not connected".into(),
                }
            } else {
                Availability::Unavailable {
                    reason: slot.unavailable_reason.clone(),
                }
            };
            out.push(PlacementCandidate {
                location: self.inner.location(agent),
                availability,
                capabilities,
                capacity_pool: agent.to_string(),
                capacity_ceiling: ceiling,
                demand: request.resources,
                disk: slot.heartbeat.as_ref().map(|(hb, _)| disk_report(hb)),
                memory: slot
                    .heartbeat
                    .as_ref()
                    .filter(|(hb, _)| hb.memory_available_bytes > 0)
                    .map(|(hb, _)| crate::model::MemoryReport {
                        available_bytes: hb.memory_available_bytes,
                        reserve_bytes: hb.memory_reserve_bytes,
                    }),
                has_project_snapshot: request
                    .project_snapshot
                    .as_ref()
                    .is_some_and(|k| slot.snapshots.contains(k.as_str())),
            });
        }
        Ok(out)
    }

    async fn submit(&self, delivery: CommandDelivery) -> Result<(), BackendError> {
        let agent = self.inner.agent_of(&delivery.location)?;
        let secrets = match &delivery.secrets {
            CommandSecrets::None => None,
            CommandSecrets::Bootstrap(env) => Some(pb::CommandSecrets {
                guest_env: env
                    .0
                    .iter()
                    .map(|(n, v)| pb::EnvVar {
                        name: n.clone(),
                        value: v.expose_secret().to_owned(),
                    })
                    .collect(),
            }),
        };
        let msg = pb::CommandDelivery {
            command: Some((&delivery.command).into()),
            controller_epoch: delivery.controller_epoch,
            orb: Some((&delivery.orb).try_into().map_err(to_bytes_err)?),
            work_remaining_ms: delivery.budget.work_remaining_ms,
            cleanup_remaining_ms: delivery.budget.cleanup_remaining_ms,
            secrets,
        };
        let key = delivery.command.key();
        self.inner.lock().op_keys.insert(key.operation_id, key);
        self.inner.send_to(
            &agent,
            pb::CoordinatorEvent {
                event: Some(pb::coordinator_event::Event::Command(msg)),
            },
        )
    }

    async fn cancel(&self, request: CancelRequest) -> Result<(), BackendError> {
        let agent = self.inner.agent_of(&request.location)?;
        self.inner.send_to(
            &agent,
            pb::CoordinatorEvent {
                event: Some(pb::coordinator_event::Event::Cancel(pb::CancelCommand {
                    key: Some(request.key.into()),
                    reason: request.reason,
                })),
            },
        )
    }

    async fn reconcile(&self, query: ReconcileQuery) -> Result<ReconcilePage, BackendError> {
        let agent = self.inner.agent_of(&query.location)?;
        let correlation_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        let req = pb::ReconcileRequest {
            correlation_id: correlation_id.clone(),
            operation_ids: query
                .operations
                .iter()
                .map(|k| k.operation_id.to_string())
                .collect(),
            orb_ids: query.orbs.iter().map(|o| o.to_string()).collect(),
            all_orbs: query.operations.is_empty() && query.orbs.is_empty(),
            page_token: query.page_token.unwrap_or_default(),
        };
        {
            let mut st = self.inner.lock();
            let session = st
                .agents
                .get_mut(&agent)
                .and_then(|s| s.session.as_mut())
                .ok_or_else(|| BackendError::Unavailable(format!("agent {agent} not connected")))?;
            session
                .tx
                .send(pb::CoordinatorEvent {
                    event: Some(pb::coordinator_event::Event::Reconcile(req)),
                })
                .map_err(|_| BackendError::Unavailable(format!("agent {agent} session closed")))?;
            session.pending_reconcile.insert(correlation_id.clone(), tx);
        }
        let resp = match tokio::time::timeout(RECONCILE_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => {
                return Err(BackendError::Unavailable(format!(
                    "agent {agent} session ended during reconcile"
                )));
            }
            Err(_) => {
                if let Some(s) = self
                    .inner
                    .lock()
                    .agents
                    .get_mut(&agent)
                    .and_then(|s| s.session.as_mut())
                {
                    s.pending_reconcile.remove(&correlation_id);
                }
                return Err(BackendError::Unavailable(format!(
                    "agent {agent} did not answer reconcile in time"
                )));
            }
        };
        page_from_wire(resp).map_err(|e| BackendError::Other(format!("agent {agent}: {e}")))
    }

    async fn acknowledge_completion(
        &self,
        location: &Location,
        key: CommandKey,
    ) -> Result<(), BackendError> {
        let agent = self.inner.agent_of(location)?;
        self.inner.lock().op_keys.remove(&key.operation_id);
        self.inner.send_to(
            &agent,
            pb::CoordinatorEvent {
                event: Some(pb::coordinator_event::Event::Ack(
                    pb::AcknowledgeCompletion {
                        key: Some(key.into()),
                    },
                )),
            },
        )
    }
}

fn page_from_wire(resp: pb::ReconcileResponse) -> Result<ReconcilePage, wire::WireError> {
    let bad = |m: &str| wire::WireError(m.to_owned());
    let mut operations = Vec::with_capacity(resp.operations.len());
    for op in &resp.operations {
        let key = CommandKey::try_from(op.key.as_ref().ok_or_else(|| bad("operation key"))?)?;
        let status = match pb::OperationStatus::try_from(op.status) {
            Ok(pb::OperationStatus::NotAccepted) => OperationStatus::NotAccepted,
            Ok(pb::OperationStatus::CancelledBeforeAccept) => {
                OperationStatus::CancelledBeforeAccept
            }
            Ok(pb::OperationStatus::Running) => OperationStatus::Running,
            Ok(pb::OperationStatus::Unknown) => OperationStatus::Unknown,
            Ok(pb::OperationStatus::Completed) => OperationStatus::Completed(
                op.outcome
                    .as_ref()
                    .ok_or_else(|| bad("completed operation without outcome"))?
                    .try_into()?,
            ),
            _ => return Err(wire::WireError(format!("operation status {}", op.status))),
        };
        operations.push((key, status));
    }
    let mut orbs = Vec::with_capacity(resp.orbs.len());
    for o in &resp.orbs {
        orbs.push((
            wire::orb_id(&o.orb_id)?,
            o.generation,
            o.state
                .as_ref()
                .ok_or_else(|| bad("orb state"))?
                .try_into()?,
        ));
    }
    Ok(ReconcilePage {
        operations,
        orbs,
        next_page_token: (!resp.next_page_token.is_empty()).then_some(resp.next_page_token),
    })
}
