//! Agent: dials the coordinator, durably accepts commands, runs one executor per orb
//! against the local runtimes, and replays terminal results until acknowledged.
//!
//! * [`db`]: SQLite owned by one thread; receipts, tombstones, orbs, epoch.
//! * [`session`]: the AgentControl client (connect/backoff, Hello, heartbeat, delivery
//!   handling, cancel, reconcile, ack, completion replay).
//! * [`executor`]: one actor per orb; strictly serialized, idempotent action logic,
//!   pinned deadlines, recovery after restart.
//! * [`logs`]: bounded log queue kept apart from control messages.
//!
//! Crash points (feature `failpoints`, `FAILPOINTS="<name>=return"` aborts):
//! `agent::accepted_committed` (receipt durable, Accepted not yet sent),
//! `agent::spawn::after_start`, `agent::spawn::after_bootstrap`,
//! `agent::pause::after_stop`, `agent::pause::after_snapshot`, `agent::delete::after_stop`,
//! `agent::delete::after_delete` (each after a runtime side effect, before the terminal
//! commit) and `agent::terminal_committed` (result durable, Completed not yet sent).
//! The fake runtime adds `fake::<method>::after_effect`.

mod db;
mod executor;
mod logs;
mod session;

use crate::config::agent::AgentConfig;
use crate::model::{OperationId, OrbId, Resources, RuntimeKind};
use crate::runtime::Runtime;
use crate::secrets::SecretKey;
use anyhow::Context;
use orbd_proto::control::v1 as pb;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

/// Handle for pushing control messages into the current session, if any.
pub(crate) struct SessionHandle {
    pub ctrl: mpsc::Sender<pb::AgentEvent>,
    pub kill: CancellationToken,
}

/// State shared by the session loop and the executors.
pub(crate) struct Shared {
    pub config: AgentConfig,
    pub db: db::Db,
    pub key: SecretKey,
    pub runtimes: HashMap<RuntimeKind, Arc<dyn Runtime>>,
    pub capacity: Resources,
    pub boot_id: String,
    pub shutdown: CancellationToken,
    pub reserved: Mutex<HashMap<OrbId, Resources>>,
    pub tokens: Mutex<HashMap<OperationId, CancellationToken>>,
    pub actors: Mutex<HashMap<OrbId, mpsc::UnboundedSender<OperationId>>>,
    pub session: Mutex<Option<SessionHandle>>,
    pub logs: Arc<logs::LogQueue>,
    pub tasks: TaskTracker,
}

impl Shared {
    /// Queues a control message for the current session. Never blocks; if the session's
    /// queue is full the session is dropped (everything is replayed on reconnect).
    pub(crate) fn send_control(&self, ev: pb::AgentEvent) {
        let g = self.session.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(s) = g.as_ref()
            && s.ctrl.try_send(ev).is_err()
        {
            s.kill.cancel();
        }
    }

    pub(crate) fn reserved_total(&self) -> Resources {
        self.reserved
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .fold(Resources::default(), |a, r| {
                a.checked_add(*r).unwrap_or(Resources {
                    vcpus: u32::MAX,
                    memory_bytes: u64::MAX,
                    disk_bytes: u64::MAX,
                })
            })
    }

    pub(crate) fn token_for(&self, op: OperationId) -> CancellationToken {
        self.tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(op)
            .or_default()
            .clone()
    }
}

fn acquire_lock(path: &std::path::Path) -> anyhow::Result<nix::fcntl::Flock<std::fs::File>> {
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))?;
    nix::fcntl::Flock::lock(f, nix::fcntl::FlockArg::LockExclusiveNonblock)
        .map_err(|(_, e)| anyhow::anyhow!("another agent holds {}: {e}", path.display()))
}

/// Runs the agent until `shutdown` is cancelled. Running orbs survive agent exit.
pub async fn run(config: AgentConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    std::fs::create_dir_all(&config.agent.state_dir)?;
    std::fs::create_dir_all(&config.agent.run_dir)?;
    let _lock = acquire_lock(&config.agent.run_dir.join("agent.lock"))?;
    let key = SecretKey::load(&config.agent.secret_key_file)?;
    let db = db::Db::open(&config.db_path())?;
    let capacity = config.agent.capacity.to_resources()?;

    let mut runtimes: HashMap<RuntimeKind, Arc<dyn Runtime>> = HashMap::new();
    if let Some(fake) = &config.runtime.fake {
        runtimes.insert(
            RuntimeKind::Fake,
            Arc::new(crate::runtime::fake::FakeRuntime::new(
                config.agent.state_dir.clone(),
                fake,
            )),
        );
    }
    if config.runtime.docker.is_some() {
        anyhow::bail!("the docker runtime is not available in this build");
    }

    if let Some(addr) = config.agent.metrics_listen
        && let Err(e) = crate::telemetry::init_metrics(addr)
    {
        tracing::warn!(error = %e, "metrics exporter not started");
    }

    let shared = Arc::new(Shared {
        config,
        db,
        key,
        runtimes,
        capacity,
        boot_id: uuid::Uuid::new_v4().to_string(),
        shutdown: shutdown.clone(),
        reserved: Mutex::default(),
        tokens: Mutex::default(),
        actors: Mutex::default(),
        session: Mutex::default(),
        logs: Arc::new(logs::LogQueue::new(256)),
        tasks: TaskTracker::new(),
    });
    tracing::info!(agent = %shared.config.agent.id, boot_id = %shared.boot_id, "agent starting");

    executor::recover(&shared).await?;
    session::run_loop(&shared).await;

    shutdown.cancel();
    shared.tasks.close();
    if tokio::time::timeout(std::time::Duration::from_secs(10), shared.tasks.wait())
        .await
        .is_err()
    {
        tracing::warn!("executors did not stop within 10s; they resume after restart");
    }
    shared
        .actors
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
    Ok(())
}
