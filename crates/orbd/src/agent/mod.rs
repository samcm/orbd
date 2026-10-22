//! Agent: dials the coordinator, durably accepts commands, runs one executor per orb
//! against the local runtimes, and replays terminal results until acknowledged.
//!
//! * [`db`]: SQLite owned by one thread; receipts, tombstones, orbs, epoch.
//! * [`session`]: the AgentControl client (connect/backoff, Hello, heartbeat, delivery
//!   handling, cancel, reconcile, ack, completion replay).
//! * [`executor`]: one actor per orb; strictly serialized, idempotent action logic,
//!   pinned deadlines, recovery after restart.
//! * [`logs`]: bounded log queue kept apart from control messages.
//! * [`maintenance`]: graceful stop (drain: checkpoint running orbs, leave restore
//!   intents) and their restore at the next start.
//!
//! Crash points (feature `failpoints`, `FAILPOINTS="<name>=return"` aborts):
//! `agent::accepted_committed` (receipt durable, Accepted not yet sent),
//! `agent::spawn::after_start`, `agent::spawn::after_bootstrap`,
//! `agent::pause::after_stop`, `agent::pause::after_snapshot`, `agent::delete::after_stop`,
//! `agent::delete::after_delete` (each after a runtime side effect, before the terminal
//! commit) and `agent::terminal_committed` (result durable, Completed not yet sent).
//! The fake runtime adds `fake::<method>::after_effect`.

mod db;
mod detect;
mod executor;
mod identity;
mod logs;
mod maintenance;
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
    pub identity: identity::Identity,
    pub db: db::Db,
    pub key: SecretKey,
    pub runtimes: HashMap<RuntimeKind, Arc<dyn Runtime>>,
    pub capacity: Resources,
    pub boot_id: String,
    /// Stops the session loop and the executors.
    pub shutdown: CancellationToken,
    /// Graceful stop in progress: no new commands, in-flight work interrupted (it stays
    /// accepted), running orbs checkpointed.
    pub draining: CancellationToken,
    pub reserved: Mutex<HashMap<OrbId, Resources>>,
    pub tokens: Mutex<HashMap<OperationId, CancellationToken>>,
    pub actors: Mutex<HashMap<OrbId, mpsc::UnboundedSender<executor::Work>>>,
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

/// Runs the agent until `stop` is cancelled (SIGTERM), then drains: running
/// memory-preserving orbs are checkpointed and restored at the next start, so they
/// survive the agent's process tree being killed (e.g. a container stop). Orbs also
/// survive a crash of the agent alone (VMMs are adopted at the next start).
/// `join` is the join token, used only until the agent is enrolled.
pub async fn run(
    mut config: AgentConfig,
    join: Option<String>,
    stop: CancellationToken,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(&config.agent.state_dir)?;
    std::fs::create_dir_all(&config.agent.run_dir)?;
    let _lock = acquire_lock(&config.agent.run_dir.join("agent.lock"))?;
    let first_init = !config.db_path().exists();
    let key = SecretKey::load_or_create(&config.agent.secret_key_file, first_init)?;
    let db = db::Db::open(&config.db_path())?;
    let Some(identity) =
        identity::Identity::establish(&config.agent.state_dir, join.as_deref(), &stop).await?
    else {
        return Ok(());
    };
    detect::runtime(&mut config).await?;
    let capacity = detect::capacity(&config).await?;

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
    if let Some(docker) = &config.runtime.docker {
        let rt = crate::runtime::docker::DockerRuntime::connect(docker.clone())
            .await
            .map_err(|e| anyhow::anyhow!("docker runtime: {e}"))?;
        runtimes.insert(RuntimeKind::Docker, Arc::new(rt));
    }
    // One image source for every VM runtime (one build lock, one cache).
    let vm_images = if config.runtime.qemu.is_some() || config.runtime.firecracker.is_some() {
        let images: Arc<dyn crate::runtime::vm::images::ImageProvider> = match &config.images {
            Some(i) if !i.static_images.is_empty() => Arc::new(
                crate::runtime::vm::images::StaticImages(i.static_images.clone()),
            ),
            Some(i) => Arc::new(
                crate::image::ImageStore::open(i, &config.agent.state_dir)
                    .map_err(|e| anyhow::anyhow!("image store: {e}"))?,
            ),
            None => anyhow::bail!("VM runtimes need `images`"),
        };
        Some(images)
    } else {
        None
    };
    if let Some(q) = &config.runtime.qemu {
        let rt = crate::runtime::qemu::QemuRuntime::new(
            q.clone(),
            &config.agent.state_dir,
            &config.agent.run_dir,
            vm_images.clone().expect("VM runtime configured"),
        )
        .map_err(|e| anyhow::anyhow!("qemu runtime: {e}"))?;
        runtimes.insert(RuntimeKind::Qemu, Arc::new(rt));
    }
    if let Some(f) = &config.runtime.firecracker {
        let rt = crate::runtime::firecracker::FirecrackerRuntime::new(
            f.clone(),
            &config.agent.state_dir,
            &config.agent.run_dir,
            vm_images.clone().expect("VM runtime configured"),
        )
        .map_err(|e| anyhow::anyhow!("firecracker runtime: {e}"))?;
        runtimes.insert(RuntimeKind::Firecracker, Arc::new(rt));
    }

    if let Some(addr) = config.agent.metrics_listen
        && let Err(e) = crate::telemetry::init_metrics(addr)
    {
        tracing::warn!(error = %e, "metrics exporter not started");
    }

    let shared = Arc::new(Shared {
        config,
        identity,
        db,
        key,
        runtimes,
        capacity,
        boot_id: uuid::Uuid::new_v4().to_string(),
        shutdown: CancellationToken::new(),
        draining: CancellationToken::new(),
        reserved: Mutex::default(),
        tokens: Mutex::default(),
        actors: Mutex::default(),
        session: Mutex::default(),
        logs: Arc::new(logs::LogQueue::new(256)),
        tasks: TaskTracker::new(),
    });
    tracing::info!(agent = %shared.identity.agent_id, boot_id = %shared.boot_id, "agent starting");

    // Ready (Hello) only after interrupted work is requeued and drained orbs restored.
    executor::recover(&shared).await?;
    tokio::spawn(identity::renew_loop(shared.clone()));
    let session = tokio::spawn({
        let sh = shared.clone();
        async move { session::run_loop(&sh).await }
    });
    stop.cancelled().await;
    maintenance::drain(&shared).await;

    shared.shutdown.cancel();
    let _ = session.await;
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
