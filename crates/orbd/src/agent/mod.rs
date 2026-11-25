//! Agent: dials the coordinator, durably accepts commands, runs one executor per orb
//! against the local runtimes, and replays terminal results until acknowledged.
//!
//! * [`db`]: SQLite owned by one thread; receipts, tombstones, orbs, epoch.
//! * [`session`]: the AgentControl client (connect/backoff, Hello, heartbeat, delivery
//!   handling, cancel, reconcile, ack, completion replay).
//! * [`executor`]: one actor per orb; strictly serialized, idempotent action logic,
//!   pinned deadlines, recovery after restart, liveness checks of running orbs.
//! * [`logs`]: bounded log queue kept apart from control messages.
//! * [`maintenance`]: graceful stop (drain: checkpoint running orbs, leave restore
//!   intents) and their restore at the next start.
//! * [`gc`]: deletes project snapshots and prepared images no orb is built on.
//! * [`checkpoint_age`]: opt-in, drops the memory checkpoints of orbs asleep longer than
//!   `ORBD_CHECKPOINT_MAX_AGE_HOURS` (they cold-boot on their next wake).
//! * [`hold`]: sleeps held in RAM (`ORBD_SLEEP_HOLD_MINUTES`): their checkpoints are
//!   written when the hold runs out, when an orb being admitted needs the memory, and on
//!   a graceful stop.
//! * [`archive`]: compresses the disks of orbs stopped and unused for
//!   `ORBD_ARCHIVE_AFTER_HOURS`.
//! * The egress flow log (`ORBD_FLOW_LOG`, [`crate::runtime::vm::flowlog`]) runs on a
//!   thread of its own, naming orbs from the network index and this agent's records.
//! * On a Linux VM agent, the DNS proxy of orbs with restricted egress
//!   ([`crate::runtime::vm::egress`]) also runs on a thread of its own, started with
//!   the first VM runtime; what orbs send it is read by codecs, jailed child processes,
//!   one per restricted orb's launch ([`crate::runtime::vm::codec`]).
//! * Before any of this, `orbd agent` seccomp-filters itself ([`crate::seccomp`]): it
//!   and every process it starts are refused the system calls that act on the whole
//!   kernel.
//!
//! Crash points (feature `failpoints`, `FAILPOINTS="<name>=return"` aborts):
//! `agent::accepted_committed` (receipt durable, Accepted not yet sent),
//! `agent::spawn::after_start`, `agent::spawn::after_bootstrap`,
//! `agent::pause::after_stop`, `agent::pause::after_snapshot`, `agent::delete::after_stop`,
//! `agent::delete::after_delete` (each after a runtime side effect, before the terminal
//! commit) and `agent::terminal_committed` (result durable, Completed not yet sent).
//! The fake runtime adds `fake::<method>::after_effect`.

mod archive;
mod checkpoint_age;
mod db;
mod detect;
mod executor;
mod gc;
mod hold;
mod identity;
mod logs;
mod maintenance;
mod session;

use crate::config::agent::AgentConfig;
use crate::model::{GIB, OperationId, OrbId, Resources, RuntimeKind};
use crate::runtime::Runtime;
use crate::secrets::SecretKey;
use anyhow::Context;
use orbd_proto::control::v1 as pb;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Orbs with a liveness check queued or running, so a slow `inspect` cannot pile
    /// checks up behind it.
    pub liveness_queued: Mutex<std::collections::HashSet<OrbId>>,
    /// Orbs with a write of their held sleep queued or running (see [`hold`]).
    pub flush_queued: Mutex<std::collections::HashSet<OrbId>>,
    /// Archives running, by orb: a command for the orb cancels its archive (see
    /// [`archive`]).
    pub archiving: Mutex<HashMap<OrbId, CancellationToken>>,
    /// Archives that failed or were not worth it, by orb: not tried again before then.
    pub archive_backoff: Mutex<HashMap<OrbId, std::time::Instant>>,
    /// The most an archived orb's restore needs beyond its archive (last measured):
    /// kept free, so one archived orb can always be restored.
    pub archive_restore_max: AtomicU64,
    /// Last measurement of the bytes allocated under the state directory.
    pub disk_allocated: AtomicU64,
    /// The same measurement per orb directory.
    pub orb_disk: Mutex<Vec<pb::OrbDiskUsage>>,
    /// Orbs asleep on a checkpoint on disk whose wake would free it, as of the last
    /// measurement (see [`disk_wake_releases`]); held sleeps are added per heartbeat.
    pub disk_wake_releases: Mutex<Vec<pb::WakeRelease>>,
    /// Free disk to keep (see [`crate::model::disk_reserve`]).
    pub disk_reserve: u64,
    /// Host memory to keep available (see `executor::admit`).
    pub memory_reserve: u64,
    /// Memory of orbs admitted lately, whose guests have not yet touched it: the host's
    /// available memory does not show it yet.
    pub recent_admits: Mutex<Vec<(std::time::Instant, u64, OrbId)>>,
    /// Writes of held sleeps that failed, by orb: the hold's expiry is not retried
    /// before then (see [`hold`]).
    pub flush_backoff: Mutex<HashMap<OrbId, std::time::Instant>>,
    /// Memory of holds pauses have reserved but not yet made (see `hold::reserve`).
    pub hold_pending: Mutex<u64>,
    /// Measured checkpoint write speed, bytes/s (0 until measured; see `hold::max_held`).
    pub save_rate: AtomicU64,
    pub tokens: Mutex<HashMap<OperationId, CancellationToken>>,
    pub actors: Mutex<HashMap<OrbId, mpsc::UnboundedSender<executor::Work>>>,
    pub session: Mutex<Option<SessionHandle>>,
    pub logs: Arc<logs::LogQueue>,
    pub tasks: TaskTracker,
    /// The image pipeline, when VM runtimes use it (for garbage collection).
    pub images: Option<crate::image::ImageStore>,
    /// Signalled after an orb was deleted: its base may now be unused.
    pub orb_deleted: tokio::sync::Notify,
    /// Project snapshots deleted since the last heartbeat, which tells the coordinator.
    pub deleted_snapshots: Mutex<Vec<String>>,
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

    /// Notes project snapshots deleted (their records forgotten), for the next heartbeat.
    pub(crate) fn snapshots_deleted(&self, keys: impl IntoIterator<Item = String>) {
        let mut d = self
            .deleted_snapshots
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        d.extend(keys);
    }

    /// Notes a project snapshot published (recorded) again: no heartbeat may report it
    /// deleted from an earlier collection.
    pub(crate) fn snapshot_published(&self, key: &str) {
        self.deleted_snapshots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k| k != key);
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

    /// The host's available memory; None where it does not bound orbs (the fake
    /// runtime, whose orbs use none).
    pub(crate) fn memory_available(&self) -> Option<u64> {
        if self.config.runtime.fake.is_some() {
            return None;
        }
        detect::mem_available()
    }

    /// Memory held by sleeps in RAM: available to new orbs once their checkpoints are
    /// written (which admission does when it needs it).
    pub(crate) fn memory_held(&self) -> u64 {
        self.held_resident_except(OrbId(uuid::Uuid::nil()))
    }

    /// [`memory_held`](Self::memory_held) less `orb`'s own.
    pub(crate) fn held_resident_except(&self, orb: OrbId) -> u64 {
        self.runtimes
            .values()
            .flat_map(|r| r.held())
            .filter(|h| h.orb != orb)
            .map(|h| h.resident_bytes)
            .sum()
    }

    /// What held sleeps' checkpoints take on disk once written.
    pub(crate) fn held_memory_bytes(&self) -> u64 {
        self.runtimes
            .values()
            .flat_map(|r| r.held())
            .map(|h| h.memory_bytes)
            .sum()
    }

    /// Disk the holds have committed: held sleeps' checkpoints, and those of holds
    /// pauses have reserved.
    pub(crate) fn holds_committed(&self) -> u64 {
        let pending = *self.hold_pending.lock().unwrap_or_else(|e| e.into_inner());
        self.held_memory_bytes().saturating_add(pending)
    }

    /// Disk committed but not written yet: what the holds have committed, restores
    /// running, and room for the largest archived orb's restore.
    pub(crate) fn disk_committed(&self) -> u64 {
        let in_flight: u64 = self.runtimes.values().map(|r| r.disk_in_flight()).sum();
        self.holds_committed()
            .saturating_add(in_flight)
            .saturating_add(self.archive_restore_max.load(Ordering::Relaxed))
    }

    /// Free disk less what is committed ([`disk_committed`](Self::disk_committed)).
    pub(crate) fn disk_room(&self) -> u64 {
        self.disk_stats().0.saturating_sub(self.disk_committed())
    }

    /// Free and total bytes of the filesystem holding the state directory.
    pub(crate) fn disk_stats(&self) -> (u64, u64) {
        disk_stats(&self.config)
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

/// Free and total bytes of the filesystem holding the state directory (0, 0 if it
/// cannot be read).
pub(crate) fn disk_stats(config: &AgentConfig) -> (u64, u64) {
    if let Some(d) = config.runtime.fake.as_ref().and_then(|f| f.disk) {
        return (d.free_gib * GIB, d.total_gib * GIB);
    }
    nix::sys::statvfs::statvfs(&config.agent.state_dir)
        .map(|s| {
            // Field widths differ per OS (block counts are u32 on macOS).
            #[allow(clippy::unnecessary_cast)]
            (
                s.blocks_available() as u64 * s.fragment_size() as u64,
                s.blocks() as u64 * s.fragment_size() as u64,
            )
        })
        .unwrap_or((0, 0))
}

/// What [`measure`] found under the state directory.
#[derive(Debug, Default, PartialEq, Eq)]
struct DiskUse {
    total: u64,
    /// Per `orbs/<orb id>` directory, sorted by orb id: (orb id, bytes, checkpoint bytes,
    /// disk archived). Blocks shared with another file (a reflinked disk) count in full.
    orbs: Vec<(String, u64, u64, bool)>,
    /// The most an archived orb's restore writes beyond its archive.
    archive_restore_max: u64,
}

/// Bytes allocated under `dir`: every file's blocks (sparse disks and checkpoints count
/// only what they hold), each inode once, without crossing into other filesystems; also
/// per orb directory (`orbs/<id>`), and the part of it under `checkpoints`.
fn measure(dir: &std::path::Path) -> DiskUse {
    use std::os::unix::fs::MetadataExt;
    let Ok(root) = std::fs::symlink_metadata(dir) else {
        return DiskUse::default();
    };
    let mut seen = std::collections::HashSet::new();
    let mut per_orb: std::collections::BTreeMap<String, (u64, u64)> = Default::default();
    let orbs_dir = dir.join("orbs");
    // (directory, its orb, inside the orb's checkpoints)
    let mut stack = vec![(dir.to_path_buf(), None::<String>, false)];
    let mut total = 0u64;
    while let Some((d, orb, in_ckpt)) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        let orbs_root = orb.is_none() && d == orbs_dir;
        for e in entries.flatten() {
            // Does not follow symlinks.
            let Ok(m) = e.metadata() else { continue };
            if m.dev() != root.dev() {
                continue;
            }
            if m.is_dir() {
                let name = e.file_name().to_string_lossy().into_owned();
                let (orb, in_ckpt) = match &orb {
                    None if orbs_root => {
                        per_orb.entry(name.clone()).or_default();
                        (Some(name), false)
                    }
                    None => (None, false),
                    Some(o) => (Some(o.clone()), in_ckpt || name == "checkpoints"),
                };
                stack.push((e.path(), orb, in_ckpt));
            } else if m.nlink() <= 1 || seen.insert(m.ino()) {
                let bytes = m.blocks().saturating_mul(512);
                total = total.saturating_add(bytes);
                if let Some(o) = &orb {
                    let u = per_orb.entry(o.clone()).or_default();
                    u.0 = u.0.saturating_add(bytes);
                    if in_ckpt {
                        u.1 = u.1.saturating_add(bytes);
                    }
                }
            }
        }
    }
    let archive_restore_max = per_orb
        .keys()
        .filter_map(|o| {
            let info: crate::runtime::ArchiveInfo =
                serde_json::from_slice(&std::fs::read(orbs_dir.join(o).join("archive.json")).ok()?)
                    .ok()?;
            (!orbs_dir.join(o).join("disk.raw").exists())
                .then(|| info.allocated_bytes.saturating_sub(info.compressed_bytes))
        })
        .max()
        .unwrap_or(0);
    DiskUse {
        total,
        archive_restore_max,
        orbs: per_orb
            .into_iter()
            .map(|(o, (b, c))| {
                let d = orbs_dir.join(&o);
                let archived = !d.join("disk.raw").exists() && d.join("disk.raw.zst").is_file();
                (o, b, c, archived)
            })
            .collect(),
    }
}

/// What waking `def`, asleep on `checkpoint` on disk, frees: the checkpoint's bytes,
/// if its runtime frees it at the wake ([`crate::runtime::Runtime::restore_frees_checkpoint`])
/// and the orb is not held in RAM (held sleeps are reported per heartbeat).
async fn disk_wake_release(
    sh: &Arc<Shared>,
    def: &crate::model::OrbDefinition,
    checkpoint: &crate::model::SleepCheckpointRef,
) -> Option<pb::WakeRelease> {
    let rt = executor::runtime_for(sh, def)?;
    if !rt.restore_frees_checkpoint() || rt.held().iter().any(|h| h.orb == def.id) {
        return None;
    }
    match rt.checkpoint_info(def.id, checkpoint).await {
        Ok(Some(info)) if info.bytes > 0 => Some(pb::WakeRelease {
            orb_id: def.id.to_string(),
            checkpoint_generation: checkpoint.generation,
            bytes: info.bytes,
        }),
        _ => None,
    }
}

/// [`disk_wake_release`] of every orb recorded asleep on a memory checkpoint, with no
/// operation or restore under way.
async fn disk_wake_releases(sh: &Arc<Shared>) -> Vec<pb::WakeRelease> {
    let found = sh
        .db
        .call(|c| Ok((db::list_orb_definitions(c)?, db::list_orbs_claimed(c)?)))
        .await;
    let Ok((defs, rows)) = found else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (row, claimed) in rows {
        let Some(crate::model::OrbState::Known {
            state: crate::model::StableOrbState::Sleeping { checkpoint },
        }) = &row.last_state
        else {
            continue;
        };
        if claimed {
            continue;
        }
        let Some(def) = defs.iter().find(|d| d.id == row.orb_id) else {
            continue;
        };
        out.extend(disk_wake_release(sh, def, checkpoint).await);
    }
    out
}

/// Updates what waking `def` frees once one of its operations ended in `observed`, so
/// the next heartbeat tells the coordinator without waiting for the next measurement.
pub(crate) async fn refresh_wake_release(
    sh: &Arc<Shared>,
    def: &crate::model::OrbDefinition,
    observed: &crate::model::OrbState,
) {
    let release = match observed {
        crate::model::OrbState::Known {
            state: crate::model::StableOrbState::Sleeping { checkpoint },
        } => disk_wake_release(sh, def, checkpoint).await,
        _ => None,
    };
    let id = def.id.to_string();
    let mut v = sh.disk_wake_releases.lock().unwrap();
    v.retain(|r| r.orb_id != id);
    v.extend(release);
}

/// Measures the state directory now and then every minute, for heartbeats.
async fn measure_disk(sh: &Arc<Shared>) {
    let dir = sh.config.agent.state_dir.clone();
    let used = tokio::task::spawn_blocking(move || measure(&dir))
        .await
        .unwrap_or_default();
    sh.disk_allocated.store(used.total, Ordering::Relaxed);
    sh.archive_restore_max
        .store(used.archive_restore_max, Ordering::Relaxed);
    *sh.orb_disk.lock().unwrap() = used
        .orbs
        .into_iter()
        .map(
            |(orb_id, allocated_bytes, checkpoint_bytes, archived)| pb::OrbDiskUsage {
                orb_id,
                allocated_bytes,
                checkpoint_bytes,
                archived,
            },
        )
        .collect();
    let releases = disk_wake_releases(sh).await;
    *sh.disk_wake_releases.lock().unwrap() = releases;
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
    let Some(identity) = identity::Identity::establish(
        &config.agent.state_dir,
        join.as_deref(),
        config.agent.coordinator_url.as_deref(),
        &stop,
    )
    .await?
    else {
        return Ok(());
    };
    #[cfg(target_os = "linux")]
    let flow_log = flow_log_socket(&mut config.agent);
    detect::runtime(&mut config).await?;
    // Only orbs in orbd's network namespaces are logged: every Linux VM orb.
    #[cfg(target_os = "linux")]
    let flow_log =
        flow_log.filter(|_| config.runtime.firecracker.is_some() || config.runtime.qemu.is_some());
    let allow = config.agent.image_allow.clone();
    if allow.is_empty() {
        tracing::info!("ORBD_IMAGE_ALLOW is empty: any image may be prepared and run");
    } else {
        tracing::info!(%allow, "only images ORBD_IMAGE_ALLOW lists are prepared and run");
    }
    if let Some(i) = config.images.as_mut() {
        i.allow = allow.clone();
        i.sandbox = config.agent.image_sandbox;
    }
    if let Some(d) = config.runtime.docker.as_mut() {
        d.prewarm_images.retain(|r| match allow.check(r) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(image_ref = %r, "not prewarming: {e}");
                false
            }
        });
    }
    let capacity = detect::capacity(&config).await?;
    let disk_reserve = match config.agent.capacity.disk_reserve_gib {
        Some(g) => g.saturating_mul(GIB),
        None => crate::model::disk_reserve(disk_stats(&config).1),
    };
    tracing::info!(disk_reserve_gib = disk_reserve / GIB, "disk reserve");
    if let Some(f) = config.runtime.firecracker.as_mut() {
        f.disk_reserve = disk_reserve;
    }
    let memory_reserve = match config.agent.capacity.memory_reserve_gib {
        Some(g) => g.saturating_mul(GIB),
        None => (4 * GIB).max(detect::mem_total().unwrap_or(0) / 10),
    };
    tracing::info!(memory_reserve_gib = memory_reserve / GIB, "memory reserve");

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
    let mut image_store = None;
    let vm_images = if config.runtime.qemu.is_some() || config.runtime.firecracker.is_some() {
        let images: Arc<dyn crate::runtime::vm::images::ImageProvider> = match &config.images {
            Some(i) if !i.static_images.is_empty() => Arc::new(
                crate::runtime::vm::images::StaticImages(i.static_images.clone()),
            ),
            Some(i) => {
                let store = crate::image::ImageStore::open(i, &config.agent.state_dir)
                    .map_err(|e| anyhow::anyhow!("image store: {e}"))?;
                store.check_sandbox().await.map_err(anyhow::Error::msg)?;
                image_store = Some(store.clone());
                Arc::new(store)
            }
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
    if vm_images.is_some() {
        crate::runtime::vm::reflink::probe(&config.agent.state_dir);
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
        liveness_queued: Mutex::default(),
        flush_queued: Mutex::default(),
        flush_backoff: Mutex::default(),
        hold_pending: Mutex::default(),
        save_rate: AtomicU64::new(0),
        archiving: Mutex::default(),
        archive_backoff: Mutex::default(),
        archive_restore_max: AtomicU64::new(0),
        disk_allocated: AtomicU64::new(0),
        orb_disk: Mutex::default(),
        disk_wake_releases: Mutex::default(),
        disk_reserve,
        memory_reserve,
        recent_admits: Mutex::default(),
        tokens: Mutex::default(),
        actors: Mutex::default(),
        session: Mutex::default(),
        logs: Arc::new(logs::LogQueue::new(256)),
        tasks: TaskTracker::new(),
        images: image_store,
        orb_deleted: tokio::sync::Notify::new(),
        deleted_snapshots: Mutex::default(),
    });
    tracing::info!(agent = %shared.identity.agent_id, boot_id = %shared.boot_id, "agent starting");
    #[cfg(target_os = "linux")]
    if let Some(socket) = flow_log {
        start_flow_log(&shared, socket);
    }

    // Ready (Hello) only after interrupted work is requeued and drained orbs restored.
    executor::recover(&shared).await?;
    tokio::spawn(executor::watch(shared.clone()));
    tokio::spawn(gc::run_loop(shared.clone()));
    tokio::spawn(checkpoint_age::run_loop(shared.clone()));
    tokio::spawn(hold::run_loop(shared.clone()));
    tokio::spawn(archive::run_loop(shared.clone()));
    measure_disk(&shared).await;
    tokio::spawn({
        let sh = shared.clone();
        async move {
            loop {
                tokio::select! {
                    _ = sh.shutdown.cancelled() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
                }
                measure_disk(&sh).await;
            }
        }
    });
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

/// The egress flow log's socket, bound before the runtimes get `agent.egress` and
/// install orbd's nftables table, which logs to it only when this worked.
#[cfg(target_os = "linux")]
fn flow_log_socket(
    agent: &mut crate::config::agent::AgentSection,
) -> Option<crate::runtime::vm::flowlog::Socket> {
    use crate::runtime::vm::flowlog;
    if !agent.egress.flow_log {
        return None;
    }
    match flowlog::Socket::open(flowlog::GROUP) {
        Ok(s) => Some(s),
        Err(e) => {
            agent.egress.flow_log = false;
            tracing::error!(error = %e, "egress flow log off: cannot read NFLOG");
            None
        }
    }
}

/// Reads the flow log on its own thread until the agent stops, naming each orb by its
/// network index and its Amp orb id from this agent's records.
#[cfg(target_os = "linux")]
fn start_flow_log(sh: &Arc<Shared>, socket: crate::runtime::vm::flowlog::Socket) {
    use crate::runtime::vm::{flowlog, net};
    let state_dir = sh.config.agent.state_dir.clone();
    let agent_db = sh.db.clone();
    let handle = tokio::runtime::Handle::current();
    let mut amp_ids: HashMap<crate::model::OrbId, String> = HashMap::new();
    let resolve = move |index: u32| {
        let orb = net::owner(&state_dir, index)?;
        if amp_ids.len() >= 4096 {
            amp_ids.clear();
        }
        let amp_orb_id = match amp_ids.get(&orb) {
            Some(a) => Some(a.clone()),
            None => {
                let found = handle
                    .block_on(agent_db.call(move |c| db::get_orb_definition(c, orb)))
                    .ok()
                    .flatten()
                    .map(|d| d.identity.amp_orb_id.0);
                if let Some(a) = &found {
                    amp_ids.insert(orb, a.clone());
                }
                found
            }
        };
        Some(flowlog::FlowOrb {
            orb: orb.to_string(),
            amp_orb_id,
        })
    };
    if let Err(e) = flowlog::spawn(socket, resolve, net::flow_log_unlogged, sh.shutdown.clone()) {
        tracing::error!(error = %e, "egress flow log off: starting its thread");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measure_splits_orbs_and_their_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let w = |p: &str, len: usize| {
            let p = dir.path().join(p);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, vec![1u8; len]).unwrap();
        };
        w("orbs/a/disk.raw", 64 << 10);
        w("orbs/a/checkpoints/3/memory", 128 << 10);
        w("orbs/b/logs/console.log", 16 << 10);
        w("images/x/rootfs", 32 << 10);
        w("orbs/c/disk.raw.zst", 4 << 10);
        std::fs::create_dir_all(dir.path().join("orbs/empty")).unwrap();
        let used = measure(dir.path());
        let archived: Vec<&str> = used
            .orbs
            .iter()
            .filter(|o| o.3)
            .map(|o| o.0.as_str())
            .collect();
        assert_eq!(archived, ["c"], "an archive without its disk is archived");
        let orbs: HashMap<&str, (u64, u64)> = used
            .orbs
            .iter()
            .map(|(o, b, c, _)| (o.as_str(), (*b, *c)))
            .collect();
        assert_eq!(orbs.len(), 4, "{orbs:?}");
        assert_eq!(
            orbs["empty"],
            (0, 0),
            "an orb directory with nothing in it is 0"
        );
        let (a, a_ckpt) = orbs["a"];
        assert!(
            a >= 192 << 10 && a_ckpt >= 128 << 10 && a_ckpt < a,
            "{a} {a_ckpt}"
        );
        assert!(orbs["b"].0 >= 16 << 10 && orbs["b"].1 == 0);
        assert!(used.total >= a + orbs["b"].0 + (32 << 10));
    }
}
