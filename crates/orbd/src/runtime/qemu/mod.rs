//! QEMU runtime: one q35 KVM VM per orb with nested `/dev/kvm` and memory-preserving
//! sleep.
//!
//! Storage (per the agent layout):
//!
//! ```text
//! <state>/orbs/<orb>/vm.json                  immutable recipe (resources, base disk, network)
//!                   /disk.qcow2               overlay over a raw base (image or project snapshot)
//!                   /checkpoints/<gen>/       published: memory, manifest.json[, consumed]
//!                   /checkpoints/<gen>.tmp/   save in progress (complete once manifest.json exists)
//!                   /logs/{qemu.log,console.log}
//! <state>/projects/<key>/{disk.raw,meta.json} standalone sparse raw project snapshots
//! <state>/staging/, <state>/trash/            build-then-rename and delete-by-rename areas
//! <run>/qemu/<orb>/{launch.json,qemu.pid,qmp.sock,guest.sock}
//! ```
//!
//! Every publication is rename-based and fsynced, and every method first reconciles
//! what is on the host (adopting QEMUs that outlived an earlier agent, finishing or
//! discarding interrupted saves and restores), so a crash at any point converges:
//!
//! * a save has QEMU write `<gen>.tmp/memory` itself (file migration, `mapped-ram`) and
//!   publishes the checkpoint only after QEMU reported `completed`, the file was fsynced,
//!   the manifest was written and QEMU has exited; an interrupted save is either
//!   finished (complete file, QEMU never resumed) or cancelled and discarded before the
//!   guest is resumed;
//! * a restore marks the checkpoint consumed (durably) after the load and before `cont`,
//!   so stale RAM is never replayed against a disk the guest has since written; a
//!   loaded-but-unconsumed QEMU is killed (its vCPUs never ran);
//! * a cold boot consumes every published checkpoint first.
//!
//! Requires QEMU 9.0 or newer (file migration with `mapped-ram`); checkpoints restore
//! only on the exact QEMU binary, machine type, host CPU and kernel they were saved with.
//!
//! Crash points (feature `failpoints`): `qemu::create::after_publish`,
//! `qemu::save::after_stop`, `qemu::save::after_migrate` (QEMU still writing),
//! `qemu::save::after_manifest`, `qemu::save::after_quit`, `qemu::save::after_publish`,
//! `qemu::restore::after_load`, `qemu::restore::after_consume`,
//! `qemu::clone::after_publish`. Fault injection: `qemu::save::write=return` fails a
//! save that is still writing as QEMU does on ENOSPC (cancelled, partial file left);
//! `qemu::save::throttle=return(<MiB/s>)` caps the save's bandwidth.

mod fsutil;
mod guest;
pub mod images;
mod net;
mod process;
mod qmp;

pub use guest::{ByteStream, GuestConnector, VirtioSerialConnector};

use super::{
    GuestControl, LogStream, OpContext, Runtime, RuntimeCapabilities, RuntimeError,
    RuntimeObservation, StartSource,
};
use crate::config::agent::QemuRuntimeConfig;
use crate::image::ImageError;
use crate::model::{
    InitialDiskSource, MIB, OrbDefinition, OrbId, ProjectSnapshotKey, ProjectSnapshotRef,
    RuntimeKind, SleepCheckpointRef, unix_ms_now,
};
use async_trait::async_trait;
use fsutil::{
    free_bytes, fsync_dir, read_json, remove_via_trash, rename_durable, sha256_file, write_atomic,
    write_json_atomic,
};
use images::ImageProvider;
use net::NetAttachment;
use process::{
    CpuIdentity, KernelIdentity, LaunchConfig, LaunchKind, LaunchRecord, Process, QemuIdentity,
};
use qmp::{FileMigration, Qmp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Oldest QEMU with file migration and `mapped-ram`.
const MIN_QEMU: (u32, u32) = (9, 0);
/// Memory files are written and read in place (`mapped-ram`) by 8 multifd threads,
/// bypassing the page cache. Measured on a Ryzen 7940HS: about 2.2 GB/s save and 4.9 GB/s
/// restore with 4 GiB dirty, against 1.5 and 3.3 GB/s for 4 channels through the cache.
/// Without O_DIRECT on the state filesystem (tmpfs) the page cache is used.
const MIGRATION: FileMigration = FileMigration {
    multifd_channels: 8,
    direct_io: true,
};
const QMP_TIMEOUT: Duration = Duration::from_secs(5);
const PREPARE_SLEEP_TIMEOUT: Duration = Duration::from_secs(15);
/// Orb's own kernel command line (guest contract in `guest/README.md`).
const KERNEL_CMDLINE: &str = "console=ttyS0 root=/dev/vda rw";
/// How long a cold stop waits for a clean guest shutdown before quitting QEMU.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
/// Memory saves at once on this host (disk bandwidth is the bottleneck).
const CONCURRENT_SAVES: usize = 1;
/// Checkpoint headroom beyond guest RAM (device state, ROMs, stream framing).
const CHECKPOINT_MARGIN: u64 = 256 * MIB;
const POLL: Duration = Duration::from_millis(100);

/// Immutable per-orb recipe, written once with the disk.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct VmRecipe {
    orb: OrbId,
    vcpus: u32,
    memory_bytes: u64,
    disk_bytes: u64,
    base: BaseDisk,
    net: NetAttachment,
    created_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum BaseSource {
    Image {
        image_ref: String,
        key: String,
        manifest_digest: String,
    },
    ProjectSnapshot {
        key: ProjectSnapshotKey,
    },
}

/// Raw backing file of the overlay, with the identity it had when the orb was created.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BaseDisk {
    source: BaseSourceKey,
    path: PathBuf,
    size: u64,
    inode: u64,
    mtime_ns: i64,
}

/// `BaseSource` flattened for equality checks.
type BaseSourceKey = String;

impl BaseDisk {
    fn of(path: &Path, source: &BaseSource) -> std::io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let m = std::fs::metadata(path)?;
        Ok(Self {
            source: serde_json::to_string(source).map_err(std::io::Error::other)?,
            path: path.to_owned(),
            size: m.len(),
            inode: m.ino(),
            mtime_ns: m.mtime() * 1_000_000_000 + m.mtime_nsec(),
        })
    }
}

/// `checkpoints/<gen>/manifest.json`. Its presence in a `.tmp` directory marks the
/// memory file complete (QMP `completed`, stream EOF, fsync).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckpointManifest {
    orb: OrbId,
    generation: u64,
    created_unix_ms: u64,
    /// QEMU binary/version, host CPU, kernel digest and the exact machine/device args.
    launch: LaunchConfig,
    /// Base disk (image key = platform digest + recipe + guest integration digest).
    base: BaseDisk,
    ram_bytes: u64,
    /// How QEMU wrote the memory file (and must read it).
    migration: FileMigration,
    /// Apparent size; `mapped-ram` files are sparse (pages at fixed offsets, zero pages
    /// never written).
    memory_file_bytes: u64,
    /// Guest boot id at PrepareSleep, if the guest answered.
    guest_boot_id: Option<String>,
    save_ms: u64,
}

struct OrbPaths {
    dir: PathBuf,
    recipe: PathBuf,
    disk: PathBuf,
    checkpoints: PathBuf,
    console_log: PathBuf,
    qemu_log: PathBuf,
}

struct RunPaths {
    dir: PathBuf,
    launch: PathBuf,
    pidfile: PathBuf,
    qmp: PathBuf,
    guest_sock: PathBuf,
}

/// What reconciliation found on the host.
enum Observed {
    Absent,
    /// A validated QEMU process for this orb, and its QMP run state.
    Live {
        proc: Process,
        status: String,
    },
    Stopped {
        checkpoint: Option<u64>,
    },
    Inconsistent(String),
}

impl std::fmt::Display for Observed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => f.write_str("absent"),
            Self::Live { status, .. } => write!(f, "live ({status})"),
            Self::Stopped {
                checkpoint: Some(g),
            } => write!(f, "sleeping (checkpoint {g})"),
            Self::Stopped { checkpoint: None } => f.write_str("stopped"),
            Self::Inconsistent(r) => write!(f, "inconsistent: {r}"),
        }
    }
}

pub struct QemuRuntime {
    config: QemuRuntimeConfig,
    state_dir: PathBuf,
    run_dir: PathBuf,
    images: Arc<dyn ImageProvider>,
    qemu: QemuIdentity,
    /// Versioned machine type (`q35` resolved at startup), recorded in each launch.
    machine: String,
    migration: FileMigration,
    cpu: CpuIdentity,
    direct_io: bool,
    nested_kvm: bool,
    locks: Mutex<HashMap<OrbId, Arc<tokio::sync::Mutex<()>>>>,
    saves: tokio::sync::Semaphore,
}

impl QemuRuntime {
    /// Checks host prerequisites and identifies the QEMU binary and host CPU. Running
    /// VMs are adopted lazily, by the first `inspect` of each orb (agent recovery).
    pub fn new(
        config: QemuRuntimeConfig,
        state_dir: &Path,
        run_dir: &Path,
        images: Arc<dyn ImageProvider>,
    ) -> Result<Self, RuntimeError> {
        let other = |what: &str, e: &dyn std::fmt::Display| {
            RuntimeError::Other(format!("qemu runtime: {what}: {e}"))
        };
        // Disks and checkpoints hold guest secrets (credentials in files and RAM).
        for d in ["orbs", "projects", "staging", "trash"] {
            create_private_dir(&state_dir.join(d)).map_err(|e| other(d, &e))?;
        }
        let qemu_run = run_dir.join("qemu");
        std::fs::create_dir_all(&qemu_run).map_err(|e| other("run dir", &e))?;
        // Longest socket path must fit sockaddr_un.sun_path.
        let longest = qemu_run
            .join(OrbId(uuid::Uuid::nil()).to_string())
            .join("guest.sock");
        if longest.as_os_str().len() >= 108 {
            return Err(other(
                "run_dir",
                &format!("{} is too long for unix socket paths", run_dir.display()),
            ));
        }
        if !config.kernel.is_file() {
            return Err(other("kernel", &config.kernel.display()));
        }
        let qemu_path = on_path(&config.qemu)
            .ok_or_else(|| other("qemu", &format!("{} not found", config.qemu.display())))?;
        let out = std::process::Command::new(&qemu_path)
            .arg("--version")
            .output()
            .map_err(|e| other(&qemu_path.display().to_string(), &e))?;
        let version = String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_owned();
        check_qemu_version(&version).map_err(|e| other("qemu", &e))?;
        let machine = q35_machine(&qemu_path).map_err(|e| other("qemu", &e))?;
        let qemu = QemuIdentity {
            sha256: sha256_file(&qemu_path).map_err(|e| other("qemu binary", &e))?,
            path: qemu_path,
            version,
        };
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let field = |name: &str| {
            cpuinfo
                .lines()
                .find(|l| l.starts_with(name))
                .and_then(|l| l.split_once(':'))
                .map(|(_, v)| v.trim().to_owned())
                .unwrap_or_default()
        };
        use sha2::{Digest, Sha256};
        let cpu = CpuIdentity {
            model: field("model name"),
            flags_sha256: hex::encode(Sha256::digest(field("flags").as_bytes())),
        };
        // Nested virtualization is on unless the loaded KVM module says it is off.
        let nested_kvm = Path::new("/dev/kvm").exists()
            && !["kvm_amd", "kvm_intel"].iter().any(|m| {
                std::fs::read_to_string(format!("/sys/module/{m}/parameters/nested"))
                    .is_ok_and(|v| matches!(v.trim(), "0" | "N"))
            });
        let direct_io = probe_direct_io(&state_dir.join("orbs"));
        let migration = FileMigration {
            direct_io: MIGRATION.direct_io && direct_io,
            ..MIGRATION
        };
        tracing::info!(qemu = %qemu.version, %machine, cpu = %cpu.model, direct_io, ?migration, nested_kvm, "qemu runtime ready");
        Ok(Self {
            saves: tokio::sync::Semaphore::new(CONCURRENT_SAVES),
            config,
            state_dir: state_dir.to_owned(),
            run_dir: run_dir.to_owned(),
            images,
            qemu,
            machine,
            migration,
            cpu,
            direct_io,
            nested_kvm,
            locks: Mutex::default(),
        })
    }

    fn orb_paths(&self, orb: OrbId) -> OrbPaths {
        let dir = self.state_dir.join("orbs").join(orb.to_string());
        OrbPaths {
            recipe: dir.join("vm.json"),
            disk: dir.join("disk.qcow2"),
            checkpoints: dir.join("checkpoints"),
            console_log: dir.join("logs/console.log"),
            qemu_log: dir.join("logs/qemu.log"),
            dir,
        }
    }

    fn run_paths(&self, orb: OrbId) -> RunPaths {
        let dir = self.run_dir.join("qemu").join(orb.to_string());
        RunPaths {
            launch: dir.join("launch.json"),
            pidfile: dir.join("qemu.pid"),
            qmp: dir.join("qmp.sock"),
            guest_sock: dir.join("guest.sock"),
            dir,
        }
    }

    fn trash(&self) -> PathBuf {
        self.state_dir.join("trash")
    }

    fn project_dir(&self, key: &ProjectSnapshotKey) -> PathBuf {
        self.state_dir.join("projects").join(key.as_str())
    }

    fn cgroup_dir(&self, orb: OrbId) -> Option<PathBuf> {
        self.config
            .cgroup_parent
            .as_ref()
            .map(|p| p.join(format!("orbd-{orb}")))
    }

    async fn lock(&self, orb: OrbId) -> tokio::sync::OwnedMutexGuard<()> {
        let m = self
            .locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(orb)
            .or_default()
            .clone();
        m.lock_owned().await
    }

    fn read_recipe(&self, orb: OrbId) -> Result<VmRecipe, RuntimeError> {
        read_json(&self.orb_paths(orb).recipe)
            .map_err(|e| RuntimeError::Other(format!("reading vm.json: {e}")))?
            .ok_or_else(|| RuntimeError::NotFound(format!("orb {orb} has no disk")))
    }

    // --- checkpoints on disk ----------------------------------------------------

    fn checkpoint_dir(&self, orb: OrbId, generation: u64) -> PathBuf {
        self.orb_paths(orb).checkpoints.join(generation.to_string())
    }

    fn tmp_dir(&self, orb: OrbId, generation: u64) -> PathBuf {
        self.orb_paths(orb)
            .checkpoints
            .join(format!("{generation}.tmp"))
    }

    /// Published generations (with their consumed flag) and in-progress generations.
    fn list_checkpoints(&self, orb: OrbId) -> (Vec<(u64, bool)>, Vec<u64>) {
        let (mut published, mut tmp) = (Vec::new(), Vec::new());
        if let Ok(rd) = std::fs::read_dir(self.orb_paths(orb).checkpoints) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if let Some(g) = name.strip_suffix(".tmp").and_then(|g| g.parse().ok()) {
                    tmp.push(g);
                } else if let Ok(g) = name.parse::<u64>() {
                    published.push((g, e.path().join("consumed").exists()));
                }
            }
        }
        published.sort_unstable();
        tmp.sort_unstable();
        (published, tmp)
    }

    /// Consumed: marked so, or already discarded (only consumed checkpoints are).
    fn is_consumed(&self, orb: OrbId, generation: u64) -> bool {
        let dir = self.checkpoint_dir(orb, generation);
        !dir.is_dir() || dir.join("consumed").exists()
    }

    /// Durably marks a published checkpoint consumed: it is never restored again.
    fn consume(&self, orb: OrbId, generation: u64) -> std::io::Result<()> {
        let dir = self.checkpoint_dir(orb, generation);
        if dir.is_dir() && !dir.join("consumed").exists() {
            write_atomic(&dir.join("consumed"), b"")?;
        }
        Ok(())
    }

    fn consume_all(&self, orb: OrbId) -> std::io::Result<()> {
        for (g, consumed) in self.list_checkpoints(orb).0 {
            if !consumed {
                tracing::info!(orb = %orb, generation = g, "checkpoint is stale; marking consumed");
                self.consume(orb, g)?;
            }
        }
        Ok(())
    }

    fn discard_tmps(&self, orb: OrbId) -> std::io::Result<()> {
        for g in self.list_checkpoints(orb).1 {
            tracing::info!(orb = %orb, generation = g, "discarding unfinished checkpoint");
            remove_via_trash(&self.tmp_dir(orb, g), &self.trash())?;
        }
        Ok(())
    }

    /// Renames a complete `.tmp` checkpoint into place (the publication boundary).
    fn publish(&self, orb: OrbId, generation: u64) -> std::io::Result<()> {
        let dest = self.checkpoint_dir(orb, generation);
        if dest.exists() {
            remove_via_trash(&dest, &self.trash())?;
        }
        rename_durable(&self.tmp_dir(orb, generation), &dest)
    }

    fn clear_run(&self, orb: OrbId) {
        let _ = std::fs::remove_dir_all(self.run_paths(orb).dir);
    }

    // --- reconciliation ---------------------------------------------------------

    /// Brings the orb to a stable state after any interruption and reports it. Callers
    /// hold the orb lock.
    async fn reconcile(&self, orb: OrbId) -> Result<Observed, RuntimeError> {
        let op = self.orb_paths(orb);
        let rp = self.run_paths(orb);
        let record = process::read_record(&rp.launch);
        let proc = record
            .as_ref()
            .and_then(|r| Process::adopt(r, &orb.to_string(), &rp.pidfile));
        if !op.dir.is_dir() {
            if let Some(p) = proc {
                return Ok(Observed::Inconsistent(format!(
                    "QEMU process {} runs without a disk",
                    p.pid
                )));
            }
            self.clear_run(orb);
            return Ok(Observed::Absent);
        }
        match (proc, record) {
            (Some(proc), Some(record)) => {
                if let Some(live) = self.settle_live(orb, proc, &record).await? {
                    return Ok(live);
                }
            }
            _ => self.clear_run(orb),
        }
        self.settle_stopped(orb)
    }

    /// Resolves a live QEMU left in a transitional state. `None` means the process was
    /// stopped and the orb is now judged from its files.
    async fn settle_live(
        &self,
        orb: OrbId,
        proc: Process,
        record: &LaunchRecord,
    ) -> Result<Option<Observed>, RuntimeError> {
        let rp = self.run_paths(orb);
        let qmp = match Qmp::connect(&rp.qmp, QMP_TIMEOUT).await {
            Ok(q) => q,
            Err(e) => {
                return Ok(Some(Observed::Inconsistent(format!(
                    "QEMU process {} does not answer on QMP: {e}",
                    proc.pid
                ))));
            }
        };
        let expected = record.name(&orb.to_string());
        if qmp.name().await.map_err(qmp_err)?.as_deref() != Some(expected.as_str()) {
            return Ok(Some(Observed::Inconsistent(format!(
                "QEMU process {} is not this orb's launch",
                proc.pid
            ))));
        }
        let status = qmp.status().await.map_err(qmp_err)?;
        if let LaunchKind::Restore { generation } = record.kind
            && !self.is_consumed(orb, generation)
        {
            if status == "running" {
                // Cannot happen (consumed before cont); be safe: RAM has moved on.
                tracing::error!(orb = %orb, generation, "restored VM runs with an unconsumed checkpoint");
                self.consume(orb, generation).map_err(io_err)?;
            } else {
                // Loaded or loading, vCPUs never ran: drop it, the checkpoint stays valid.
                tracing::warn!(orb = %orb, generation, %status, "discarding an unfinished restore");
                drop(qmp);
                proc.kill_and_wait(Duration::from_secs(10)).await;
                self.clear_run(orb);
                return Ok(None);
            }
        }
        // QEMU writes a save's memory file itself, so a save whose agent died may still
        // be running. Stop it: without its agent it gets no manifest and is discarded.
        cancel_migration(&qmp).await?;
        let mut status = qmp.status().await.map_err(qmp_err)?;
        for _ in 0..100 {
            if !matches!(status.as_str(), "finish-migrate" | "save-vm" | "restore-vm") {
                break;
            }
            tokio::time::sleep(POLL).await;
            status = qmp.status().await.map_err(qmp_err)?;
        }
        match status.as_str() {
            "running" => {
                self.discard_tmps(orb).map_err(io_err)?;
                self.consume_all(orb).map_err(io_err)?;
            }
            "paused" | "postmigrate" | "prelaunch" => {
                let complete = self
                    .list_checkpoints(orb)
                    .1
                    .into_iter()
                    .rev()
                    .find(|g| self.tmp_dir(orb, *g).join("manifest.json").exists());
                if let (Some(g), "postmigrate") = (complete, status.as_str()) {
                    // A save finished but its agent died before QEMU exited: the guest
                    // never resumed, so finish the save.
                    tracing::warn!(orb = %orb, generation = g, "finishing an interrupted save");
                    let _ = qmp.quit().await;
                    drop(qmp);
                    if !proc
                        .wait_exit(Instant::now() + Duration::from_secs(10))
                        .await
                        && !proc.kill_and_wait(Duration::from_secs(10)).await
                    {
                        return Ok(Some(Observed::Inconsistent(format!(
                            "QEMU process {} does not exit",
                            proc.pid
                        ))));
                    }
                    self.clear_run(orb);
                    return Ok(None);
                }
                // An interrupted save (or a restore whose cont was lost): discard any
                // partial checkpoint, then resume the guest where it stopped.
                tracing::warn!(orb = %orb, %status, "resuming a VM left stopped by an interrupted operation");
                self.discard_tmps(orb).map_err(io_err)?;
                qmp.cont().await.map_err(qmp_err)?;
                status = qmp.status().await.map_err(qmp_err)?;
            }
            _ => {}
        }
        Ok(Some(Observed::Live { proc, status }))
    }

    /// No process: publish complete saves, discard partial ones, report the latest
    /// restorable checkpoint.
    fn settle_stopped(&self, orb: OrbId) -> Result<Observed, RuntimeError> {
        for g in self.list_checkpoints(orb).1 {
            if self.tmp_dir(orb, g).join("manifest.json").exists() {
                tracing::warn!(orb = %orb, generation = g, "publishing a complete checkpoint left by an interrupted save");
                self.publish(orb, g).map_err(io_err)?;
            } else {
                remove_via_trash(&self.tmp_dir(orb, g), &self.trash()).map_err(io_err)?;
            }
        }
        let live: Vec<u64> = self
            .list_checkpoints(orb)
            .0
            .into_iter()
            .filter(|(_, consumed)| !consumed)
            .map(|(g, _)| g)
            .collect();
        let mut latest = live.last().copied();
        for g in &live[..live.len().saturating_sub(1)] {
            tracing::warn!(orb = %orb, generation = g, "older unconsumed checkpoint; marking consumed");
            self.consume(orb, *g).map_err(io_err)?;
        }
        // A checkpoint this host can no longer restore exactly (QEMU, CPU, kernel or
        // machine changed) is dropped: the orb cold-boots and its processes are lost.
        if let Some(g) = latest
            && let Err(e) = self.restorable(orb, g)
        {
            tracing::error!(orb = %orb, generation = g, error = %e, "checkpoint cannot be restored; the orb will cold-boot");
            self.consume(orb, g).map_err(io_err)?;
            latest = None;
        }
        Ok(Observed::Stopped { checkpoint: latest })
    }

    async fn inspect_inner(&self, orb: OrbId) -> Result<RuntimeObservation, RuntimeError> {
        let _g = self.lock(orb).await;
        Ok(match self.reconcile(orb).await? {
            Observed::Absent => RuntimeObservation::Absent,
            Observed::Live { status, .. } if status == "running" => RuntimeObservation::Running,
            Observed::Live { proc, status } => RuntimeObservation::Inconsistent {
                reason: format!("QEMU process {} is {status}", proc.pid),
            },
            Observed::Stopped { checkpoint } => RuntimeObservation::Stopped {
                checkpoint: checkpoint.map(|generation| SleepCheckpointRef { generation }),
            },
            Observed::Inconsistent(reason) => RuntimeObservation::Inconsistent { reason },
        })
    }

    // --- launching --------------------------------------------------------------

    fn boot_config(&self, recipe: &VmRecipe) -> Result<LaunchConfig, RuntimeError> {
        if !recipe.memory_bytes.is_multiple_of(MIB) || recipe.memory_bytes == 0 {
            return Err(RuntimeError::Other(format!(
                "guest memory {} is not a whole number of MiB",
                recipe.memory_bytes
            )));
        }
        let kernel = KernelIdentity {
            path: self.config.kernel.clone(),
            sha256: sha256_file(&self.config.kernel)
                .map_err(|e| RuntimeError::Other(format!("kernel: {e}")))?,
        };
        let mut a: Vec<String> = [
            "-machine",
            &format!("{},accel=kvm", self.machine),
            "-cpu",
            "host",
            "-smp",
            &recipe.vcpus.to_string(),
            "-m",
            &format!("{}M", recipe.memory_bytes / MIB),
            "-kernel",
            &kernel.path.display().to_string(),
            "-append",
            &format!(
                "{KERNEL_CMDLINE} systemd.machine_id={}",
                machine_id(recipe.orb)
            ),
            "-serial",
            "chardev:serial0",
            "-device",
            "virtio-serial-pci,id=guestbus",
            "-device",
            "virtserialport,id=guestctl,bus=guestbus.0,nr=1,chardev=guest,name=org.orbd.control",
            "-device",
            "virtio-blk-pci,id=disk0dev,drive=disk0",
        ]
        .map(String::from)
        .into();
        a.extend(recipe.net.qemu_args());
        a.extend(
            [
                "-device",
                "virtio-balloon-pci,id=balloon0,free-page-reporting=on",
            ]
            .map(String::from),
        );
        Ok(LaunchConfig {
            qemu: self.qemu.clone(),
            cpu: self.cpu.clone(),
            kernel,
            machine_args: a,
        })
    }

    /// Host-side plumbing for one launch (not part of the guest-visible machine).
    fn host_args(&self, orb: OrbId, record: &LaunchRecord) -> Vec<String> {
        let op = self.orb_paths(orb);
        let rp = self.run_paths(orb);
        let esc = |p: &Path| p.display().to_string().replace(',', ",,");
        let file = serde_json::json!({
            "driver": "file",
            "filename": op.disk,
            "aio": if self.direct_io { "native" } else { "threads" },
            "cache": { "direct": self.direct_io },
        });
        let blockdev = serde_json::json!({
            "driver": "qcow2",
            "node-name": "disk0",
            "discard": "unmap",
            "cache": { "direct": self.direct_io },
            "file": file,
        });
        let mut a = vec![
            "-name".into(),
            format!("guest={},debug-threads=on", record.name(&orb.to_string())),
            "-nodefaults".into(),
            "-no-user-config".into(),
            "-nographic".into(),
            "-sandbox".into(),
            "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny".into(),
            "-pidfile".into(),
            rp.pidfile.display().to_string(),
            "-chardev".into(),
            format!("socket,id=qmp,path={},server=on,wait=off", esc(&rp.qmp)),
            "-mon".into(),
            "chardev=qmp,mode=control".into(),
            "-chardev".into(),
            format!(
                "socket,id=guest,path={},server=on,wait=off",
                esc(&rp.guest_sock)
            ),
            "-chardev".into(),
            format!("file,id=serial0,path={},append=on", esc(&op.console_log)),
            "-blockdev".into(),
            blockdev.to_string(),
        ];
        if let LaunchKind::Restore { .. } = record.kind {
            a.extend(["-S", "-incoming", "defer"].map(String::from));
        }
        a
    }

    /// Starts QEMU and waits until its QMP socket answers as this launch.
    async fn launch(
        &self,
        orb: OrbId,
        kind: LaunchKind,
        config: LaunchConfig,
        ctx: &OpContext,
    ) -> Result<Process, RuntimeError> {
        let rp = self.run_paths(orb);
        self.clear_run(orb);
        create_private_dir(&rp.dir).map_err(io_err)?;
        let record = LaunchRecord {
            nonce: uuid::Uuid::new_v4().simple().to_string(),
            boot_id: process::host_boot_id(),
            kind,
            config,
            pid: None,
            start_time: None,
        };
        let mut args = self.host_args(orb, &record);
        args.extend(record.config.machine_args.iter().cloned());
        let op = self.orb_paths(orb);
        let (proc, record) = process::spawn(process::Spawn {
            qemu: &record.config.qemu.path.clone(),
            args,
            log_file: &op.qemu_log,
            record_path: &rp.launch,
            record,
            cgroup: self.cgroup_dir(orb),
        })
        .map_err(|e| RuntimeError::Other(format!("launching QEMU: {e}")))?;
        log_ctx(ctx, &format!("QEMU started (pid {})", proc.pid));
        let expected = record.name(&orb.to_string());
        let ready_by = Instant::now() + Duration::from_secs(30);
        loop {
            if !proc.is_alive() {
                return Err(RuntimeError::Other(format!(
                    "QEMU exited during start: {}",
                    log_tail(&op.qemu_log)
                )));
            }
            if let Ok(q) = Qmp::connect(&rp.qmp, Duration::from_secs(1)).await
                && q.name().await.ok().flatten().as_deref() == Some(expected.as_str())
            {
                return Ok(proc);
            }
            if Instant::now() >= ready_by {
                proc.kill_and_wait(Duration::from_secs(5)).await;
                return Err(RuntimeError::Other(
                    "QEMU did not open its QMP socket within 30s".into(),
                ));
            }
            if let Err(e) = bounded(ctx, tokio::time::sleep(POLL)).await {
                proc.kill_and_wait(Duration::from_secs(5)).await;
                return Err(e);
            }
        }
    }

    async fn qmp(&self, orb: OrbId) -> Result<Qmp, RuntimeError> {
        Qmp::connect(&self.run_paths(orb).qmp, QMP_TIMEOUT)
            .await
            .map_err(qmp_err)
    }

    fn verify_base(&self, recipe: &VmRecipe) -> Result<(), RuntimeError> {
        let source: BaseSource = serde_json::from_str(&recipe.base.source)
            .map_err(|e| RuntimeError::Other(format!("vm.json base: {e}")))?;
        let now = BaseDisk::of(&recipe.base.path, &source).map_err(|e| {
            RuntimeError::Other(format!(
                "base disk {} is unavailable: {e}",
                recipe.base.path.display()
            ))
        })?;
        if now != recipe.base {
            return Err(RuntimeError::Other(format!(
                "base disk {} changed since the orb was created",
                recipe.base.path.display()
            )));
        }
        recipe.net.adopt().map_err(RuntimeError::Other)
    }

    async fn boot(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), RuntimeError> {
        let recipe = self.read_recipe(orb.id)?;
        self.verify_base(&recipe)?;
        let config = self.boot_config(&recipe)?;
        let proc = self.launch(orb.id, LaunchKind::Boot, config, ctx).await?;
        let status = self.qmp(orb.id).await?.status().await.map_err(qmp_err)?;
        if status != "running" {
            proc.kill_and_wait(Duration::from_secs(5)).await;
            return Err(RuntimeError::Other(format!("QEMU is {status} after boot")));
        }
        log_ctx(ctx, "VM booted");
        Ok(())
    }

    async fn create_disk(
        &self,
        orb: &OrbDefinition,
        src: InitialDiskSource,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        let (base_path, source) = match src {
            InitialDiskSource::Image { image_ref } => {
                let img = self
                    .images
                    .prepared(&image_ref)
                    .await
                    .map_err(|e| match e {
                        ImageError::NotReady(m) => RuntimeError::ImageNotReady(m),
                        ImageError::Failed(m) => RuntimeError::Other(m),
                    })?;
                (
                    img.rootfs,
                    BaseSource::Image {
                        image_ref,
                        key: img.key,
                        manifest_digest: img.manifest_digest,
                    },
                )
            }
            InitialDiskSource::ProjectSnapshot { snapshot: r, .. } => {
                let disk = self.project_dir(&r.key).join("disk.raw");
                if !disk.is_file() {
                    return Err(RuntimeError::NotFound(format!(
                        "project snapshot {} is not on this host",
                        r.key
                    )));
                }
                (disk, BaseSource::ProjectSnapshot { key: r.key })
            }
        };
        let res = orb.spec.template.resources;
        let base = BaseDisk::of(&base_path, &source).map_err(io_err)?;
        if res.disk_bytes < base.size {
            return Err(RuntimeError::Other(format!(
                "offering disk ({} bytes) is smaller than the base image ({} bytes)",
                res.disk_bytes, base.size
            )));
        }
        let recipe = VmRecipe {
            orb: orb.id,
            vcpus: res.vcpus,
            memory_bytes: res.memory_bytes,
            disk_bytes: res.disk_bytes,
            base,
            net: NetAttachment::prepare(orb.id),
            created_unix_ms: unix_ms_now(),
        };
        let staging = self.state_dir.join("staging").join(format!(
            "orb-{}-{}",
            orb.id,
            uuid::Uuid::new_v4().simple()
        ));
        let built = async {
            for d in ["logs", "checkpoints"] {
                std::fs::create_dir_all(staging.join(d)).map_err(io_err)?;
            }
            write_json_atomic(&staging.join("vm.json"), &recipe).map_err(io_err)?;
            let disk = staging.join("disk.qcow2");
            log_ctx(ctx, "creating disk overlay");
            run_tool(
                ctx,
                &self.config.qemu_img,
                &[
                    "create".into(),
                    "-q".into(),
                    "-f".into(),
                    "qcow2".into(),
                    "-F".into(),
                    "raw".into(),
                    "-b".into(),
                    base_path.display().to_string(),
                    disk.display().to_string(),
                    res.disk_bytes.to_string(),
                ],
            )
            .await?;
            std::fs::File::open(&disk)
                .and_then(|f| f.sync_all())
                .map_err(io_err)?;
            fsync_dir(&staging).map_err(io_err)
        }
        .await;
        if let Err(e) = built {
            let _ = remove_via_trash(&staging, &self.trash());
            return Err(e);
        }
        rename_durable(&staging, &self.orb_paths(orb.id).dir).map_err(io_err)?;
        Ok(())
    }

    // --- save -------------------------------------------------------------------

    /// stop → QEMU writes `<gen>.tmp/memory` → completed → fsync → manifest. Leaves
    /// QEMU in `postmigrate`; the caller quits it and publishes.
    async fn save(
        &self,
        orb: OrbId,
        recipe: &VmRecipe,
        generation: u64,
        guest_boot_id: Option<String>,
        ctx: &OpContext,
    ) -> Result<CheckpointManifest, RuntimeError> {
        let started = Instant::now();
        let rp = self.run_paths(orb);
        let record = process::read_record(&rp.launch)
            .ok_or_else(|| RuntimeError::Other("launch record is missing".into()))?;
        let qmp = bounded(ctx, self.qmp(orb)).await??;
        bounded(
            ctx,
            qmp.configure_migration(self.migration, save_bandwidth()),
        )
        .await?
        .map_err(qmp_err)?;
        bounded(ctx, qmp.stop()).await?.map_err(qmp_err)?;
        crate::crash_point!("qemu::save::after_stop");

        let tmp = self.tmp_dir(orb, generation);
        remove_via_trash(&tmp, &self.trash()).map_err(io_err)?;
        create_private_dir(&tmp).map_err(io_err)?;
        fsync_dir(&self.orb_paths(orb).checkpoints).map_err(io_err)?;
        // Created private here; QEMU reopens it (O_TRUNC) and writes it in place.
        let memory = tmp.join("memory");
        let file = {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&memory)
                .map_err(io_err)?
        };
        bounded(ctx, qmp.migrate_to_file(&memory))
            .await?
            .map_err(qmp_err)?;
        crate::crash_point!("qemu::save::after_migrate");
        let mut next_log = 512 * MIB;
        let info = loop {
            let info = bounded(ctx, qmp.query_migrate()).await?.map_err(qmp_err)?;
            match info.status.as_deref() {
                Some("completed") => break info,
                Some("failed" | "cancelled") => {
                    return Err(RuntimeError::CheckpointFailed(format!(
                        "migration {}: {}",
                        info.status.unwrap_or_default(),
                        info.error_desc.unwrap_or_default()
                    )));
                }
                _ => {
                    inject_write_fault()?;
                    let done = info.ram.map_or(0, |r| r.transferred);
                    if done >= next_log {
                        log_ctx(ctx, &format!("checkpoint: {} MiB written", done / MIB));
                        next_log = (done / (512 * MIB) + 1) * 512 * MIB;
                    }
                    bounded(ctx, tokio::time::sleep(POLL)).await?;
                }
            }
        };
        let status = bounded(ctx, qmp.status()).await?.map_err(qmp_err)?;
        if status != "postmigrate" {
            return Err(RuntimeError::CheckpointFailed(format!(
                "QEMU is {status} after migration"
            )));
        }
        let fsync = |e: std::io::Error| {
            RuntimeError::CheckpointFailed(format!("fsync {}: {e}", memory.display()))
        };
        let synced = tokio::task::spawn_blocking(move || {
            file.sync_all()?;
            file.metadata().map(|m| m.len())
        });
        let bytes = bounded(ctx, synced)
            .await?
            .map_err(|e| RuntimeError::Other(e.to_string()))?
            .map_err(fsync)?;
        let manifest = CheckpointManifest {
            orb,
            generation,
            created_unix_ms: unix_ms_now(),
            launch: record.config,
            base: recipe.base.clone(),
            ram_bytes: recipe.memory_bytes,
            migration: self.migration,
            memory_file_bytes: bytes,
            guest_boot_id,
            save_ms: started.elapsed().as_millis() as u64,
        };
        // The manifest marks the memory file complete.
        write_json_atomic(&tmp.join("manifest.json"), &manifest).map_err(io_err)?;
        tracing::info!(
            orb = %orb,
            generation,
            ram_transferred = info.ram.map(|r| r.transferred).unwrap_or(0),
            file_bytes = bytes,
            ms = manifest.save_ms,
            "memory checkpoint written"
        );
        Ok(manifest)
    }

    /// After a failed or interrupted save: cancel the migration, discard the partial
    /// checkpoint, and resume the guest exactly where it stopped. Both failure policies
    /// start from a running guest (KeepRunning keeps it; ColdStop shuts it down cleanly).
    async fn abort_save(&self, orb: OrbId, generation: u64, deadline: Instant) {
        let fut = async {
            let qmp = self.qmp(orb).await?;
            cancel_migration(&qmp).await?;
            remove_via_trash(&self.tmp_dir(orb, generation), &self.trash()).map_err(io_err)?;
            if qmp.status().await.map_err(qmp_err)? != "running" {
                qmp.cont().await.map_err(qmp_err)?;
            }
            let status = qmp.status().await.map_err(qmp_err)?;
            if status != "running" {
                return Err(RuntimeError::Other(format!("QEMU is {status} after cont")));
            }
            Ok::<_, RuntimeError>(())
        };
        match tokio::time::timeout_at(deadline.into(), fut).await {
            Ok(Ok(())) => tracing::info!(orb = %orb, generation, "save aborted; guest resumed"),
            Ok(Err(e)) => {
                tracing::error!(orb = %orb, generation, error = %e, "could not resume the guest after a failed save")
            }
            Err(_) => {
                tracing::error!(orb = %orb, generation, "resuming the guest after a failed save timed out")
            }
        }
    }

    /// Ends a QEMU that must not run again: quit, then SIGKILL. True once it exited.
    async fn quit_and_reap(&self, orb: OrbId, proc: &Process, deadline: Instant) -> bool {
        if let Ok(q) = self.qmp(orb).await {
            let _ = tokio::time::timeout(QMP_TIMEOUT, q.quit()).await;
        }
        let soft = deadline.min(Instant::now() + Duration::from_secs(15));
        proc.wait_exit(soft).await || proc.kill_and_wait(Duration::from_secs(10)).await
    }

    // --- restore ----------------------------------------------------------------

    /// The checkpoint's manifest, if this host can restore it exactly.
    fn restorable(&self, orb: OrbId, generation: u64) -> Result<CheckpointManifest, RuntimeError> {
        let dir = self.checkpoint_dir(orb, generation);
        let manifest: CheckpointManifest =
            read_json(&dir.join("manifest.json"))
                .map_err(io_err)?
                .ok_or_else(|| RuntimeError::CheckpointIncompatible("manifest missing".into()))?;
        let recipe = self.read_recipe(orb)?;
        self.check_compatible(&manifest, &recipe, &dir.join("memory"))?;
        Ok(manifest)
    }

    fn check_compatible(
        &self,
        m: &CheckpointManifest,
        recipe: &VmRecipe,
        memory: &Path,
    ) -> Result<(), RuntimeError> {
        let bad = |why: String| Err(RuntimeError::CheckpointIncompatible(why));
        if m.launch.qemu != self.qemu {
            return bad(format!(
                "saved by {:?}, this host runs {:?} (or another binary)",
                m.launch.qemu.version, self.qemu.version
            ));
        }
        if m.launch.cpu != self.cpu {
            return bad(format!(
                "saved on CPU {:?}, this host has {:?} (or different flags)",
                m.launch.cpu.model, self.cpu.model
            ));
        }
        if m.ram_bytes != recipe.memory_bytes || m.base != recipe.base {
            return bad("recipe changed since the save".into());
        }
        match sha256_file(&m.launch.kernel.path) {
            Ok(d) if d == m.launch.kernel.sha256 => {}
            Ok(_) => {
                return bad(format!(
                    "kernel {} changed since the save",
                    m.launch.kernel.path.display()
                ));
            }
            Err(e) => {
                return bad(format!("kernel {}: {e}", m.launch.kernel.path.display()));
            }
        }
        match std::fs::metadata(memory) {
            Ok(meta) if meta.len() == m.memory_file_bytes => Ok(()),
            Ok(meta) => bad(format!(
                "memory file has {} bytes, manifest says {}",
                meta.len(),
                m.memory_file_bytes
            )),
            Err(e) => bad(format!("memory file: {e}")),
        }
    }

    async fn restore(
        &self,
        orb: &OrbDefinition,
        cp: SleepCheckpointRef,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        let g = cp.generation;
        match self.reconcile(orb.id).await? {
            Observed::Stopped {
                checkpoint: Some(latest),
            } if latest == g => {}
            other => {
                return Err(RuntimeError::CheckpointIncompatible(format!(
                    "checkpoint {g} is not restorable: orb is {other}"
                )));
            }
        }
        let manifest = self.restorable(orb.id, g)?;
        let memory = self.checkpoint_dir(orb.id, g).join("memory");
        self.verify_base(&self.read_recipe(orb.id)?)?;
        let started = Instant::now();
        let proc = self
            .launch(
                orb.id,
                LaunchKind::Restore { generation: g },
                manifest.launch.clone(),
                ctx,
            )
            .await?;
        if let Err(e) = self.load(orb.id, &memory, manifest.migration, ctx).await {
            proc.kill_and_wait(Duration::from_secs(10)).await;
            self.clear_run(orb.id);
            return Err(e);
        }
        crate::crash_point!("qemu::restore::after_load");
        // From here the restored guest may run and write its disk: the checkpoint must
        // never be restored again.
        if let Err(e) = self.consume(orb.id, g) {
            proc.kill_and_wait(Duration::from_secs(10)).await;
            self.clear_run(orb.id);
            return Err(io_err(e));
        }
        crate::crash_point!("qemu::restore::after_consume");
        let qmp = self.qmp(orb.id).await?;
        qmp.cont().await.map_err(qmp_err)?;
        let status = qmp.status().await.map_err(qmp_err)?;
        if status != "running" {
            return Err(RuntimeError::Other(format!("QEMU is {status} after cont")));
        }
        let ms = started.elapsed().as_millis();
        tracing::info!(orb = %orb.id, generation = g, ms, "restored from memory checkpoint");
        log_ctx(ctx, &format!("restored checkpoint {g} in {ms} ms"));
        Ok(())
    }

    /// Has a `-incoming defer` QEMU read the memory file and waits for the load.
    async fn load(
        &self,
        orb: OrbId,
        memory: &Path,
        migration: FileMigration,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        let qmp = self.qmp(orb).await?;
        bounded(ctx, qmp.configure_migration(migration, 0))
            .await?
            .map_err(qmp_err)?;
        bounded(ctx, qmp.migrate_incoming_file(memory))
            .await?
            .map_err(qmp_err)?;
        loop {
            let info = bounded(ctx, qmp.query_migrate()).await?.map_err(qmp_err)?;
            match info.status.as_deref() {
                Some("completed") => break,
                Some("failed" | "cancelled") => {
                    return Err(RuntimeError::CheckpointIncompatible(format!(
                        "QEMU could not load the checkpoint: {} {}",
                        info.error_desc.unwrap_or_default(),
                        log_tail(&self.orb_paths(orb).qemu_log)
                    )));
                }
                _ => bounded(ctx, tokio::time::sleep(POLL)).await?,
            }
        }
        loop {
            let status = bounded(ctx, qmp.status()).await?.map_err(qmp_err)?;
            match status.as_str() {
                "paused" | "postmigrate" | "prelaunch" => return Ok(()),
                "inmigrate" => bounded(ctx, tokio::time::sleep(POLL)).await?,
                other => {
                    return Err(RuntimeError::Other(format!(
                        "QEMU is {other} after loading"
                    )));
                }
            }
        }
    }

    // --- stop -------------------------------------------------------------------

    /// Clean guest shutdown within the grace period, then quit, then SIGKILL.
    async fn stop_process(
        &self,
        orb: OrbId,
        proc: &Process,
        status: &str,
        deadline: Instant,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        if status == "running" {
            let grace = Instant::now() + SHUTDOWN_GRACE;
            let soft = grace.min(
                deadline
                    .checked_sub(Duration::from_secs(5))
                    .unwrap_or(deadline),
            );
            log_ctx(ctx, "shutting the guest down");
            self.guest_shutdown(orb, (Instant::now() + Duration::from_secs(3)).min(soft))
                .await;
            if let Ok(q) = self.qmp(orb).await {
                let _ = q.system_powerdown().await;
            }
            if proc.wait_exit(soft).await {
                log_ctx(ctx, "guest shut down");
                return Ok(());
            }
            log_ctx(ctx, "guest did not shut down in time; stopping QEMU");
        }
        if self.quit_and_reap(orb, proc, deadline).await {
            Ok(())
        } else {
            Err(RuntimeError::Other(format!(
                "QEMU process {} does not exit",
                proc.pid
            )))
        }
    }
}

#[async_trait]
impl Runtime for QemuRuntime {
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            kind: RuntimeKind::Qemu,
            version: self.qemu.version.clone(),
            memory_preserving_sleep: true,
            nested_kvm: self.nested_kvm,
            nested_state_qualified: false,
            project_snapshots: true,
        }
    }

    async fn inspect(&self, orb: OrbId) -> Result<RuntimeObservation, RuntimeError> {
        self.inspect_inner(orb).await
    }

    async fn start(
        &self,
        orb: &OrbDefinition,
        source: StartSource,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        let _g = bounded(ctx, self.lock(orb.id)).await?;
        match source {
            StartSource::NewDisk(src) => {
                match self.reconcile(orb.id).await? {
                    Observed::Absent => {}
                    other => {
                        return Err(RuntimeError::Other(format!(
                            "cannot create a disk: orb is {other}"
                        )));
                    }
                }
                self.create_disk(orb, src, ctx).await?;
                crate::crash_point!("qemu::create::after_publish");
                self.boot(orb, ctx).await
            }
            StartSource::ExistingDiskColdBoot => {
                match self.reconcile(orb.id).await? {
                    Observed::Stopped { .. } => {}
                    Observed::Live { status, .. } if status == "running" => return Ok(()),
                    Observed::Absent => {
                        return Err(RuntimeError::NotFound(format!(
                            "orb {} has no disk",
                            orb.id
                        )));
                    }
                    other => {
                        return Err(RuntimeError::Other(format!("cannot boot: orb is {other}")));
                    }
                }
                // Booting writes the disk: published checkpoints become stale first.
                self.consume_all(orb.id).map_err(io_err)?;
                self.boot(orb, ctx).await
            }
            StartSource::MemoryCheckpoint(cp) => self.restore(orb, cp, ctx).await,
        }
    }

    async fn checkpoint_and_stop(
        &self,
        orb: &OrbDefinition,
        checkpoint_generation: u64,
        ctx: &OpContext,
    ) -> Result<SleepCheckpointRef, RuntimeError> {
        let g = checkpoint_generation;
        let done = SleepCheckpointRef { generation: g };
        let _lock = bounded(ctx, self.lock(orb.id)).await?;
        let proc = match self.reconcile(orb.id).await? {
            Observed::Live { proc, status } if status == "running" => proc,
            Observed::Stopped {
                checkpoint: Some(latest),
            } if latest == g => return Ok(done),
            other => {
                return Err(RuntimeError::CheckpointFailed(format!(
                    "orb is not running ({other})"
                )));
            }
        };
        let recipe = self.read_recipe(orb.id)?;
        let checkpoints = self.orb_paths(orb.id).checkpoints;
        let need = recipe.memory_bytes + CHECKPOINT_MARGIN;
        let free = free_bytes(&checkpoints).map_err(io_err)?;
        if free < need {
            return Err(RuntimeError::InsufficientDisk(format!(
                "checkpoint needs up to {need} bytes, {free} free"
            )));
        }
        let _permit = bounded(ctx, self.saves.acquire())
            .await?
            .map_err(|_| RuntimeError::Other("save semaphore closed".into()))?;
        log_ctx(ctx, "asking the guest to prepare for sleep");
        let prep_deadline = (Instant::now() + PREPARE_SLEEP_TIMEOUT).min(ctx.work_deadline);
        let guest_boot_id = match bounded(ctx, self.prepare_sleep(orb.id, prep_deadline)).await? {
            Ok(boot_id) => Some(boot_id),
            Err(e) => {
                // The memory image is consistent without it; the sync only matters if
                // the checkpoint is later lost and the disk cold-boots.
                tracing::warn!(orb = %orb.id, error = %e, "guest did not prepare for sleep; saving anyway");
                log_ctx(
                    ctx,
                    &format!("guest did not prepare for sleep ({e}); saving anyway"),
                );
                None
            }
        };
        log_ctx(ctx, &format!("saving memory checkpoint {g}"));
        let manifest = match self.save(orb.id, &recipe, g, guest_boot_id, ctx).await {
            Ok(m) => m,
            Err(e) => {
                log_ctx(ctx, &format!("save failed ({e}); resuming the guest"));
                self.abort_save(orb.id, g, ctx.cleanup_deadline).await;
                return Err(match e {
                    RuntimeError::Cancelled
                    | RuntimeError::DeadlineExceeded
                    | RuntimeError::InsufficientDisk(_)
                    | RuntimeError::CheckpointFailed(_) => e,
                    other => RuntimeError::CheckpointFailed(other.to_string()),
                });
            }
        };
        crate::crash_point!("qemu::save::after_manifest");
        // Never publish while the source could still run.
        if !self
            .quit_and_reap(orb.id, &proc, ctx.cleanup_deadline)
            .await
        {
            return Err(RuntimeError::Other(format!(
                "QEMU process {} does not exit after the save",
                proc.pid
            )));
        }
        crate::crash_point!("qemu::save::after_quit");
        self.publish(orb.id, g).map_err(io_err)?;
        crate::crash_point!("qemu::save::after_publish");
        self.clear_run(orb.id);
        log_ctx(
            ctx,
            &format!(
                "checkpoint {g} published: {} MiB for {} MiB of RAM in {} ms",
                manifest.memory_file_bytes / MIB,
                manifest.ram_bytes / MIB,
                manifest.save_ms
            ),
        );
        Ok(done)
    }

    async fn cold_stop(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), RuntimeError> {
        let _g = self.lock(orb.id).await;
        match self.reconcile(orb.id).await? {
            Observed::Absent => Err(RuntimeError::NotFound(format!(
                "orb {} has no disk",
                orb.id
            ))),
            Observed::Stopped { .. } => Ok(()),
            Observed::Inconsistent(r) => Err(RuntimeError::Other(r)),
            Observed::Live { proc, status } => {
                self.stop_process(orb.id, &proc, &status, ctx.work_deadline, ctx)
                    .await?;
                self.clear_run(orb.id);
                Ok(())
            }
        }
    }

    async fn discard_checkpoint(
        &self,
        orb: OrbId,
        checkpoint: &SleepCheckpointRef,
    ) -> Result<(), RuntimeError> {
        let _g = self.lock(orb).await;
        let dir = self.checkpoint_dir(orb, checkpoint.generation);
        self.consume(orb, checkpoint.generation).map_err(io_err)?;
        remove_via_trash(&dir, &self.trash()).map_err(io_err)
    }

    /// `qemu-img convert` of the stopped overlay (with its backing) into a standalone
    /// sparse raw image, published as `projects/<key>/`. Crash-consistent; the guest
    /// synced its filesystems in PrepareSleep when the orb was put to sleep.
    async fn clone_disk(
        &self,
        orb: &OrbDefinition,
        key: &ProjectSnapshotKey,
        ctx: &OpContext,
    ) -> Result<ProjectSnapshotRef, RuntimeError> {
        let done = ProjectSnapshotRef { key: key.clone() };
        let dest = self.project_dir(key);
        if dest.join("disk.raw").is_file() {
            return Ok(done);
        }
        let _g = bounded(ctx, self.lock(orb.id)).await?;
        match self.reconcile(orb.id).await? {
            Observed::Stopped { .. } => {}
            other => {
                return Err(RuntimeError::Other(format!(
                    "snapshot needs a stopped orb, found {other}"
                )));
            }
        }
        let staging = self
            .state_dir
            .join("staging")
            .join(format!("project-{key}-{}", uuid::Uuid::new_v4().simple()));
        let disk = self.orb_paths(orb.id).disk;
        let built = async {
            std::fs::create_dir_all(&staging).map_err(io_err)?;
            let raw = staging.join("disk.raw");
            log_ctx(ctx, &format!("converting disk into project snapshot {key}"));
            run_tool(
                ctx,
                &self.config.qemu_img,
                &[
                    "convert".into(),
                    "-q".into(),
                    "-f".into(),
                    "qcow2".into(),
                    "-O".into(),
                    "raw".into(),
                    disk.display().to_string(),
                    raw.display().to_string(),
                ],
            )
            .await?;
            std::fs::File::open(&raw)
                .and_then(|f| f.sync_all())
                .map_err(io_err)?;
            let meta = serde_json::json!({
                "key": key,
                "source_orb": orb.id,
                "created_unix_ms": unix_ms_now(),
                "bytes": std::fs::metadata(&raw).map(|m| m.len()).unwrap_or(0),
            });
            write_json_atomic(&staging.join("meta.json"), &meta).map_err(io_err)?;
            fsync_dir(&staging).map_err(io_err)
        }
        .await;
        if let Err(e) = built {
            let _ = remove_via_trash(&staging, &self.trash());
            return Err(e);
        }
        if dest.exists() {
            remove_via_trash(&dest, &self.trash()).map_err(io_err)?;
        }
        rename_durable(&staging, &dest).map_err(io_err)?;
        crate::crash_point!("qemu::clone::after_publish");
        log_ctx(ctx, &format!("project snapshot {key} published"));
        Ok(done)
    }

    async fn delete(&self, orb: OrbId, ctx: &OpContext) -> Result<(), RuntimeError> {
        let _g = bounded(ctx, self.lock(orb)).await?;
        let rp = self.run_paths(orb);
        if let Some(record) = process::read_record(&rp.launch)
            && let Some(proc) = Process::adopt(&record, &orb.to_string(), &rp.pidfile)
        {
            log_ctx(ctx, &format!("killing QEMU (pid {})", proc.pid));
            if !proc.kill_and_wait(Duration::from_secs(10)).await {
                return Err(RuntimeError::Other(format!(
                    "QEMU process {} does not exit",
                    proc.pid
                )));
            }
        }
        if let Ok(recipe) = self.read_recipe(orb) {
            recipe.net.remove();
        }
        let dir = self.orb_paths(orb).dir;
        if dir.exists() {
            log_ctx(ctx, "removing disk and checkpoints");
        }
        remove_via_trash(&dir, &self.trash()).map_err(io_err)?;
        self.clear_run(orb);
        if let Some(cg) = self.cgroup_dir(orb) {
            let _ = std::fs::remove_dir(cg);
        }
        Ok(())
    }

    fn guest(&self) -> &dyn GuestControl {
        self
    }
}

/// Cancels an outgoing migration that is still running and waits (up to 30 s) until
/// it stopped.
async fn cancel_migration(qmp: &Qmp) -> Result<(), RuntimeError> {
    const ACTIVE: &[&str] = &["setup", "active", "device", "pre-switchover", "cancelling"];
    let active = || async {
        let info = qmp.query_migrate().await.map_err(qmp_err)?;
        Ok::<_, RuntimeError>(ACTIVE.contains(&info.status.unwrap_or_default().as_str()))
    };
    if !active().await? {
        return Ok(());
    }
    qmp.migrate_cancel().await.map_err(qmp_err)?;
    let give_up = Instant::now() + Duration::from_secs(30);
    while active().await? {
        if Instant::now() >= give_up {
            return Err(RuntimeError::Other("the migration does not cancel".into()));
        }
        tokio::time::sleep(POLL).await;
    }
    Ok(())
}

/// QEMU's `--version` line must name 9.0 or newer.
pub(crate) fn check_qemu_version(line: &str) -> Result<(), String> {
    let v = line
        .strip_prefix("QEMU emulator version ")
        .and_then(|r| r.split_whitespace().next())
        .unwrap_or_default();
    let mut parts = v.split('.').map(|p| p.parse::<u32>());
    match (parts.next(), parts.next()) {
        (Some(Ok(major)), Some(Ok(minor))) if (major, minor) >= MIN_QEMU => Ok(()),
        (Some(Ok(_)), Some(Ok(_))) => Err(format!(
            "{line:?} is too old: orbd needs QEMU {}.{} or newer (file migration with mapped-ram)",
            MIN_QEMU.0, MIN_QEMU.1
        )),
        _ => Err(format!("unrecognised QEMU version {line:?}")),
    }
}

/// The versioned machine type `q35` stands for in this QEMU (e.g. `pc-q35-11.0`).
fn q35_machine(qemu: &Path) -> Result<String, String> {
    let out = std::process::Command::new(qemu)
        .args(["-machine", "help"])
        .output()
        .map_err(|e| format!("{} -machine help: {e}", qemu.display()))?;
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.split_whitespace().next() == Some("q35"))
        .find_map(|l| Some(l.split("(alias of ").nth(1)?.split(')').next()?.to_owned()))
        .ok_or_else(|| format!("{} has no q35 machine", qemu.display()))
}

/// Bandwidth cap for saves: unlimited unless `qemu::save::throttle=return(<MiB/s>)`.
fn save_bandwidth() -> u64 {
    #[cfg(feature = "failpoints")]
    if let Some(mib) = fail::eval("qemu::save::throttle", |a| {
        a.and_then(|a| a.parse::<u64>().ok())
    })
    .flatten()
    {
        return mib * MIB;
    }
    0
}

/// `qemu::save::write=return`: the running save fails as QEMU's does when the disk
/// fills (the caller cancels it, leaving a partial file).
fn inject_write_fault() -> Result<(), RuntimeError> {
    #[cfg(feature = "failpoints")]
    if fail::eval("qemu::save::write", |_| ()).is_some() {
        return Err(RuntimeError::CheckpointFailed(format!(
            "migration failed: writing the checkpoint: {}",
            std::io::Error::from_raw_os_error(nix::libc::ENOSPC)
        )));
    }
    Ok(())
}

/// Runs a host tool (qemu-img) with fixed arguments, bounded by the context; killed
/// if cancelled or late.
async fn run_tool(ctx: &OpContext, program: &Path, args: &[String]) -> Result<(), RuntimeError> {
    let child = tokio::process::Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output();
    let out = bounded(ctx, child)
        .await?
        .map_err(|e| RuntimeError::Other(format!("{}: {e}", program.display())))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let msg = format!("{} {}: {}", program.display(), args[0], err.trim());
        return Err(if err.contains("No space left") {
            RuntimeError::InsufficientDisk(msg)
        } else {
            RuntimeError::Other(msg)
        });
    }
    Ok(())
}

/// `program` itself if it has a directory part, else its first match on PATH.
fn on_path(program: &Path) -> Option<PathBuf> {
    if program.components().count() > 1 {
        return Some(program.to_owned());
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|d| d.join(program))
        .find(|p| p.is_file())
}

fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

/// True when files in `dir` can be opened with O_DIRECT (cache=none).
fn probe_direct_io(dir: &Path) -> bool {
    use std::os::unix::fs::OpenOptionsExt;
    let probe = dir.join(".direct-io-probe");
    let ok = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .custom_flags(nix::libc::O_DIRECT)
        .open(&probe)
        .is_ok();
    let _ = std::fs::remove_file(&probe);
    ok
}

/// Stable per-orb systemd machine id (project-snapshot clones get their own).
fn machine_id(orb: OrbId) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(&Sha256::digest(format!("orbd-machine-id:{orb}").as_bytes())[..16])
}

/// Last lines of a QEMU log, for error messages.
fn log_tail(path: &Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let lines: Vec<&str> = text.lines().rev().take(5).collect();
    lines.into_iter().rev().collect::<Vec<_>>().join(" | ")
}

fn io_err(e: std::io::Error) -> RuntimeError {
    if e.raw_os_error() == Some(nix::libc::ENOSPC) {
        RuntimeError::InsufficientDisk(e.to_string())
    } else {
        RuntimeError::Other(e.to_string())
    }
}

fn qmp_err(e: qmp::QmpError) -> RuntimeError {
    RuntimeError::Other(e.to_string())
}

/// Streams a progress line to the operation log. Advisory: dropped if the sink is full.
pub(crate) fn log_ctx(ctx: &OpContext, line: &str) {
    tracing::debug!(operation = %ctx.operation_id, "{line}");
    let _ = ctx
        .log
        .try_send((LogStream::Stdout, bytes::Bytes::from(format!("{line}\n"))));
}

use log_ctx as log;

/// Runs `fut` until it finishes, `ctx.cancel` fires or the work deadline passes.
async fn bounded<F: Future>(ctx: &OpContext, fut: F) -> Result<F::Output, RuntimeError> {
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(RuntimeError::Cancelled),
        _ = tokio::time::sleep_until(ctx.work_deadline.into()) => Err(RuntimeError::DeadlineExceeded),
        out = fut => Ok(out),
    }
}
