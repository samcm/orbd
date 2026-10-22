//! Firecracker runtime: one microVM per orb with memory-preserving sleep and a fast,
//! lazy wake. Orbs get no `/dev/kvm`: Firecracker does not save nested virtualization
//! state, so every VM gets a CPU template that hides VMX and SVM (without it an AMD
//! host exposes SVM, and a nested guest running at sleep makes the restore crash).
//!
//! Storage (per the agent layout, next to QEMU orbs):
//!
//! ```text
//! <state>/orbs/<orb>/vm.json                 immutable recipe (resources, disk source, network)
//!                   /disk.raw                standalone raw disk (reflink or sparse copy)
//!                   /checkpoints/<gen>/      published: memory, vmstate, manifest.json[, consumed]
//!                   /checkpoints/<gen>.tmp/  save in progress (complete once manifest.json exists)
//!                   /logs/{firecracker.log,console.log}
//! <state>/net/<index>                        network index claimed by an orb
//! <run>/firecracker/<orb>/{launch.json,api.sock,vsock.sock}
//! ```
//!
//! The durability contract is QEMU's: every method first reconciles the host (adopting
//! VMMs that outlived an earlier agent, finishing or discarding interrupted saves and
//! restores), publications are fsynced renames, and a restore marks its checkpoint
//! consumed after the load and before the vCPUs resume.
//!
//! * Sleep: pause → full snapshot into a fresh `<gen>.tmp` (Firecracker fsyncs memory
//!   and vmstate; orbd fsyncs the disk itself) → manifest → kill the VMM and confirm its
//!   exit → publish. A failed save waits for Firecracker to finish the request (its API
//!   runs one request at a time and a dropped request is not cancelled), discards the
//!   files and resumes the guest, or kills the VMM if it cannot.
//! * Wake: a new VMM loads the snapshot with `File` memory: pages fault in from the
//!   memory file for the life of the VM, so that file is never written again. Files
//!   are only created fresh, renamed and unlinked; a discarded checkpoint's memory file
//!   stays valid (unlinked) for the VMM that maps it, and its disk blocks are freed when
//!   that VMM exits.
//! * Cold stop: Ctrl+Alt+Del (the guest reboots and `reboot=k` ends the VMM; a guest
//!   poweroff only halts it), then SIGKILL.
//!
//! Crash points (feature `failpoints`): `firecracker::create::after_publish`,
//! `firecracker::save::after_stop`, `firecracker::save::after_migrate` (Firecracker
//! still writing the snapshot), `firecracker::save::after_manifest`,
//! `firecracker::save::after_quit`, `firecracker::save::after_publish`,
//! `firecracker::restore::after_load`, `firecracker::restore::after_consume`,
//! `firecracker::clone::after_publish`, and the shared `vm::launch::after_exec`. Fault
//! injection: `firecracker::save::write=return` fails a save in progress as a full disk
//! would; `firecracker::save::throttle=return(<MiB/s>)` stretches a save to that rate.

mod api;
mod guest;

use super::vm::checkpoints::{Checkpoints, MANIFEST};
use super::vm::fsutil::write_json_atomic;
use super::vm::fsutil::{free_bytes, fsync_dir, read_json, remove_via_trash, rename_durable};
use super::vm::guest as channel;
use super::vm::images::ImageProvider;
use super::vm::net::{self, NetAttachment};
use super::vm::process::{self, CpuIdentity, KernelIdentity, LaunchKind, Process, VmmIdentity};
use super::vm::{
    OrbLocks, SAVES, bounded, copy_disk, create_private_dir, file_digest, io_err, log, log_tail,
    machine_id, on_path, publish_project,
};
use super::{
    GuestControl, OpContext, Runtime, RuntimeCapabilities, RuntimeError, RuntimeObservation,
    StartSource,
};
use crate::config::agent::FirecrackerRuntimeConfig;
use crate::image::ImageError;
use crate::model::{
    InitialDiskSource, MIB, OrbDefinition, OrbId, ProjectSnapshotKey, ProjectSnapshotRef,
    RuntimeKind, SleepCheckpointRef, unix_ms_now,
};
use api::Api;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Guest-side vsock port of `orbd-guest` (selected by `orbd.vsock=` on the cmdline).
const VSOCK_PORT: u32 = 1024;
const GUEST_CID: u32 = 3;
const KERNEL_CMDLINE: &str = "console=ttyS0 reboot=k panic=1 root=/dev/vda rw";
const API_TIMEOUT: Duration = Duration::from_secs(10);
/// How long reconciliation waits for a request still executing in a VMM (a snapshot
/// being written) before it judges the VMM unresponsive.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(120);
const PREPARE_SLEEP_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a cold stop waits for the guest to reboot out of the VMM.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
/// Checkpoint headroom beyond guest RAM (vmstate).
const CHECKPOINT_MARGIN: u64 = 64 * MIB;
const POLL: Duration = Duration::from_millis(100);

/// Hides VMX (CPUID 1 ECX bit 5) and SVM (CPUID 0x8000_0001 ECX bit 2).
fn cpu_template() -> serde_json::Value {
    let clear = |bit: usize| {
        let mut b = vec![b'x'; 32];
        b[31 - bit] = b'0';
        format!("0b{}", String::from_utf8(b).expect("ascii"))
    };
    serde_json::json!({ "cpuid_modifiers": [
        { "leaf": "0x1", "subleaf": "0x0", "flags": 0,
          "modifiers": [{ "register": "ecx", "bitmap": clear(5) }] },
        { "leaf": "0x80000001", "subleaf": "0x0", "flags": 0,
          "modifiers": [{ "register": "ecx", "bitmap": clear(2) }] },
    ]})
}

/// Immutable per-orb recipe, written once with the disk.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct VmRecipe {
    orb: OrbId,
    vcpus: u32,
    memory_bytes: u64,
    disk_bytes: u64,
    /// Where the disk was copied from (provenance only; the disk is standalone).
    source: DiskSource,
    net: NetAttachment,
    created_unix_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum DiskSource {
    Image {
        image_ref: String,
        key: String,
        manifest_digest: String,
    },
    ProjectSnapshot {
        key: ProjectSnapshotKey,
    },
}

/// The guest-visible machine of one launch. A checkpoint restores only on the same
/// Firecracker binary and host CPU, with the same vCPUs and memory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Machine {
    firecracker: VmmIdentity,
    cpu: CpuIdentity,
    /// The kernel the VM booted (a restore does not read it).
    kernel: KernelIdentity,
    vcpus: u32,
    memory_mib: u64,
    boot_args: String,
}

type LaunchRecord = process::LaunchRecord<Machine>;

/// Firecracker's `--id` (alphanumeric and dashes, at most 64), also the argv marker.
fn marker(record: &LaunchRecord) -> String {
    format!("orbd-{}", record.nonce)
}

/// `checkpoints/<gen>/manifest.json`; marks memory and vmstate complete.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckpointManifest {
    orb: OrbId,
    generation: u64,
    created_unix_ms: u64,
    machine: Machine,
    memory_bytes: u64,
    vmstate_bytes: u64,
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
    vmm_log: PathBuf,
}

struct RunPaths {
    dir: PathBuf,
    launch: PathBuf,
    api: PathBuf,
    vsock: PathBuf,
}

/// What reconciliation found on the host.
enum Observed {
    Absent,
    /// A validated, running Firecracker for this orb.
    Running(Process),
    Stopped {
        checkpoint: Option<u64>,
    },
    Inconsistent(String),
}

impl std::fmt::Display for Observed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absent => f.write_str("absent"),
            Self::Running(_) => f.write_str("running"),
            Self::Stopped {
                checkpoint: Some(g),
            } => write!(f, "sleeping (checkpoint {g})"),
            Self::Stopped { checkpoint: None } => f.write_str("stopped"),
            Self::Inconsistent(r) => write!(f, "inconsistent: {r}"),
        }
    }
}

pub struct FirecrackerRuntime {
    config: FirecrackerRuntimeConfig,
    state_dir: PathBuf,
    run_dir: PathBuf,
    images: Arc<dyn ImageProvider>,
    firecracker: VmmIdentity,
    cpu: CpuIdentity,
    locks: OrbLocks,
}

impl FirecrackerRuntime {
    /// Checks host prerequisites, identifies the Firecracker binary and host CPU and
    /// installs orbd's firewall table. Running VMs are adopted lazily, by the first
    /// `inspect` of each orb.
    pub fn new(
        config: FirecrackerRuntimeConfig,
        state_dir: &Path,
        run_dir: &Path,
        images: Arc<dyn ImageProvider>,
    ) -> Result<Self, RuntimeError> {
        let other = |what: &str, e: &dyn std::fmt::Display| {
            RuntimeError::Other(format!("firecracker runtime: {what}: {e}"))
        };
        for d in ["orbs", "projects", "staging", "trash"] {
            create_private_dir(&state_dir.join(d)).map_err(|e| other(d, &e))?;
        }
        let fc_run = run_dir.join("firecracker");
        std::fs::create_dir_all(&fc_run).map_err(|e| other("run dir", &e))?;
        let longest = fc_run
            .join(OrbId(uuid::Uuid::nil()).to_string())
            .join("vsock.sock");
        if longest.as_os_str().len() >= 108 {
            return Err(other(
                "run_dir",
                &format!("{} is too long for unix socket paths", run_dir.display()),
            ));
        }
        if !config.kernel.is_file() {
            return Err(other("kernel", &config.kernel.display()));
        }
        let path = on_path(&config.firecracker).ok_or_else(|| {
            other(
                "firecracker",
                &format!("{} not found", config.firecracker.display()),
            )
        })?;
        let version = version(&path).map_err(|e| other("firecracker", &e))?;
        let firecracker = VmmIdentity {
            sha256: file_digest(&path, "firecracker binary")?,
            path,
            version,
        };
        net::prepare_host().map_err(|e| other("network", &e))?;
        let cpu = CpuIdentity::host();
        tracing::info!(firecracker = %firecracker.version, cpu = %cpu.model, "firecracker runtime ready");
        Ok(Self {
            config,
            state_dir: state_dir.to_owned(),
            run_dir: run_dir.to_owned(),
            images,
            firecracker,
            cpu,
            locks: OrbLocks::default(),
        })
    }

    fn orb_paths(&self, orb: OrbId) -> OrbPaths {
        let dir = self.state_dir.join("orbs").join(orb.to_string());
        OrbPaths {
            recipe: dir.join("vm.json"),
            disk: dir.join("disk.raw"),
            checkpoints: dir.join("checkpoints"),
            console_log: dir.join("logs/console.log"),
            vmm_log: dir.join("logs/firecracker.log"),
            dir,
        }
    }

    fn run_paths(&self, orb: OrbId) -> RunPaths {
        let dir = self.run_dir.join("firecracker").join(orb.to_string());
        RunPaths {
            launch: dir.join("launch.json"),
            api: dir.join("api.sock"),
            vsock: dir.join("vsock.sock"),
            dir,
        }
    }

    fn api(&self, orb: OrbId) -> Api {
        Api::new(self.run_paths(orb).api)
    }

    fn trash(&self) -> PathBuf {
        self.state_dir.join("trash")
    }

    fn checkpoints(&self, orb: OrbId) -> Checkpoints {
        Checkpoints::new(orb, self.orb_paths(orb).checkpoints, self.trash())
    }

    fn cgroup_dir(&self, orb: OrbId) -> Option<PathBuf> {
        self.config
            .cgroup_parent
            .as_ref()
            .map(|p| p.join(format!("orbd-{orb}")))
    }

    fn read_recipe(&self, orb: OrbId) -> Result<VmRecipe, RuntimeError> {
        read_json(&self.orb_paths(orb).recipe)
            .map_err(|e| RuntimeError::Other(format!("reading vm.json: {e}")))?
            .ok_or_else(|| RuntimeError::NotFound(format!("orb {orb} has no disk")))
    }

    /// Only once no VMM for the orb can run (its sockets are reused by the next launch).
    fn clear_run(&self, orb: OrbId) {
        let _ = std::fs::remove_dir_all(self.run_paths(orb).dir);
    }

    /// Kills a VMM that must not run again and confirms its exit.
    async fn kill(&self, orb: OrbId, proc: &Process) -> Result<(), RuntimeError> {
        if proc.kill_and_wait(Duration::from_secs(10)).await {
            self.clear_run(orb);
            Ok(())
        } else {
            Err(RuntimeError::Other(format!(
                "Firecracker process {} does not exit",
                proc.pid
            )))
        }
    }

    // --- reconciliation ---------------------------------------------------------

    /// Brings the orb to a stable state after any interruption and reports it. Callers
    /// hold the orb lock.
    async fn reconcile(&self, orb: OrbId) -> Result<Observed, RuntimeError> {
        let rp = self.run_paths(orb);
        let record: Option<LaunchRecord> = process::read_record(&rp.launch);
        let proc = record
            .as_ref()
            .and_then(|r| Process::adopt(r, &marker(r), None));
        if !self.orb_paths(orb).dir.is_dir() {
            if let Some(p) = proc {
                return Ok(Observed::Inconsistent(format!(
                    "Firecracker process {} runs without a disk",
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
        let checkpoint = self
            .checkpoints(orb)
            .settle(|g| self.restorable(orb, g).map(|_| ()))?;
        Ok(Observed::Stopped { checkpoint })
    }

    /// Resolves a live VMM left in a transitional state. `None` means it was killed and
    /// the orb is now judged from its files.
    async fn settle_live(
        &self,
        orb: OrbId,
        proc: Process,
        record: &LaunchRecord,
    ) -> Result<Option<Observed>, RuntimeError> {
        let api = self.api(orb);
        // Waits behind a request still executing (a snapshot being written).
        let info = match api.info(SETTLE_TIMEOUT).await {
            Ok(i) => i,
            Err(e) => {
                return Ok(Some(Observed::Inconsistent(format!(
                    "Firecracker process {} does not answer: {e}",
                    proc.pid
                ))));
            }
        };
        if info.id != marker(record) {
            return Ok(Some(Observed::Inconsistent(format!(
                "Firecracker process {} is not this orb's launch",
                proc.pid
            ))));
        }
        let cps = self.checkpoints(orb);
        if let LaunchKind::Restore { generation } = record.kind
            && !cps.is_consumed(generation)
        {
            if info.state == "Running" {
                // Cannot happen (consumed before resume); be safe: RAM has moved on.
                tracing::error!(orb = %orb, generation, "restored VM runs with an unconsumed checkpoint");
                cps.consume(generation)?;
            } else {
                // Loaded or loading, vCPUs never ran: drop it, the checkpoint stays valid.
                tracing::warn!(orb = %orb, generation, state = %info.state, "discarding an unfinished restore");
                self.kill(orb, &proc).await?;
                return Ok(None);
            }
        }
        match info.state.as_str() {
            "Running" => {}
            // Configured but never started: nothing ran; boot again from the disk.
            "Not started" => {
                tracing::warn!(orb = %orb, "discarding a VM that never started");
                self.kill(orb, &proc).await?;
                return Ok(None);
            }
            "Paused" => {
                if let Some(g) = cps.complete_tmp() {
                    // A save finished but its agent died before the VMM exited: the guest
                    // never resumed, so finish the save.
                    tracing::warn!(orb = %orb, generation = g, "finishing an interrupted save");
                    self.kill(orb, &proc).await?;
                    return Ok(None);
                }
                // An interrupted save (no snapshot request is executing any more: the
                // GET above waited for it) or a restore whose resume was lost.
                tracing::warn!(orb = %orb, "resuming a VM left paused by an interrupted operation");
                cps.discard_tmps()?;
                api.set_state("Resumed", API_TIMEOUT)
                    .await
                    .map_err(api_err)?;
            }
            other => {
                return Ok(Some(Observed::Inconsistent(format!(
                    "Firecracker process {} is {other}",
                    proc.pid
                ))));
            }
        }
        cps.discard_tmps()?;
        cps.consume_all()?;
        Ok(Some(Observed::Running(proc)))
    }

    async fn inspect_inner(&self, orb: OrbId) -> Result<RuntimeObservation, RuntimeError> {
        let _g = self.locks.lock(orb).await;
        Ok(match self.reconcile(orb).await? {
            Observed::Absent => RuntimeObservation::Absent,
            Observed::Running(_) => RuntimeObservation::Running,
            Observed::Stopped { checkpoint } => RuntimeObservation::Stopped {
                checkpoint: checkpoint.map(|generation| SleepCheckpointRef { generation }),
            },
            Observed::Inconsistent(reason) => RuntimeObservation::Inconsistent { reason },
        })
    }

    // --- launching --------------------------------------------------------------

    fn machine(&self, recipe: &VmRecipe) -> Result<Machine, RuntimeError> {
        if !recipe.memory_bytes.is_multiple_of(MIB) || recipe.memory_bytes == 0 {
            return Err(RuntimeError::Other(format!(
                "guest memory {} is not a whole number of MiB",
                recipe.memory_bytes
            )));
        }
        if recipe.vcpus != 1 && !recipe.vcpus.is_multiple_of(2) {
            return Err(RuntimeError::Other(format!(
                "Firecracker needs 1 or an even number of vCPUs, not {}",
                recipe.vcpus
            )));
        }
        Ok(Machine {
            firecracker: self.firecracker.clone(),
            cpu: self.cpu.clone(),
            kernel: KernelIdentity {
                path: self.config.kernel.clone(),
                sha256: file_digest(&self.config.kernel, "kernel")?,
            },
            vcpus: recipe.vcpus,
            memory_mib: recipe.memory_bytes / MIB,
            boot_args: format!(
                "{KERNEL_CMDLINE} systemd.machine_id={} orbd.vsock={VSOCK_PORT}",
                machine_id(recipe.orb)
            ),
        })
    }

    /// Starts Firecracker in the orb's network namespace and waits until its API
    /// answers as this launch.
    async fn launch(
        &self,
        orb: OrbId,
        kind: LaunchKind,
        machine: Machine,
        net: &NetAttachment,
        ctx: &OpContext,
    ) -> Result<Process, RuntimeError> {
        let rp = self.run_paths(orb);
        let op = self.orb_paths(orb);
        // Firecracker binds the API and vsock sockets itself (a stale one fails it).
        self.clear_run(orb);
        create_private_dir(&rp.dir).map_err(io_err)?;
        let record = LaunchRecord::new(kind, machine);
        let id = marker(&record);
        let args = vec![
            "--api-sock".into(),
            rp.api.display().to_string(),
            "--id".into(),
            id.clone(),
            "--log-path".into(),
            op.vmm_log.display().to_string(),
            "--level".into(),
            "Info".into(),
        ];
        let (proc, _) = process::spawn(process::Spawn {
            program: &record.config.firecracker.path.clone(),
            args,
            stdout: &op.console_log,
            stderr: &op.vmm_log,
            record_path: &rp.launch,
            record,
            cgroup: self.cgroup_dir(orb),
            netns: net.netns_path(),
        })
        .map_err(|e| RuntimeError::Other(format!("launching Firecracker: {e}")))?;
        log(ctx, &format!("Firecracker started (pid {})", proc.pid));
        let api = self.api(orb);
        let ready_by = Instant::now() + Duration::from_secs(10);
        loop {
            if !proc.is_alive() {
                return Err(RuntimeError::Other(format!(
                    "Firecracker exited during start: {}",
                    log_tail(&op.vmm_log)
                )));
            }
            if let Ok(info) = api.info(Duration::from_secs(1)).await
                && info.id == id
            {
                return Ok(proc);
            }
            if Instant::now() >= ready_by {
                self.kill(orb, &proc).await?;
                return Err(RuntimeError::Other(
                    "Firecracker did not open its API socket within 10s".into(),
                ));
            }
            if let Err(e) = bounded(ctx, tokio::time::sleep(POLL)).await {
                self.kill(orb, &proc).await?;
                return Err(e);
            }
        }
    }

    async fn boot(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), RuntimeError> {
        let recipe = self.read_recipe(orb.id)?;
        adopt_net(&recipe.net).await?;
        let machine = self.machine(&recipe)?;
        let proc = self
            .launch(orb.id, LaunchKind::Boot, machine.clone(), &recipe.net, ctx)
            .await?;
        if let Err(e) = self.configure(orb.id, &recipe, &machine, ctx).await {
            self.kill(orb.id, &proc).await?;
            return Err(e);
        }
        log(ctx, "VM booted");
        Ok(())
    }

    async fn configure(
        &self,
        orb: OrbId,
        recipe: &VmRecipe,
        machine: &Machine,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        let api = self.api(orb);
        let op = self.orb_paths(orb);
        let rp = self.run_paths(orb);
        let t = API_TIMEOUT;
        let steps = async {
            api.put("/cpu-config", &cpu_template(), t).await?;
            api.put(
                "/machine-config",
                &api::MachineConfig {
                    vcpu_count: machine.vcpus,
                    mem_size_mib: machine.memory_mib,
                    smt: false,
                },
                t,
            )
            .await?;
            api.put(
                "/boot-source",
                &api::BootSource {
                    kernel_image_path: &machine.kernel.path,
                    boot_args: &machine.boot_args,
                },
                t,
            )
            .await?;
            api.put(
                "/drives/rootfs",
                &api::Drive {
                    drive_id: "rootfs",
                    path_on_host: &op.disk,
                    is_root_device: true,
                    is_read_only: false,
                },
                t,
            )
            .await?;
            api.put(
                "/network-interfaces/eth0",
                &api::NetworkInterface {
                    iface_id: "eth0",
                    host_dev_name: net::TAP,
                    guest_mac: &recipe.net.mac,
                },
                t,
            )
            .await?;
            api.put(
                "/vsock",
                &api::Vsock {
                    guest_cid: GUEST_CID,
                    uds_path: &rp.vsock,
                },
                t,
            )
            .await?;
            // Freed guest pages go back to the host, as with QEMU.
            api.put(
                "/balloon",
                &api::Balloon {
                    amount_mib: 0,
                    deflate_on_oom: false,
                    free_page_reporting: true,
                },
                t,
            )
            .await?;
            api.action("InstanceStart", t).await
        };
        bounded(ctx, steps).await?.map_err(api_err)
    }

    async fn create_disk(
        &self,
        orb: &OrbDefinition,
        src: InitialDiskSource,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        let (base, source) = match src {
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
                    DiskSource::Image {
                        image_ref,
                        key: img.key,
                        manifest_digest: img.manifest_digest,
                    },
                )
            }
            InitialDiskSource::ProjectSnapshot { snapshot: r, .. } => {
                let disk = self
                    .state_dir
                    .join("projects")
                    .join(r.key.as_str())
                    .join("disk.raw");
                if !disk.is_file() {
                    return Err(RuntimeError::NotFound(format!(
                        "project snapshot {} is not on this host",
                        r.key
                    )));
                }
                (disk, DiskSource::ProjectSnapshot { key: r.key })
            }
        };
        let res = orb.spec.template.resources;
        let base_bytes = std::fs::metadata(&base).map_err(io_err)?.len();
        if res.disk_bytes < base_bytes {
            return Err(RuntimeError::Other(format!(
                "offering disk ({} bytes) is smaller than the base image ({base_bytes} bytes)",
                res.disk_bytes
            )));
        }
        // The network index is claimed (durably, by this orb) before anything is built.
        let (id, state) = (orb.id, self.state_dir.clone());
        let net = tokio::task::spawn_blocking(move || NetAttachment::tap(id, &state))
            .await
            .map_err(|e| RuntimeError::Other(e.to_string()))?
            .map_err(|e| RuntimeError::Other(format!("network: {e}")))?;
        let recipe = VmRecipe {
            orb: orb.id,
            vcpus: res.vcpus,
            memory_bytes: res.memory_bytes,
            disk_bytes: res.disk_bytes,
            source,
            net,
            created_unix_ms: unix_ms_now(),
        };
        self.machine(&recipe)?;
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
            let disk = staging.join("disk.raw");
            log(ctx, "copying the root disk");
            copy_disk(ctx, &base, &disk).await?;
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&disk)
                .map_err(io_err)?;
            f.set_len(res.disk_bytes).map_err(io_err)?;
            f.sync_all().map_err(io_err)?;
            fsync_dir(&staging).map_err(io_err)
        }
        .await;
        if let Err(e) = built {
            let _ = remove_via_trash(&staging, &self.trash());
            net::release(orb.id, &self.state_dir);
            return Err(e);
        }
        rename_durable(&staging, &self.orb_paths(orb.id).dir).map_err(io_err)?;
        Ok(())
    }

    // --- save -------------------------------------------------------------------

    /// pause → snapshot into a fresh `<gen>.tmp` → disk fsync → manifest. Leaves the
    /// VMM paused; the caller kills it and publishes.
    async fn save(
        &self,
        orb: OrbId,
        recipe: &VmRecipe,
        generation: u64,
        guest_boot_id: Option<String>,
        ctx: &OpContext,
    ) -> Result<CheckpointManifest, RuntimeError> {
        let started = Instant::now();
        let record: LaunchRecord = process::read_record(&self.run_paths(orb).launch)
            .ok_or_else(|| RuntimeError::Other("launch record is missing".into()))?;
        let api = self.api(orb);
        bounded(ctx, api.set_state("Paused", API_TIMEOUT))
            .await?
            .map_err(api_err)?;
        crate::crash_point!("firecracker::save::after_stop");

        let tmp = self.checkpoints(orb).fresh_tmp(generation)?;
        let (memory, vmstate) = (tmp.join("memory"), tmp.join("vmstate"));
        for f in [&memory, &vmstate] {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(f)
                .map_err(io_err)?;
        }
        // Held across the snapshot: Firecracker flushes the drive but does not report
        // a failed flush, so orbd checks its own fsync.
        let disk = std::fs::File::open(self.orb_paths(orb).disk).map_err(io_err)?;
        let request = async {
            if let Some(delay) = throttle(recipe.memory_bytes) {
                tokio::time::sleep(delay).await;
            }
            api.put(
                "/snapshot/create",
                &api::SnapshotCreate {
                    snapshot_type: "Full",
                    snapshot_path: &vmstate,
                    mem_file_path: &memory,
                },
                ctx.work_deadline.saturating_duration_since(Instant::now()),
            )
            .await
        };
        bounded(ctx, poll_save(request, &memory))
            .await??
            .map_err(|e| RuntimeError::CheckpointFailed(e.to_string()))?;
        let synced = tokio::task::spawn_blocking(move || disk.sync_all());
        bounded(ctx, synced)
            .await?
            .map_err(|e| RuntimeError::Other(e.to_string()))?
            .map_err(|e| RuntimeError::CheckpointFailed(format!("fsync of the disk: {e}")))?;
        let len = |p: &Path| std::fs::metadata(p).map(|m| m.len()).map_err(io_err);
        let manifest = CheckpointManifest {
            orb,
            generation,
            created_unix_ms: unix_ms_now(),
            machine: record.config,
            memory_bytes: len(&memory)?,
            vmstate_bytes: len(&vmstate)?,
            guest_boot_id,
            save_ms: started.elapsed().as_millis() as u64,
        };
        write_json_atomic(&tmp.join(MANIFEST), &manifest).map_err(io_err)?;
        tracing::info!(orb = %orb, generation, file_bytes = manifest.memory_bytes, ms = manifest.save_ms, "memory checkpoint written");
        Ok(manifest)
    }

    /// After a failed save: once no snapshot request executes any more, discard the
    /// partial checkpoint and resume the guest where it stopped. If that cannot happen
    /// in time the VMM is killed (processes lost) and false is returned.
    async fn abort_save(&self, orb: OrbId, proc: &Process, generation: u64, deadline: Instant) {
        let api = self.api(orb);
        let resume = async {
            // Waits behind a snapshot request still executing.
            let info = api
                .info(deadline.saturating_duration_since(Instant::now()))
                .await
                .map_err(api_err)?;
            self.checkpoints(orb).discard_tmp(generation)?;
            if info.state != "Running" {
                api.set_state("Resumed", API_TIMEOUT)
                    .await
                    .map_err(api_err)?;
            }
            Ok::<_, RuntimeError>(())
        };
        match tokio::time::timeout_at(deadline.into(), resume).await {
            Ok(Ok(())) => {
                tracing::info!(orb = %orb, generation, "save aborted; guest resumed");
                return;
            }
            Ok(Err(e)) => {
                tracing::error!(orb = %orb, generation, error = %e, "could not resume the guest after a failed save; stopping it")
            }
            Err(_) => {
                tracing::error!(orb = %orb, generation, "resuming the guest after a failed save timed out; stopping it")
            }
        }
        if self.kill(orb, proc).await.is_ok() {
            let _ = self.checkpoints(orb).discard_tmp(generation);
        }
    }

    // --- restore ----------------------------------------------------------------

    /// The checkpoint's manifest, if this host can restore it exactly.
    fn restorable(&self, orb: OrbId, generation: u64) -> Result<CheckpointManifest, RuntimeError> {
        let dir = self.checkpoints(orb).published(generation);
        let m: CheckpointManifest = read_json(&dir.join(MANIFEST))
            .map_err(io_err)?
            .ok_or_else(|| RuntimeError::CheckpointIncompatible("manifest missing".into()))?;
        let recipe = self.read_recipe(orb)?;
        let bad = |why: String| Err(RuntimeError::CheckpointIncompatible(why));
        if m.machine.firecracker != self.firecracker {
            return bad(format!(
                "saved by {:?}, this host runs {:?} (or another binary)",
                m.machine.firecracker.version, self.firecracker.version
            ));
        }
        if m.machine.cpu != self.cpu {
            return bad(format!(
                "saved on CPU {:?}, this host has {:?} (or different flags)",
                m.machine.cpu.model, self.cpu.model
            ));
        }
        if m.machine.memory_mib * MIB != recipe.memory_bytes || m.machine.vcpus != recipe.vcpus {
            return bad("recipe changed since the save".into());
        }
        for (file, want) in [("memory", m.memory_bytes), ("vmstate", m.vmstate_bytes)] {
            match std::fs::metadata(dir.join(file)) {
                Ok(meta) if meta.len() == want => {}
                Ok(meta) => {
                    return bad(format!(
                        "{file} has {} bytes, manifest says {want}",
                        meta.len()
                    ));
                }
                Err(e) => return bad(format!("{file}: {e}")),
            }
        }
        Ok(m)
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
        let recipe = self.read_recipe(orb.id)?;
        adopt_net(&recipe.net).await?;
        let dir = self.checkpoints(orb.id).published(g);
        let started = Instant::now();
        let proc = self
            .launch(
                orb.id,
                LaunchKind::Restore { generation: g },
                manifest.machine,
                &recipe.net,
                ctx,
            )
            .await?;
        let api = self.api(orb.id);
        let (vmstate, memory) = (dir.join("vmstate"), dir.join("memory"));
        let body = api::SnapshotLoad {
            snapshot_path: &vmstate,
            mem_backend: api::MemBackend {
                backend_type: "File",
                backend_path: &memory,
            },
            resume_vm: false,
        };
        let load = api.put(
            "/snapshot/load",
            &body,
            ctx.work_deadline.saturating_duration_since(Instant::now()),
        );
        let loaded = match bounded(ctx, load).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(RuntimeError::CheckpointIncompatible(format!(
                "Firecracker could not load the checkpoint: {e}"
            ))),
            Err(e) => Err(e),
        };
        if let Err(e) = loaded {
            self.kill(orb.id, &proc).await?;
            return Err(e);
        }
        crate::crash_point!("firecracker::restore::after_load");
        // From here the restored guest may run and write its disk: the checkpoint must
        // never be restored again.
        if let Err(e) = self.checkpoints(orb.id).consume(g) {
            self.kill(orb.id, &proc).await?;
            return Err(e);
        }
        crate::crash_point!("firecracker::restore::after_consume");
        api.set_state("Resumed", API_TIMEOUT)
            .await
            .map_err(api_err)?;
        let ms = started.elapsed().as_millis();
        tracing::info!(orb = %orb.id, generation = g, ms, "restored from memory checkpoint");
        log(ctx, &format!("restored checkpoint {g} in {ms} ms"));
        Ok(())
    }

    /// Ctrl+Alt+Del (the guest reboots, which ends the VMM), then SIGKILL.
    async fn stop_process(
        &self,
        orb: OrbId,
        proc: &Process,
        deadline: Instant,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        let soft = (Instant::now() + SHUTDOWN_GRACE).min(
            deadline
                .checked_sub(Duration::from_secs(5))
                .unwrap_or(deadline),
        );
        log(ctx, "shutting the guest down");
        if self
            .api(orb)
            .action("SendCtrlAltDel", API_TIMEOUT)
            .await
            .is_ok()
            && proc.wait_exit(soft).await
        {
            log(ctx, "guest shut down");
            self.clear_run(orb);
            return Ok(());
        }
        log(ctx, "guest did not shut down in time; killing Firecracker");
        self.kill(orb, proc).await
    }
}

#[async_trait]
impl Runtime for FirecrackerRuntime {
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            kind: RuntimeKind::Firecracker,
            version: self.firecracker.version.clone(),
            memory_preserving_sleep: true,
            nested_kvm: false,
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
        let _g = bounded(ctx, self.locks.lock(orb.id)).await?;
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
                crate::crash_point!("firecracker::create::after_publish");
                self.boot(orb, ctx).await
            }
            StartSource::ExistingDiskColdBoot => {
                match self.reconcile(orb.id).await? {
                    Observed::Stopped { .. } => {}
                    Observed::Running(_) => return Ok(()),
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
                self.checkpoints(orb.id).consume_all()?;
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
        let _lock = bounded(ctx, self.locks.lock(orb.id)).await?;
        let proc = match self.reconcile(orb.id).await? {
            Observed::Running(proc) => proc,
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
        let need = recipe.memory_bytes + CHECKPOINT_MARGIN;
        let free = free_bytes(&self.orb_paths(orb.id).checkpoints).map_err(io_err)?;
        if free < need {
            return Err(RuntimeError::InsufficientDisk(format!(
                "checkpoint needs up to {need} bytes, {free} free"
            )));
        }
        let _permit = bounded(ctx, SAVES.acquire())
            .await?
            .map_err(|_| RuntimeError::Other("save semaphore closed".into()))?;
        log(ctx, "asking the guest to prepare for sleep");
        let connector = self.connector(orb.id);
        let prep_deadline = (Instant::now() + PREPARE_SLEEP_TIMEOUT).min(ctx.work_deadline);
        let guest_boot_id = match bounded(ctx, channel::prepare_sleep(&connector, prep_deadline))
            .await?
        {
            Ok(boot_id) => Some(boot_id),
            Err(e) => {
                tracing::warn!(orb = %orb.id, error = %e, "guest did not prepare for sleep; saving anyway");
                log(
                    ctx,
                    &format!("guest did not prepare for sleep ({e}); saving anyway"),
                );
                None
            }
        };
        log(ctx, &format!("saving memory checkpoint {g}"));
        let manifest = match self.save(orb.id, &recipe, g, guest_boot_id, ctx).await {
            Ok(m) => m,
            Err(e) => {
                log(ctx, &format!("save failed ({e}); resuming the guest"));
                self.abort_save(orb.id, &proc, g, ctx.cleanup_deadline)
                    .await;
                return Err(match e {
                    RuntimeError::Cancelled
                    | RuntimeError::DeadlineExceeded
                    | RuntimeError::InsufficientDisk(_)
                    | RuntimeError::CheckpointFailed(_) => e,
                    other => RuntimeError::CheckpointFailed(other.to_string()),
                });
            }
        };
        crate::crash_point!("firecracker::save::after_manifest");
        // Never publish while the source could still run.
        self.kill(orb.id, &proc).await?;
        crate::crash_point!("firecracker::save::after_quit");
        self.checkpoints(orb.id).publish(g)?;
        crate::crash_point!("firecracker::save::after_publish");
        log(
            ctx,
            &format!(
                "checkpoint {g} published: {} MiB of RAM in {} ms",
                manifest.memory_bytes / MIB,
                manifest.save_ms
            ),
        );
        Ok(done)
    }

    async fn cold_stop(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), RuntimeError> {
        let _g = self.locks.lock(orb.id).await;
        match self.reconcile(orb.id).await? {
            Observed::Absent => Err(RuntimeError::NotFound(format!(
                "orb {} has no disk",
                orb.id
            ))),
            Observed::Stopped { .. } => Ok(()),
            Observed::Inconsistent(r) => Err(RuntimeError::Other(r)),
            Observed::Running(proc) => {
                self.stop_process(orb.id, &proc, ctx.work_deadline, ctx)
                    .await
            }
        }
    }

    async fn discard_checkpoint(
        &self,
        orb: OrbId,
        checkpoint: &SleepCheckpointRef,
    ) -> Result<(), RuntimeError> {
        let _g = self.locks.lock(orb).await;
        self.checkpoints(orb).discard(checkpoint.generation)
    }

    /// Copies the stopped orb's disk (reflink or sparse) into a project snapshot, the
    /// same raw format QEMU publishes.
    async fn clone_disk(
        &self,
        orb: &OrbDefinition,
        key: &ProjectSnapshotKey,
        ctx: &OpContext,
    ) -> Result<ProjectSnapshotRef, RuntimeError> {
        let done = ProjectSnapshotRef { key: key.clone() };
        if self
            .state_dir
            .join("projects")
            .join(key.as_str())
            .join("disk.raw")
            .is_file()
        {
            return Ok(done);
        }
        let _g = bounded(ctx, self.locks.lock(orb.id)).await?;
        match self.reconcile(orb.id).await? {
            Observed::Stopped { .. } => {}
            other => {
                return Err(RuntimeError::Other(format!(
                    "snapshot needs a stopped orb, found {other}"
                )));
            }
        }
        let disk = self.orb_paths(orb.id).disk;
        log(ctx, &format!("copying disk into project snapshot {key}"));
        publish_project(&self.state_dir, key, orb.id, |raw| async move {
            copy_disk(ctx, &disk, &raw).await
        })
        .await?;
        crate::crash_point!("firecracker::clone::after_publish");
        log(ctx, &format!("project snapshot {key} published"));
        Ok(done)
    }

    async fn delete(&self, orb: OrbId, ctx: &OpContext) -> Result<(), RuntimeError> {
        let _g = bounded(ctx, self.locks.lock(orb)).await?;
        let rp = self.run_paths(orb);
        if let Some(record) = process::read_record::<Machine>(&rp.launch)
            && let Some(proc) = Process::adopt(&record, &marker(&record), None)
        {
            log(ctx, &format!("killing Firecracker (pid {})", proc.pid));
            self.kill(orb, &proc).await?;
        }
        let state = self.state_dir.clone();
        tokio::task::spawn_blocking(move || net::release(orb, &state))
            .await
            .map_err(|e| RuntimeError::Other(e.to_string()))?;
        let dir = self.orb_paths(orb).dir;
        if dir.exists() {
            log(ctx, "removing disk and checkpoints");
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

/// `firecracker --version`'s first line, e.g. `Firecracker v1.17.0`.
fn version(path: &Path) -> Result<String, String> {
    let out = std::process::Command::new(path)
        .arg("--version")
        .output()
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let line = String::from_utf8_lossy(&out.stdout)
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_owned();
    if !out.status.success() || !line.starts_with("Firecracker v") {
        return Err(format!(
            "{} --version: unexpected output {line:?}",
            path.display()
        ));
    }
    Ok(line)
}

/// Repairs the orb's network namespace if it is missing (after a host reboot).
async fn adopt_net(net: &NetAttachment) -> Result<(), RuntimeError> {
    let net = net.clone();
    tokio::task::spawn_blocking(move || net.adopt())
        .await
        .map_err(|e| RuntimeError::Other(e.to_string()))?
        .map_err(|e| RuntimeError::Other(format!("network: {e}")))
}

fn api_err(e: api::ApiError) -> RuntimeError {
    RuntimeError::Other(e.to_string())
}

/// Drives a snapshot request, evaluating the save fault points while it runs.
async fn poll_save<F: std::future::Future<Output = Result<(), api::ApiError>>>(
    request: F,
    #[cfg_attr(not(feature = "failpoints"), allow(unused_variables))] memory: &Path,
) -> Result<Result<(), api::ApiError>, RuntimeError> {
    tokio::pin!(request);
    loop {
        tokio::select! {
            r = &mut request => return Ok(r),
            _ = tokio::time::sleep(POLL) => {
                #[cfg(feature = "failpoints")]
                {
                    if fail::eval("firecracker::save::write", |_| ()).is_some() {
                        return Err(RuntimeError::CheckpointFailed(format!(
                            "writing the checkpoint: {}",
                            std::io::Error::from_raw_os_error(nix::libc::ENOSPC)
                        )));
                    }
                    // Crash only once Firecracker is executing the request (it sizes
                    // the memory file first).
                    if fail::eval("firecracker::save::after_migrate", |_| ()).is_some()
                        && std::fs::metadata(memory).is_ok_and(|m| m.len() > 0)
                    {
                        tracing::error!("crash point firecracker::save::after_migrate");
                        std::process::abort();
                    }
                }
            }
        }
    }
}

/// `firecracker::save::throttle=return(<MiB/s>)`: how long a save of `bytes` should take
/// beyond its real duration.
fn throttle(
    #[cfg_attr(not(feature = "failpoints"), allow(unused_variables))] bytes: u64,
) -> Option<Duration> {
    #[cfg(feature = "failpoints")]
    if let Some(mib) = fail::eval("firecracker::save::throttle", |a| {
        a.and_then(|a| a.parse::<u64>().ok())
    })
    .flatten()
    {
        return Some(Duration::from_millis(bytes / MIB * 1000 / mib.max(1)));
    }
    None
}
