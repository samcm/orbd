//! Host-local runtimes used by agents (agent-fleet backend only).
//!
//! A runtime turns lifecycle steps into VMM/container actions. Scheduling, credentials,
//! journals, retries and serialization stay in the agent executor, which owns one actor
//! per orb and calls at most one runtime method per orb at a time.
//!
//! Every method must honour `ctx.cancel` and `ctx.work_deadline` and be safe to call
//! again after a crash (inspect first, then act). Runtimes never receive shell
//! commands, host paths or free-form VMM arguments from the coordinator.

pub mod docker;
pub mod fake;
pub mod firecracker;
pub mod qemu;
pub mod vm;

use crate::model::{
    Arch, InitialDiskSource, OrbDefinition, OrbId, ProjectSnapshotKey, ProjectSnapshotRef,
    RuntimeKind, SleepCheckpointRef,
};
use crate::secrets::PermittedGuestEnv;
use async_trait::async_trait;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// The orb's hostname: an RFC 1123 label from Amp's orb id (`[A-Za-z0-9._-]`), for
/// prompts and logs.
pub(crate) fn orb_hostname(orb: &OrbDefinition) -> String {
    let h: String = orb
        .identity
        .amp_orb_id
        .0
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .take(63)
        .collect();
    let h = h.trim_matches('-');
    if h.is_empty() { "orb".into() } else { h.into() }
}

/// Why an agent without `ORBD_NESTED_KVM=true` does not start an orb.
pub(crate) const NESTED_KVM_REFUSED: &str = "this orb's type has kvm, and this agent runs \
     no orbs with /dev/kvm: start it with ORBD_NESTED_KVM=true to run them";

/// Whether `orb` gets `/dev/kvm` inside it (its type has `kvm`).
pub(crate) fn needs_nested_kvm(orb: &OrbDefinition) -> bool {
    orb.spec.template.requirements.nested_kvm != crate::model::NestedKvmRequirement::NotRequired
}

/// Why an agent does not start an orb whose type restricts egress.
pub(crate) const EGRESS_REFUSED: &str = "this agent cannot enforce restricted egress \
     (the orb type's `egress`) for this runtime; it runs such orbs only in Linux VMs";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    pub kind: RuntimeKind,
    /// Exact runtime version (VMM binary version, docker engine version, ...).
    pub version: String,
    pub memory_preserving_sleep: bool,
    pub nested_kvm: bool,
    pub nested_state_qualified: bool,
    pub project_snapshots: bool,
    /// CPU architecture of the orbs it runs: the host's.
    pub arch: Arch,
    /// Enforces restricted egress (an orb type's `egress`): the Linux VM runtimes, with
    /// their DNS proxy running.
    pub egress_policy: bool,
}

/// How to start an orb. Distinct cases, no boolean soup.
#[derive(Clone, Debug)]
pub enum StartSource {
    /// Create a fresh disk from a prepared image or project snapshot, then boot.
    NewDisk(InitialDiskSource),
    /// Restore memory from a published checkpoint (warm wake, processes preserved).
    MemoryCheckpoint(SleepCheckpointRef),
    /// Boot the orb's existing disk (processes lost).
    ExistingDiskColdBoot,
}

/// What the runtime can see on the host for one orb.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeObservation {
    /// No disk, no process, no checkpoint.
    Absent,
    Running,
    /// Not running; disk present; the latest valid published checkpoint, if any.
    Stopped {
        checkpoint: Option<SleepCheckpointRef>,
    },
    /// Something exists but is inconsistent (e.g. process without disk).
    Inconsistent {
        reason: String,
    },
}

/// A published memory checkpoint's age and size (see [`Runtime::checkpoint_info`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// When the save completed: the orb went to sleep then (wall clock).
    pub saved_unix_ms: u64,
    /// Bytes its files hold on disk (allocated blocks; memory files may be sparse).
    pub bytes: u64,
    /// The guest synced its filesystems before the save, so its disk is consistent
    /// without the checkpoint. Without that, a cold boot of the disk can find data the
    /// guest had not yet written out.
    pub synced: bool,
}

/// A sleep held in RAM: the orb's VMM, paused, still holds its memory, and its memory
/// checkpoint is not written yet (see [`Runtime::checkpoint_and_stop`]'s `hold`). The
/// orb counts as asleep on `checkpoint`: a wake resumes the VM in place, and the
/// checkpoint is written ([`Runtime::flush_held`]) only when the agent needs the memory
/// back, the hold has lasted long enough, or the agent stops.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeldSleep {
    pub orb: OrbId,
    pub checkpoint: SleepCheckpointRef,
    /// When the VM was paused (wall clock).
    pub since_unix_ms: u64,
    /// Host memory the paused VMM holds now (its anonymous resident memory: what
    /// writing its checkpoint frees).
    pub resident_bytes: u64,
    /// The orb's memory size: what its checkpoint takes on disk once written.
    pub memory_bytes: u64,
}

/// What [`Runtime::flush_held`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushOutcome {
    /// The checkpoint was written and the VMM stopped: the orb sleeps on disk.
    Written,
    /// The orb is not held (woken, deleted, or written already).
    NotHeld,
    /// The held VMM was gone (killed, or the host lost it): the orb is stopped without a
    /// checkpoint, its disk as the guest synced it before the pause.
    Lost,
}

/// A stopped orb's disk compressed in place (see [`Runtime::archive_disk`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ArchiveInfo {
    pub archived_unix_ms: u64,
    /// The compressed file's size.
    pub compressed_bytes: u64,
    /// What the disk held when it was archived (allocated blocks): what a restore
    /// writes again.
    pub allocated_bytes: u64,
    /// The disk's size (apparent).
    pub disk_bytes: u64,
    /// A restore of it is running.
    #[serde(skip)]
    pub restoring: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// Bounded sink for operation output streamed to Amp's setup log.
pub type LogSink = tokio::sync::mpsc::Sender<(LogStream, bytes::Bytes)>;

/// Per-call context.
#[derive(Clone)]
pub struct OpContext {
    pub operation_id: crate::model::OperationId,
    pub generation: u64,
    pub work_deadline: Instant,
    pub cleanup_deadline: Instant,
    pub cancel: CancellationToken,
    pub log: LogSink,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("image not ready: {0}")]
    ImageNotReady(String),
    #[error("checkpoint incompatible: {0}")]
    CheckpointIncompatible(String),
    #[error("checkpoint failed: {0}")]
    CheckpointFailed(String),
    #[error("insufficient disk: {0}")]
    InsufficientDisk(String),
    /// The orb's archived disk is being restored; the spawn is worth retrying.
    #[error("disk not ready: {0}")]
    DiskNotReady(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("cancelled")]
    Cancelled,
    #[error("deadline exceeded")]
    DeadlineExceeded,
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("{0}")]
    Other(String),
}

#[async_trait]
pub trait Runtime: Send + Sync {
    fn capabilities(&self) -> RuntimeCapabilities;

    async fn inspect(&self, orb: OrbId) -> Result<RuntimeObservation, RuntimeError>;

    /// Cheap check, without the orb lock or a call into the VMM, that the orb's VMM
    /// process launched last still exists: `Some(false)` once it is gone, `None` when the
    /// runtime cannot tell. Lets the agent's liveness sweep skip `inspect` (which settles
    /// and may wait on the VMM) for orbs that are plainly still running.
    fn vmm_alive(&self, _orb: OrbId) -> Option<bool> {
        None
    }

    /// Starts the orb. Must not return Ok until the VMM/container is running.
    ///
    /// `MemoryCheckpoint`: the runtime must durably mark the checkpoint consumed *before*
    /// the restored guest can run or write its disk (e.g. load with vCPUs stopped, record
    /// consumption, then resume), so a crash can never replay stale RAM against a newer
    /// disk. "Consumed" keeps the files; `discard_checkpoint` deletes them later.
    async fn start(
        &self,
        orb: &OrbDefinition,
        source: StartSource,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError>;

    /// Saves memory to a new checkpoint generation, stops and reaps the VMM, then
    /// atomically publishes the checkpoint. Never returns Ok while the source can run.
    ///
    /// With `hold`, a runtime that can (Firecracker) instead pauses the VM in place once
    /// the guest synced its disk, durably records the hold and returns: the orb sleeps
    /// on `checkpoint_generation` held in RAM ([`HeldSleep`]) until it is woken (resumed
    /// in place by `start` from that checkpoint) or [`flush_held`](Self::flush_held)
    /// writes the checkpoint. Others ignore `hold`.
    async fn checkpoint_and_stop(
        &self,
        orb: &OrbDefinition,
        checkpoint_generation: u64,
        hold: bool,
        ctx: &OpContext,
    ) -> Result<SleepCheckpointRef, RuntimeError>;

    /// Stops the orb without saving memory (processes lost). Idempotent.
    async fn cold_stop(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), RuntimeError>;

    /// Orbs asleep in RAM (see [`HeldSleep`]). Cheap: no orb lock, no VMM call.
    fn held(&self) -> Vec<HeldSleep> {
        Vec::new()
    }

    /// Whether `checkpoint_and_stop`'s `hold` does anything here.
    fn holds_sleeps(&self) -> bool {
        false
    }

    /// Whether a start from a memory checkpoint on disk frees that checkpoint's blocks
    /// when `discard_checkpoint` removes it right after: the restore read it into the
    /// guest's memory (QEMU). Not where the restored VMM maps the file for the life of
    /// the VM (Firecracker): its blocks stay allocated until that VMM exits, after the
    /// orb's next checkpoint is written. The coordinator credits a wake with what it
    /// frees (see the heartbeat's `wake_releases`).
    fn restore_frees_checkpoint(&self) -> bool {
        false
    }

    /// Writes a held sleep's checkpoint exactly as an unheld sleep would, then stops the
    /// VMM, freeing its memory. A failed write leaves the orb held.
    async fn flush_held(
        &self,
        _orb: &OrbDefinition,
        _ctx: &OpContext,
    ) -> Result<FlushOutcome, RuntimeError> {
        Ok(FlushOutcome::NotHeld)
    }

    /// Disk bytes this runtime has committed but not yet written: restores of archived
    /// disks in progress (what they still write). Cheap.
    fn disk_in_flight(&self) -> u64 {
        0
    }

    /// Whether this runtime can archive stopped orbs' disks ([`archive_disk`](Self::archive_disk)).
    fn archives(&self) -> bool {
        false
    }

    /// Compresses a stopped orb's disk (no VMM, no memory checkpoint) in place; `start`
    /// restores it before a cold boot. None if the orb is not such an orb, or is
    /// archived already. Honours `ctx.cancel` promptly, leaving the disk as it was.
    async fn archive_disk(
        &self,
        _orb: &OrbDefinition,
        _ctx: &OpContext,
    ) -> Result<Option<ArchiveInfo>, RuntimeError> {
        Err(RuntimeError::Unsupported("archiving disks".into()))
    }

    /// The orb's archive, if its disk is archived. Cheap: no orb lock.
    fn archived(&self, _orb: OrbId) -> Option<ArchiveInfo> {
        None
    }

    /// Removes a checkpoint, marking it consumed first so it is never restored: one that
    /// `start` already consumed, or (agent maintenance, orb stopped) one dropped so that
    /// the orb cold-boots on its next start. The files (GiB) are deleted off the async
    /// workers. Idempotent.
    async fn discard_checkpoint(
        &self,
        orb: OrbId,
        checkpoint: &SleepCheckpointRef,
    ) -> Result<(), RuntimeError>;

    /// When a published, unconsumed checkpoint was saved and what it holds on disk.
    /// `None` when there is no such checkpoint, or the runtime keeps none (then the
    /// agent never drops one for its age). Reads only immutable published files, so it
    /// is safe beside the orb's operations.
    async fn checkpoint_info(
        &self,
        _orb: OrbId,
        _checkpoint: &SleepCheckpointRef,
    ) -> Result<Option<CheckpointInfo>, RuntimeError> {
        Ok(None)
    }

    /// Clones the (stopped) orb's disk into an immutable project snapshot.
    async fn clone_disk(
        &self,
        orb: &OrbDefinition,
        key: &ProjectSnapshotKey,
        ctx: &OpContext,
    ) -> Result<ProjectSnapshotRef, RuntimeError>;

    /// Removes everything for the orb. Idempotent.
    async fn delete(&self, orb: OrbId, ctx: &OpContext) -> Result<(), RuntimeError>;

    /// Guest control adapter for this runtime (Docker: exec; VMs: vsock).
    fn guest(&self) -> &dyn GuestControl;
}

#[derive(Debug)]
pub struct BootstrapRequest {
    /// Generation of the spawn command; the guest acks it.
    pub generation: u64,
    pub env: PermittedGuestEnv,
    /// Cold boot (or new disk): stop any stale amp-headless before bootstrapping.
    /// Warm restore: do not restart amp-headless; step the clock, refresh credentials.
    pub cold_boot: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum GuestError {
    #[error("guest unreachable: {0}")]
    Unreachable(String),
    #[error("guest bootstrap failed: {0}")]
    BootstrapFailed(String),
    #[error("cancelled")]
    Cancelled,
    #[error("deadline exceeded")]
    DeadlineExceeded,
}

#[async_trait]
pub trait GuestControl: Send + Sync {
    /// Delivers credentials and (re)starts `amp orb bootstrap` inside the guest.
    async fn bootstrap(
        &self,
        orb: &OrbDefinition,
        request: BootstrapRequest,
        ctx: &OpContext,
    ) -> Result<(), GuestError>;

    /// After a memory restore that no spawn asked for (agent maintenance): steps the
    /// guest clock and reapplies its network. No credentials, Amp's units untouched.
    async fn resync(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), GuestError>;
}
