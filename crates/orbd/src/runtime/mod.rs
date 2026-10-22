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
    InitialDiskSource, OrbDefinition, OrbId, ProjectSnapshotKey, ProjectSnapshotRef, RuntimeKind,
    SleepCheckpointRef,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    pub kind: RuntimeKind,
    /// Exact runtime version (VMM binary version, docker engine version, ...).
    pub version: String,
    pub memory_preserving_sleep: bool,
    pub nested_kvm: bool,
    pub nested_state_qualified: bool,
    pub project_snapshots: bool,
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
    async fn checkpoint_and_stop(
        &self,
        orb: &OrbDefinition,
        checkpoint_generation: u64,
        ctx: &OpContext,
    ) -> Result<SleepCheckpointRef, RuntimeError>;

    /// Stops the orb without saving memory (processes lost). Idempotent.
    async fn cold_stop(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), RuntimeError>;

    /// Cleanup only: removes a checkpoint that `start` already consumed. Idempotent.
    async fn discard_checkpoint(
        &self,
        orb: OrbId,
        checkpoint: &SleepCheckpointRef,
    ) -> Result<(), RuntimeError>;

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
