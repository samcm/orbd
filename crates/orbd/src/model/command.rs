//! Commands, budgets and outcomes.
//!
//! Delivery is at-least-once with no external job id: every command carries an internal
//! operation id and a per-orb monotonic generation. Same operation id + different
//! `command_digest` is a protocol conflict. Epoch and remaining budget belong to the
//! delivery envelope, not to the command's identity.

use super::artifact::{ProjectSnapshotRef, SleepCheckpointRef};
use super::identity::{OperationId, OrbId, ProjectSnapshotKey};
use super::spec::SpecDigest;
use super::state::OrbState;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Work must finish this long after the dispatch invocation started.
pub const WORK_BUDGET: Duration = Duration::from_secs(240);
/// Cleanup (cancellation, cold-stop fallback) must finish by this point.
pub const CLEANUP_BUDGET: Duration = Duration::from_secs(270);
/// `orbd dispatch` returns by this point; Amp kills jobs at 300 s.
pub const DISPATCH_BUDGET: Duration = Duration::from_secs(280);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Action {
    /// Create, restore or cold-boot the orb, then (re)bootstrap it with the command's
    /// sealed guest env. Runs bootstrap even when the orb is already running.
    Spawn,
    Pause {
        publish_snapshot: Option<ProjectSnapshotKey>,
    },
    Delete,
}

impl Action {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Spawn => "spawn",
            Self::Pause { .. } => "pause",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CommandDigest(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub operation_id: OperationId,
    pub orb_id: OrbId,
    pub generation: u64,
    pub spec_digest: SpecDigest,
    pub command_digest: CommandDigest,
    pub action: Action,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CommandKey {
    pub operation_id: OperationId,
    pub orb_id: OrbId,
    pub generation: u64,
}

impl Command {
    pub fn key(&self) -> CommandKey {
        CommandKey {
            operation_id: self.operation_id,
            orb_id: self.orb_id,
            generation: self.generation,
        }
    }
}

/// Remaining budget carried by each delivery. Executors pin absolute deadlines on first
/// acceptance; redelivery may shorten them, never extend them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBudget {
    pub work_remaining_ms: u64,
    pub cleanup_remaining_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    BackendUnavailable,
    InsufficientMemory,
    InsufficientDisk,
    QuotaExceeded,
    ImageNotReady,
    UnsupportedCapability,
    CheckpointIncompatible,
    CheckpointFailed,
    SnapshotFailed,
    GuestBootstrapFailed,
    SecretUnavailable,
    SpecMismatch,
    OperationConflict,
    StaleGeneration,
    StaleController,
    DeadlineExceeded,
    Cancelled,
    BackendFailure,
    OrbDeleted,
    InvalidRequest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandFailure {
    pub code: FailureCode,
    pub message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartKind {
    NewDisk,
    MemoryRestore,
    ExistingDiskColdBoot,
    AlreadyRunning,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ActionResult {
    Spawned {
        start: StartKind,
        bootstrap_generation: u64,
    },
    Paused {
        checkpoint: Option<SleepCheckpointRef>,
    },
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessDisposition {
    NotApplicable,
    Preserved,
    Lost,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SnapshotDisposition {
    NotRequested,
    Published { snapshot: ProjectSnapshotRef },
    NotPublished { reason: String },
}

/// Terminal outcome of a command: no unresolved mutation from it can still occur.
/// (It does not imply the orb is stopped: a bootstrap failure can leave it running.)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandOutcome {
    pub result: Result<ActionResult, CommandFailure>,
    pub observed: OrbState,
    pub processes: ProcessDisposition,
    pub snapshot: SnapshotDisposition,
}
