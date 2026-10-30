//! The compute-backend seam.
//!
//! The coordinator core owns orb identity, placement choice, atomic admission and
//! reservations, request queueing, per-orb serialization, the command journal and
//! outbox, and applying results. A [`ComputeBackend`] supplies placement candidates and
//! capabilities, delivers commands, forwards cancellation, answers reconciliation, and
//! reports acceptance/progress/logs/completion as [`BackendEvent`]s.
//!
//! Executors (durable acceptance, execution, terminal receipts) live *behind* a
//! backend: on agents for the agent fleet ([`fleet`]); inside the coordinator, with a
//! backend-private store, for API backends ([`boat`]).
//!
//! Rules every backend follows:
//! * `submit` is a delivery attempt. It is neither durable acceptance nor completion.
//!   An `Err` does not prove the command was not accepted.
//! * Acceptance and completion are replayable: completion is re-sent until
//!   [`ComputeBackend::acknowledge_completion`] is called (after the core commits it).
//! * A transport disconnect does not end the event stream or any accepted operation.
//! * `NotAccepted` in reconciliation requires authoritative receipt evidence.

pub mod boat;
pub mod fleet;

use crate::model::{
    BackendCapabilities, BackendId, CommandKey, CommandOutcome, ExecutionBudget, ExecutionTarget,
    Location, OrbDefinition, Resources,
};
use crate::secrets::PermittedGuestEnv;
use async_trait::async_trait;
use futures::stream::BoxStream;
use std::sync::Arc;

pub struct BackendHandle {
    pub backend: Arc<dyn ComputeBackend>,
    /// Single consumer: the coordinator core. Reconnects are handled inside the backend.
    pub events: BoxStream<'static, BackendEvent>,
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// Transport or location unavailable. Does not prove anything about acceptance.
    #[error("backend unavailable: {0}")]
    Unavailable(String),
    #[error("backend rejected request: {0}")]
    Invalid(String),
    #[error("backend failure: {0}")]
    Other(String),
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct CapabilityError(pub String);

/// Input to [`ComputeBackend::candidates`].
#[derive(Clone, Debug)]
pub struct PlacementRequest {
    pub target: ExecutionTarget,
    pub resources: Resources,
    /// For an existing orb: its recorded location. Candidates are then restricted to it
    /// (used to check availability/capacity before resuming), never anywhere else.
    pub existing: Option<Location>,
    /// Project snapshot the new orb should start from, if any (prefer locations holding it).
    pub project_snapshot: Option<crate::model::ProjectSnapshotKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Availability {
    Available,
    /// Disconnected or unhealthy. Not evidence that its orbs stopped.
    Unavailable {
        reason: String,
    },
    /// Connected but shutting down: takes no new commands until it reconnects.
    Draining,
}

#[derive(Clone, Debug)]
pub struct PlacementCandidate {
    pub location: Location,
    pub availability: Availability,
    pub capabilities: BackendCapabilities,
    /// Capacity pool this candidate draws from (an agent, a provider quota).
    pub capacity_pool: String,
    /// Gross ceiling of the pool. The core subtracts its own reservations; backends must
    /// not report an already-net figure.
    pub capacity_ceiling: Resources,
    /// Resources one orb of this request reserves in the pool (spec + overhead). The
    /// core replaces the disk figure with its headroom policy.
    pub demand: Resources,
    /// The location's disk as last reported; None until it reports.
    pub disk: Option<crate::model::DiskReport>,
    /// The location's memory as last reported; None when it does not report it.
    pub memory: Option<crate::model::MemoryReport>,
    /// True when the location already holds the requested project snapshot.
    pub has_project_snapshot: bool,
}

/// Domain form of a delivery. `secrets` is secret-bearing: never logged.
#[derive(Clone, Debug)]
pub struct CommandDelivery {
    pub command: crate::model::Command,
    pub controller_epoch: u64,
    pub budget: ExecutionBudget,
    pub orb: OrbDefinition,
    pub location: Location,
    pub secrets: CommandSecrets,
}

#[derive(Clone, Debug, Default)]
pub enum CommandSecrets {
    #[default]
    None,
    Bootstrap(PermittedGuestEnv),
}

#[derive(Clone, Debug)]
pub struct CancelRequest {
    pub key: CommandKey,
    pub location: Location,
    pub reason: String,
}

#[derive(Clone, Debug)]
pub struct ReconcileQuery {
    pub location: Location,
    pub operations: Vec<CommandKey>,
    pub orbs: Vec<crate::model::OrbId>,
    pub page_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OperationStatus {
    NotAccepted,
    CancelledBeforeAccept,
    Running,
    Completed(CommandOutcome),
    Unknown,
}

#[derive(Clone, Debug)]
pub struct ReconcilePage {
    pub operations: Vec<(CommandKey, OperationStatus)>,
    pub orbs: Vec<(crate::model::OrbId, u64, crate::model::OrbState)>,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug)]
pub struct LogChunk {
    pub seq: u64,
    pub stream: LogStream,
    pub data: bytes::Bytes,
    /// Chunks discarded before this one by bounded buffers.
    pub dropped_before: u64,
}

#[derive(Clone, Debug)]
pub enum BackendEvent {
    /// The executor durably accepted the command.
    Accepted {
        key: CommandKey,
    },
    /// The executor refused the delivery without executing anything.
    Rejected {
        key: CommandKey,
        failure: crate::model::CommandFailure,
    },
    Progress {
        key: CommandKey,
        phase: String,
        message: String,
    },
    Log {
        key: CommandKey,
        chunk: LogChunk,
    },
    /// Terminal; replayed until acknowledged.
    Completed {
        key: CommandKey,
        outcome: CommandOutcome,
    },
    /// The backend lost track of an operation's status; the core should reconcile.
    ReconcileNeeded {
        key: CommandKey,
    },
    AvailabilityChanged {
        location: Location,
        availability: Availability,
    },
}

#[async_trait]
pub trait ComputeBackend: Send + Sync {
    fn id(&self) -> &BackendId;

    /// Static/configured feature ceiling for one execution profile.
    fn capabilities(
        &self,
        target: &ExecutionTarget,
    ) -> Result<BackendCapabilities, CapabilityError>;

    /// Eligible locations. No side effects.
    async fn candidates(
        &self,
        request: PlacementRequest,
    ) -> Result<Vec<PlacementCandidate>, BackendError>;

    /// Delivery attempt (see module docs).
    async fn submit(&self, delivery: CommandDelivery) -> Result<(), BackendError>;

    /// Requests cancellation; returning does not establish it.
    async fn cancel(&self, request: CancelRequest) -> Result<(), BackendError>;

    /// Bounded, paginated inspection of one location.
    async fn reconcile(&self, query: ReconcileQuery) -> Result<ReconcilePage, BackendError>;

    /// Called only after the core has committed the result and its consequences.
    async fn acknowledge_completion(
        &self,
        location: &Location,
        key: CommandKey,
    ) -> Result<(), BackendError>;
}
