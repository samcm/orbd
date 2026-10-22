//! Conversions between the domain model and `orbd.control.v1` protobuf types.
//! Unknown or unspecified enum values and missing `oneof`s are rejected, never guessed.

use super::*;
use orbd_proto::control::v1 as pb;

#[derive(Debug, thiserror::Error)]
#[error("invalid wire message: {0}")]
pub struct WireError(pub String);

fn err<T>(msg: impl Into<String>) -> Result<T, WireError> {
    Err(WireError(msg.into()))
}

fn uuid<T>(s: &str, what: &str, f: impl FnOnce(uuid::Uuid) -> T) -> Result<T, WireError> {
    uuid::Uuid::parse_str(s)
        .map(f)
        .map_err(|_| WireError(format!("bad {what}: {s:?}")))
}

pub fn orb_id(s: &str) -> Result<OrbId, WireError> {
    uuid(s, "orb_id", OrbId)
}

pub fn operation_id(s: &str) -> Result<OperationId, WireError> {
    uuid(s, "operation_id", OperationId)
}

// --- enums ------------------------------------------------------------------

impl From<RuntimeKind> for pb::RuntimeKind {
    fn from(k: RuntimeKind) -> Self {
        match k {
            RuntimeKind::Fake => Self::Fake,
            RuntimeKind::Docker => Self::Docker,
            RuntimeKind::Qemu => Self::Qemu,
            RuntimeKind::Firecracker => Self::Firecracker,
        }
    }
}

pub fn runtime_kind(v: i32) -> Result<RuntimeKind, WireError> {
    Ok(match pb::RuntimeKind::try_from(v) {
        Ok(pb::RuntimeKind::Fake) => RuntimeKind::Fake,
        Ok(pb::RuntimeKind::Docker) => RuntimeKind::Docker,
        Ok(pb::RuntimeKind::Qemu) => RuntimeKind::Qemu,
        Ok(pb::RuntimeKind::Firecracker) => RuntimeKind::Firecracker,
        _ => return err(format!("runtime kind {v}")),
    })
}

macro_rules! failure_codes {
    ($($d:ident),* $(,)?) => {
        impl From<FailureCode> for pb::FailureCode {
            fn from(c: FailureCode) -> Self {
                match c { $(FailureCode::$d => Self::$d,)* }
            }
        }
        pub fn failure_code(v: i32) -> Result<FailureCode, WireError> {
            match pb::FailureCode::try_from(v) {
                $(Ok(pb::FailureCode::$d) => Ok(FailureCode::$d),)*
                _ => err(format!("failure code {v}")),
            }
        }
    };
}

failure_codes!(
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
);

impl From<ProcessDisposition> for pb::ProcessDisposition {
    fn from(p: ProcessDisposition) -> Self {
        match p {
            ProcessDisposition::NotApplicable => Self::NotApplicable,
            ProcessDisposition::Preserved => Self::Preserved,
            ProcessDisposition::Lost => Self::Lost,
            ProcessDisposition::Unknown => Self::Unknown,
        }
    }
}

fn process_disposition(v: i32) -> Result<ProcessDisposition, WireError> {
    Ok(match pb::ProcessDisposition::try_from(v) {
        Ok(pb::ProcessDisposition::NotApplicable) => ProcessDisposition::NotApplicable,
        Ok(pb::ProcessDisposition::Preserved) => ProcessDisposition::Preserved,
        Ok(pb::ProcessDisposition::Lost) => ProcessDisposition::Lost,
        Ok(pb::ProcessDisposition::Unknown) => ProcessDisposition::Unknown,
        _ => return err(format!("process disposition {v}")),
    })
}

impl From<StartKind> for pb::StartKind {
    fn from(s: StartKind) -> Self {
        match s {
            StartKind::NewDisk => Self::NewDisk,
            StartKind::MemoryRestore => Self::MemoryRestore,
            StartKind::ExistingDiskColdBoot => Self::ExistingDiskColdBoot,
            StartKind::AlreadyRunning => Self::AlreadyRunning,
        }
    }
}

fn start_kind(v: i32) -> Result<StartKind, WireError> {
    Ok(match pb::StartKind::try_from(v) {
        Ok(pb::StartKind::NewDisk) => StartKind::NewDisk,
        Ok(pb::StartKind::MemoryRestore) => StartKind::MemoryRestore,
        Ok(pb::StartKind::ExistingDiskColdBoot) => StartKind::ExistingDiskColdBoot,
        Ok(pb::StartKind::AlreadyRunning) => StartKind::AlreadyRunning,
        _ => return err(format!("start kind {v}")),
    })
}

// --- resources and spec -----------------------------------------------------

impl From<Resources> for pb::Resources {
    fn from(r: Resources) -> Self {
        Self {
            vcpus: r.vcpus,
            memory_bytes: r.memory_bytes,
            disk_bytes: r.disk_bytes,
        }
    }
}

impl From<pb::Resources> for Resources {
    fn from(r: pb::Resources) -> Self {
        Self {
            vcpus: r.vcpus,
            memory_bytes: r.memory_bytes,
            disk_bytes: r.disk_bytes,
        }
    }
}

/// Fleet specs only: a `Provider` execution target never reaches an agent.
impl TryFrom<&OrbSpec> for pb::OrbSpec {
    type Error = WireError;
    fn try_from(s: &OrbSpec) -> Result<Self, WireError> {
        let ExecutionTarget::Fleet { runtime } = &s.template.target else {
            return err("provider execution target cannot be sent to an agent");
        };
        let (pause_mode, on_failure) = match s.template.pause_policy {
            PausePolicy::DiskOnly => (pb::PauseMode::DiskOnly, None),
            PausePolicy::MemoryPreserving { on_failure } => {
                (pb::PauseMode::MemoryPreserving, Some(on_failure))
            }
        };
        let on_failure = match on_failure {
            None => pb::CheckpointFailurePolicy::Unspecified,
            Some(CheckpointFailurePolicy::KeepRunning) => pb::CheckpointFailurePolicy::KeepRunning,
            Some(CheckpointFailurePolicy::ColdStop) => pb::CheckpointFailurePolicy::ColdStop,
        };
        let req = s.template.requirements;
        let nested = match req.nested_kvm {
            NestedKvmRequirement::NotRequired => pb::NestedKvm::NotRequired,
            NestedKvmRequirement::GuestDevice => pb::NestedKvm::GuestDevice,
            NestedKvmRequirement::GuestDeviceAndPreservedNestedState => {
                pb::NestedKvm::GuestDeviceAndPreservedState
            }
        };
        let snap_scope = match req.project_snapshots {
            SnapshotRequirement::NotRequired => pb::SnapshotScope::Unspecified,
            SnapshotRequirement::Required {
                scope: SnapshotScope::RecordedLocation,
            } => pb::SnapshotScope::RecordedLocation,
            SnapshotRequirement::Required {
                scope: SnapshotScope::BackendScope,
            } => pb::SnapshotScope::Backend,
        };
        let source = match &s.initial_disk {
            InitialDiskSource::Image { image_ref } => {
                pb::initial_disk_source::Source::ImageRef(image_ref.clone())
            }
            InitialDiskSource::ProjectSnapshot(r) => {
                pb::initial_disk_source::Source::ProjectSnapshotKey(r.key.0.clone())
            }
        };
        Ok(Self {
            runtime: pb::RuntimeKind::from(*runtime) as i32,
            resources: Some(s.template.resources.into()),
            pause_mode: pause_mode as i32,
            on_checkpoint_failure: on_failure as i32,
            nested_kvm: nested as i32,
            initial_disk: Some(pb::InitialDiskSource {
                source: Some(source),
            }),
            release_compute_when_paused: req.release_compute_when_paused,
            required_snapshot_scope: snap_scope as i32,
        })
    }
}

impl TryFrom<&pb::OrbSpec> for OrbSpec {
    type Error = WireError;
    fn try_from(s: &pb::OrbSpec) -> Result<Self, WireError> {
        let runtime = runtime_kind(s.runtime)?;
        let pause_policy = match pb::PauseMode::try_from(s.pause_mode) {
            Ok(pb::PauseMode::DiskOnly) => PausePolicy::DiskOnly,
            Ok(pb::PauseMode::MemoryPreserving) => PausePolicy::MemoryPreserving {
                on_failure: match pb::CheckpointFailurePolicy::try_from(s.on_checkpoint_failure) {
                    Ok(pb::CheckpointFailurePolicy::KeepRunning) => {
                        CheckpointFailurePolicy::KeepRunning
                    }
                    Ok(pb::CheckpointFailurePolicy::ColdStop) => CheckpointFailurePolicy::ColdStop,
                    _ => return err("checkpoint failure policy"),
                },
            },
            _ => return err("pause mode"),
        };
        let nested_kvm = match pb::NestedKvm::try_from(s.nested_kvm) {
            Ok(pb::NestedKvm::NotRequired) => NestedKvmRequirement::NotRequired,
            Ok(pb::NestedKvm::GuestDevice) => NestedKvmRequirement::GuestDevice,
            Ok(pb::NestedKvm::GuestDeviceAndPreservedState) => {
                NestedKvmRequirement::GuestDeviceAndPreservedNestedState
            }
            _ => return err("nested kvm"),
        };
        let project_snapshots = match pb::SnapshotScope::try_from(s.required_snapshot_scope) {
            Ok(pb::SnapshotScope::Unspecified) => SnapshotRequirement::NotRequired,
            Ok(pb::SnapshotScope::RecordedLocation) => SnapshotRequirement::Required {
                scope: SnapshotScope::RecordedLocation,
            },
            Ok(pb::SnapshotScope::Backend) => SnapshotRequirement::Required {
                scope: SnapshotScope::BackendScope,
            },
            Err(_) => return err("snapshot scope"),
        };
        let initial_disk = match s.initial_disk.as_ref().and_then(|d| d.source.as_ref()) {
            Some(pb::initial_disk_source::Source::ImageRef(r)) => InitialDiskSource::Image {
                image_ref: r.clone(),
            },
            Some(pb::initial_disk_source::Source::ProjectSnapshotKey(k)) => {
                InitialDiskSource::ProjectSnapshot(ProjectSnapshotRef {
                    key: ProjectSnapshotKey(k.clone()),
                })
            }
            None => return err("initial disk source"),
        };
        Ok(Self {
            template: SpecTemplate {
                target: ExecutionTarget::Fleet { runtime },
                resources: s
                    .resources
                    .map(Into::into)
                    .ok_or(WireError("resources".into()))?,
                requirements: CapabilityRequirements {
                    release_compute_when_paused: s.release_compute_when_paused,
                    nested_kvm,
                    project_snapshots,
                },
                pause_policy,
            },
            initial_disk,
        })
    }
}

impl TryFrom<&OrbDefinition> for pb::OrbDefinition {
    type Error = WireError;
    fn try_from(d: &OrbDefinition) -> Result<Self, WireError> {
        Ok(Self {
            orb_id: d.id.to_string(),
            amp_endpoint: d.identity.amp_endpoint.0.clone(),
            amp_orb_id: d.identity.amp_orb_id.0.clone(),
            offering_id: d.offering_id.0.clone(),
            spec: Some((&d.spec).try_into()?),
            spec_digest: d.spec_digest.0.clone(),
        })
    }
}

impl TryFrom<&pb::OrbDefinition> for OrbDefinition {
    type Error = WireError;
    fn try_from(d: &pb::OrbDefinition) -> Result<Self, WireError> {
        Ok(Self {
            id: orb_id(&d.orb_id)?,
            identity: OrbIdentity {
                amp_endpoint: AmpEndpointId(d.amp_endpoint.clone()),
                amp_orb_id: AmpOrbId::parse(&d.amp_orb_id).map_err(|e| WireError(e.to_string()))?,
            },
            offering_id: OfferingId(d.offering_id.clone()),
            spec: d
                .spec
                .as_ref()
                .ok_or(WireError("spec".into()))?
                .try_into()?,
            spec_digest: SpecDigest(d.spec_digest.clone()),
        })
    }
}

// --- commands ---------------------------------------------------------------

impl From<&Command> for pb::Command {
    fn from(c: &Command) -> Self {
        use pb::command::Action as A;
        let action = match &c.action {
            Action::Spawn => A::Spawn(pb::SpawnAction {}),
            Action::Pause { publish_snapshot } => A::Pause(pb::PauseAction {
                project_snapshot_key: publish_snapshot.as_ref().map(|k| k.0.clone()),
            }),
            Action::Delete => A::Delete(pb::DeleteAction {}),
        };
        Self {
            operation_id: c.operation_id.to_string(),
            orb_id: c.orb_id.to_string(),
            generation: c.generation,
            spec_digest: c.spec_digest.0.clone(),
            command_digest: c.command_digest.0.clone(),
            action: Some(action),
        }
    }
}

impl TryFrom<&pb::Command> for Command {
    type Error = WireError;
    fn try_from(c: &pb::Command) -> Result<Self, WireError> {
        use pb::command::Action as A;
        let action = match &c.action {
            Some(A::Spawn(_)) => Action::Spawn,
            Some(A::Pause(p)) => Action::Pause {
                publish_snapshot: p.project_snapshot_key.clone().map(ProjectSnapshotKey),
            },
            Some(A::Delete(_)) => Action::Delete,
            None => return err("unknown or missing command action"),
        };
        Ok(Self {
            operation_id: operation_id(&c.operation_id)?,
            orb_id: orb_id(&c.orb_id)?,
            generation: c.generation,
            spec_digest: SpecDigest(c.spec_digest.clone()),
            command_digest: CommandDigest(c.command_digest.clone()),
            action,
        })
    }
}

impl From<CommandKey> for pb::CommandKey {
    fn from(k: CommandKey) -> Self {
        Self {
            operation_id: k.operation_id.to_string(),
            orb_id: k.orb_id.to_string(),
            generation: k.generation,
        }
    }
}

impl TryFrom<&pb::CommandKey> for CommandKey {
    type Error = WireError;
    fn try_from(k: &pb::CommandKey) -> Result<Self, WireError> {
        Ok(Self {
            operation_id: operation_id(&k.operation_id)?,
            orb_id: orb_id(&k.orb_id)?,
            generation: k.generation,
        })
    }
}

impl From<&CommandFailure> for pb::CommandFailure {
    fn from(f: &CommandFailure) -> Self {
        Self {
            code: pb::FailureCode::from(f.code) as i32,
            message: f.message.clone(),
        }
    }
}

impl TryFrom<&pb::CommandFailure> for CommandFailure {
    type Error = WireError;
    fn try_from(f: &pb::CommandFailure) -> Result<Self, WireError> {
        Ok(Self {
            code: failure_code(f.code)?,
            message: f.message.clone(),
        })
    }
}

// --- state and outcomes -----------------------------------------------------

impl From<&OrbState> for pb::OrbState {
    fn from(s: &OrbState) -> Self {
        use pb::orb_state::Stable;
        let (last, fault) = match s {
            OrbState::Known { state } => (state, None),
            OrbState::Faulted { last_known, reason } => (last_known, Some(reason.clone())),
        };
        let mut out = Self {
            fault,
            ..Default::default()
        };
        out.stable = match last {
            StableOrbState::Uncreated => Stable::Uncreated,
            StableOrbState::Running => Stable::Running,
            StableOrbState::Sleeping { checkpoint } => {
                out.checkpoint_generation = checkpoint.generation;
                Stable::Sleeping
            }
            StableOrbState::DiskOnlyStopped { reason } => {
                out.disk_only_reason = reason.clone();
                Stable::DiskOnlyStopped
            }
            StableOrbState::Deleted => Stable::Deleted,
        } as i32;
        out
    }
}

impl TryFrom<&pb::OrbState> for OrbState {
    type Error = WireError;
    fn try_from(s: &pb::OrbState) -> Result<Self, WireError> {
        use pb::orb_state::Stable;
        let stable = match Stable::try_from(s.stable) {
            Ok(Stable::Uncreated) => StableOrbState::Uncreated,
            Ok(Stable::Running) => StableOrbState::Running,
            Ok(Stable::Sleeping) => StableOrbState::Sleeping {
                checkpoint: SleepCheckpointRef {
                    generation: s.checkpoint_generation,
                },
            },
            Ok(Stable::DiskOnlyStopped) => StableOrbState::DiskOnlyStopped {
                reason: s.disk_only_reason.clone(),
            },
            Ok(Stable::Deleted) => StableOrbState::Deleted,
            _ => return err(format!("orb state {}", s.stable)),
        };
        Ok(match &s.fault {
            None => OrbState::Known { state: stable },
            Some(reason) => OrbState::Faulted {
                last_known: stable,
                reason: reason.clone(),
            },
        })
    }
}

impl From<&CommandOutcome> for pb::CommandOutcome {
    fn from(o: &CommandOutcome) -> Self {
        use pb::command_outcome::Result as R;
        use pb::snapshot_disposition::Disposition as D;
        let result = match &o.result {
            Ok(ActionResult::Spawned {
                start,
                bootstrap_generation,
            }) => R::Spawned(pb::SpawnResult {
                start: pb::StartKind::from(*start) as i32,
                bootstrap_generation: *bootstrap_generation,
            }),
            Ok(ActionResult::Paused { checkpoint }) => R::Paused(pb::PauseResult {
                checkpoint_generation: checkpoint.as_ref().map(|c| c.generation),
            }),
            Ok(ActionResult::Deleted) => R::Deleted(pb::DeleteResult {}),
            Err(f) => R::Failure(f.into()),
        };
        let snapshot = match &o.snapshot {
            SnapshotDisposition::NotRequested => D::NotRequested(true),
            SnapshotDisposition::Published { snapshot } => {
                D::PublishedSnapshotKey(snapshot.key.0.clone())
            }
            SnapshotDisposition::NotPublished { reason } => D::NotPublishedReason(reason.clone()),
        };
        Self {
            result: Some(result),
            observed: Some((&o.observed).into()),
            processes: pb::ProcessDisposition::from(o.processes) as i32,
            snapshot: Some(pb::SnapshotDisposition {
                disposition: Some(snapshot),
            }),
        }
    }
}

impl TryFrom<&pb::CommandOutcome> for CommandOutcome {
    type Error = WireError;
    fn try_from(o: &pb::CommandOutcome) -> Result<Self, WireError> {
        use pb::command_outcome::Result as R;
        use pb::snapshot_disposition::Disposition as D;
        let result = match &o.result {
            Some(R::Spawned(s)) => Ok(ActionResult::Spawned {
                start: start_kind(s.start)?,
                bootstrap_generation: s.bootstrap_generation,
            }),
            Some(R::Paused(p)) => Ok(ActionResult::Paused {
                checkpoint: p
                    .checkpoint_generation
                    .map(|generation| SleepCheckpointRef { generation }),
            }),
            Some(R::Deleted(_)) => Ok(ActionResult::Deleted),
            Some(R::Failure(f)) => Err(f.try_into()?),
            None => return err("outcome result"),
        };
        let snapshot = match o.snapshot.as_ref().and_then(|s| s.disposition.as_ref()) {
            Some(D::NotRequested(_)) => SnapshotDisposition::NotRequested,
            Some(D::PublishedSnapshotKey(k)) => SnapshotDisposition::Published {
                snapshot: ProjectSnapshotRef {
                    key: ProjectSnapshotKey(k.clone()),
                },
            },
            Some(D::NotPublishedReason(r)) => {
                SnapshotDisposition::NotPublished { reason: r.clone() }
            }
            None => return err("snapshot disposition"),
        };
        Ok(Self {
            result,
            observed: o
                .observed
                .as_ref()
                .ok_or(WireError("observed".into()))?
                .try_into()?,
            processes: process_disposition(o.processes)?,
            snapshot,
        })
    }
}
