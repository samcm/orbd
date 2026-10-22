//! Static capability ceilings per fleet runtime, and their intersection with what an
//! agent actually advertised in its `Hello`.

use crate::backend::CapabilityError;
use crate::model::{
    BackendCapabilities, CapacityModel, ExecutionTarget, IdleCharge, IdleCost,
    NestedStatePreservation, ResumeLocality, RuntimeKind, SleepCapability, SleepingCompute,
    SnapshotCapability, SnapshotScope, StateLocation, StateStore, Support,
};
use orbd_proto::control::v1 as pb;

/// What the runtime kind can ever promise (agents may advertise less).
pub fn ceiling(runtime: RuntimeKind) -> BackendCapabilities {
    let (sleep, nested_kvm) = match runtime {
        RuntimeKind::Fake => (
            SleepCapability::MemoryPreserving {
                compute: SleepingCompute::Released,
                nested_state: NestedStatePreservation::Qualified,
            },
            Support::Supported,
        ),
        RuntimeKind::Docker => (
            SleepCapability::DiskOnly {
                compute: SleepingCompute::Released,
            },
            Support::Unsupported,
        ),
        RuntimeKind::Qemu => (
            SleepCapability::MemoryPreserving {
                compute: SleepingCompute::Released,
                nested_state: NestedStatePreservation::Unqualified,
            },
            Support::Supported,
        ),
        RuntimeKind::Firecracker => (
            SleepCapability::MemoryPreserving {
                compute: SleepingCompute::Released,
                nested_state: NestedStatePreservation::Unsupported,
            },
            Support::Unsupported,
        ),
    };
    BackendCapabilities {
        sleep,
        nested_kvm,
        project_snapshots: SnapshotCapability::Supported {
            scope: SnapshotScope::RecordedLocation,
        },
        capacity: CapacityModel::BinPackedHosts,
        resume_locality: ResumeLocality::RecordedHost,
        state_location: StateLocation {
            disk: StateStore::OwnerLocal,
            sleep_checkpoint: StateStore::OwnerLocal,
        },
        idle_cost: IdleCost {
            compute: IdleCharge::None,
            storage: IdleCharge::DeploymentDependent,
        },
    }
}

pub fn for_target(target: &ExecutionTarget) -> Result<BackendCapabilities, CapabilityError> {
    match target {
        ExecutionTarget::Fleet { runtime } => Ok(ceiling(*runtime)),
        ExecutionTarget::Provider { profile } => Err(CapabilityError(format!(
            "agent-fleet backend cannot serve provider profile {profile:?}"
        ))),
    }
}

/// Ceiling intersected with the agent's advertised flags. Flags never raise a ceiling.
pub fn effective(runtime: RuntimeKind, info: &pb::RuntimeInfo) -> BackendCapabilities {
    let mut caps = ceiling(runtime);
    caps.sleep = match caps.sleep {
        SleepCapability::MemoryPreserving {
            compute,
            nested_state,
        } => {
            if info.memory_preserving_sleep {
                let nested_state = match nested_state {
                    NestedStatePreservation::Qualified if !info.nested_state_qualified => {
                        NestedStatePreservation::Unqualified
                    }
                    other => other,
                };
                SleepCapability::MemoryPreserving {
                    compute,
                    nested_state,
                }
            } else {
                SleepCapability::DiskOnly { compute }
            }
        }
        other => other,
    };
    if !info.nested_kvm {
        caps.nested_kvm = Support::Unsupported;
    }
    if !info.project_snapshots {
        caps.project_snapshots = SnapshotCapability::Unsupported;
    }
    caps
}
