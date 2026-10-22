//! What a backend execution profile can promise. Capabilities describe one execution
//! profile (e.g. "fleet + qemu"), not a whole heterogeneous fleet. Unknown or
//! unqualified capabilities never satisfy a requirement.

use super::spec::{
    CapabilityRequirements, NestedKvmRequirement, PausePolicy, SnapshotRequirement, SpecTemplate,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Support {
    Unsupported,
    Supported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SleepingCompute {
    /// Sleeping orbs hold no compute (no VMM process, no host RAM).
    Released,
    Retained,
    ProviderManagedUnknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NestedStatePreservation {
    Unsupported,
    Unqualified,
    Qualified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SleepCapability {
    Unsupported,
    DiskOnly {
        compute: SleepingCompute,
    },
    MemoryPreserving {
        compute: SleepingCompute,
        nested_state: NestedStatePreservation,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotScope {
    /// Snapshots are usable only at the location that made them (agent-local in v1).
    RecordedLocation,
    /// Snapshots are usable anywhere in the backend.
    BackendScope,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SnapshotCapability {
    Unsupported,
    Supported { scope: SnapshotScope },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityModel {
    BinPackedHosts,
    QuotaLimited,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeLocality {
    /// Resumes only on the recorded host.
    RecordedHost,
    /// The provider manages physical placement. Does not authorise orbd to re-place.
    BackendManaged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateStore {
    None,
    OwnerLocal,
    BackendShared,
    ProviderManaged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateLocation {
    pub disk: StateStore,
    pub sleep_checkpoint: StateStore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdleCharge {
    None,
    Charged,
    DeploymentDependent,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdleCost {
    pub compute: IdleCharge,
    pub storage: IdleCharge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    pub sleep: SleepCapability,
    pub nested_kvm: Support,
    pub project_snapshots: SnapshotCapability,
    pub capacity: CapacityModel,
    pub resume_locality: ResumeLocality,
    pub state_location: StateLocation,
    pub idle_cost: IdleCost,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CapabilityMismatch {
    #[error("offering requires memory-preserving sleep, which this execution profile lacks")]
    MemoryPreservingSleep,
    #[error("offering requires paused orbs to release compute, which this profile does not")]
    ReleaseComputeWhenPaused,
    #[error("offering requires /dev/kvm inside the orb, which this profile lacks")]
    NestedKvm,
    #[error("offering requires nested guests to survive sleep, which is not qualified here")]
    NestedStatePreservation,
    #[error("offering requires project snapshots with {0:?} scope")]
    ProjectSnapshots(SnapshotScope),
    #[error("execution profile cannot pause orbs")]
    NoSleep,
}

impl BackendCapabilities {
    /// Checks every requirement of `spec` against these capabilities and returns all
    /// mismatches (empty = satisfied).
    pub fn check(&self, spec: &SpecTemplate) -> Vec<CapabilityMismatch> {
        let mut out = Vec::new();
        let CapabilityRequirements {
            release_compute_when_paused,
            nested_kvm,
            project_snapshots,
        } = spec.requirements;

        let (compute, nested_state) = match (self.sleep, spec.pause_policy) {
            (SleepCapability::Unsupported, _) => {
                out.push(CapabilityMismatch::NoSleep);
                (None, None)
            }
            (SleepCapability::DiskOnly { .. }, PausePolicy::MemoryPreserving { .. }) => {
                out.push(CapabilityMismatch::MemoryPreservingSleep);
                (None, None)
            }
            (SleepCapability::DiskOnly { compute }, PausePolicy::DiskOnly) => (Some(compute), None),
            (
                SleepCapability::MemoryPreserving {
                    compute,
                    nested_state,
                },
                _,
            ) => (Some(compute), Some(nested_state)),
        };
        if release_compute_when_paused
            && compute.is_some()
            && compute != Some(SleepingCompute::Released)
        {
            out.push(CapabilityMismatch::ReleaseComputeWhenPaused);
        }
        match nested_kvm {
            NestedKvmRequirement::NotRequired => {}
            NestedKvmRequirement::GuestDevice => {
                if self.nested_kvm != Support::Supported {
                    out.push(CapabilityMismatch::NestedKvm);
                }
            }
            NestedKvmRequirement::GuestDeviceAndPreservedNestedState => {
                if self.nested_kvm != Support::Supported {
                    out.push(CapabilityMismatch::NestedKvm);
                }
                let preserved = match spec.pause_policy {
                    PausePolicy::DiskOnly => false,
                    PausePolicy::MemoryPreserving { .. } => {
                        nested_state == Some(NestedStatePreservation::Qualified)
                    }
                };
                if !preserved {
                    out.push(CapabilityMismatch::NestedStatePreservation);
                }
            }
        }
        if let SnapshotRequirement::Required { scope } = project_snapshots {
            let ok = match self.project_snapshots {
                SnapshotCapability::Unsupported => false,
                SnapshotCapability::Supported { scope: have } => {
                    have == scope || have == SnapshotScope::BackendScope
                }
            };
            if !ok {
                out.push(CapabilityMismatch::ProjectSnapshots(scope));
            }
        }
        out
    }
}
