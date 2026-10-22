//! Offerings and the immutable spec recorded for each orb.

use super::artifact::ProjectSnapshotRef;
use super::identity::{AmpEndpointId, BackendId, BackendProfileId, OfferingId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeKind {
    /// Test runtime with durable fake state and failpoints.
    Fake,
    /// Systemd container; routing tests only (pause loses processes).
    Docker,
    /// QEMU q35 VM with nested KVM and memory-preserving sleep.
    Qemu,
    /// Firecracker microVM; fast wake, no nested KVM.
    Firecracker,
}

impl RuntimeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fake => "fake",
            Self::Docker => "docker",
            Self::Qemu => "qemu",
            Self::Firecracker => "firecracker",
        }
    }
}

/// Exact integer resource quantities.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Resources {
    pub vcpus: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

impl Resources {
    pub fn checked_add(self, o: Self) -> Option<Self> {
        Some(Self {
            vcpus: self.vcpus.checked_add(o.vcpus)?,
            memory_bytes: self.memory_bytes.checked_add(o.memory_bytes)?,
            disk_bytes: self.disk_bytes.checked_add(o.disk_bytes)?,
        })
    }

    pub fn saturating_sub(self, o: Self) -> Self {
        Self {
            vcpus: self.vcpus.saturating_sub(o.vcpus),
            memory_bytes: self.memory_bytes.saturating_sub(o.memory_bytes),
            disk_bytes: self.disk_bytes.saturating_sub(o.disk_bytes),
        }
    }

    /// True when every dimension of `self` fits within `ceiling`.
    pub fn fits_within(self, ceiling: Self) -> bool {
        self.vcpus <= ceiling.vcpus
            && self.memory_bytes <= ceiling.memory_bytes
            && self.disk_bytes <= ceiling.disk_bytes
    }
}

/// Where an offering executes.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ExecutionTarget {
    /// Agent fleet backend: an agent's local runtime.
    Fleet { runtime: RuntimeKind },
    /// API backend (E2B, exe.dev, ...): a backend-specific profile. Not implemented yet.
    Provider { profile: BackendProfileId },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NestedKvmRequirement {
    #[default]
    NotRequired,
    /// `/dev/kvm` inside the orb.
    GuestDevice,
    /// `/dev/kvm` inside the orb and nested guests survive sleep.
    GuestDeviceAndPreservedNestedState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SnapshotRequirement {
    NotRequired,
    Required {
        scope: super::capability::SnapshotScope,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CapabilityRequirements {
    /// Paused orbs must not hold compute (host RAM / billed compute).
    pub release_compute_when_paused: bool,
    pub nested_kvm: NestedKvmRequirement,
    pub project_snapshots: SnapshotRequirement,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointFailurePolicy {
    /// Leave the orb running and fail the pause.
    KeepRunning,
    /// Cold-stop the orb and fail the pause with an explicit process-loss result.
    ColdStop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum PausePolicy {
    /// Pause stops the orb; processes are lost (reported, not an error).
    DiskOnly,
    /// Pause saves memory; processes resume on wake.
    MemoryPreserving { on_failure: CheckpointFailurePolicy },
}

/// The per-offering template, copied into each orb's [`OrbSpec`] when first placed.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpecTemplate {
    pub target: ExecutionTarget,
    pub resources: Resources,
    pub requirements: CapabilityRequirements,
    pub pause_policy: PausePolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Offering {
    pub id: OfferingId,
    pub amp_endpoint: AmpEndpointId,
    pub backend_id: BackendId,
    pub spec: SpecTemplate,
}

/// How a new orb's disk is created. Resolved once, before first execution.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum InitialDiskSource {
    /// Image reference as given by Amp; the location must have it prepared.
    Image {
        image_ref: String,
    },
    ProjectSnapshot(ProjectSnapshotRef),
}

/// Immutable spec recorded for an orb. Later spawns never change it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrbSpec {
    pub template: SpecTemplate,
    pub initial_disk: InitialDiskSource,
}

/// Hex sha256 of a canonical serialization.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SpecDigest(pub String);

impl OrbSpec {
    /// Digest over the canonical JSON form (struct field order is fixed by the type).
    pub fn digest(&self) -> SpecDigest {
        let json = serde_json::to_vec(self).expect("OrbSpec serializes");
        SpecDigest(hex::encode(Sha256::digest(json)))
    }
}

pub const MIB: u64 = 1024 * 1024;
pub const GIB: u64 = 1024 * MIB;
