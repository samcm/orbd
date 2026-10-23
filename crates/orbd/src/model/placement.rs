//! Placement: one recorded backend location per orb, for life.

use super::identity::{
    AgentId, AllocationId, BackendId, BackendScopeId, OfferingId, OrbId, OrbIdentity,
};
use super::spec::{OrbSpec, SpecDigest};
use super::state::VersionedObservation;
use serde::{Deserialize, Serialize};

/// Where an orb lives. Committed (with its resource reservation) before anything is
/// created, and never changed automatically afterwards — not when the agent is
/// offline, not when the first execution fails, not when a disk is missing.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Location {
    Fleet {
        backend_id: BackendId,
        agent_id: AgentId,
    },
    /// API backend allocation. The provider's resource id is bound later in
    /// executor-owned state ([`ExternalBinding`]), because placement is committed
    /// before the provider is called.
    External {
        backend_id: BackendId,
        scope: BackendScopeId,
        allocation_id: AllocationId,
    },
}

impl Location {
    pub fn backend_id(&self) -> &BackendId {
        match self {
            Self::Fleet { backend_id, .. } | Self::External { backend_id, .. } => backend_id,
        }
    }
}

/// One host's orb filesystem as its agent last reported it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DiskReport {
    /// Bytes available on the filesystem holding the agent's state directory.
    pub free_bytes: u64,
    /// Size of that filesystem; 0 when the agent does not report it.
    pub total_bytes: u64,
    /// Bytes orbd has allocated under the state directory (sparse files count only
    /// their allocated blocks).
    pub allocated_bytes: u64,
    /// Free space admission never hands out.
    pub reserve_bytes: u64,
}

/// The default disk reserve of a host: max(10 GiB, 5% of the filesystem). Admission
/// never hands out a host's reserve; below it every new or waking orb is refused.
pub fn disk_reserve(total_bytes: u64) -> u64 {
    (10 * super::GIB).max(total_bytes / 20)
}

/// Executor-owned binding of an external allocation to the provider's resource id.
/// An unknown binding after a lost create response requires reconciliation; it is never
/// permission to allocate again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ExternalBinding {
    Unbound,
    Bound { external_ref: String },
}

/// Coordinator record for one orb.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrbRecord {
    pub id: OrbId,
    pub identity: OrbIdentity,
    pub offering_id: OfferingId,
    pub spec: OrbSpec,
    pub spec_digest: SpecDigest,
    pub location: Location,
    pub last_issued_generation: u64,
    pub observation: VersionedObservation,
}

/// Everything an executor needs to know about the orb a command targets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrbDefinition {
    pub id: OrbId,
    pub identity: OrbIdentity,
    pub offering_id: OfferingId,
    pub spec: OrbSpec,
    pub spec_digest: SpecDigest,
}
