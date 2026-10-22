//! Logical artifact references. Sleep checkpoints (one orb's memory) and project
//! snapshots (immutable disk clones that cold-boot other orbs) are distinct types.
//! References are logical ids, never host paths.

use super::identity::ProjectSnapshotKey;
use serde::{Deserialize, Serialize};

/// A published memory checkpoint for one orb, by checkpoint generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SleepCheckpointRef {
    pub generation: u64,
}

/// A published, immutable project snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProjectSnapshotRef {
    pub key: ProjectSnapshotKey,
}

impl ProjectSnapshotKey {
    /// Derives a storage-safe key from Amp's opaque ORB_SNAPSHOT_ID.
    pub fn from_external(external_id: &str) -> Self {
        use sha2::{Digest, Sha256};
        Self(hex::encode(Sha256::digest(external_id.as_bytes())))
    }
}
