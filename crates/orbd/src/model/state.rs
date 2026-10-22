//! Orb states. Starting/saving/bootstrapping/deleting are command phases, not states.
//! Agent connectivity is tracked separately: a disconnected agent does not change the
//! recorded state of its orbs.

use super::artifact::SleepCheckpointRef;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum StableOrbState {
    Uncreated,
    Running,
    Sleeping { checkpoint: SleepCheckpointRef },
    DiskOnlyStopped { reason: String },
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum OrbState {
    Known {
        state: StableOrbState,
    },
    Faulted {
        last_known: StableOrbState,
        reason: String,
    },
}

impl OrbState {
    pub fn known(state: StableOrbState) -> Self {
        Self::Known { state }
    }

    pub fn last_known(&self) -> &StableOrbState {
        match self {
            Self::Known { state } => state,
            Self::Faulted { last_known, .. } => last_known,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedObservation {
    pub state: OrbState,
    /// Generation of the command that produced this observation.
    pub generation: u64,
    pub observed_at_unix_ms: u64,
}
