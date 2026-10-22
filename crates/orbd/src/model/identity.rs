//! Identifiers. Internal ids are UUIDs and safe to use as storage names; external ids
//! (Amp's ORB_ID, backend/agent ids from config) are validated strings.

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! string_id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_owned())
            }
        }
    };
}

string_id!(
    /// Configured Amp endpoint id (e.g. `amp`). Orb identity is scoped by it.
    AmpEndpointId
);
string_id!(
    /// Amp's `ORB_ID` (e.g. `amp-<thread uuid>`). Untrusted input: validate with
    /// [`AmpOrbId::parse`].
    AmpOrbId
);
string_id!(
    /// Configured backend id (e.g. `fleet`).
    BackendId
);
string_id!(
    /// Configured offering id (e.g. `qemu-4vcpu-8gb`).
    OfferingId
);
string_id!(
    /// Agent id, bound to its mTLS certificate.
    AgentId
);
string_id!(
    /// Backend-specific scope for external allocations (provider account/region).
    BackendScopeId
);
string_id!(
    /// Backend-specific execution profile id for API backends.
    BackendProfileId
);
string_id!(
    /// Project snapshot key derived from Amp's opaque ORB_SNAPSHOT_ID (hash, hex).
    ProjectSnapshotKey
);

macro_rules! uuid_id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub uuid::Uuid);

        impl $name {
            /// New time-ordered id.
            pub fn generate() -> Self {
                Self(uuid::Uuid::now_v7())
            }
            pub fn parse(s: &str) -> Result<Self, uuid::Error> {
                uuid::Uuid::parse_str(s).map(Self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

uuid_id!(
    /// Internal orb id.
    OrbId
);
uuid_id!(
    /// Internal command operation id.
    OperationId
);
uuid_id!(
    /// Dispatch invocation id (one `orbd dispatch` process).
    InvocationId
);
uuid_id!(
    /// Allocation id recorded before an external backend creates anything.
    AllocationId
);

impl AmpOrbId {
    /// Validates an untrusted ORB_ID: 1..=128 chars of `[A-Za-z0-9._-]`.
    pub fn parse(s: &str) -> Result<Self, InvalidId> {
        let ok = !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
        if ok {
            Ok(Self(s.to_owned()))
        } else {
            Err(InvalidId(s.chars().take(64).collect()))
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid identifier {0:?}")]
pub struct InvalidId(pub String);

/// Orb identity as Amp sees it, scoped by the configured endpoint.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrbIdentity {
    pub amp_endpoint: AmpEndpointId,
    pub amp_orb_id: AmpOrbId,
}
