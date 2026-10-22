//! Backend-neutral domain model shared by the coordinator, backends and agents.
//! Protobuf conversions live in [`wire`]; keep tonic/prost types out of everything else.

pub mod artifact;
pub mod capability;
pub mod command;
pub mod identity;
pub mod placement;
pub mod spec;
pub mod state;
pub mod wire;

pub use artifact::*;
pub use capability::*;
pub use command::*;
pub use identity::*;
pub use placement::*;
pub use spec::*;
pub use state::*;

/// Milliseconds since the Unix epoch (wall clock; for records, not for timers).
pub fn unix_ms_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
