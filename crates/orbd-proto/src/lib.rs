//! Generated protobuf and gRPC types for orbd.
//!
//! * [`control::v1`] (feature `control`, on by default): coordinator `Dispatch` (local
//!   unix socket) and `AgentControl` (agent-initiated mTLS stream).
//! * [`guest::v1`]: host ↔ guest agent (`orbd-guest`) messages; plain prost, no gRPC.

#![allow(clippy::large_enum_variant)]

#[cfg(feature = "control")]
pub mod control {
    pub mod v1 {
        tonic::include_proto!("orbd.control.v1");
    }
}

pub mod guest {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/orbd.guest.v1.rs"));
    }
}

/// Current `orbd.control.v1` protocol revision negotiated in `Hello`/`Welcome`.
#[cfg(feature = "control")]
pub const CONTROL_PROTOCOL_VERSION: u32 = 1;
