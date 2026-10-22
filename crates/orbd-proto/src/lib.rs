//! Generated protobuf and gRPC types for orbd.
//!
//! * [`control::v1`]: coordinator `Dispatch` (local unix socket) and `AgentControl`
//!   (agent-initiated mTLS stream).
//! * [`guest::v1`]: host agent ↔ `orbd-guest` messages over vsock.

#![allow(clippy::large_enum_variant)]

pub mod control {
    pub mod v1 {
        tonic::include_proto!("orbd.control.v1");
    }
}

pub mod guest {
    pub mod v1 {
        tonic::include_proto!("orbd.guest.v1");
    }
}

/// Current `orbd.control.v1` protocol revision negotiated in `Hello`/`Welcome`.
pub const CONTROL_PROTOCOL_VERSION: u32 = 1;
