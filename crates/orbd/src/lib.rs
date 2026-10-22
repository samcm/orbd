//! orbd: a self-hosted backend for Amp orbs.
//!
//! * [`coordinator`] wraps `amp orb provider` (one per offering) and owns orb identity,
//!   placement, admission and the durable command journal. Backend-neutral.
//! * [`backend`] is the compute-backend seam; [`backend::fleet`] is the first backend
//!   (agents dialling the coordinator over gRPC).
//! * [`agent`] executes commands on a host with a [`runtime::Runtime`].

pub mod agent;
pub mod amp;
pub mod backend;
pub mod config;
pub mod coordinator;
pub mod model;
pub mod runtime;
pub mod secrets;
pub mod telemetry;

/// Crash-test hook. Built with the `failpoints` feature, setting
/// `FAILPOINTS="<name>=return"` aborts the process at this point (no cleanup, like a
/// power cut); `<name>=sleep(ms)` / `pause` delay it. Compiles to nothing otherwise.
#[macro_export]
macro_rules! crash_point {
    ($name:literal) => {
        fail::fail_point!($name, |_| std::process::abort())
    };
}
