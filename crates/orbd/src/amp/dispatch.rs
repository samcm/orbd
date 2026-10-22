//! `orbd dispatch`: executed (via the `orbd-dispatch` wrapper) by `amp orb provider`
//! once per job, with no arguments. Reads the job from the environment, forwards it to
//! the coordinator's local `Dispatch` socket, streams logs to stdout/stderr and exits
//! with the result.

/// Exit codes. Diagnostic only: Amp does not treat them differently.
pub mod exit {
    pub const SUCCESS: i32 = 0;
    pub const FAILED: i32 = 1;
    pub const INVALID: i32 = 2;
    pub const UNAVAILABLE_OR_UNKNOWN: i32 = 75;
    pub const DEADLINE: i32 = 124;
}

/// Environment set by the coordinator for its `amp orb provider` children.
pub const ENV_OFFERING_ID: &str = "ORBD_OFFERING_ID";
pub const ENV_DISPATCH_SOCKET: &str = "ORBD_DISPATCH_SOCKET";

/// Runs one dispatch job from the process environment; returns the exit code.
pub async fn run_from_env() -> i32 {
    eprintln!("orbd dispatch: not implemented yet");
    exit::UNAVAILABLE_OR_UNKNOWN
}
