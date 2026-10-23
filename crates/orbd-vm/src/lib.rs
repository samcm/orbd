//! Host plumbing for KVM virtual machine monitors, independent of which VMM runs:
//!
//! * [`fsutil`]: durable publication (write → fsync → rename → fsync(parent)) and
//!   delete-by-rename.
//! * [`process`]: launching a VMM as a supervised, non-daemonized process with a durable
//!   launch record, and adopting it again after the supervisor restarts.
//! * [`checkpoint`]: a directory of named checkpoints, each built in `<name>.tmp/` and
//!   published by one fsynced rename.
//!
//! Failpoint (feature `failpoints`): `vm::launch::after_exec` aborts the process right
//! after the VMM was exec'd, before its pid is recorded.

pub mod checkpoint;
pub mod fsutil;
pub mod process;
