//! The orbd guest agent as a library: the `orbd.guest.v1` server ([`server`]) over a
//! host transport ([`transport`]: virtio-serial port or vsock), with the building blocks
//! a guest binary needs ([`cmd`]: bounded runs of the guest's own tools; [`files`]:
//! symlink-safe atomic writes; [`net`]: static IPv4 and DNS). The `orbd-guest` binary
//! serves orbd's own [`wake::Bootstrap`] handler; another guest binary can serve its
//! own [`server::Handler`] or use the pieces directly.

macro_rules! log {
    ($($arg:tt)*) => { eprintln!("orbd-guest: {}", format_args!($($arg)*)) };
}

pub mod cmd;
pub mod files;
pub mod init;
pub mod net;
pub mod server;
pub mod transport;
pub mod wake;

/// The guest protocol, shared with the host side.
pub use orbd_proto::guest::v1 as proto;

/// The virtio-serial port orbd's QEMU VMs give the guest agent.
pub const DEFAULT_PORT: &str = "org.orbd.control";
