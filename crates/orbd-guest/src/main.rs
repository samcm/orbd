//! `orbd-guest`: runs inside orbd VMs as `orbd-guest.service` and speaks `orbd.guest.v1`
//! with the host agent: delivers credentials, steps the clock, applies static network and
//! DNS (and keeps that DNS working if an in-orb tailscaled dies), (re)starts Amp's
//! bootstrap, and syncs before memory saves. No shell or exec RPC.
//!
//! * `orbd-guest init`: early-boot setup (grow the root filesystem, /dev/kvm access,
//!   machine identity for new orbs and clones).
//! * `orbd-guest serve [--port NAME]`: serve the host on the virtio-serial port
//!   (default `org.orbd.control`), or on vsock when the kernel command line has
//!   `orbd.vsock=<port>` (Firecracker).

use orbd_guest::{DEFAULT_PORT, init, net, server, transport, wake};
use std::sync::Arc;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["init"] => init::init(),
        ["serve"] => serve(DEFAULT_PORT),
        ["serve", "--port", port] => serve(port),
        ["--version"] => println!("orbd-guest {}", env!("CARGO_PKG_VERSION")),
        _ => {
            eprintln!("usage: orbd-guest init | serve [--port NAME] | --version");
            std::process::exit(2);
        }
    }
}

fn serve(port: &str) -> ! {
    transport::block_sigio();
    std::thread::spawn(|| net::watch_dns());
    match vsock_port() {
        Some(p) => server::serve(&transport::Vsock::new(p), Arc::new(wake::Bootstrap)),
        None => server::serve(
            &transport::VirtioSerial::new(port),
            Arc::new(wake::Bootstrap),
        ),
    }
}

/// `orbd.vsock=<port>` on the kernel command line (Firecracker) selects vsock.
fn vsock_port() -> Option<u32> {
    std::fs::read_to_string("/proc/cmdline")
        .ok()?
        .split_whitespace()
        .find_map(|a| a.strip_prefix("orbd.vsock=")?.parse().ok())
}
