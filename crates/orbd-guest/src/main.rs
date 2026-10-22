//! `orbd-guest`: runs inside orbd VMs as `orbd-guest.service` and speaks `orbd.guest.v1`
//! with the host agent: delivers credentials, steps the clock, applies static network and
//! DNS, (re)starts Amp's bootstrap, and syncs before memory saves. No shell or exec RPC.
//!
//! * `orbd-guest init`: early-boot setup (grow the root filesystem, /dev/kvm access,
//!   machine identity for new orbs and clones).
//! * `orbd-guest serve [--port NAME]`: serve the host on the virtio-serial port
//!   (default `org.orbd.control`).

macro_rules! log {
    ($($arg:tt)*) => { eprintln!("orbd-guest: {}", format_args!($($arg)*)) };
}

mod cmd;
mod files;
mod init;
mod net;
mod server;
mod transport;
mod wake;

mod proto {
    include!(concat!(env!("OUT_DIR"), "/orbd.guest.v1.rs"));
}

const DEFAULT_PORT: &str = "org.orbd.control";

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
    server::serve(&transport::VirtioSerial::new(port))
}
