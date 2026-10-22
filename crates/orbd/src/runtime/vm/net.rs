//! Agent-local network attachment for one VM, chosen when the orb's disk is created and
//! persisted in its recipe; a sleeping VM is always restored with the attachment (and
//! MAC) it was saved with. Guests always see the same addressing, `10.0.2.15/24` via
//! `10.0.2.2`, applied by the guest helper from `Wake.network`.
//!
//! * `User`: QEMU's built-in slirp. No host privileges, guest-initiated traffic only.
//! * `Tap`: a per-orb network namespace `orbd-<orb>` holding the VM's `tap0`
//!   (`10.0.2.2/24`) and one end of a veth pair; the other end, `orbd<i>`, sits in the
//!   agent's namespace with a `/30` from `169.254.0.0/16` (`i` is a durable per-host
//!   index). The namespace masquerades the guest behind its veth address and the agent's
//!   namespace masquerades that onwards, so every guest has the same address and only
//!   leaves through NAT. nftables table `inet orbd` drops orb → agent traffic (except
//!   replies), orb ↔ orb forwarding, orb → link-local (cloud metadata) and IPv6. The
//!   VMM runs inside the namespace and sees nothing but its TAP.
//!
//! `Tap` is built with `ip` and `nft` run with fixed arguments; every step is
//! idempotent, so `adopt` repairs what is missing (after a host reboot) without touching
//! what a running VMM uses.

use orbd_proto::guest::v1 as gpb;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;

use crate::model::OrbId;

/// Typed backend; serialized into the VM recipe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum NetBackend {
    /// QEMU built-in slirp: no host privileges, guest-initiated traffic only.
    User,
    /// TAP in the orb's own network namespace (see the module docs).
    Tap { netns: String, index: u32 },
}

/// Durable per-orb attachment metadata (stable across sleep).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetAttachment {
    pub backend: NetBackend,
    pub mac: String,
    pub ipv4_cidr: String,
    pub ipv4_gateway: String,
    pub mtu: u32,
}

/// The guest's fixed private network (each VM has its own).
const GUEST_NET: &str = "10.0.2.0/24";
const GUEST_GATEWAY: &str = "10.0.2.2";
const GUEST_ADDR: &str = "10.0.2.15";
/// The TAP device inside an orb's namespace.
pub const TAP: &str = "tap0";
/// Highest `Tap` index: 169.254.0.0/16 in /30s, from index 1.
const MAX_INDEX: u32 = 16383;

/// Serializes index allocation within this agent.
static ALLOCATION: Mutex<()> = Mutex::new(());

impl NetAttachment {
    /// Slirp attachment for a new QEMU orb.
    pub fn user(orb: OrbId) -> Self {
        Self::with_backend(orb, NetBackend::User)
    }

    /// Allocates a durable index for `orb` (`<state>/net/<index>` names its owner, so a
    /// retry after a crash finds it again) and builds the orb's namespace. Undone by
    /// [`release`].
    pub fn tap(orb: OrbId, state_dir: &Path) -> Result<Self, String> {
        let index = allocate(&state_dir.join("net"), orb)?;
        let att = Self::with_backend(
            orb,
            NetBackend::Tap {
                netns: format!("orbd-{orb}"),
                index,
            },
        );
        att.adopt()?;
        Ok(att)
    }

    fn with_backend(orb: OrbId, backend: NetBackend) -> Self {
        Self {
            backend,
            mac: stable_mac(orb),
            ipv4_cidr: format!("{GUEST_ADDR}/24"),
            ipv4_gateway: GUEST_GATEWAY.into(),
            mtu: 1500,
        }
    }

    /// Makes sure the host side exists before a (re)launch, creating only what is
    /// missing.
    pub fn adopt(&self) -> Result<(), String> {
        match &self.backend {
            NetBackend::User => Ok(()),
            NetBackend::Tap { netns, index } => build_namespace(netns, *index),
        }
    }

    /// The namespace the VMM must run in.
    pub fn netns_path(&self) -> Option<PathBuf> {
        match &self.backend {
            NetBackend::User => None,
            NetBackend::Tap { netns, .. } => Some(Path::new("/run/netns").join(netns)),
        }
    }

    /// Guest-visible QEMU arguments (part of the device topology recorded in manifests).
    pub fn qemu_args(&self) -> Vec<String> {
        match self.backend {
            NetBackend::User => vec![
                "-netdev".into(),
                format!(
                    "user,id=net0,ipv6=off,net={GUEST_NET},host={GUEST_GATEWAY},dhcpstart={GUEST_ADDR}"
                ),
                "-device".into(),
                format!("virtio-net-pci,id=net0dev,netdev=net0,mac={}", self.mac),
            ],
            NetBackend::Tap { .. } => vec![
                "-netdev".into(),
                format!("tap,id=net0,ifname={TAP},script=no,downscript=no"),
                "-device".into(),
                format!("virtio-net-pci,id=net0dev,netdev=net0,mac={}", self.mac),
            ],
        }
    }

    /// Static addressing for `Wake.network`; DNS is explicit config, never the host's
    /// resolv.conf.
    pub fn guest_config(&self, dns_servers: &[String]) -> gpb::NetworkConfig {
        gpb::NetworkConfig {
            dns_servers: dns_servers.to_vec(),
            search_domains: Vec::new(),
            mac: self.mac.clone(),
            ipv4_cidr: self.ipv4_cidr.clone(),
            ipv4_gateway: self.ipv4_gateway.clone(),
            mtu: self.mtu,
        }
    }
}

/// Enables forwarding in the agent's namespace and installs orbd's nftables table
/// (replaced atomically; nothing else is touched). Run once before any `Tap` VM starts.
pub fn prepare_host() -> Result<(), String> {
    std::fs::write("/proc/sys/net/ipv4/ip_forward", "1")
        .map_err(|e| format!("enabling IPv4 forwarding: {e}"))?;
    nft(r#"table inet orbd
delete table inet orbd
table inet orbd {
    chain input {
        type filter hook input priority filter; policy accept;
        iifname "orbd*" ct state established,related accept
        iifname "orbd*" drop
    }
    chain forward {
        type filter hook forward priority filter; policy accept;
        iifname "orbd*" meta nfproto ipv6 drop
        iifname "orbd*" oifname "orbd*" drop
        iifname "orbd*" ip daddr 169.254.0.0/16 drop
    }
    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
        iifname "orbd*" oifname != "orbd*" masquerade
    }
}
"#)
}

/// Releases everything `orb` holds on the host (namespace, index), also what a create
/// left behind before its recipe was published. Idempotent; call once no VMM runs.
pub fn release(orb: OrbId, state_dir: &Path) {
    let netns = format!("orbd-{orb}");
    let path = Path::new("/run/netns").join(&netns);
    if !is_netns(&path) {
        let _ = std::fs::remove_file(&path);
    } else if let Err(e) = ip(&["netns", "delete", &netns]) {
        tracing::warn!(%netns, error = %e, "removing network namespace");
    }
    let dir = state_dir.join("net");
    let _g = ALLOCATION.lock().unwrap_or_else(|e| e.into_inner());
    for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
        if std::fs::read_to_string(e.path()).is_ok_and(|o| o.trim() == orb.to_string()) {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

/// `<dir>/<index>` files name their orb. Returns the orb's existing index or the
/// lowest free one, durably claimed.
fn allocate(dir: &Path, orb: OrbId) -> Result<u32, String> {
    let _g = ALLOCATION.lock().unwrap_or_else(|e| e.into_inner());
    std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let owner = orb.to_string();
    let mut used = std::collections::HashSet::new();
    for e in std::fs::read_dir(dir).map_err(|e| e.to_string())?.flatten() {
        let Some(i) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            continue;
        };
        if std::fs::read_to_string(e.path()).is_ok_and(|o| o.trim() == owner) {
            return Ok(i);
        }
        used.insert(i);
    }
    // 169.254.169.0/24 (cloud metadata) is never handed out, nor an index whose
    // interface exists (left by an earlier agent whose state was wiped).
    let index = (1..=MAX_INDEX)
        .filter(|i| i / 64 != 169)
        .find(|i| !used.contains(i) && !Path::new(&format!("/sys/class/net/orbd{i}")).exists())
        .ok_or("no free network index")?;
    super::fsutil::write_atomic(&dir.join(index.to_string()), owner.as_bytes())
        .map_err(|e| format!("claiming network index {index}: {e}"))?;
    Ok(index)
}

/// Host-side (`.1`) and namespace-side (`.2`) addresses of index `i`'s /30.
fn transit(i: u32) -> (String, String) {
    let (a, b) = (i / 64, (i % 64) * 4);
    (
        format!("169.254.{a}.{}", b + 1),
        format!("169.254.{a}.{}", b + 2),
    )
}

fn build_namespace(netns: &str, index: u32) -> Result<(), String> {
    let host_if = format!("orbd{index}");
    let (host_addr, ns_addr) = transit(index);
    let path = Path::new("/run/netns").join(netns);
    if !is_netns(&path) {
        // A name left without its mount (the agent's container restarted) is stale.
        let _ = std::fs::remove_file(&path);
        ip(&["netns", "add", netns])?;
    }
    let exists = |args: &[&str]| {
        Command::new("ip")
            .args(args)
            .output()
            .is_ok_and(|o| o.status.success())
    };
    if !exists(&["-n", netns, "link", "show", TAP]) {
        ip(&["-n", netns, "tuntap", "add", "dev", TAP, "mode", "tap"])?;
    }
    if !exists(&["link", "show", &host_if]) {
        // A leftover peer in the namespace (crash mid-setup) blocks the new pair.
        if exists(&["-n", netns, "link", "show", "eth0"]) {
            ip(&["-n", netns, "link", "delete", "eth0"])?;
        }
        ip(&[
            "link", "add", &host_if, "type", "veth", "peer", "name", "eth0", "netns", netns,
        ])?;
    }
    ip(&[
        "addr",
        "replace",
        &format!("{host_addr}/30"),
        "dev",
        &host_if,
    ])?;
    ip(&["link", "set", &host_if, "up"])?;
    for args in [
        vec!["addr", "replace", "10.0.2.2/24", "dev", TAP],
        vec!["link", "set", TAP, "up"],
        vec!["link", "set", "lo", "up"],
        vec!["addr", "replace", &format!("{ns_addr}/30"), "dev", "eth0"],
        vec!["link", "set", "eth0", "up"],
        vec!["route", "replace", "default", "via", &host_addr],
    ] {
        let mut full = vec!["-n", netns];
        full.extend(args);
        ip(&full)?;
    }
    in_netns(netns, || {
        for (key, value) in [
            ("net/ipv4/ip_forward", "1"),
            ("net/ipv6/conf/all/disable_ipv6", "1"),
            ("net/ipv6/conf/default/disable_ipv6", "1"),
        ] {
            std::fs::write(Path::new("/proc/sys").join(key), value)
                .map_err(|e| format!("{key}: {e}"))?;
        }
        nft(r#"table ip orbd
delete table ip orbd
table ip orbd {
    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
        oifname "eth0" masquerade
    }
}
"#)
    })
}

/// Whether `path` is a mounted network namespace (not just a leftover name).
fn is_netns(path: &Path) -> bool {
    const NSFS_MAGIC: i64 = 0x6e73_6673;
    let Ok(c) = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()) else {
        return false;
    };
    // SAFETY: statfs fills a zeroed struct for a valid C string.
    unsafe {
        let mut st: nix::libc::statfs = std::mem::zeroed();
        nix::libc::statfs(c.as_ptr(), &mut st) == 0 && st.f_type as i64 == NSFS_MAGIC
    }
}

/// Runs `f` on a thread that has joined `netns`; commands it starts run there too.
fn in_netns<T: Send>(
    netns: &str,
    f: impl FnOnce() -> Result<T, String> + Send,
) -> Result<T, String> {
    let path = Path::new("/run/netns").join(netns);
    let ns = std::fs::File::open(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    std::thread::scope(|s| {
        s.spawn(|| {
            // SAFETY: setns on this (short-lived) thread only; the fd is valid.
            if unsafe { nix::libc::setns(ns.as_raw_fd(), nix::libc::CLONE_NEWNET) } < 0 {
                return Err(format!(
                    "joining {netns}: {}",
                    std::io::Error::last_os_error()
                ));
            }
            f()
        })
        .join()
        .map_err(|_| "network namespace thread panicked".to_owned())?
    })
}

fn ip(args: &[&str]) -> Result<(), String> {
    let out = Command::new("ip")
        .args(args)
        .output()
        .map_err(|e| format!("ip: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "ip {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

/// Applies an nftables script atomically.
fn nft(script: &str) -> Result<(), String> {
    use std::io::Write;
    let mut child = Command::new("nft")
        .args(["-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("nft: {e}"))?;
    child
        .stdin
        .take()
        .expect("piped")
        .write_all(script.as_bytes())
        .map_err(|e| format!("nft: {e}"))?;
    let out = child.wait_with_output().map_err(|e| format!("nft: {e}"))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "nft: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

/// QEMU's OUI plus three bytes derived from the orb id.
fn stable_mac(orb: OrbId) -> String {
    let h = Sha256::digest(orb.0.as_bytes());
    format!("52:54:00:{:02x}:{:02x}:{:02x}", h[0], h[1], h[2])
}
