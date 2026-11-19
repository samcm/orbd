//! Static IPv4 addressing and DNS from `Wake.network`. Uses the kernel's interface
//! ioctls directly: orb images need not ship iproute2 (Amp's does not), and the image's
//! own network managers (if any) are not configured for orbd's addressing. [`watch_dns`]
//! keeps that DNS working when an in-orb tailscaled dies without restoring resolv.conf.

use crate::proto::NetworkConfig;
use std::io;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

pub fn apply(net: &NetworkConfig) -> Result<(), String> {
    if !net.ipv4_cidr.is_empty() {
        let iface = interface(&net.mac)?;
        let (addr, prefix) = parse_cidr(&net.ipv4_cidr)?;
        let gateway = match net.ipv4_gateway.as_str() {
            "" => None,
            g => Some(
                g.parse::<Ipv4Addr>()
                    .map_err(|e| format!("gateway {g}: {e}"))?,
            ),
        };
        configure(&iface, addr, prefix, gateway, net.mtu)
            .map_err(|e| format!("configuring {iface}: {e}"))?;
        log!(
            "network: {iface} {}/{prefix} via {}{}",
            addr,
            gateway.map_or("-".into(), |g| g.to_string()),
            if net.mtu > 0 {
                format!(" mtu {}", net.mtu)
            } else {
                String::new()
            }
        );
    }
    if net.dns_servers.is_empty() {
        let mut applied = APPLIED_DNS.lock().unwrap_or_else(PoisonError::into_inner);
        remember_dns(&mut applied, None);
    } else {
        write_resolv_conf(&net.dns_servers, &net.search_domains)?;
    }
    Ok(())
}

/// The interface with `mac`, or the only non-loopback device-backed interface.
fn interface(mac: &str) -> Result<String, String> {
    let mut candidates = Vec::new();
    let dir = std::fs::read_dir("/sys/class/net").map_err(|e| format!("/sys/class/net: {e}"))?;
    for entry in dir.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        if !path.join("device").exists() {
            continue;
        }
        let addr = std::fs::read_to_string(path.join("address")).unwrap_or_default();
        if !mac.is_empty() && addr.trim().eq_ignore_ascii_case(mac) {
            return Ok(name);
        }
        candidates.push(name);
    }
    match (mac.is_empty(), candidates.as_slice()) {
        (true, [only]) => Ok(only.clone()),
        (false, _) => Err(format!("no interface with MAC {mac} (have {candidates:?})")),
        (true, _) => Err(format!("cannot pick an interface from {candidates:?}")),
    }
}

fn parse_cidr(cidr: &str) -> Result<(Ipv4Addr, u8), String> {
    let (a, p) = cidr
        .split_once('/')
        .ok_or_else(|| format!("{cidr}: not a CIDR"))?;
    let addr = a.parse().map_err(|e| format!("{cidr}: {e}"))?;
    let prefix: u8 = p.parse().map_err(|e| format!("{cidr}: {e}"))?;
    if prefix > 32 {
        return Err(format!("{cidr}: bad prefix"));
    }
    Ok((addr, prefix))
}

fn sockaddr(ip: Ipv4Addr) -> libc::sockaddr {
    let sin = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: 0,
        sin_addr: libc::in_addr {
            s_addr: u32::from(ip).to_be(),
        },
        sin_zero: [0; 8],
    };
    // SAFETY: sockaddr_in and sockaddr have the same size on Linux.
    unsafe { std::mem::transmute(sin) }
}

fn ip_of(sa: &libc::sockaddr) -> Ipv4Addr {
    // SAFETY: the kernel filled an AF_INET address.
    let sin: &libc::sockaddr_in = unsafe { &*(sa as *const _ as *const libc::sockaddr_in) };
    Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr))
}

struct Ifreq(libc::ifreq);

impl Ifreq {
    fn new(name: &str) -> io::Result<Self> {
        // SAFETY: ifreq is plain data.
        let mut r: libc::ifreq = unsafe { std::mem::zeroed() };
        if name.len() >= r.ifr_name.len() {
            return Err(io::Error::other("interface name too long"));
        }
        for (d, s) in r.ifr_name.iter_mut().zip(name.bytes()) {
            *d = s as libc::c_char;
        }
        Ok(Self(r))
    }
}

fn ioctl(sock: &OwnedFd, req: libc::Ioctl, arg: *mut libc::c_void) -> io::Result<()> {
    // SAFETY: arg points at the struct the request expects.
    if unsafe { libc::ioctl(sock.as_raw_fd(), req, arg) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Idempotent: leaves an already-correct address alone (no connection reset on a warm
/// wake) and replaces a different one (e.g. a clone of another orb's disk).
fn configure(
    iface: &str,
    addr: Ipv4Addr,
    prefix: u8,
    gateway: Option<Ipv4Addr>,
    mtu: u32,
) -> io::Result<()> {
    // SAFETY: plain socket(2).
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd was just created and is owned here.
    let sock = unsafe { OwnedFd::from_raw_fd(fd) };
    let mask = Ipv4Addr::from(if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    });

    if mtu > 0 {
        let mut r = Ifreq::new(iface)?;
        r.0.ifr_ifru.ifru_mtu = mtu as libc::c_int;
        ioctl(&sock, libc::SIOCSIFMTU as _, &mut r.0 as *mut _ as *mut _)?;
    }

    let mut r = Ifreq::new(iface)?;
    let current = ioctl(&sock, libc::SIOCGIFADDR as _, &mut r.0 as *mut _ as *mut _)
        // SAFETY: SIOCGIFADDR filled ifru_addr.
        .map(|()| ip_of(unsafe { &r.0.ifr_ifru.ifru_addr }))
        .ok();
    let mut r = Ifreq::new(iface)?;
    let current_mask = ioctl(
        &sock,
        libc::SIOCGIFNETMASK as _,
        &mut r.0 as *mut _ as *mut _,
    )
    // SAFETY: SIOCGIFNETMASK filled ifru_netmask.
    .map(|()| ip_of(unsafe { &r.0.ifr_ifru.ifru_netmask }))
    .ok();
    if current != Some(addr) {
        let mut r = Ifreq::new(iface)?;
        r.0.ifr_ifru.ifru_addr = sockaddr(addr);
        ioctl(&sock, libc::SIOCSIFADDR as _, &mut r.0 as *mut _ as *mut _)?;
    }
    if current != Some(addr) || current_mask != Some(mask) {
        let mut r = Ifreq::new(iface)?;
        r.0.ifr_ifru.ifru_netmask = sockaddr(mask);
        ioctl(
            &sock,
            libc::SIOCSIFNETMASK as _,
            &mut r.0 as *mut _ as *mut _,
        )?;
    }

    let mut r = Ifreq::new(iface)?;
    ioctl(&sock, libc::SIOCGIFFLAGS as _, &mut r.0 as *mut _ as *mut _)?;
    // SAFETY: SIOCGIFFLAGS filled ifru_flags.
    let flags = unsafe { r.0.ifr_ifru.ifru_flags };
    let want = flags | (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
    if want != flags {
        r.0.ifr_ifru.ifru_flags = want;
        ioctl(&sock, libc::SIOCSIFFLAGS as _, &mut r.0 as *mut _ as *mut _)?;
    }

    if let Some(gw) = gateway
        && default_route(iface)? != Some(gw)
    {
        let mut route = rtentry(iface, gw);
        // A stale default route (other gateway) must go first; absent is fine.
        let _ = ioctl(
            &sock,
            libc::SIOCDELRT as _,
            &mut route.0 as *mut _ as *mut _,
        );
        ioctl(
            &sock,
            libc::SIOCADDRT as _,
            &mut route.0 as *mut _ as *mut _,
        )?;
    }
    Ok(())
}

/// `rtentry` borrows the device name, so keep the C string alive alongside it.
struct Route(libc::rtentry, #[allow(dead_code)] std::ffi::CString);

fn rtentry(iface: &str, gw: Ipv4Addr) -> Route {
    let dev = std::ffi::CString::new(iface).unwrap();
    // SAFETY: rtentry is plain data.
    let mut rt: libc::rtentry = unsafe { std::mem::zeroed() };
    rt.rt_dst = sockaddr(Ipv4Addr::UNSPECIFIED);
    rt.rt_genmask = sockaddr(Ipv4Addr::UNSPECIFIED);
    rt.rt_gateway = sockaddr(gw);
    rt.rt_flags = libc::RTF_UP | libc::RTF_GATEWAY;
    rt.rt_dev = dev.as_ptr() as *mut _;
    Route(rt, dev)
}

/// The IPv4 default gateway on `iface` from /proc/net/route.
fn default_route(iface: &str) -> io::Result<Option<Ipv4Addr>> {
    let table = std::fs::read_to_string("/proc/net/route")?;
    Ok(table.lines().skip(1).find_map(|l| {
        let f: Vec<&str> = l.split_whitespace().collect();
        if f.len() > 2 && f[0] == iface && f[1] == "00000000" {
            let gw = u32::from_str_radix(f[2], 16).ok()?;
            Some(Ipv4Addr::from(u32::from_be(gw)))
        } else {
            None
        }
    }))
}

const RESOLV_CONF: &str = "/etc/resolv.conf";
/// Where Tailscale keeps the upstream configuration while it owns resolv.conf.
const TAILSCALE_BACKUP: &str = "/etc/resolv.pre-tailscale-backup.conf";
/// How often [`watch_dns`] looks for a resolv.conf left behind by a dead tailscaled.
const DNS_WATCH_INTERVAL: Duration = Duration::from_secs(5);

/// The resolv.conf the last Wake or Resync applied, which [`watch_dns`] puts back. The
/// lock also keeps a wake and the watcher from writing at the same time.
static APPLIED_DNS: Mutex<Option<String>> = Mutex::new(None);
/// [`APPLIED_DNS`] on tmpfs, so a restarted helper keeps watching before the next Wake
/// (a repeated Wake generation is acked without applying it again). Gone after a reboot.
const APPLIED_DNS_FILE: &str = "/run/orbd-guest/dns";
const HEADER: &str = "# Written by orbd-guest from the host's configuration.\n";

/// Writes explicit DNS servers (never the host's). When Tailscale inside the orb owns
/// resolv.conf (pointing at 100.100.100.100), the servers go into Tailscale's backup of
/// the upstream configuration instead, so it forwards to a real resolver. After a cold
/// boot, tailscaled is not running yet and its resolv.conf answers nothing, so it is
/// replaced; tailscaled takes it over again when it starts.
fn write_resolv_conf(servers: &[String], search: &[String]) -> Result<(), String> {
    let mut body = String::from(HEADER);
    for s in servers {
        s.parse::<std::net::IpAddr>()
            .map_err(|e| format!("DNS server {s}: {e}"))?;
        body.push_str(&format!("nameserver {s}\n"));
    }
    if !search.is_empty() {
        if search
            .iter()
            .any(|d| d.chars().any(|c| c.is_whitespace() || c.is_control()))
        {
            return Err("bad DNS search domain".into());
        }
        body.push_str(&format!("search {}\n", search.join(" ")));
    }
    let mut applied = APPLIED_DNS.lock().unwrap_or_else(PoisonError::into_inner);
    let current = std::fs::read_to_string(RESOLV_CONF).unwrap_or_default();
    let target = if is_tailscale(&current) && tailscaled_running() {
        TAILSCALE_BACKUP
    } else {
        RESOLV_CONF
    };
    remember_dns(&mut applied, Some(body.clone()));
    if std::fs::read_to_string(target).ok().as_deref() == Some(body.as_str()) {
        return Ok(());
    }
    crate::files::write_atomic(Path::new(target), body.as_bytes(), 0o644, None)
        .map_err(|e| format!("writing {target}: {e}"))?;
    log!("dns: wrote {target} ({})", servers.join(", "));
    Ok(())
}

/// Runs forever, on a thread of its own: puts the DNS servers of the last Wake back into
/// resolv.conf whenever Tailscale's resolv.conf is there but tailscaled is not running.
///
/// tailscaled points resolv.conf at its own resolver and restores the original only when
/// it exits cleanly. Killed instead (e.g. by `amp orb restart-processes`, which kills
/// every process Amp started in the orb), it leaves the orb with no DNS. Amp's executor
/// then cannot reach Amp, so nothing in the orb starts tailscaled again, and before this
/// only the next wake repaired it. tailscaled takes resolv.conf over again when it starts.
/// Does nothing until a wake applied DNS servers, or when the host sends none.
pub fn watch_dns() -> ! {
    {
        let mut applied = APPLIED_DNS.lock().unwrap_or_else(PoisonError::into_inner);
        if applied.is_none() {
            *applied = load_dns(Path::new(APPLIED_DNS_FILE));
        }
    }
    let mut last_error = None;
    loop {
        std::thread::sleep(DNS_WATCH_INTERVAL);
        let applied = APPLIED_DNS.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(body) = applied.as_deref() else {
            continue;
        };
        match replace_stale_tailscale(Path::new(RESOLV_CONF), body, tailscaled_running) {
            Ok(replaced) => {
                if replaced {
                    log!(
                        "dns: tailscaled is not running but its {RESOLV_CONF} was left; restored it"
                    );
                }
                last_error = None;
            }
            Err(e) => {
                if last_error.as_ref() != Some(&e) {
                    log!("dns: {e}");
                }
                last_error = Some(e);
            }
        }
    }
}

/// Writes `body` to `path` if `path` holds Tailscale's configuration and tailscaled is not
/// running (asked only then). Returns whether it wrote.
fn replace_stale_tailscale(
    path: &Path,
    body: &str,
    tailscaled_running: impl FnOnce() -> bool,
) -> Result<bool, String> {
    let current = std::fs::read_to_string(path).unwrap_or_default();
    if !is_tailscale(&current) || tailscaled_running() {
        return Ok(false);
    }
    crate::files::write_atomic(path, body.as_bytes(), 0o644, None)
        .map_err(|e| format!("writing {}: {e}", path.display()))?;
    Ok(true)
}

/// Sets [`APPLIED_DNS`] (whose guard is `applied`) and its copy in [`APPLIED_DNS_FILE`].
/// The copy is best effort: without it a restarted helper waits for the next Wake.
fn remember_dns(applied: &mut Option<String>, body: Option<String>) {
    if *applied == body {
        return;
    }
    save_dns(Path::new(APPLIED_DNS_FILE), body.as_deref());
    *applied = body;
}

fn save_dns(path: &Path, body: Option<&str>) {
    let _ = match body {
        Some(b) => path
            .parent()
            .map_or(Ok(()), std::fs::create_dir_all)
            .and_then(|()| crate::files::write_atomic(path, b.as_bytes(), 0o644, None)),
        None => std::fs::remove_file(path),
    };
}

fn load_dns(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .filter(|b| b.starts_with(HEADER))
}

fn is_tailscale(resolv_conf: &str) -> bool {
    resolv_conf.contains("generated by tailscale")
}

fn tailscaled_running() -> bool {
    std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| {
            std::fs::read_to_string(e.path().join("stat"))
                .is_ok_and(|s| live_process_named(&s, "tailscaled"))
        })
}

/// Whether a `/proc/<pid>/stat` line (`pid (comm) state ...`) is a process named `name`
/// that has not exited: a zombie (`Z`, or `X` while being reaped) keeps its stat file
/// until its parent reaps it.
fn live_process_named(stat: &str, name: &str) -> bool {
    let Some((head, rest)) = stat.rsplit_once(')') else {
        return false;
    };
    head.split_once('(').is_some_and(|(_, comm)| comm == name)
        && rest
            .trim_start()
            .chars()
            .next()
            .is_some_and(|state| state != 'Z' && state != 'X')
}

#[cfg(test)]
mod tests {
    use super::*;

    const OURS: &str =
        "# Written by orbd-guest from the host's configuration.\nnameserver 1.1.1.1\n";
    const TAILSCALE: &str =
        "# resolv.conf(5) file generated by tailscale\nnameserver 100.100.100.100\n";

    #[test]
    fn zombies_and_other_processes_are_not_a_running_tailscaled() {
        let live = "4242 (tailscaled) S 1 4242 4242 0 -1 4194560 2131 0 0 0";
        assert!(live_process_named(live, "tailscaled"));
        assert!(live_process_named(
            &live.replace(" S ", " R "),
            "tailscaled"
        ));
        assert!(!live_process_named(
            &live.replace(" S ", " Z "),
            "tailscaled"
        ));
        assert!(!live_process_named(
            &live.replace(" S ", " X "),
            "tailscaled"
        ));
        assert!(!live_process_named("17 (tailscale) S 1", "tailscaled"));
        assert!(!live_process_named("18 (tailscaled) x) S 1", "tailscaled"));
        assert!(!live_process_named("", "tailscaled"));
    }

    #[test]
    fn the_applied_dns_survives_a_helper_restart_until_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("orbd-guest/dns");
        assert_eq!(load_dns(&file), None);
        save_dns(&file, Some(OURS));
        assert_eq!(load_dns(&file).as_deref(), Some(OURS));
        save_dns(&file, None);
        assert_eq!(load_dns(&file), None);
        std::fs::write(&file, "nameserver 10.0.0.2\n").unwrap();
        assert_eq!(
            load_dns(&file),
            None,
            "only orbd-guest's own resolv.conf is put back"
        );
    }

    fn resolv_conf(content: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resolv.conf");
        std::fs::write(&path, content).unwrap();
        (dir, path)
    }

    #[test]
    fn a_dead_tailscaleds_resolv_conf_is_replaced() {
        let (_dir, path) = resolv_conf(TAILSCALE);
        assert_eq!(replace_stale_tailscale(&path, OURS, || false), Ok(true));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), OURS);
    }

    #[test]
    fn a_running_tailscaled_keeps_its_resolv_conf() {
        let (_dir, path) = resolv_conf(TAILSCALE);
        assert_eq!(replace_stale_tailscale(&path, OURS, || true), Ok(false));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), TAILSCALE);
    }

    #[test]
    fn other_resolv_confs_are_left_alone() {
        let image = "nameserver 10.0.0.2\n";
        for content in [OURS, image, ""] {
            let (_dir, path) = resolv_conf(content);
            let unasked = || panic!("tailscaled checked for a resolv.conf that is not Tailscale's");
            assert_eq!(replace_stale_tailscale(&path, OURS, unasked), Ok(false));
            assert_eq!(std::fs::read_to_string(&path).unwrap(), content);
        }
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("resolv.conf");
        assert_eq!(replace_stale_tailscale(&missing, OURS, || false), Ok(false));
        assert!(!missing.exists());
    }
}
