//! `orbd-guest init`: early-boot setup run by `orbd-guest.service` before the helper
//! serves the host. Every step is idempotent and runs on every boot; failures are logged
//! and do not block boot.

use crate::cmd::run;
use crate::files::write_atomic;
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::Duration;

const STATE_DIR: &str = "/var/lib/orbd-guest";
/// The MAC this disk last booted with. orbd gives every orb a stable MAC, so a change
/// means this disk now belongs to a different orb (a project-snapshot clone).
const IDENTITY_FILE: &str = "/var/lib/orbd-guest/identity";
const TOOL: Duration = Duration::from_secs(60);

pub fn init() {
    for (what, result) in [
        ("growing root filesystem", grow_root()),
        ("/dev/kvm access", kvm_access()),
        ("machine identity", identity()),
        ("/etc/hosts", hosts()),
    ] {
        if let Err(e) = result {
            log!("init: {what}: {e}");
        }
    }
}

/// Grows the root ext4 filesystem online to the size of its block device (the per-orb
/// disk is larger than the image it was created from).
fn grow_root() -> Result<(), String> {
    // EXT4_IOC_RESIZE_FS = _IOW('f', 16, __u64)
    const EXT4_IOC_RESIZE_FS: libc::c_ulong = 0x4008_6610;
    let root = std::fs::File::open("/").map_err(|e| e.to_string())?;
    let meta = root.metadata().map_err(|e| e.to_string())?;
    let dev = std::os::unix::fs::MetadataExt::dev(&meta);
    let (major, minor) = (libc::major(dev), libc::minor(dev));
    let sectors: u64 = std::fs::read_to_string(format!("/sys/dev/block/{major}:{minor}/size"))
        .map_err(|e| format!("device {major}:{minor}: {e}"))?
        .trim()
        .parse()
        .map_err(|e| format!("device size: {e}"))?;
    // SAFETY: statvfs fills the struct; fd is valid.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstatvfs(std::os::fd::AsRawFd::as_raw_fd(&root), &mut st) } < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let block = st.f_frsize as u64;
    let mut blocks: u64 = sectors * 512 / block;
    let before = st.f_blocks as u64;
    // SAFETY: the ioctl reads one u64 (the new block count). Equal size is a no-op;
    // the kernel refuses to shrink.
    let r = unsafe {
        libc::ioctl(
            std::os::fd::AsRawFd::as_raw_fd(&root),
            EXT4_IOC_RESIZE_FS as _,
            &mut blocks as *mut u64,
        )
    };
    if r < 0 {
        return Err(format!(
            "EXT4_IOC_RESIZE_FS: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: as above.
    unsafe { libc::fstatvfs(std::os::fd::AsRawFd::as_raw_fd(&root), &mut st) };
    if st.f_blocks as u64 != before {
        log!(
            "init: grew / from {} to {} MiB",
            (before * block) >> 20,
            ((st.f_blocks as u64) * block) >> 20
        );
    }
    Ok(())
}

/// `/dev/kvm` as root:kvm 0660 with `user` in the kvm group (images without udev get the
/// kernel's root-only node).
fn kvm_access() -> Result<(), String> {
    let has_group = std::fs::read_to_string("/etc/group")
        .map_err(|e| e.to_string())?
        .lines()
        .any(|l| l.starts_with("kvm:"));
    if !has_group {
        let out = run(&["groupadd", "--system", "kvm"], TOOL)?;
        if !out.ok {
            return Err(format!("groupadd kvm: {}", out.stderr.trim()));
        }
        log!("init: created group kvm");
    }
    let member = run(&["id", "-nG", "user"], TOOL)?;
    if member.ok && !member.stdout.split_whitespace().any(|g| g == "kvm") {
        let out = run(&["usermod", "-aG", "kvm", "user"], TOOL)?;
        if !out.ok {
            return Err(format!("usermod: {}", out.stderr.trim()));
        }
        log!("init: added user to group kvm");
    }
    let gid = group_id("kvm")?;
    let path = CString::new("/dev/kvm").unwrap();
    if !Path::new("/dev/kvm").exists() {
        return Err("/dev/kvm is missing (no nested virtualization from the host?)".into());
    }
    // SAFETY: valid path.
    unsafe {
        if libc::chown(path.as_ptr(), 0, gid) < 0 || libc::chmod(path.as_ptr(), 0o660) < 0 {
            return Err(format!("/dev/kvm: {}", std::io::Error::last_os_error()));
        }
    }
    Ok(())
}

/// Container images rely on the runtime for /etc/hosts; a VM has to resolve its own
/// hostname (sudo complains on every call otherwise).
fn hosts() -> Result<(), String> {
    let hostname = std::fs::read_to_string("/etc/hostname")
        .unwrap_or_default()
        .trim()
        .to_owned();
    let current = std::fs::read_to_string("/etc/hosts").unwrap_or_default();
    let known = |name: &str| {
        current.lines().any(|l| {
            l.split('#')
                .next()
                .unwrap_or("")
                .split_whitespace()
                .skip(1)
                .any(|n| n == name)
        })
    };
    let mut body = current.clone();
    if !body.is_empty() && !body.ends_with('\n') {
        body.push('\n');
    }
    if !known("localhost") {
        body.push_str("127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n");
    }
    if !hostname.is_empty() && !known(&hostname) {
        body.push_str(&format!("127.0.1.1\t{hostname}\n"));
    }
    if body != current {
        write_atomic(Path::new("/etc/hosts"), body.as_bytes(), 0o644, None)
            .map_err(|e| e.to_string())?;
        log!("init: updated /etc/hosts");
    }
    Ok(())
}

fn group_id(name: &str) -> Result<u32, String> {
    let c = CString::new(name).unwrap();
    // SAFETY: getgrnam returns static storage or null; single-threaded here.
    let gr = unsafe { libc::getgrnam(c.as_ptr()) };
    if gr.is_null() {
        return Err(format!("no group {name}"));
    }
    // SAFETY: non-null group entry.
    Ok(unsafe { (*gr).gr_gid })
}

fn primary_mac() -> Option<String> {
    let mut macs: Vec<String> = std::fs::read_dir("/sys/class/net")
        .ok()?
        .flatten()
        .filter(|e| e.path().join("device").exists())
        .filter_map(|e| std::fs::read_to_string(e.path().join("address")).ok())
        .map(|s| s.trim().to_owned())
        .collect();
    macs.sort();
    macs.into_iter().next()
}

/// Resets per-machine identity when this disk first boots as a new orb (from the shared
/// image, or as a clone of another orb's disk): SSH host keys and the D-Bus machine id
/// follow the current machine id instead of the image's or the source orb's.
///
/// The machine id itself is set before this runs: the image ships an empty
/// /etc/machine-id (systemd generates one on first boot), and the host may pass
/// `systemd.machine_id=` on the kernel command line, which covers clones.
fn identity() -> Result<(), String> {
    let Some(mac) = primary_mac() else {
        return Err("no network interface".into());
    };
    let machine_id = std::fs::read_to_string("/etc/machine-id")
        .unwrap_or_default()
        .trim()
        .to_owned();
    let current = format!("mac={mac}\nmachine_id={machine_id}\n");
    let recorded = std::fs::read_to_string(IDENTITY_FILE).unwrap_or_default();
    if recorded == current {
        return Ok(());
    }
    let recorded_mac = recorded.lines().find_map(|l| l.strip_prefix("mac="));
    let recorded_id = recorded.lines().find_map(|l| l.strip_prefix("machine_id="));
    log!(
        "init: new machine identity (mac {} -> {mac}, machine id {} -> {machine_id})",
        recorded_mac.unwrap_or("none"),
        recorded_id.unwrap_or("none")
    );
    if recorded_mac.is_some() && recorded_id == Some(machine_id.as_str()) {
        log!(
            "init: warning: this disk moved to a new orb but kept its machine id; pass \
             systemd.machine_id= on the kernel command line to give clones their own"
        );
    }
    if !machine_id.is_empty() && Path::new("/var/lib/dbus").is_dir() {
        let _ = write_atomic(
            Path::new("/var/lib/dbus/machine-id"),
            format!("{machine_id}\n").as_bytes(),
            0o444,
            None,
        );
    }
    let _ = std::fs::remove_file("/var/lib/systemd/random-seed");
    if Path::new("/etc/ssh").is_dir() {
        for entry in std::fs::read_dir("/etc/ssh")
            .map_err(|e| e.to_string())?
            .flatten()
        {
            let name = entry.file_name();
            if name.as_bytes().starts_with(b"ssh_host_") {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        if Path::new("/usr/bin/ssh-keygen").exists() {
            let out = run(&["ssh-keygen", "-A"], TOOL)?;
            if !out.ok {
                return Err(format!("ssh-keygen -A: {}", out.stderr.trim()));
            }
            log!("init: regenerated SSH host keys");
        }
    }
    std::fs::create_dir_all(STATE_DIR).map_err(|e| e.to_string())?;
    write_atomic(Path::new(IDENTITY_FILE), current.as_bytes(), 0o644, None)
        .map_err(|e| e.to_string())
}
