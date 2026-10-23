//! Host and QEMU probes, run once before launching VMs.

use std::path::Path;

/// Oldest QEMU with file migration and `mapped-ram`.
pub const MIN_QEMU: (u32, u32) = (9, 0);

/// The first line of `qemu --version`, e.g. `QEMU emulator version 10.0.2 (...)`.
pub fn qemu_version(qemu: &Path) -> std::io::Result<String> {
    let out = std::process::Command::new(qemu).arg("--version").output()?;
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_owned())
}

/// QEMU's `--version` line must name [`MIN_QEMU`] or newer.
pub fn check_qemu_version(line: &str) -> Result<(), String> {
    let v = line
        .strip_prefix("QEMU emulator version ")
        .and_then(|r| r.split_whitespace().next())
        .unwrap_or_default();
    let mut parts = v.split('.').map(|p| p.parse::<u32>());
    match (parts.next(), parts.next()) {
        (Some(Ok(major)), Some(Ok(minor))) if (major, minor) >= MIN_QEMU => Ok(()),
        (Some(Ok(_)), Some(Ok(_))) => Err(format!(
            "{line:?} is too old: orbd needs QEMU {}.{} or newer (file migration with mapped-ram)",
            MIN_QEMU.0, MIN_QEMU.1
        )),
        _ => Err(format!("unrecognised QEMU version {line:?}")),
    }
}

/// The versioned machine type `q35` stands for in this QEMU (e.g. `pc-q35-11.0`).
pub fn q35_machine(qemu: &Path) -> Result<String, String> {
    let out = std::process::Command::new(qemu)
        .args(["-machine", "help"])
        .output()
        .map_err(|e| format!("{} -machine help: {e}", qemu.display()))?;
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter(|l| l.split_whitespace().next() == Some("q35"))
        .find_map(|l| Some(l.split("(alias of ").nth(1)?.split(')').next()?.to_owned()))
        .ok_or_else(|| format!("{} has no q35 machine", qemu.display()))
}

/// True when `/dev/kvm` exists and the loaded KVM module does not turn nested
/// virtualization off.
pub fn nested_kvm() -> bool {
    Path::new("/dev/kvm").exists()
        && !["kvm_amd", "kvm_intel"].iter().any(|m| {
            std::fs::read_to_string(format!("/sys/module/{m}/parameters/nested"))
                .is_ok_and(|v| matches!(v.trim(), "0" | "N"))
        })
}

/// True when files in `dir` can be opened with O_DIRECT (disk `cache.direct` and
/// [`FileMigration::direct_io`](crate::qmp::FileMigration::direct_io)).
pub fn direct_io(dir: &Path) -> bool {
    use std::os::unix::fs::OpenOptionsExt;
    let probe = dir.join(".direct-io-probe");
    let ok = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .custom_flags(nix::libc::O_DIRECT)
        .open(&probe)
        .is_ok();
    let _ = std::fs::remove_file(&probe);
    ok
}
