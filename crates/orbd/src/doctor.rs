//! `orbd doctor`: reports whether this host has what the VM runtime and image
//! preparation need. Prints one line per check; fails if a required check fails.

use crate::image::UMOCI_VERSION;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Paths to check. Defaults come from `PATH`; `--config` fills them from an agent
/// config's `[images]` and `[runtime.qemu]` sections.
#[derive(Debug, Default)]
pub struct DoctorOptions {
    pub config: Option<PathBuf>,
    pub kernel: Option<PathBuf>,
}

struct Report {
    failed: bool,
}

impl Report {
    fn line(&mut self, ok: Result<String, String>, what: &str, required: bool) {
        match ok {
            Ok(detail) => println!("ok    {what:<22} {detail}"),
            Err(detail) if required => {
                self.failed = true;
                println!("FAIL  {what:<22} {detail}");
            }
            Err(detail) => println!("warn  {what:<22} {detail}"),
        }
    }
}

fn first_line(program: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new(program)
        .args(args)
        .output()
        .map_err(|e| format!("{}: {e}", program.display()))?;
    let text = if out.stdout.is_empty() {
        out.stderr
    } else {
        out.stdout
    };
    let line = String::from_utf8_lossy(&text)
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_owned();
    if !out.status.success() && line.is_empty() {
        return Err(format!("{} exited {}", program.display(), out.status));
    }
    Ok(line)
}

fn sha256_file(path: &Path) -> Result<(String, u64), String> {
    let mut f = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    let mut len = 0u64;
    loop {
        let n = f.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        len += n as u64;
        h.update(&buf[..n]);
    }
    Ok((hex::encode(h.finalize()), len))
}

fn kvm() -> Result<String, String> {
    // KVM_GET_API_VERSION = _IO(0xAE, 0x00)
    const KVM_GET_API_VERSION: nix::libc::c_ulong = 0xAE00;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/kvm")
        .map_err(|e| format!("/dev/kvm: {e}"))?;
    // SAFETY: argument-less ioctl on an open /dev/kvm fd.
    let api = unsafe {
        nix::libc::ioctl(
            std::os::fd::AsRawFd::as_raw_fd(&f),
            KVM_GET_API_VERSION as _,
            0,
        )
    };
    let nested = ["kvm_intel", "kvm_amd"].iter().find_map(|m| {
        let v = std::fs::read_to_string(format!("/sys/module/{m}/parameters/nested")).ok()?;
        Some(format!("{m} nested={}", v.trim()))
    });
    let nested = nested.unwrap_or_else(|| "nested support unknown".into());
    if api != 12 {
        return Err(format!("KVM API version {api}, expected 12"));
    }
    if nested.ends_with("=N") || nested.ends_with("=0") {
        return Err(format!("API 12, {nested}: orbs get no /dev/kvm"));
    }
    Ok(format!("API 12, {nested}"))
}

fn kernel(path: &Path) -> Result<String, String> {
    let mut header = [0u8; 0x206];
    std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut header))
        .map_err(|e| format!("{}: {e}", path.display()))?;
    if &header[0x202..0x206] != b"HdrS" {
        return Err(format!("{} is not a bzImage", path.display()));
    }
    let (sha, len) = sha256_file(path)?;
    Ok(format!(
        "{} ({} KiB, sha256 {sha})",
        path.display(),
        len / 1024
    ))
}

fn mke2fs(path: &Path) -> Result<String, String> {
    let line = first_line(path, &["-V"])?;
    // `mke2fs 1.47.0 (5-Feb-2023)`; `-d` with hard links and xattrs needs >= 1.45.
    let version = line.split_whitespace().nth(1).unwrap_or("");
    let parts: Vec<u32> = version.split('.').filter_map(|p| p.parse().ok()).collect();
    if parts.len() < 2 || (parts[0], parts[1]) < (1, 45) {
        return Err(format!("{line}: need e2fsprogs >= 1.45"));
    }
    Ok(line)
}

fn guest_helper(path: &Path) -> Result<String, String> {
    let layer = crate::image::IntegrationLayer::build(path).map_err(|e| e.to_string())?;
    let (sha, len) = sha256_file(path)?;
    Ok(format!(
        "{} ({} KiB, sha256 {sha}; guest integration {})",
        path.display(),
        len / 1024,
        layer.digest
    ))
}

/// Runs the checks; returns false if a required one failed.
pub fn run(opts: &DoctorOptions) -> anyhow::Result<bool> {
    let mut umoci = PathBuf::from("umoci");
    let mut mkfs = PathBuf::from("mkfs.ext4");
    let mut helper: Option<PathBuf> = None;
    let mut kernel_path = opts.kernel.clone();
    let mut qemu = PathBuf::from("qemu-system-x86_64");
    let mut qemu_img = PathBuf::from("qemu-img");
    if let Some(config) = &opts.config {
        let text = std::fs::read_to_string(config)?;
        let v: serde_json::Value = serde_saphyr::from_str(&text)?;
        let get = |section: &[&str], key: &str| -> Option<PathBuf> {
            let mut t = &v;
            for s in section {
                t = t.get(s)?;
            }
            t.get(key)?.as_str().map(PathBuf::from)
        };
        umoci = get(&["images"], "umoci").unwrap_or(umoci);
        mkfs = get(&["images"], "mkfs_ext4").unwrap_or(mkfs);
        helper = get(&["images"], "guest_helper");
        kernel_path = kernel_path.or_else(|| get(&["runtime", "qemu"], "kernel"));
        qemu = get(&["runtime", "qemu"], "qemu").unwrap_or(qemu);
        qemu_img = get(&["runtime", "qemu"], "qemu_img").unwrap_or(qemu_img);
    }
    if !nix::unistd::geteuid().is_root() && Path::new(&mkfs).components().count() == 1 {
        // Non-root PATHs often lack /sbin, where e2fsprogs lives.
        if let Some(p) = ["/usr/sbin/mkfs.ext4", "/sbin/mkfs.ext4"]
            .iter()
            .find(|p| Path::new(p).exists())
        {
            mkfs = PathBuf::from(p);
        }
    }

    let mut r = Report { failed: false };
    r.line(
        first_line(&qemu, &["--version"]),
        "qemu-system-x86_64",
        true,
    );
    r.line(first_line(&qemu_img, &["--version"]), "qemu-img", true);
    r.line(kvm(), "/dev/kvm", true);
    r.line(
        match &kernel_path {
            Some(p) => kernel(p),
            None => Err("not configured (runtime.qemu.kernel or --kernel)".into()),
        },
        "guest kernel",
        true,
    );
    let umoci_check = first_line(&umoci, &["--version"]).and_then(|l| {
        if l == format!("umoci version {UMOCI_VERSION}") {
            Ok(l)
        } else {
            Err(format!("{l:?}; need umoci {UMOCI_VERSION}"))
        }
    });
    r.line(umoci_check, "umoci", true);
    r.line(mke2fs(&mkfs), "mkfs.ext4", true);
    r.line(
        match &helper {
            Some(p) => guest_helper(p),
            None => Err("not configured (images.guest_helper)".into()),
        },
        "orbd-guest",
        true,
    );
    let privileged = if nix::unistd::geteuid().is_root() {
        Ok("running as root".to_owned())
    } else {
        match Command::new("sudo").args(["-n", "true"]).status() {
            Ok(s) if s.success() => Ok("not root; `sudo -n` works (development only)".into()),
            _ => Err("not root and `sudo -n` fails: image unpacking needs root".into()),
        }
    };
    r.line(privileged, "privileges", true);
    Ok(!r.failed)
}
