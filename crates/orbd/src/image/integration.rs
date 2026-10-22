//! orbd's guest integration as one extra OCI layer on top of Amp's image: the
//! `orbd-guest` helper, its unit, the credential gate for Amp's units, and a few files
//! a container image leaves to its runtime. Applying it as a layer lets umoci do all
//! writes inside the untrusted root filesystem; orbd never edits it in place.
//!
//! The layer is deterministic (fixed owners, modes and mtimes), so its sha256 is the
//! guest integration digest in the image cache key and in checkpoint manifests.

use sha2::{Digest, Sha256};
use std::path::Path;

/// Files from `guest/rootfs/` (path inside the guest, mode, content).
const FILES: &[(&str, u32, &str)] = &[
    (
        "etc/systemd/system/orbd-guest.service",
        0o644,
        include_str!("../../../../guest/rootfs/etc/systemd/system/orbd-guest.service"),
    ),
    (
        "etc/systemd/system/amp-headless.service.d/orbd-credentials.conf",
        0o644,
        include_str!(
            "../../../../guest/rootfs/etc/systemd/system/amp-headless.service.d/orbd-credentials.conf"
        ),
    ),
    (
        "etc/systemd/system/amp-headless-bootstrap.service.d/orbd-credentials.conf",
        0o644,
        include_str!(
            "../../../../guest/rootfs/etc/systemd/system/amp-headless-bootstrap.service.d/orbd-credentials.conf"
        ),
    ),
    (
        "etc/udev/rules.d/60-orbd-kvm.rules",
        0o644,
        include_str!("../../../../guest/rootfs/etc/udev/rules.d/60-orbd-kvm.rules"),
    ),
    // Empty: systemd generates a machine id on each new orb's first boot instead of
    // every orb sharing the one baked into the image.
    ("etc/machine-id", 0o444, ""),
];

const SYMLINKS: &[(&str, &str)] = &[
    (
        "etc/systemd/system/sysinit.target.wants/orbd-guest.service",
        "/etc/systemd/system/orbd-guest.service",
    ),
    // D-Bus follows the machine id instead of keeping the image's copy.
    ("var/lib/dbus/machine-id", "/etc/machine-id"),
    // The serial console is a log file on the host; without udev, the getty would hold
    // boot for 90 s waiting for dev-ttyS0.device.
    ("etc/systemd/system/serial-getty@ttyS0.service", "/dev/null"),
];

pub const GUEST_HELPER_PATH: &str = "usr/local/sbin/orbd-guest";

/// The integration layer (uncompressed tar) and its digest (`sha256:<hex>`).
pub struct IntegrationLayer {
    pub tar: Vec<u8>,
    pub digest: String,
}

impl IntegrationLayer {
    pub fn build(guest_helper: &Path) -> std::io::Result<Self> {
        let helper = std::fs::read(guest_helper).map_err(|e| {
            std::io::Error::new(e.kind(), format!("{}: {e}", guest_helper.display()))
        })?;
        if !helper.starts_with(b"\x7fELF") {
            return Err(std::io::Error::other(format!(
                "{} is not an ELF binary",
                guest_helper.display()
            )));
        }
        let mut b = tar::Builder::new(Vec::new());
        b.mode(tar::HeaderMode::Deterministic);
        let mut add = |path: &str, mode: u32, data: &[u8]| -> std::io::Result<()> {
            let mut h = header(tar::EntryType::Regular, mode);
            h.set_size(data.len() as u64);
            b.append_data(&mut h, path, data)
        };
        add(GUEST_HELPER_PATH, 0o755, &helper)?;
        for (path, mode, content) in FILES {
            add(path, *mode, content.as_bytes())?;
        }
        for (path, target) in SYMLINKS {
            let mut h = header(tar::EntryType::Symlink, 0o777);
            h.set_size(0);
            b.append_link(&mut h, path, target)?;
        }
        let tar = b.into_inner()?;
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(&tar)));
        Ok(Self { tar, digest })
    }
}

fn header(kind: tar::EntryType, mode: u32) -> tar::Header {
    let mut h = tar::Header::new_ustar();
    h.set_entry_type(kind);
    h.set_mode(mode);
    h.set_uid(0);
    h.set_gid(0);
    h.set_mtime(0);
    h
}
