//! Image preparation for VM runtimes: Amp's OCI image → a bootable ext4 root disk with
//! orbd's guest integration installed.
//!
//! Pipeline (never mounts or executes untrusted guest content on the host):
//! resolve `ORB_IMAGE` to its linux/amd64 manifest digest → stream blobs into an OCI
//! layout (oci-client) → unpack with a pinned `umoci` → install guest integration
//! (orbd-guest + unit, kvm group/udev rule, network/DNS hooks) → `mkfs.ext4 -d` into a
//! sparse raw file → publish atomically under `images/<cache key>/`.
//!
//! Cache key: platform manifest digest + recipe version + guest integration digest.
//! A cache miss starts preparation in the background and the caller fails with
//! `IMAGE_NOT_READY`; agents prewarm configured images at start.

use std::path::PathBuf;

/// A published, immutable root disk. Per-orb disks are copy-on-write overlays of it,
/// so it must not be deleted while any orb or project snapshot references it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedImage {
    /// Cache key (hex), also the directory name under `images/`.
    pub key: String,
    /// The image reference as Amp gave it (e.g. `...amp-orb:latest`).
    pub image_ref: String,
    /// Resolved linux/amd64 manifest digest (`sha256:...`).
    pub manifest_digest: String,
    /// Raw ext4 file with the root filesystem (read-only once published).
    pub rootfs: PathBuf,
    /// Size of `rootfs` in bytes (the filesystem size; orb disks grow beyond it).
    pub rootfs_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    /// Not prepared yet; preparation has been started (or is running).
    #[error("image not ready: {0}")]
    NotReady(String),
    #[error("image preparation failed: {0}")]
    Failed(String),
}
