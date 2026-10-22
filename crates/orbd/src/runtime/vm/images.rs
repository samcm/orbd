//! Where the VM runtimes get base root disks from.
//!
//! Normally the image pipeline ([`ImageStore`]); [`StaticImages`] serves
//! operator-provided raw ext4 files from config (development and KVM tests).
//!
//! The runtime asks only when it creates an orb's disk. The orb's recipe then records
//! the prepared image's key, digest and file identity, and the orb keeps that exact
//! image for life: a tag that later resolves to a newer digest never rebases it.

use crate::config::agent::StaticImage;
use crate::image::{ImageError, ImageStore, PreparedImage};
use async_trait::async_trait;

#[async_trait]
pub trait ImageProvider: Send + Sync {
    /// The published root disk for `image_ref`. A cache miss starts preparation and
    /// returns `ImageError::NotReady`.
    async fn prepared(&self, image_ref: &str) -> Result<PreparedImage, ImageError>;
}

#[async_trait]
impl ImageProvider for ImageStore {
    async fn prepared(&self, image_ref: &str) -> Result<PreparedImage, ImageError> {
        self.get(image_ref)
    }
}

/// Fixed `image_ref → rootfs` table (development and tests).
pub struct StaticImages(pub Vec<StaticImage>);

#[async_trait]
impl ImageProvider for StaticImages {
    async fn prepared(&self, image_ref: &str) -> Result<PreparedImage, ImageError> {
        let img = self
            .0
            .iter()
            .find(|i| i.image_ref == image_ref)
            .ok_or_else(|| ImageError::NotReady(format!("no prepared image for {image_ref}")))?;
        let meta = std::fs::metadata(&img.rootfs)
            .map_err(|e| ImageError::Failed(format!("{}: {e}", img.rootfs.display())))?;
        use sha2::{Digest, Sha256};
        let key = hex::encode(Sha256::digest(
            format!("static:{image_ref}:{}", img.rootfs.display()).as_bytes(),
        ));
        Ok(PreparedImage {
            key,
            image_ref: image_ref.to_owned(),
            manifest_digest: "static".into(),
            rootfs: img.rootfs.clone(),
            rootfs_bytes: meta.len(),
        })
    }
}
