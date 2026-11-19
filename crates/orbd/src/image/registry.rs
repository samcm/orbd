//! Registry side: resolve a reference to its manifest for this host's architecture
//! ([`IMAGE_ARCH`]: linux/amd64, or linux/arm64 on Apple Silicon) and stream blobs into
//! a content-addressed cache. Uses oci-client's per-blob streaming API (`pull_blob` into
//! a file), never its whole-image `pull`, which buffers layers in memory.

use crate::model::Arch;
use futures::{StreamExt, TryStreamExt};
use oci_client::client::{ClientConfig, ClientProtocol};
use oci_client::manifest::{
    IMAGE_MANIFEST_LIST_MEDIA_TYPE, OCI_IMAGE_INDEX_MEDIA_TYPE, OCI_IMAGE_MEDIA_TYPE,
    OciDescriptor, OciImageManifest,
};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

const PARALLEL_BLOBS: usize = 4;

/// The OCI architecture of the images this agent prepares: its own, since orbs run
/// natively (KVM, or Hypervisor.framework on Apple Silicon).
pub const IMAGE_ARCH: &str = Arch::HOST.as_str();

/// Whether an index entry's platform is what this agent runs: `linux/amd64` or
/// `linux/arm64`, with no variant or the baseline one.
fn is_native(os: &str, arch: &str, variant: Option<&str>) -> bool {
    let baseline = match Arch::HOST {
        Arch::Amd64 => "v1",
        Arch::Arm64 => "v8",
    };
    os == "linux" && arch == IMAGE_ARCH && variant.is_none_or(|v| v == baseline)
}

fn linux_native_resolver(manifests: &[oci_client::manifest::ImageIndexEntry]) -> Option<String> {
    manifests
        .iter()
        .find(|e| {
            e.platform.as_ref().is_some_and(|p| {
                is_native(
                    &p.os.to_string(),
                    &p.architecture.to_string(),
                    p.variant.as_deref(),
                )
            })
        })
        .map(|e| e.digest.clone())
}

pub struct Resolved {
    pub reference: Reference,
    /// Digest of the linux/[`IMAGE_ARCH`] image manifest (not of an index).
    pub manifest_digest: String,
    pub manifest_raw: Vec<u8>,
    pub manifest: OciImageManifest,
}

impl Resolved {
    /// The config and layer blobs, each once: a manifest may list the same layer more
    /// than once (Amp's image repeats an empty layer).
    pub fn blobs(&self) -> impl Iterator<Item = &OciDescriptor> {
        let mut seen = std::collections::HashSet::new();
        std::iter::once(&self.manifest.config)
            .chain(self.manifest.layers.iter())
            .filter(move |d| seen.insert(d.digest.as_str()))
    }

    pub fn compressed_bytes(&self) -> u64 {
        self.blobs().map(|d| d.size.max(0) as u64).sum()
    }
}

pub fn client() -> Client {
    // reqwest is built without a bundled provider; use ring like the rest of orbd.
    let _ = rustls::crypto::ring::default_provider().install_default();
    Client::new(ClientConfig {
        protocol: ClientProtocol::Https,
        platform_resolver: Some(Box::new(linux_native_resolver)),
        ..Default::default()
    })
}

fn sha256(data: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(data)))
}

fn hex_of(digest: &str) -> Result<&str, String> {
    match digest.strip_prefix("sha256:") {
        Some(h) if h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit()) => Ok(h),
        _ => Err(format!("unsupported digest {digest}")),
    }
}

/// Resolves `image_ref` (tag or digest, index or manifest) to the linux/[`IMAGE_ARCH`]
/// manifest.
pub async fn resolve(client: &Client, image_ref: &str) -> Result<Resolved, String> {
    let reference: Reference = image_ref
        .parse()
        .map_err(|e| format!("bad image reference {image_ref}: {e}"))?;
    let accepted = [
        OCI_IMAGE_INDEX_MEDIA_TYPE,
        OCI_IMAGE_MEDIA_TYPE,
        IMAGE_MANIFEST_LIST_MEDIA_TYPE,
    ];
    let auth = RegistryAuth::Anonymous;
    let (raw, digest) = client
        .pull_manifest_raw(&reference, &auth, &accepted)
        .await
        .map_err(|e| format!("fetching manifest for {image_ref}: {e}"))?;
    let value: serde_json::Value =
        serde_json::from_slice(&raw).map_err(|e| format!("manifest of {image_ref}: {e}"))?;
    let (raw, digest) = if let Some(manifests) = value.get("manifests").and_then(|m| m.as_array()) {
        let pick = manifests
            .iter()
            .find(|m| {
                let p = &m["platform"];
                let s = |k: &str| p[k].as_str().unwrap_or_default();
                is_native(s("os"), s("architecture"), p["variant"].as_str())
            })
            .and_then(|m| m["digest"].as_str())
            .ok_or_else(|| format!("{image_ref} has no linux/{IMAGE_ARCH} manifest"))?
            .to_owned();
        let child = Reference::with_digest(
            reference.registry().to_owned(),
            reference.repository().to_owned(),
            pick.clone(),
        );
        let (raw, _) = client
            .pull_manifest_raw(&child, &auth, &[OCI_IMAGE_MEDIA_TYPE])
            .await
            .map_err(|e| format!("fetching linux/{IMAGE_ARCH} manifest of {image_ref}: {e}"))?;
        (raw, pick)
    } else {
        (raw, digest)
    };
    hex_of(&digest)?;
    if sha256(&raw) != digest {
        return Err(format!(
            "manifest of {image_ref} does not match digest {digest}"
        ));
    }
    let manifest: OciImageManifest =
        serde_json::from_slice(&raw).map_err(|e| format!("manifest of {image_ref}: {e}"))?;
    let oci = |t: &str| t.starts_with("application/vnd.oci.image.");
    if !manifest.media_type.as_deref().is_none_or(oci)
        || !oci(&manifest.config.media_type)
        || !manifest.layers.iter().all(|l| oci(&l.media_type))
    {
        return Err(format!(
            "{image_ref}: only OCI media types are supported (not Docker schema 2)"
        ));
    }
    Ok(Resolved {
        reference,
        manifest_digest: digest,
        manifest_raw: raw.to_vec(),
        manifest,
    })
}

/// Content-addressed blob cache shared by all preparations (`images/blobs/sha256/`).
pub struct BlobCache {
    dir: PathBuf,
}

impl BlobCache {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn path(&self, digest: &str) -> Result<PathBuf, String> {
        Ok(self.dir.join(hex_of(digest)?))
    }

    /// Downloads every blob of `resolved` that is not cached yet; `progress` counts bytes.
    pub async fn fetch_all(
        &self,
        client: &Client,
        resolved: &Resolved,
        progress: Arc<AtomicU64>,
    ) -> Result<(), String> {
        tokio::fs::create_dir_all(&self.dir)
            .await
            .map_err(|e| format!("{}: {e}", self.dir.display()))?;
        futures::stream::iter(resolved.blobs().cloned())
            .map(|desc| {
                let progress = progress.clone();
                async move {
                    self.fetch(client, &resolved.reference, &desc, progress)
                        .await
                }
            })
            .buffer_unordered(PARALLEL_BLOBS)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }

    async fn fetch(
        &self,
        client: &Client,
        reference: &Reference,
        desc: &OciDescriptor,
        progress: Arc<AtomicU64>,
    ) -> Result<(), String> {
        let path = self.path(&desc.digest)?;
        if self.cached(desc)? {
            progress.fetch_add(desc.size.max(0) as u64, Ordering::Relaxed);
            return Ok(());
        }
        let tmp = path.with_extension(format!("part-{}", uuid::Uuid::new_v4().simple()));
        let result = async {
            let file = tokio::fs::File::create(&tmp)
                .await
                .map_err(|e| format!("{}: {e}", tmp.display()))?;
            let mut out = Counting {
                inner: tokio::io::BufWriter::with_capacity(1 << 20, file),
                progress: progress.clone(),
            };
            // Verifies the content against `desc.digest` while streaming.
            client
                .pull_blob(reference, desc, &mut out)
                .await
                .map_err(|e| format!("blob {}: {e}", desc.digest))?;
            let file = out.inner.into_inner();
            file.sync_all().await.map_err(|e| e.to_string())?;
            let len = file.metadata().await.map_err(|e| e.to_string())?.len();
            if desc.size >= 0 && len != desc.size as u64 {
                return Err(format!(
                    "blob {}: got {len} bytes, expected {}",
                    desc.digest, desc.size
                ));
            }
            tokio::fs::rename(&tmp, &path)
                .await
                .map_err(|e| format!("{}: {e}", path.display()))
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        result
    }

    /// Whether `desc` is in the cache with its full size; if so, marks it used so garbage
    /// collection keeps it. A wrong-sized entry is reported as missing, and the download
    /// that follows renames a good copy over it: older orbd emptied the cached blob of a
    /// layer that a manifest lists twice.
    fn cached(&self, desc: &OciDescriptor) -> Result<bool, String> {
        let path = self.path(&desc.digest)?;
        let Ok(meta) = std::fs::metadata(&path) else {
            return Ok(false);
        };
        if desc.size >= 0 && meta.len() != desc.size as u64 {
            tracing::warn!(
                digest = %desc.digest,
                cached = meta.len(),
                expected = desc.size,
                "cached blob has the wrong size; downloading it again"
            );
            return Ok(false);
        }
        if let Ok(f) = std::fs::File::options().append(true).open(&path) {
            let _ = f.set_modified(std::time::SystemTime::now());
        }
        Ok(true)
    }

    /// Hard-links (or copies across filesystems) a cached blob into an OCI layout. A blob
    /// already in the layout is left alone: copying onto it would truncate the hard-linked
    /// cache entry.
    pub fn link_into(&self, digest: &str, layout: &Path) -> Result<(), String> {
        let dst = layout.join("blobs/sha256").join(hex_of(digest)?);
        let src = self.path(digest)?;
        match std::fs::hard_link(&src, &dst) {
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            r => r.or_else(|_| std::fs::copy(&src, &dst).map(|_| ())),
        }
        .map_err(|e| format!("{} -> {}: {e}", src.display(), dst.display()))
    }
}

/// Counts bytes written through it (download progress).
struct Counting<W> {
    inner: W,
    progress: Arc<AtomicU64>,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Counting<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &r {
            self.progress.fetch_add(*n as u64, Ordering::Relaxed);
        }
        r
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cache entry shorter than its descriptor (as older orbd left the blob of a
    /// repeated layer) is downloaded again instead of reused.
    #[test]
    fn only_a_full_sized_cached_blob_is_reused() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = BlobCache::new(tmp.path().to_path_buf());
        let desc = OciDescriptor {
            digest: format!("sha256:{}", "b".repeat(64)),
            size: 34,
            ..Default::default()
        };
        assert!(!cache.cached(&desc).unwrap(), "missing");
        std::fs::write(cache.path(&desc.digest).unwrap(), b"").unwrap();
        assert!(!cache.cached(&desc).unwrap(), "emptied");
        std::fs::write(cache.path(&desc.digest).unwrap(), [0u8; 34]).unwrap();
        assert!(cache.cached(&desc).unwrap(), "intact");
    }
}
