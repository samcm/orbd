//! Registry side: resolve a reference to its linux/amd64 manifest and stream blobs into
//! a content-addressed cache. Uses oci-client's per-blob streaming API (`pull_blob` into
//! a file), never its whole-image `pull`, which buffers layers in memory.

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

pub struct Resolved {
    pub reference: Reference,
    /// Digest of the linux/amd64 image manifest (not of an index).
    pub manifest_digest: String,
    pub manifest_raw: Vec<u8>,
    pub manifest: OciImageManifest,
}

impl Resolved {
    pub fn blobs(&self) -> impl Iterator<Item = &OciDescriptor> {
        std::iter::once(&self.manifest.config).chain(self.manifest.layers.iter())
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
        platform_resolver: Some(Box::new(oci_client::client::linux_amd64_resolver)),
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

/// Resolves `image_ref` (tag or digest, index or manifest) to the linux/amd64 manifest.
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
                p["os"] == "linux"
                    && p["architecture"] == "amd64"
                    && p.get("variant").is_none_or(|v| v == "v1")
            })
            .and_then(|m| m["digest"].as_str())
            .ok_or_else(|| format!("{image_ref} has no linux/amd64 manifest"))?
            .to_owned();
        let child = Reference::with_digest(
            reference.registry().to_owned(),
            reference.repository().to_owned(),
            pick.clone(),
        );
        let (raw, _) = client
            .pull_manifest_raw(&child, &auth, &[OCI_IMAGE_MEDIA_TYPE])
            .await
            .map_err(|e| format!("fetching linux/amd64 manifest of {image_ref}: {e}"))?;
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
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            // Reused: garbage collection keeps blobs builds still use.
            if let Ok(f) = std::fs::File::options().append(true).open(&path) {
                let _ = f.set_modified(std::time::SystemTime::now());
            }
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

    /// Hard-links (or copies across filesystems) a cached blob into an OCI layout.
    pub fn link_into(&self, digest: &str, layout: &Path) -> Result<(), String> {
        let dst = layout.join("blobs/sha256").join(hex_of(digest)?);
        let src = self.path(digest)?;
        std::fs::hard_link(&src, &dst)
            .or_else(|_| std::fs::copy(&src, &dst).map(|_| ()))
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
