//! Image preparation for VM runtimes: Amp's OCI image → a bootable ext4 root disk with
//! orbd's guest integration installed.
//!
//! Pipeline (never mounts or executes untrusted guest content on the host):
//! resolve `ORB_IMAGE` to its linux/amd64 manifest digest → stream blobs into a shared
//! blob cache (oci-client, one blob at a time into a file) → OCI layout → append the
//! guest integration layer and unpack as root with a pinned `umoci` → `mkfs.ext4 -d`
//! into a sparse raw file → publish atomically under `images/<cache key>/`.
//!
//! Cache key: platform manifest digest + recipe version + guest integration digest.
//! A cache miss starts preparation in the background and the caller fails with
//! `IMAGE_NOT_READY`; [`ImageStore::open`] prewarms configured images. A tag is resolved
//! again every `refresh_interval_secs`; the image prepared for its previous digest keeps
//! serving until the new one is published.
//!
//! On disk (`<state_dir>`):
//! `images/blobs/sha256/<hex>` (blob cache), `images/refs/<sha256(ref)>.json` (last
//! resolution of each reference), `images/<key>/{rootfs.ext4,image.json}` (published,
//! immutable), `staging/image-<key>-<nonce>/` (in progress; removed at startup).

mod integration;
mod registry;
mod rootfs;

pub use integration::IntegrationLayer;
pub use rootfs::UMOCI_VERSION;

use crate::config::agent::ImagesConfig;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Bump when the conversion changes in a way that must produce new images.
pub const RECIPE_VERSION: u32 = 1;
const RETRY_AFTER_FAILURE: Duration = Duration::from_secs(60);
const ROOTFS_FILE: &str = "rootfs.ext4";
const META_FILE: &str = "image.json";

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

/// `images/<key>/image.json`.
#[derive(Debug, Serialize, Deserialize)]
struct ImageMeta {
    key: String,
    image_ref: String,
    manifest_digest: String,
    recipe_version: u32,
    guest_integration_digest: String,
    rootfs_bytes: u64,
    created_unix: u64,
}

/// `images/refs/<sha256(ref)>.json`: what a reference last resolved to.
#[derive(Debug, Serialize, Deserialize)]
struct RefRecord {
    image_ref: String,
    manifest_digest: String,
    resolved_unix: u64,
}

enum Job {
    Running {
        status: Arc<Mutex<String>>,
        started: Instant,
    },
    Failed {
        error: String,
        at: Instant,
    },
}

struct Inner {
    config: ImagesConfig,
    images: PathBuf,
    staging: PathBuf,
    blobs: registry::BlobCache,
    integration: IntegrationLayer,
    client: oci_client::Client,
    runtime: tokio::runtime::Handle,
    /// Background preparations by image reference.
    jobs: Mutex<HashMap<String, Job>>,
    /// Serializes builds of the same cache key (two refs, one digest).
    building: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// Prepared root disks for VM runtimes. Cheap to clone.
#[derive(Clone)]
pub struct ImageStore {
    inner: Arc<Inner>,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> std::io::Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4().simple()));
    let f = std::fs::File::create(&tmp)?;
    serde_json::to_writer_pretty(&f, value)?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(path.parent().unwrap_or(Path::new(".")))?.sync_all()
}

impl ImageStore {
    /// Opens the store under `state_dir`, clears stale staging, and starts preparing
    /// `config.prewarm` in the background. Must be called inside a tokio runtime.
    pub fn open(config: &ImagesConfig, state_dir: &Path) -> Result<Self, ImageError> {
        let failed = |e: String| ImageError::Failed(e);
        let integration = IntegrationLayer::build(&config.guest_helper)
            .map_err(|e| failed(format!("guest integration: {e}")))?;
        let images = state_dir.join("images");
        let staging = state_dir.join("staging");
        for d in [
            images.join("refs"),
            images.join("blobs/sha256"),
            staging.clone(),
        ] {
            std::fs::create_dir_all(&d).map_err(|e| failed(format!("{}: {e}", d.display())))?;
        }
        let store = Self {
            inner: Arc::new(Inner {
                config: config.clone(),
                blobs: registry::BlobCache::new(images.join("blobs/sha256")),
                images,
                staging,
                integration,
                client: registry::client(),
                runtime: tokio::runtime::Handle::current(),
                jobs: Mutex::new(HashMap::new()),
                building: Mutex::new(HashMap::new()),
            }),
        };
        tracing::info!(
            guest_integration = %store.inner.integration.digest,
            "image store opened"
        );
        // Listed now, before any preparation of this process can create its own.
        let stale: Vec<PathBuf> = std::fs::read_dir(&store.inner.staging)
            .map_err(|e| failed(e.to_string()))?
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("image-"))
            .map(|e| e.path())
            .collect();
        let s = store.clone();
        store.inner.runtime.spawn(async move {
            for path in stale {
                if let Err(e) = rootfs::remove_tree(&path).await {
                    tracing::warn!("removing stale {}: {e}", path.display());
                }
            }
            for image_ref in s.inner.config.prewarm.clone() {
                if let Err(ImageError::Failed(e)) = s.get(&image_ref) {
                    tracing::warn!(image_ref, "prewarm: {e}");
                }
            }
        });
        Ok(store)
    }

    /// Digest of the guest integration layer (`sha256:...`) baked into new images.
    pub fn guest_integration_digest(&self) -> &str {
        &self.inner.integration.digest
    }

    /// The prepared image for `image_ref`, without waiting. On a miss, starts (or reports
    /// the progress of) background preparation and returns `NotReady`; after a failed
    /// attempt returns `Failed` until the retry delay has passed.
    pub fn get(&self, image_ref: &str) -> Result<PreparedImage, ImageError> {
        let record = self.read_ref(image_ref);
        let ready = record
            .as_ref()
            .and_then(|r| self.published(image_ref, &r.manifest_digest));
        let stale = record.as_ref().is_none_or(|r| {
            !image_ref.contains('@')
                && now_unix().saturating_sub(r.resolved_unix)
                    >= self.inner.config.refresh_interval_secs
        });
        if let Some(image) = ready {
            if stale {
                // Refresh the tag in the background; keep serving this image meanwhile.
                let _ = self.start(image_ref);
            }
            return Ok(image);
        }
        Err(self.start(image_ref))
    }

    /// Prepares `image_ref` and waits for it (tests and tools; the agent uses [`get`]).
    ///
    /// [`get`]: Self::get
    pub async fn prepare(&self, image_ref: &str) -> Result<PreparedImage, ImageError> {
        let status = Arc::new(Mutex::new(String::new()));
        self.prepare_inner(image_ref, &status)
            .await
            .map_err(ImageError::Failed)
    }

    /// Starts a background job for `image_ref` unless one is running or recently failed.
    fn start(&self, image_ref: &str) -> ImageError {
        let mut jobs = self.inner.jobs.lock().unwrap_or_else(|e| e.into_inner());
        match jobs.get(image_ref) {
            Some(Job::Running { status, started }) => {
                let s = status.lock().unwrap_or_else(|e| e.into_inner()).clone();
                return ImageError::NotReady(format!(
                    "preparing {image_ref} ({}s): {s}",
                    started.elapsed().as_secs()
                ));
            }
            Some(Job::Failed { error, at }) if at.elapsed() < RETRY_AFTER_FAILURE => {
                return ImageError::Failed(format!(
                    "{image_ref}: {error} (retrying in {}s)",
                    (RETRY_AFTER_FAILURE - at.elapsed()).as_secs()
                ));
            }
            _ => {}
        }
        let status = Arc::new(Mutex::new("resolving".to_owned()));
        jobs.insert(
            image_ref.to_owned(),
            Job::Running {
                status: status.clone(),
                started: Instant::now(),
            },
        );
        let msg = ImageError::NotReady(format!("preparing {image_ref}: started"));
        let (store, image_ref) = (self.clone(), image_ref.to_owned());
        self.inner.runtime.spawn(async move {
            let started = Instant::now();
            let result = store.prepare_inner(&image_ref, &status).await;
            let mut jobs = store.inner.jobs.lock().unwrap_or_else(|e| e.into_inner());
            match result {
                Ok(image) => {
                    tracing::info!(
                        image_ref,
                        key = %image.key,
                        manifest = %image.manifest_digest,
                        secs = started.elapsed().as_secs(),
                        "image ready"
                    );
                    jobs.remove(&image_ref);
                }
                Err(error) => {
                    tracing::warn!(image_ref, "image preparation failed: {error}");
                    jobs.insert(
                        image_ref,
                        Job::Failed {
                            error,
                            at: Instant::now(),
                        },
                    );
                }
            }
        });
        msg
    }

    fn cache_key(&self, manifest_digest: &str) -> String {
        sha256_hex(
            format!(
                "orbd-image\nrecipe={RECIPE_VERSION}\numoci={UMOCI_VERSION}\nmanifest={manifest_digest}\nguest={}\n",
                self.inner.integration.digest
            )
            .as_bytes(),
        )
    }

    fn ref_path(&self, image_ref: &str) -> PathBuf {
        self.inner
            .images
            .join("refs")
            .join(format!("{}.json", sha256_hex(image_ref.as_bytes())))
    }

    fn read_ref(&self, image_ref: &str) -> Option<RefRecord> {
        let r: RefRecord =
            serde_json::from_slice(&std::fs::read(self.ref_path(image_ref)).ok()?).ok()?;
        (r.image_ref == image_ref).then_some(r)
    }

    /// The published image for this digest (and the current guest integration), if any.
    fn published(&self, image_ref: &str, manifest_digest: &str) -> Option<PreparedImage> {
        let key = self.cache_key(manifest_digest);
        let dir = self.inner.images.join(&key);
        let meta: ImageMeta =
            serde_json::from_slice(&std::fs::read(dir.join(META_FILE)).ok()?).ok()?;
        Some(PreparedImage {
            key,
            image_ref: image_ref.to_owned(),
            manifest_digest: meta.manifest_digest,
            rootfs: dir.join(ROOTFS_FILE),
            rootfs_bytes: meta.rootfs_bytes,
        })
    }

    async fn prepare_inner(
        &self,
        image_ref: &str,
        status: &Arc<Mutex<String>>,
    ) -> Result<PreparedImage, String> {
        let set = |s: &str| *status.lock().unwrap_or_else(|e| e.into_inner()) = s.to_owned();
        set("resolving");
        let resolved = registry::resolve(&self.inner.client, image_ref).await?;
        let digest = resolved.manifest_digest.clone();
        let record = RefRecord {
            image_ref: image_ref.to_owned(),
            manifest_digest: digest.clone(),
            resolved_unix: now_unix(),
        };
        if let Some(image) = self.published(image_ref, &digest) {
            write_json_atomic(&self.ref_path(image_ref), &record).map_err(|e| e.to_string())?;
            return Ok(image);
        }
        let key = self.cache_key(&digest);
        let lock = self
            .inner
            .building
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key.clone())
            .or_default()
            .clone();
        let _guard = lock.lock().await;
        if self.published(image_ref, &digest).is_none() {
            self.build(&resolved, &key, &set).await?;
        }
        write_json_atomic(&self.ref_path(image_ref), &record).map_err(|e| e.to_string())?;
        self.published(image_ref, &digest)
            .ok_or_else(|| format!("image {key} vanished after publication"))
    }

    async fn build(
        &self,
        resolved: &registry::Resolved,
        key: &str,
        set: &(dyn Fn(&str) + Sync),
    ) -> Result<(), String> {
        let tools = rootfs::Tools {
            umoci: self.inner.config.umoci.clone(),
            mkfs_ext4: self.inner.config.mkfs_ext4.clone(),
        };
        rootfs::umoci_version(&tools.umoci).await?;
        let total = resolved.compressed_bytes();
        self.check_space(total)?;

        let progress = Arc::new(AtomicU64::new(0));
        let fetch = self
            .inner
            .blobs
            .fetch_all(&self.inner.client, resolved, progress.clone());
        tokio::pin!(fetch);
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                r = &mut fetch => { r?; break; }
                _ = tick.tick() => set(&format!(
                    "downloading {}/{} MiB",
                    progress.load(Ordering::Relaxed) >> 20,
                    total >> 20
                )),
            }
        }

        let work = self
            .inner
            .staging
            .join(format!("image-{key}-{}", uuid::Uuid::new_v4().simple()));
        let result = self.build_in(resolved, key, &tools, &work, set).await;
        if let Err(e) = rootfs::remove_tree(&work).await {
            tracing::warn!("removing {}: {e}", work.display());
        }
        result
    }

    async fn build_in(
        &self,
        resolved: &registry::Resolved,
        key: &str,
        tools: &rootfs::Tools,
        work: &Path,
        set: &(dyn Fn(&str) + Sync),
    ) -> Result<(), String> {
        let layout = work.join("oci");
        let out = work.join("out");
        std::fs::create_dir_all(&out).map_err(|e| e.to_string())?;
        rootfs::write_layout(&layout, &self.inner.blobs, resolved)?;
        let file = out.join(ROOTFS_FILE);
        let bytes =
            rootfs::build(tools, &layout, &self.inner.integration, work, &file, set).await?;
        set("publishing");
        let f = std::fs::File::open(&file).map_err(|e| e.to_string())?;
        f.sync_all().map_err(|e| e.to_string())?;
        std::fs::set_permissions(&file, std::os::unix::fs::PermissionsExt::from_mode(0o444))
            .map_err(|e| e.to_string())?;
        write_json_atomic(
            &out.join(META_FILE),
            &ImageMeta {
                key: key.to_owned(),
                image_ref: resolved.reference.whole(),
                manifest_digest: resolved.manifest_digest.clone(),
                recipe_version: RECIPE_VERSION,
                guest_integration_digest: self.inner.integration.digest.clone(),
                rootfs_bytes: bytes,
                created_unix: now_unix(),
            },
        )
        .map_err(|e| e.to_string())?;
        let target = self.inner.images.join(key);
        match std::fs::rename(&out, &target) {
            Ok(()) => {}
            // Published concurrently by another preparation: keep that one.
            Err(_) if target.join(META_FILE).exists() => return Ok(()),
            Err(e) => return Err(format!("publishing {}: {e}", target.display())),
        }
        std::fs::File::open(&self.inner.images)
            .and_then(|d| d.sync_all())
            .map_err(|e| e.to_string())
    }

    /// Unpacked rootfs + ext4 image + cached blobs need roughly 3x the compressed size.
    fn check_space(&self, compressed: u64) -> Result<(), String> {
        let st = nix::sys::statvfs::statvfs(&self.inner.staging).map_err(|e| e.to_string())?;
        let free = st.blocks_available() as u64 * st.fragment_size() as u64;
        let need = compressed * 6 + (2 << 30);
        if free < need {
            return Err(format!(
                "insufficient disk for image preparation: {} MiB free, need ~{} MiB",
                free >> 20,
                need >> 20
            ));
        }
        Ok(())
    }
}
