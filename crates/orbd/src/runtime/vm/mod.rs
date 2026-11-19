//! What the VM runtimes (QEMU, Firecracker) share: durable file operations, checkpoint
//! directories, VMM process supervision, networking, the guest channel and root disk
//! images. Each runtime keeps its own launch configuration and the state machine that
//! interprets its VMM.

pub mod checkpoints;
pub mod guest;
pub mod images;
pub mod net;

pub use orbd_vm::fsutil::{self, create_private_dir};
pub use orbd_vm::process;

use super::{LogStream, OpContext, RuntimeError};
use crate::model::{InitialDiskSource, OrbId, ProjectSnapshotKey};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Memory saves at once on this host, across runtimes (disk bandwidth is the
/// bottleneck).
pub static SAVES: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// Base disks (prepared images, project snapshots) in use: a disk is created from a
/// base under a shared guard held until the orb's recipe names the base, and a project
/// snapshot is published under one; the agent's garbage collection holds it
/// exclusively while it decides which bases no orb is built on and deletes them.
pub static BASES: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

/// Name of the file whose modification time records a base's last use.
const LAST_USED: &str = "last_used";

/// Records that a base disk directory (`images/<key>`, `projects/<key>`) was used now.
pub fn mark_used(dir: &Path) {
    let r = std::fs::File::create(dir.join(LAST_USED))
        .and_then(|f| f.set_modified(std::time::SystemTime::now()));
    if let Err(e) = r {
        tracing::warn!(dir = %dir.display(), error = %e, "recording a base's use");
    }
}

/// When a base disk directory was last used: published, or used since.
pub fn last_used(dir: &Path) -> std::io::Result<std::time::SystemTime> {
    let published = std::fs::metadata(dir)?.modified()?;
    Ok(
        match std::fs::metadata(dir.join(LAST_USED)).and_then(|m| m.modified()) {
            Ok(used) => used.max(published),
            Err(_) => published,
        },
    )
}

/// Runs a host tool with fixed arguments, bounded by the context; killed if cancelled
/// or late.
pub async fn run_tool(
    ctx: &OpContext,
    program: &Path,
    args: &[String],
) -> Result<(), RuntimeError> {
    let child = tokio::process::Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output();
    let out = bounded(ctx, child)
        .await?
        .map_err(|e| RuntimeError::Other(format!("{}: {e}", program.display())))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr);
        let msg = format!("{} {}: {}", program.display(), args[0], err.trim());
        return Err(if err.contains("No space left") {
            RuntimeError::InsufficientDisk(msg)
        } else {
            RuntimeError::Other(msg)
        });
    }
    Ok(())
}

/// `program` itself if it has a directory part, else its first match on PATH.
pub fn on_path(program: &Path) -> Option<PathBuf> {
    if program.components().count() > 1 {
        return Some(program.to_owned());
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|d| d.join(program))
        .find(|p| p.is_file())
}

/// Stable per-orb systemd machine id (project-snapshot clones get their own).
pub fn machine_id(orb: OrbId) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(&Sha256::digest(format!("orbd-machine-id:{orb}").as_bytes())[..16])
}

/// Last lines of a log, for error messages.
pub fn log_tail(path: &Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let lines: Vec<&str> = text.lines().rev().take(5).collect();
    lines.into_iter().rev().collect::<Vec<_>>().join(" | ")
}

pub fn io_err(e: std::io::Error) -> RuntimeError {
    if e.raw_os_error() == Some(nix::libc::ENOSPC) {
        RuntimeError::InsufficientDisk(e.to_string())
    } else {
        RuntimeError::Other(e.to_string())
    }
}

/// Streams a progress line to the operation log. Advisory: dropped if the sink is full.
pub fn log(ctx: &OpContext, line: &str) {
    tracing::debug!(operation = %ctx.operation_id, "{line}");
    let _ = ctx
        .log
        .try_send((LogStream::Stdout, bytes::Bytes::from(format!("{line}\n"))));
}

/// Runs `fut` until it finishes, `ctx.cancel` fires or the work deadline passes.
pub async fn bounded<F: Future>(ctx: &OpContext, fut: F) -> Result<F::Output, RuntimeError> {
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(RuntimeError::Cancelled),
        _ = tokio::time::sleep_until(ctx.work_deadline.into()) => Err(RuntimeError::DeadlineExceeded),
        out = fut => Ok(out),
    }
}

/// One lifecycle lock per orb: a runtime runs one method per orb at a time.
#[derive(Default)]
pub struct OrbLocks(std::sync::Mutex<HashMap<OrbId, Arc<tokio::sync::Mutex<()>>>>);

impl OrbLocks {
    pub async fn lock(&self, orb: OrbId) -> tokio::sync::OwnedMutexGuard<()> {
        let m = self
            .0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(orb)
            .or_default()
            .clone();
        m.lock_owned().await
    }
}

/// Copies a raw disk image: a reflink where the filesystem supports it, else a sparse
/// copy.
pub async fn copy_disk(ctx: &OpContext, from: &Path, to: &Path) -> Result<(), RuntimeError> {
    run_tool(
        ctx,
        Path::new("cp"),
        &[
            "--reflink=auto".into(),
            "--sparse=always".into(),
            from.display().to_string(),
            to.display().to_string(),
        ],
    )
    .await
}

/// Shrinks a raw ext4 disk to the blocks its files use, so a snapshot of a big orb fits
/// any offering; the guest grows its root filesystem to the orb's disk at boot.
async fn shrink_ext4(ctx: &OpContext, raw: &Path) -> Result<(), RuntimeError> {
    let path = raw.display().to_string();
    // resize2fs wants a freshly checked filesystem. Exit 1 means e2fsck fixed something,
    // such as replaying the journal of this crash-consistent copy.
    let fsck = bounded(
        ctx,
        tokio::process::Command::new("e2fsck")
            .args(["-f", "-y", &path])
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await?
    .map_err(|e| RuntimeError::Other(format!("e2fsck: {e}")))?;
    if !matches!(fsck.status.code(), Some(0 | 1)) {
        return Err(RuntimeError::Other(format!(
            "e2fsck: {}",
            String::from_utf8_lossy(&fsck.stdout).trim()
        )));
    }
    run_tool(ctx, Path::new("resize2fs"), &["-M".into(), path.clone()]).await?;
    let out = bounded(
        ctx,
        tokio::process::Command::new("dumpe2fs")
            .args(["-h", &path])
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await?
    .map_err(|e| RuntimeError::Other(format!("dumpe2fs: {e}")))?;
    let header = String::from_utf8_lossy(&out.stdout);
    let field = |name: &str| {
        header
            .lines()
            .find_map(|l| l.strip_prefix(name)?.trim().parse::<u64>().ok())
            .ok_or_else(|| RuntimeError::Other(format!("dumpe2fs: no {name:?} in {path}")))
    };
    let bytes = field("Block count:")? * field("Block size:")?;
    std::fs::OpenOptions::new()
        .write(true)
        .open(raw)
        .and_then(|f| f.set_len(bytes))
        .map_err(io_err)
}

/// Whether the project snapshot `key` is already published; if so, records its use
/// under [`BASES`], so garbage collection cannot delete it as it is named again.
pub async fn reuse_project(
    ctx: &OpContext,
    state_dir: &Path,
    key: &ProjectSnapshotKey,
) -> Result<bool, RuntimeError> {
    let dir = state_dir.join("projects").join(key.as_str());
    let _bases = bounded(ctx, BASES.read()).await?;
    if !dir.join("disk.raw").is_file() {
        return Ok(false);
    }
    mark_used(&dir);
    Ok(true)
}

/// The image lineage ([`crate::image::lineage`]) project snapshot `key` descends from,
/// as recorded at publication; None for snapshots published by older orbd.
pub fn project_lineage(state_dir: &Path, key: &ProjectSnapshotKey) -> Option<String> {
    let meta = std::fs::read(
        state_dir
            .join("projects")
            .join(key.as_str())
            .join("meta.json"),
    );
    let meta: serde_json::Value = serde_json::from_slice(&meta.ok()?).ok()?;
    meta.get("lineage")?.as_str().map(str::to_owned)
}

/// Where a new disk really comes from: a project snapshot that descends from another
/// lineage than the spawn image's current one (an older Debian or guest integration, or
/// unknown) is passed over for that image. Project snapshots copy forward from orb to
/// orb, so without this a project would keep its first image for good. The snapshot is
/// used as before while the image is not prepared or its lineage is unknown.
pub async fn initial_source(
    images: &dyn images::ImageProvider,
    state_dir: &Path,
    src: InitialDiskSource,
    ctx: &OpContext,
) -> InitialDiskSource {
    let InitialDiskSource::ProjectSnapshot {
        snapshot,
        image_ref,
    } = src
    else {
        return src;
    };
    // A snapshot that is not here is the runtime's NotFound (and the executor's fallback).
    let here = state_dir
        .join("projects")
        .join(snapshot.key.as_str())
        .join("disk.raw");
    let current = if here.is_file() {
        images
            .prepared(&image_ref)
            .await
            .ok()
            .and_then(|i| i.lineage)
    } else {
        None
    };
    let built_on = project_lineage(state_dir, &snapshot.key);
    let Some(current) = current.filter(|c| built_on.as_ref() != Some(c)) else {
        return InitialDiskSource::ProjectSnapshot {
            snapshot,
            image_ref,
        };
    };
    let why = format!(
        "project snapshot {} was built on another image lineage ({}); creating the disk from {image_ref}",
        snapshot.key,
        match built_on {
            Some(_) => "another Debian release or guest integration",
            None => "not recorded",
        }
    );
    tracing::info!(current, "{why}");
    log(ctx, &why);
    InitialDiskSource::Image { image_ref }
}

/// Publishes the project snapshot `<state>/projects/<key>/{disk.raw, meta.json}`:
/// `write_raw` fills a standalone raw disk in staging, which is shrunk to its used size,
/// fsynced and renamed into place. The format is the same for every VM runtime.
/// `lineage` is the source orb's (see [`project_lineage`]).
pub async fn publish_project<F: Future<Output = Result<(), RuntimeError>>>(
    ctx: &OpContext,
    state_dir: &Path,
    key: &ProjectSnapshotKey,
    source_orb: OrbId,
    lineage: Option<String>,
    write_raw: impl FnOnce(PathBuf) -> F,
) -> Result<(), RuntimeError> {
    use fsutil::{fsync_dir, remove_via_trash, rename_durable, write_json_atomic};
    let trash = state_dir.join("trash");
    let staging = state_dir
        .join("staging")
        .join(format!("project-{key}-{}", uuid::Uuid::new_v4().simple()));
    let built = async {
        std::fs::create_dir_all(&staging).map_err(io_err)?;
        let raw = staging.join("disk.raw");
        write_raw(raw.clone()).await?;
        shrink_ext4(ctx, &raw).await?;
        std::fs::File::open(&raw)
            .and_then(|f| f.sync_all())
            .map_err(io_err)?;
        let meta = serde_json::json!({
            "key": key,
            "source_orb": source_orb,
            "lineage": lineage,
            "created_unix_ms": crate::model::unix_ms_now(),
            "bytes": std::fs::metadata(&raw).map(|m| m.len()).unwrap_or(0),
        });
        write_json_atomic(&staging.join("meta.json"), &meta).map_err(io_err)?;
        fsync_dir(&staging).map_err(io_err)
    }
    .await;
    if let Err(e) = built {
        let _ = remove_via_trash(&staging, &trash);
        return Err(e);
    }
    let dest = state_dir.join("projects").join(key.as_str());
    let _bases = match bounded(ctx, BASES.read()).await {
        Ok(g) => g,
        Err(e) => {
            let _ = remove_via_trash(&staging, &trash);
            return Err(e);
        }
    };
    if dest.exists() {
        remove_via_trash(&dest, &trash).map_err(io_err)?;
    }
    rename_durable(&staging, &dest).map_err(io_err)
}

/// Hex sha256 of a file, as a runtime error.
pub fn file_digest(path: &Path, what: &str) -> Result<String, RuntimeError> {
    fsutil::sha256_file(path)
        .map_err(|e| RuntimeError::Other(format!("{what} {}: {e}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::image::{ImageError, PreparedImage};
    use crate::model::ProjectSnapshotRef;

    /// Serves one image of the given lineage, or nothing (`None`: not prepared).
    struct Fake(Option<Option<&'static str>>);

    #[async_trait::async_trait]
    impl images::ImageProvider for Fake {
        async fn prepared(&self, image_ref: &str) -> Result<PreparedImage, ImageError> {
            let lineage = self
                .0
                .ok_or_else(|| ImageError::NotReady("preparing".into()))?;
            Ok(PreparedImage {
                key: "k".into(),
                image_ref: image_ref.into(),
                manifest_digest: "sha256:m".into(),
                rootfs: "/nonexistent".into(),
                rootfs_bytes: 0,
                lineage: lineage.map(str::to_owned),
            })
        }
    }

    /// A project snapshot of another (or an unrecorded) lineage than the spawn image's
    /// gives way to the image; one of the same lineage is used, and so is any snapshot
    /// while the image's lineage cannot be known.
    #[tokio::test]
    async fn a_snapshot_of_another_lineage_gives_way_to_the_image() {
        let state = tempfile::tempdir().unwrap();
        let key = ProjectSnapshotKey::from_external("project-1");
        let dir = state.path().join("projects").join(key.as_str());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("disk.raw"), b"").unwrap();
        let (log, _rx) = tokio::sync::mpsc::channel(16);
        let now = std::time::Instant::now();
        let ctx = OpContext {
            operation_id: crate::model::OperationId::generate(),
            generation: 1,
            work_deadline: now,
            cleanup_deadline: now,
            cancel: Default::default(),
            log,
        };
        let snapshot = || InitialDiskSource::ProjectSnapshot {
            snapshot: ProjectSnapshotRef { key: key.clone() },
            image_ref: "amp-orb:latest".into(),
        };
        let uses_snapshot = |images: Fake, meta: serde_json::Value| {
            std::fs::write(dir.join("meta.json"), meta.to_string()).unwrap();
            let (state, ctx) = (state.path().to_owned(), ctx.clone());
            async move {
                match initial_source(&images, &state, snapshot(), &ctx).await {
                    InitialDiskSource::ProjectSnapshot { .. } => true,
                    InitialDiskSource::Image { image_ref } => {
                        assert_eq!(image_ref, "amp-orb:latest");
                        false
                    }
                }
            }
        };
        let same = serde_json::json!({ "lineage": "trixie" });
        let older = serde_json::json!({ "lineage": "bookworm" });
        let unrecorded = serde_json::json!({ "source_orb": "x" });
        assert!(uses_snapshot(Fake(Some(Some("trixie"))), same.clone()).await);
        assert!(!uses_snapshot(Fake(Some(Some("trixie"))), older.clone()).await);
        assert!(!uses_snapshot(Fake(Some(Some("trixie"))), unrecorded).await);
        assert!(
            uses_snapshot(Fake(None), older.clone()).await,
            "image not prepared"
        );
        assert!(
            uses_snapshot(Fake(Some(None)), older.clone()).await,
            "lineage unknown"
        );
        // Not on this host: left to the runtime (NotFound) and the executor's fallback.
        std::fs::remove_file(dir.join("disk.raw")).unwrap();
        assert!(
            uses_snapshot(Fake(Some(Some("trixie"))), older).await,
            "missing"
        );
        let image = InitialDiskSource::Image {
            image_ref: "amp-orb:latest".into(),
        };
        assert_eq!(
            initial_source(
                &Fake(Some(Some("trixie"))),
                state.path(),
                image.clone(),
                &ctx
            )
            .await,
            image
        );
    }
}
