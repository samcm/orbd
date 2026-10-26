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
use crate::model::{OrbId, ProjectSnapshotKey};
use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Memory saves at once on this host, across runtimes (disk bandwidth is the
/// bottleneck).
pub static SAVES: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

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

/// Publishes the project snapshot `<state>/projects/<key>/{disk.raw, meta.json}`:
/// `write_raw` fills a standalone raw disk in staging, which is shrunk to its used size,
/// fsynced and renamed into place. The format is the same for every VM runtime.
pub async fn publish_project<F: Future<Output = Result<(), RuntimeError>>>(
    ctx: &OpContext,
    state_dir: &Path,
    key: &ProjectSnapshotKey,
    source_orb: OrbId,
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
