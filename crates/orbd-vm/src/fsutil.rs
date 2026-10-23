//! Durable file operations for publication boundaries: every publish is
//! write → fsync → rename → fsync(parent).

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io::Write;
use std::path::Path;

/// `mkdir -p` with mode 0700 for the directories it creates.
pub fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

pub fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

fn parent(path: &Path) -> &Path {
    path.parent().unwrap_or(Path::new("."))
}

pub fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let tmp = parent(path).join(format!(".{name}.tmp"));
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path)?;
    fsync_dir(parent(path))
}

pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let json = serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?;
    write_atomic(path, &json)
}

/// `Ok(None)` when the file does not exist.
pub fn read_json<T: DeserializeOwned>(path: &Path) -> std::io::Result<Option<T>> {
    match std::fs::read(path) {
        Ok(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(std::io::Error::other),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Atomically renames `from` to `to` and makes both directory entries durable.
pub fn rename_durable(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::rename(from, to)?;
    fsync_dir(parent(to))?;
    if parent(from) != parent(to) {
        fsync_dir(parent(from))?;
    }
    Ok(())
}

/// Removes a file or directory tree by first renaming it into `trash` (one durable
/// step, so a crash never leaves a half-deleted artifact in place), then deleting it.
/// Missing paths are fine.
pub fn remove_via_trash(path: &Path, trash: &Path) -> std::io::Result<()> {
    if std::fs::symlink_metadata(path).is_err() {
        return Ok(());
    }
    std::fs::create_dir_all(trash)?;
    let name = format!(
        "{}-{}",
        path.file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        uuid::Uuid::new_v4().simple()
    );
    let dest = trash.join(name);
    rename_durable(path, &dest)?;
    if dest.is_dir() {
        std::fs::remove_dir_all(&dest)
    } else {
        std::fs::remove_file(&dest)
    }
}

/// Hex sha256 of a file's contents.
pub fn sha256_file(path: &Path) -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            return Ok(hex::encode(h.finalize()));
        }
        h.update(&buf[..n]);
    }
}

/// Bytes available to unprivileged writers on the filesystem holding `path`.
pub fn free_bytes(path: &Path) -> std::io::Result<u64> {
    let s = nix::sys::statvfs::statvfs(path).map_err(std::io::Error::from)?;
    Ok(s.blocks_available() as u64 * s.fragment_size() as u64)
}
