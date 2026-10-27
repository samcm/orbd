//! Garbage collection of base disks (VM runtimes). Amp never deletes a project snapshot:
//! it reuses one for at most 72 hours, then names a new `ORB_SNAPSHOT_ID`. Image tags
//! move on to new digests, and each orbd build bakes a new guest integration. So
//! snapshots and prepared images pile up unless the agent deletes them.
//!
//! A project snapshot or prepared image is deleted once no orb on this host is built on
//! it (its `vm.json` names it; QEMU disks read through to their base) and nothing has
//! used it for [`IDLE`]. A spawn that creates a disk from it counts as a use, and so
//! does publishing it. The current image of a prewarmed reference is kept. Collection
//! runs at startup, hourly and after every orb delete, under
//! [`crate::runtime::vm::BASES`], so no disk is created from a base while it runs.

use super::{Shared, db};
use crate::runtime::vm::{self, BASES};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// How long an unused base is kept: Amp names a project snapshot for at most 72 hours.
pub const IDLE: Duration = Duration::from_secs(72 * 3600);
const EVERY: Duration = Duration::from_secs(3600);

/// Collects now, then hourly and whenever an orb was deleted, until shutdown.
pub(super) async fn run_loop(sh: Arc<Shared>) {
    loop {
        collect(&sh).await;
        tokio::select! {
            _ = sh.shutdown.cancelled() => break,
            _ = tokio::time::sleep(EVERY) => {}
            _ = sh.orb_deleted.notified() => {}
        }
    }
}

async fn collect(sh: &Arc<Shared>) {
    let state = sh.config.agent.state_dir.clone();
    if !state.join("orbs").is_dir() {
        return; // No VM runtime has run here.
    }
    // Under the lock, doomed bases are only renamed into the trash; deleting them (some
    // GiB each) waits until disk creation may go on.
    let bases = BASES.write().await;
    let found = {
        let (state, images) = (state.clone(), sh.images.clone());
        tokio::task::spawn_blocking(move || {
            let in_use = orb_bases(&state)?;
            let projects = collect_projects(&state, &in_use, IDLE);
            let images = match &images {
                Some(store) => {
                    let keys = in_use
                        .iter()
                        .filter_map(|b| b.strip_prefix("images/"))
                        .map(str::to_owned)
                        .collect();
                    store.collect_garbage(&keys, IDLE)
                }
                None => Vec::new(),
            };
            Ok::<_, String>((projects, images))
        })
        .await
    };
    let (projects, images) = match found {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            tracing::warn!("garbage collection skipped: {e}");
            return;
        }
        Err(e) => {
            tracing::warn!("garbage collection failed: {e}");
            return;
        }
    };
    // Forgotten before a snapshot of the same key can be published again.
    for (key, _) in &projects {
        let key = key.clone();
        if let Err(e) = sh
            .db
            .call(move |c| db::forget_project_snapshot(c, &key))
            .await
        {
            tracing::warn!(error = %e, "forgetting a deleted project snapshot");
        }
    }
    drop(bases);

    let mut trashed: Vec<_> = projects
        .iter()
        .chain(&images)
        .map(|(_, t)| t.clone())
        .collect();
    if let Some(store) = sh.images.clone() {
        trashed.extend(
            tokio::task::spawn_blocking(move || store.collect_blobs(IDLE))
                .await
                .unwrap_or_default(),
        );
    }
    let removed = tokio::task::spawn_blocking(move || {
        for t in &trashed {
            if let Err(e) = vm::fsutil::remove(t) {
                tracing::warn!(path = %t.display(), error = %e, "deleting from the trash");
            }
        }
    })
    .await;
    if let Err(e) = removed {
        tracing::warn!("garbage collection failed: {e}");
    }
    if !projects.is_empty() || !images.is_empty() {
        let keys = |v: &[(String, PathBuf)]| v.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
        tracing::info!(
            projects = ?keys(&projects),
            images = ?keys(&images),
            "deleted project snapshots and images no orb is built on, unused for {}h",
            IDLE.as_secs() / 3600
        );
    }
}

/// Every base directory (`images/<key>`, `projects/<key>`) an orb in `<state>/orbs`
/// is built on. Fails, so nothing is deleted, if any orb's recipe cannot be read.
fn orb_bases(state: &Path) -> Result<HashSet<String>, String> {
    let dir = state.join("orbs");
    let entries = std::fs::read_dir(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let mut bases = HashSet::new();
    for e in entries {
        let e = e.map_err(|e| format!("{}: {e}", dir.display()))?;
        if crate::model::OrbId::parse(&e.file_name().to_string_lossy()).is_err() {
            continue;
        }
        let path = e.path().join("vm.json");
        let json = match std::fs::read(&path) {
            Ok(j) => j,
            // Deleted since the listing (orb deletes do not wait for this lock).
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && !e.path().exists() => {
                continue;
            }
            Err(err) => return Err(format!("{}: {err}", path.display())),
        };
        let found = crate::runtime::qemu::recipe_bases(&json)
            .or_else(|| crate::runtime::firecracker::recipe_bases(&json))
            .ok_or_else(|| format!("{}: not a recipe this agent knows", path.display()))?;
        bases.extend(found);
    }
    Ok(bases)
}

/// Moves into the trash every project snapshot not in `in_use` and unused for `idle`;
/// returns each one's key and path in the trash.
fn collect_projects(
    state: &Path,
    in_use: &HashSet<String>,
    idle: Duration,
) -> Vec<(String, PathBuf)> {
    let mut deleted = Vec::new();
    let dir = state.join("projects");
    for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
        let key = e.file_name().to_string_lossy().into_owned();
        let path = e.path();
        if in_use.contains(&format!("projects/{key}")) {
            continue;
        }
        match vm::last_used(&path) {
            Ok(t) if t.elapsed().is_ok_and(|e| e >= idle) => {}
            Ok(_) => continue,
            Err(e) => {
                tracing::warn!(key, error = %e, "garbage collection: no use time; keeping it");
                continue;
            }
        }
        match vm::fsutil::move_to_trash(&path, &state.join("trash")) {
            Ok(Some(t)) => deleted.push((key, t)),
            Ok(None) => {}
            Err(e) => tracing::warn!(key, error = %e, "deleting an unused project snapshot"),
        }
    }
    deleted
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn age(path: &Path, days: u64) {
        let t = SystemTime::now() - Duration::from_secs(days * 86_400);
        std::fs::File::open(path).unwrap().set_modified(t).unwrap();
    }

    /// A QEMU orb's `vm.json` as the runtime writes it, built on `base` (`images/<key>`
    /// or `projects/<key>`).
    fn qemu_recipe(state: &Path, base: &str) -> String {
        let (kind, key) = base.split_once('/').unwrap();
        let (source, file) = match kind {
            "images" => (
                serde_json::json!({"kind": "image", "image_ref": "r", "key": key, "manifest_digest": "d"}),
                "rootfs.ext4",
            ),
            _ => (
                serde_json::json!({"kind": "project_snapshot", "key": key}),
                "disk.raw",
            ),
        };
        serde_json::json!({
            "orb": crate::model::OrbId::generate(),
            "vcpus": 2, "memory_bytes": 1u64 << 31, "disk_bytes": 1u64 << 32,
            "base": {
                "source": source.to_string(),
                "path": state.join(base).join(file),
                "size": 1, "inode": 1, "mtime_ns": 1,
            },
            "net": {"backend": {"kind": "user"}, "mac": "m", "ipv4_cidr": "c",
                    "ipv4_gateway": "g", "mtu": 1500},
            "created_unix_ms": 1,
        })
        .to_string()
    }

    /// Only a project snapshot that no orb is built on and nothing used for the idle
    /// time is deleted; an orb recipe that cannot be read stops all deletion.
    #[test]
    fn deletes_only_unused_idle_project_snapshots() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path();
        for (i, base) in ["projects/built-on", "images/k1"].iter().enumerate() {
            let dir = state
                .join("orbs")
                .join(format!("01a17b00-0000-7000-8000-00000000000{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("vm.json"), qemu_recipe(state, base)).unwrap();
        }
        for key in ["built-on", "idle", "fresh", "used-lately"] {
            std::fs::create_dir_all(state.join("projects").join(key)).unwrap();
        }
        vm::mark_used(&state.join("projects/used-lately"));
        for key in ["built-on", "idle", "used-lately"] {
            age(&state.join("projects").join(key), 4);
        }

        let in_use = orb_bases(state).unwrap();
        assert_eq!(
            in_use,
            HashSet::from(["projects/built-on".to_owned(), "images/k1".to_owned()])
        );
        let trashed = collect_projects(state, &in_use, IDLE);
        assert_eq!(trashed.len(), 1);
        assert_eq!(trashed[0].0, "idle");
        let mut left: Vec<String> = std::fs::read_dir(state.join("projects"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left, ["built-on", "fresh", "used-lately"]);

        let broken = state.join("orbs/01a17b00-0000-7000-8000-000000000009");
        std::fs::create_dir_all(&broken).unwrap();
        assert!(
            orb_bases(state).is_err(),
            "an orb directory without vm.json"
        );
        std::fs::write(broken.join("vm.json"), b"{").unwrap();
        assert!(orb_bases(state).is_err(), "an unreadable vm.json");
    }
}
