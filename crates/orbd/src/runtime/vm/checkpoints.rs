//! An orb's memory checkpoints on disk, the same for every VM runtime: a
//! [`CheckpointStore`] named by generation, plus orbd's consume-once rule.
//!
//! ```text
//! checkpoints/<gen>/       published: the VMM's files, manifest.json[, consumed]
//! checkpoints/<gen>.tmp/   save in progress (complete once manifest.json exists)
//! ```
//!
//! A checkpoint is consumed (durably, an empty `consumed` file) before a restored guest
//! can run, and never restored again: the guest's disk moves on from that point. A
//! consumed checkpoint only takes disk space, so it is removed as soon as its restore
//! finished or the orb is next inspected without a VMM.

use super::fsutil::write_atomic;
use super::io_err;
use crate::model::OrbId;
use crate::runtime::RuntimeError;
use orbd_vm::checkpoint::CheckpointStore;
use std::path::{Path, PathBuf};

pub use orbd_vm::checkpoint::MANIFEST;

pub struct Checkpoints {
    pub orb: OrbId,
    store: CheckpointStore,
}

impl Checkpoints {
    pub fn new(orb: OrbId, dir: PathBuf, trash: PathBuf) -> Self {
        Self {
            orb,
            store: CheckpointStore::new(dir, trash),
        }
    }

    /// A published checkpoint's directory.
    pub fn published(&self, generation: u64) -> PathBuf {
        self.store.published(&generation.to_string())
    }

    /// A save in progress.
    pub fn tmp(&self, generation: u64) -> PathBuf {
        self.store.tmp(&generation.to_string())
    }

    /// Published generations (with their consumed flag) and in-progress generations.
    pub fn list(&self) -> (Vec<(u64, bool)>, Vec<u64>) {
        let (published, tmp) = self.store.list();
        let mut published: Vec<(u64, bool)> = published
            .iter()
            .filter_map(|n| n.parse().ok())
            .map(|g| (g, self.published(g).join("consumed").exists()))
            .collect();
        let mut tmp: Vec<u64> = tmp.iter().filter_map(|n| n.parse().ok()).collect();
        published.sort_unstable();
        tmp.sort_unstable();
        (published, tmp)
    }

    /// The newest in-progress save whose files are complete (manifest written).
    pub fn complete_tmp(&self) -> Option<u64> {
        self.list()
            .1
            .into_iter()
            .rev()
            .find(|g| self.store.tmp_is_complete(&g.to_string()))
    }

    /// Consumed: marked so, or already discarded (only consumed checkpoints are).
    pub fn is_consumed(&self, generation: u64) -> bool {
        let dir = self.published(generation);
        !dir.is_dir() || dir.join("consumed").exists()
    }

    /// Durably marks a published checkpoint consumed: it is never restored again.
    pub fn consume(&self, generation: u64) -> Result<(), RuntimeError> {
        let dir = self.published(generation);
        if dir.is_dir() && !dir.join("consumed").exists() {
            write_atomic(&dir.join("consumed"), b"").map_err(io_err)?;
        }
        Ok(())
    }

    /// Discards every published checkpoint (the disk is about to move on, or already
    /// has).
    pub fn discard_all(&self) -> Result<(), RuntimeError> {
        for (g, consumed) in self.list().0 {
            if !consumed {
                tracing::info!(orb = %self.orb, generation = g, "checkpoint is stale; discarding it");
            }
            self.discard(g)?;
        }
        Ok(())
    }

    /// Consumes and removes a checkpoint. Unlinking keeps a mapped memory file valid
    /// for the VMM that restored from it.
    pub fn discard(&self, generation: u64) -> Result<(), RuntimeError> {
        self.consume(generation)?;
        self.store.discard(&generation.to_string()).map_err(io_err)
    }

    pub fn discard_tmp(&self, generation: u64) -> Result<(), RuntimeError> {
        self.store
            .discard_tmp(&generation.to_string())
            .map_err(io_err)
    }

    pub fn discard_tmps(&self) -> Result<(), RuntimeError> {
        for g in self.list().1 {
            tracing::info!(orb = %self.orb, generation = g, "discarding unfinished checkpoint");
            self.discard_tmp(g)?;
        }
        Ok(())
    }

    /// A new, empty, private `<gen>.tmp` for a save (a leftover one is discarded first),
    /// durably linked into the checkpoints directory.
    pub fn fresh_tmp(&self, generation: u64) -> Result<PathBuf, RuntimeError> {
        self.store
            .fresh_tmp(&generation.to_string())
            .map_err(io_err)
    }

    /// Renames a complete `.tmp` checkpoint into place (the publication boundary).
    /// Generations are never reused, so an existing destination is a bug.
    pub fn publish(&self, generation: u64) -> Result<(), RuntimeError> {
        self.store.publish(&generation.to_string()).map_err(io_err)
    }

    /// With no VMM running: publishes complete saves, discards partial and consumed
    /// ones and all but the newest unconsumed checkpoint, and returns that one if
    /// `restorable` accepts it. One this host cannot restore exactly is discarded: the
    /// orb cold-boots and its processes are lost.
    pub fn settle(
        &self,
        restorable: impl Fn(u64) -> Result<(), RuntimeError>,
    ) -> Result<Option<u64>, RuntimeError> {
        let orb = self.orb;
        for g in self.list().1 {
            if self.store.tmp_is_complete(&g.to_string()) {
                tracing::warn!(orb = %orb, generation = g, "publishing a complete checkpoint left by an interrupted save");
                self.publish(g)?;
            } else {
                self.discard_tmp(g)?;
            }
        }
        let mut live = Vec::new();
        for (g, consumed) in self.list().0 {
            if consumed {
                tracing::info!(orb = %orb, generation = g, "discarding a consumed checkpoint");
                self.discard(g)?;
            } else {
                live.push(g);
            }
        }
        let mut latest = live.pop();
        for g in live {
            tracing::warn!(orb = %orb, generation = g, "older unconsumed checkpoint; discarding it");
            self.discard(g)?;
        }
        if let Some(g) = latest
            && let Err(e) = restorable(g)
        {
            tracing::error!(orb = %orb, generation = g, error = %e, "checkpoint cannot be restored; discarding it, the orb will cold-boot");
            self.discard(g)?;
            latest = None;
        }
        Ok(latest)
    }
}

/// Size of a checkpoint file, for manifests and compatibility checks.
pub fn file_len(path: &Path) -> Result<u64, RuntimeError> {
    std::fs::metadata(path).map(|m| m.len()).map_err(io_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publish(cps: &Checkpoints, g: u64, consumed: bool) {
        let dir = cps.published(g);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("memory"), b"ram").unwrap();
        std::fs::write(dir.join(MANIFEST), b"{}").unwrap();
        if consumed {
            std::fs::write(dir.join("consumed"), b"").unwrap();
        }
    }

    fn on_disk(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// Only the newest unconsumed checkpoint stays on disk, and only if it can be
    /// restored: consumed, superseded and unrestorable ones never are, so they are
    /// deleted rather than kept forever.
    #[test]
    fn settle_keeps_only_the_restorable_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("checkpoints");
        let cps = Checkpoints::new(OrbId::generate(), dir.clone(), tmp.path().join("trash"));
        publish(&cps, 2, true);
        publish(&cps, 9, false);
        publish(&cps, 10, false);
        std::fs::create_dir_all(cps.tmp(11)).unwrap();

        assert_eq!(cps.settle(|_| Ok(())).unwrap(), Some(10));
        assert_eq!(on_disk(&dir), ["10"]);
        assert!(!cps.is_consumed(10));

        let incompatible = |_| Err(RuntimeError::CheckpointIncompatible("new QEMU".into()));
        assert_eq!(cps.settle(incompatible).unwrap(), None);
        assert!(on_disk(&dir).is_empty());
        assert!(cps.is_consumed(10));

        // Before a cold boot every published checkpoint goes, consumed or not.
        publish(&cps, 12, true);
        publish(&cps, 13, false);
        cps.discard_all().unwrap();
        assert!(on_disk(&dir).is_empty());
    }
}
