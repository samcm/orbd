//! An orb's memory checkpoints on disk, the same for every VM runtime:
//!
//! ```text
//! checkpoints/<gen>/       published: the VMM's files, manifest.json[, consumed]
//! checkpoints/<gen>.tmp/   save in progress (complete once manifest.json exists)
//! ```
//!
//! Publication is one fsynced rename. A checkpoint is consumed (durably, an empty
//! `consumed` file) before a restored guest can run, and never restored again. Files are
//! only ever created fresh, renamed and unlinked, never reopened for writing: a lazily
//! restored Firecracker maps its memory file for life, and an unlinked file stays valid
//! for the process that maps it.

use super::fsutil::{fsync_dir, remove_via_trash, rename_durable, write_atomic};
use super::{create_private_dir, io_err};
use crate::model::OrbId;
use crate::runtime::RuntimeError;
use std::path::{Path, PathBuf};

pub const MANIFEST: &str = "manifest.json";

pub struct Checkpoints {
    pub orb: OrbId,
    dir: PathBuf,
    trash: PathBuf,
}

impl Checkpoints {
    pub fn new(orb: OrbId, dir: PathBuf, trash: PathBuf) -> Self {
        Self { orb, dir, trash }
    }

    /// A published checkpoint's directory.
    pub fn published(&self, generation: u64) -> PathBuf {
        self.dir.join(generation.to_string())
    }

    /// A save in progress.
    pub fn tmp(&self, generation: u64) -> PathBuf {
        self.dir.join(format!("{generation}.tmp"))
    }

    /// Published generations (with their consumed flag) and in-progress generations.
    pub fn list(&self) -> (Vec<(u64, bool)>, Vec<u64>) {
        let (mut published, mut tmp) = (Vec::new(), Vec::new());
        if let Ok(rd) = std::fs::read_dir(&self.dir) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if let Some(g) = name.strip_suffix(".tmp").and_then(|g| g.parse().ok()) {
                    tmp.push(g);
                } else if let Ok(g) = name.parse::<u64>() {
                    published.push((g, e.path().join("consumed").exists()));
                }
            }
        }
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
            .find(|g| self.tmp(*g).join(MANIFEST).exists())
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

    /// Marks every published checkpoint consumed (the disk is about to move on).
    pub fn consume_all(&self) -> Result<(), RuntimeError> {
        for (g, consumed) in self.list().0 {
            if !consumed {
                tracing::info!(orb = %self.orb, generation = g, "checkpoint is stale; marking consumed");
                self.consume(g)?;
            }
        }
        Ok(())
    }

    /// Consumes and removes a checkpoint. Unlinking keeps a mapped memory file valid
    /// for the VMM that restored from it.
    pub fn discard(&self, generation: u64) -> Result<(), RuntimeError> {
        self.consume(generation)?;
        remove_via_trash(&self.published(generation), &self.trash).map_err(io_err)
    }

    pub fn discard_tmp(&self, generation: u64) -> Result<(), RuntimeError> {
        remove_via_trash(&self.tmp(generation), &self.trash).map_err(io_err)
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
        let tmp = self.tmp(generation);
        self.discard_tmp(generation)?;
        create_private_dir(&tmp).map_err(io_err)?;
        fsync_dir(&self.dir).map_err(io_err)?;
        Ok(tmp)
    }

    /// Renames a complete `.tmp` checkpoint into place (the publication boundary).
    /// Generations are never reused, so an existing destination is a bug.
    pub fn publish(&self, generation: u64) -> Result<(), RuntimeError> {
        let dest = self.published(generation);
        if dest.exists() {
            return Err(RuntimeError::Other(format!(
                "checkpoint {generation} is already published"
            )));
        }
        rename_durable(&self.tmp(generation), &dest).map_err(io_err)
    }

    /// With no VMM running: publishes complete saves, discards partial ones, consumes
    /// all but the newest unconsumed checkpoint and returns that one if `restorable`
    /// accepts it. One this host cannot restore exactly is consumed: the orb cold-boots
    /// and its processes are lost.
    pub fn settle(
        &self,
        restorable: impl Fn(u64) -> Result<(), RuntimeError>,
    ) -> Result<Option<u64>, RuntimeError> {
        let orb = self.orb;
        for g in self.list().1 {
            if self.tmp(g).join(MANIFEST).exists() {
                tracing::warn!(orb = %orb, generation = g, "publishing a complete checkpoint left by an interrupted save");
                self.publish(g)?;
            } else {
                self.discard_tmp(g)?;
            }
        }
        let live: Vec<u64> = self
            .list()
            .0
            .into_iter()
            .filter(|(_, consumed)| !consumed)
            .map(|(g, _)| g)
            .collect();
        let mut latest = live.last().copied();
        for g in &live[..live.len().saturating_sub(1)] {
            tracing::warn!(orb = %orb, generation = g, "older unconsumed checkpoint; marking consumed");
            self.consume(*g)?;
        }
        if let Some(g) = latest
            && let Err(e) = restorable(g)
        {
            tracing::error!(orb = %orb, generation = g, error = %e, "checkpoint cannot be restored; the orb will cold-boot");
            self.consume(g)?;
            latest = None;
        }
        Ok(latest)
    }
}

/// Size of a checkpoint file, for manifests and compatibility checks.
pub fn file_len(path: &Path) -> Result<u64, RuntimeError> {
    std::fs::metadata(path).map(|m| m.len()).map_err(io_err)
}
