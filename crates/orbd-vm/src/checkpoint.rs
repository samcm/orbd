//! A directory of named checkpoints:
//!
//! ```text
//! <dir>/<name>/       published: the VMM's files and manifest.json
//! <dir>/<name>.tmp/   save in progress (complete once manifest.json exists)
//! ```
//!
//! Publication is one fsynced rename and removal goes through a trash directory, so a
//! crash leaves each checkpoint either published, in progress or gone. Files are only
//! ever created fresh, renamed and unlinked, never reopened for writing: a VMM that
//! maps a memory file keeps a valid mapping after it is unlinked. What a published
//! checkpoint may be used for (restored once or many times) is the caller's policy.

use crate::fsutil::{create_private_dir, fsync_dir, remove_via_trash, rename_durable};
use std::path::{Path, PathBuf};

/// Written last into a checkpoint in progress; its presence marks the save complete.
pub const MANIFEST: &str = "manifest.json";

pub struct CheckpointStore {
    dir: PathBuf,
    trash: PathBuf,
}

impl CheckpointStore {
    /// `trash` must be on the same filesystem as `dir`.
    pub fn new(dir: PathBuf, trash: PathBuf) -> Self {
        Self { dir, trash }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// A published checkpoint's directory.
    pub fn published(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// A save in progress.
    pub fn tmp(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.tmp"))
    }

    /// Names of published checkpoints and of saves in progress, unordered.
    pub fn list(&self) -> (Vec<String>, Vec<String>) {
        let (mut published, mut tmp) = (Vec::new(), Vec::new());
        if let Ok(rd) = std::fs::read_dir(&self.dir) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                match name.strip_suffix(".tmp") {
                    Some(stem) => tmp.push(stem.to_owned()),
                    None => published.push(name),
                }
            }
        }
        (published, tmp)
    }

    /// True once a save in progress has written its manifest.
    pub fn tmp_is_complete(&self, name: &str) -> bool {
        self.tmp(name).join(MANIFEST).exists()
    }

    /// A new, empty, private `<name>.tmp` for a save (a leftover one is discarded
    /// first), durably linked into the checkpoint directory.
    pub fn fresh_tmp(&self, name: &str) -> std::io::Result<PathBuf> {
        let tmp = self.tmp(name);
        self.discard_tmp(name)?;
        create_private_dir(&tmp)?;
        fsync_dir(&self.dir)?;
        Ok(tmp)
    }

    /// Renames a complete `<name>.tmp` into place (the publication boundary). An
    /// existing destination is an error: names are never reused.
    pub fn publish(&self, name: &str) -> std::io::Result<()> {
        let dest = self.published(name);
        if dest.exists() {
            return Err(std::io::Error::other(format!(
                "checkpoint {name} is already published"
            )));
        }
        rename_durable(&self.tmp(name), &dest)
    }

    /// Removes a published checkpoint. Missing is fine.
    pub fn discard(&self, name: &str) -> std::io::Result<()> {
        remove_via_trash(&self.published(name), &self.trash)
    }

    /// Removes a save in progress. Missing is fine.
    pub fn discard_tmp(&self, name: &str) -> std::io::Result<()> {
        remove_via_trash(&self.tmp(name), &self.trash)
    }
}
