//! Saving a VM's state into a file and loading it back, with QEMU's file migration
//! (`mapped-ram`, optionally `multifd` and direct I/O; see [`FileMigration`]).
//!
//! A save is: [`Qmp::configure_migration`] → [`Qmp::stop`] → [`save_to_file`]. QEMU is
//! then in `postmigrate` with the guest paused; the caller either quits it or resumes
//! the guest with [`Qmp::cont`]. A load needs a QEMU started with `-S -incoming defer`
//! and the launch config the file was saved from: [`load_from_file`], then
//! [`Qmp::cont`]. These functions decide nothing about the checkpoint's lifecycle.
//!
//! Failpoint (feature `failpoints`): `qemu::save::after_migrate` aborts the process
//! while QEMU is writing the file.

use crate::qmp::{FileMigration, MigrationInfo, Qmp, QmpError};
use std::path::Path;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error(transparent)]
    Qmp(#[from] QmpError),
    /// QEMU reported the migration `failed` or `cancelled`.
    #[error("migration {status}: {desc}")]
    Failed { status: String, desc: String },
    /// The progress callback gave up on the save.
    #[error("{0}")]
    Aborted(String),
    /// QEMU's run state after the migration is not the expected one.
    #[error("QEMU is {0} after the migration")]
    UnexpectedStatus(String),
    /// Creating the memory file.
    #[error("{0}")]
    Io(std::io::Error),
    #[error("fsync: {0}")]
    Fsync(std::io::Error),
    #[error("the migration does not cancel")]
    CancelTimedOut,
}

/// A memory file that QEMU completed and that is durable.
pub struct SavedFile {
    /// Apparent size; `mapped-ram` files are sparse.
    pub bytes: u64,
    /// The final `query-migrate` answer.
    pub info: MigrationInfo,
}

/// Has a stopped, configured QEMU write its state into `memory`, which must not exist
/// yet (it is created 0600; QEMU reopens and writes it in place), polling every `poll`.
/// `progress` sees each poll while the save runs and may abort it by returning an
/// error (the caller then cancels the migration). On success QEMU is in `postmigrate`
/// and the file is fsynced.
pub async fn save_to_file(
    qmp: &Qmp,
    memory: &Path,
    poll: Duration,
    mut progress: impl FnMut(&MigrationInfo) -> Result<(), String>,
) -> Result<SavedFile, MigrationError> {
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(memory)
            .map_err(MigrationError::Io)?
    };
    qmp.migrate_to_file(memory).await?;
    fail::fail_point!("qemu::save::after_migrate", |_| std::process::abort());
    let info = loop {
        let info = qmp.query_migrate().await?;
        match info.status.as_deref() {
            Some("completed") => break info,
            Some("failed" | "cancelled") => return Err(failed(info)),
            _ => {
                progress(&info).map_err(MigrationError::Aborted)?;
                tokio::time::sleep(poll).await;
            }
        }
    };
    let status = qmp.status().await?;
    if status != "postmigrate" {
        return Err(MigrationError::UnexpectedStatus(status));
    }
    let synced = tokio::task::spawn_blocking(move || {
        file.sync_all()?;
        file.metadata().map(|m| m.len())
    });
    let bytes = synced
        .await
        .map_err(std::io::Error::other)
        .and_then(|r| r)
        .map_err(MigrationError::Fsync)?;
    Ok(SavedFile { bytes, info })
}

/// Has a `-incoming defer` QEMU load `memory`, written with `migration`, and waits
/// until the guest is loaded and paused (`paused`, `postmigrate` or `prelaunch`).
pub async fn load_from_file(
    qmp: &Qmp,
    memory: &Path,
    migration: FileMigration,
    poll: Duration,
) -> Result<(), MigrationError> {
    qmp.configure_migration(migration, 0).await?;
    qmp.migrate_incoming_file(memory).await?;
    loop {
        let info = qmp.query_migrate().await?;
        match info.status.as_deref() {
            Some("completed") => break,
            Some("failed" | "cancelled") => return Err(failed(info)),
            _ => tokio::time::sleep(poll).await,
        }
    }
    loop {
        let status = qmp.status().await?;
        match status.as_str() {
            "paused" | "postmigrate" | "prelaunch" => return Ok(()),
            "inmigrate" => tokio::time::sleep(poll).await,
            _ => return Err(MigrationError::UnexpectedStatus(status)),
        }
    }
}

/// Cancels an outgoing migration that is still running and waits (up to 30 s) until
/// it stopped. Nothing to do when none runs.
pub async fn cancel_outgoing(qmp: &Qmp, poll: Duration) -> Result<(), MigrationError> {
    const ACTIVE: &[&str] = &["setup", "active", "device", "pre-switchover", "cancelling"];
    let active = || async {
        let info = qmp.query_migrate().await?;
        Ok::<_, MigrationError>(ACTIVE.contains(&info.status.unwrap_or_default().as_str()))
    };
    if !active().await? {
        return Ok(());
    }
    qmp.migrate_cancel().await?;
    let give_up = std::time::Instant::now() + Duration::from_secs(30);
    while active().await? {
        if std::time::Instant::now() >= give_up {
            return Err(MigrationError::CancelTimedOut);
        }
        tokio::time::sleep(poll).await;
    }
    Ok(())
}

fn failed(info: MigrationInfo) -> MigrationError {
    MigrationError::Failed {
        status: info.status.unwrap_or_default(),
        desc: info.error_desc.unwrap_or_default(),
    }
}
