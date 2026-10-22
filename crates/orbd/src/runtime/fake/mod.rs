//! Fake runtime with durable on-disk state, effect counters and failpoints, used to
//! prove the executor and protocol without real VMs.
//!
//! State lives in `<state_dir>/fake/<orb id>.json`, written atomically (temp file, fsync,
//! rename, directory fsync). Like a real VM it survives agent restarts: `running` is a
//! persisted flag. Counters record every effect so tests can assert "no duplicate create".
//!
//! Failpoints (built with `--features failpoints`, configured via `FAILPOINTS`):
//! * `fake::<method>=return(msg)` makes the method fail with an injected error;
//! * `fake::<method>::delay=return(<ms>)` delays the method (async; cancellable and
//!   bounded by the deadline) — use `return`, not `sleep`, which would block a thread;
//! * `fake::<method>::after_effect=return` aborts the process right after the effect was
//!   persisted (crash point).
//!
//! Methods: `inspect`, `start`, `checkpoint_and_stop`, `cold_stop`, `discard_checkpoint`,
//! `clone_disk`, `delete`, `bootstrap`.

use super::{
    BootstrapRequest, GuestControl, GuestError, OpContext, Runtime, RuntimeCapabilities,
    RuntimeError, RuntimeObservation, StartSource,
};
use crate::config::agent::FakeRuntimeConfig;
use crate::model::{
    OrbDefinition, OrbId, ProjectSnapshotKey, ProjectSnapshotRef, RuntimeKind, SleepCheckpointRef,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Effect counters. Never reset, not even by `delete`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FakeCounters {
    pub disk_creates: u64,
    pub boots: u64,
    pub restores: u64,
    pub checkpoints: u64,
    pub cold_stops: u64,
    pub deletes: u64,
    pub clones: u64,
}

/// One recorded guest bootstrap. Env *names* only, never values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootstrapRecord {
    pub generation: u64,
    pub cold_boot: bool,
    pub env_names: Vec<String>,
}

/// Durable state of one fake orb.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FakeOrbState {
    pub disk: bool,
    pub running: bool,
    /// Checkpoint generations published and not invalidated.
    pub checkpoints: Vec<u64>,
    /// Checkpoint generations consumed (restored from, or invalidated by a cold boot).
    pub consumed: Vec<u64>,
    /// Project snapshot keys cloned from this orb.
    pub snapshots: Vec<String>,
    pub counters: FakeCounters,
    pub bootstraps: Vec<BootstrapRecord>,
}

impl FakeOrbState {
    fn latest_checkpoint(&self) -> Option<u64> {
        self.checkpoints
            .iter()
            .copied()
            .filter(|g| !self.consumed.contains(g))
            .max()
    }
}

fn fake_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("fake")
}

fn orb_file(state_dir: &Path, orb: OrbId) -> PathBuf {
    fake_dir(state_dir).join(format!("{orb}.json"))
}

/// Read-only helper for tests: the fake state of `orb` under the agent `state_dir`
/// (the same directory given to [`FakeRuntime::new`]). `None` if the orb never existed.
pub fn load_orb_state(state_dir: &Path, orb: OrbId) -> std::io::Result<Option<FakeOrbState>> {
    match std::fs::read(orb_file(state_dir, orb)) {
        Ok(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(std::io::Error::other),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().expect("state file has a parent");
    let tmp = path.with_extension("json.tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(dir)?.sync_all()
}

pub struct FakeRuntime {
    state_dir: PathBuf,
    memory_preserving: bool,
    /// Serializes read-modify-write of state files (held only for sync file IO).
    lock: Mutex<()>,
}

impl FakeRuntime {
    pub fn new(state_dir: PathBuf, config: &FakeRuntimeConfig) -> Self {
        let _ = std::fs::create_dir_all(fake_dir(&state_dir));
        Self {
            state_dir,
            memory_preserving: config.memory_preserving_sleep,
            lock: Mutex::new(()),
        }
    }

    fn load(&self, orb: OrbId) -> Result<FakeOrbState, RuntimeError> {
        load_orb_state(&self.state_dir, orb)
            .map(Option::unwrap_or_default)
            .map_err(|e| RuntimeError::Other(format!("fake state: {e}")))
    }

    /// Loads, mutates and atomically persists one orb's state.
    fn update<R>(
        &self,
        orb: OrbId,
        f: impl FnOnce(&mut FakeOrbState) -> Result<R, RuntimeError>,
    ) -> Result<R, RuntimeError> {
        let _g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut st = self.load(orb)?;
        let r = f(&mut st)?;
        let json = serde_json::to_vec_pretty(&st).expect("fake state serializes");
        write_atomic(&orb_file(&self.state_dir, orb), &json)
            .map_err(|e| RuntimeError::Other(format!("fake state: {e}")))?;
        Ok(r)
    }
}

/// Async delay failpoint, bounded by cancellation and the work deadline.
#[cfg(feature = "failpoints")]
async fn delay(name: &'static str, ctx: &OpContext) -> Result<(), RuntimeError> {
    let ms = fail::eval(name, |arg| arg.and_then(|a| a.parse::<u64>().ok()))
        .flatten()
        .unwrap_or(0);
    if ms > 0 {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(ms)) => {}
            _ = ctx.cancel.cancelled() => return Err(RuntimeError::Cancelled),
            _ = tokio::time::sleep_until(ctx.work_deadline.into()) => {
                return Err(RuntimeError::DeadlineExceeded)
            }
        }
    }
    Ok(())
}

#[cfg(not(feature = "failpoints"))]
async fn delay(_name: &'static str, _ctx: &OpContext) -> Result<(), RuntimeError> {
    Ok(())
}

fn check_ctx(ctx: &OpContext) -> Result<(), RuntimeError> {
    if ctx.cancel.is_cancelled() {
        Err(RuntimeError::Cancelled)
    } else if std::time::Instant::now() >= ctx.work_deadline {
        Err(RuntimeError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

#[cfg(feature = "failpoints")]
fn injected(arg: Option<String>) -> String {
    format!("injected failure: {}", arg.unwrap_or_default())
}

#[async_trait]
impl Runtime for FakeRuntime {
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            kind: RuntimeKind::Fake,
            version: "fake-1".into(),
            memory_preserving_sleep: self.memory_preserving,
            nested_kvm: true,
            nested_state_qualified: false,
            project_snapshots: true,
        }
    }

    async fn inspect(&self, orb: OrbId) -> Result<RuntimeObservation, RuntimeError> {
        fail::fail_point!("fake::inspect", |a| Err(RuntimeError::Other(injected(a))));
        let st = {
            let _g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
            self.load(orb)?
        };
        Ok(match (st.disk, st.running) {
            (false, false) => RuntimeObservation::Absent,
            (false, true) => RuntimeObservation::Inconsistent {
                reason: "process without disk".into(),
            },
            (true, true) => RuntimeObservation::Running,
            (true, false) => RuntimeObservation::Stopped {
                checkpoint: st
                    .latest_checkpoint()
                    .map(|generation| SleepCheckpointRef { generation }),
            },
        })
    }

    async fn start(
        &self,
        orb: &OrbDefinition,
        source: StartSource,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        check_ctx(ctx)?;
        delay("fake::start::delay", ctx).await?;
        fail::fail_point!("fake::start", |a| Err(RuntimeError::Other(injected(a))));
        self.update(orb.id, |st| {
            if st.running {
                return Err(RuntimeError::Other("already running".into()));
            }
            match source {
                StartSource::NewDisk(_) => {
                    if st.disk {
                        return Err(RuntimeError::Other("disk already exists".into()));
                    }
                    st.disk = true;
                    st.counters.disk_creates += 1;
                    st.counters.boots += 1;
                }
                StartSource::MemoryCheckpoint(cp) => {
                    if !st.disk || st.latest_checkpoint() != Some(cp.generation) {
                        return Err(RuntimeError::CheckpointIncompatible(format!(
                            "checkpoint {} is not restorable",
                            cp.generation
                        )));
                    }
                    st.counters.restores += 1;
                }
                StartSource::ExistingDiskColdBoot => {
                    if !st.disk {
                        return Err(RuntimeError::NotFound("disk".into()));
                    }
                    // Booting writes the disk: published checkpoints become stale.
                    let stale: Vec<u64> = st.checkpoints.clone();
                    st.consumed.extend(stale);
                    st.counters.boots += 1;
                }
            }
            st.running = true;
            Ok(())
        })?;
        crate::crash_point!("fake::start::after_effect");
        Ok(())
    }

    async fn checkpoint_and_stop(
        &self,
        orb: &OrbDefinition,
        checkpoint_generation: u64,
        ctx: &OpContext,
    ) -> Result<SleepCheckpointRef, RuntimeError> {
        check_ctx(ctx)?;
        delay("fake::checkpoint_and_stop::delay", ctx).await?;
        fail::fail_point!("fake::checkpoint_and_stop", |a| Err(
            RuntimeError::CheckpointFailed(injected(a))
        ));
        if !self.memory_preserving {
            return Err(RuntimeError::Unsupported("memory-preserving sleep".into()));
        }
        self.update(orb.id, |st| {
            if !st.running {
                return Err(RuntimeError::CheckpointFailed("not running".into()));
            }
            st.running = false;
            st.checkpoints.push(checkpoint_generation);
            st.counters.checkpoints += 1;
            Ok(())
        })?;
        crate::crash_point!("fake::checkpoint_and_stop::after_effect");
        Ok(SleepCheckpointRef {
            generation: checkpoint_generation,
        })
    }

    async fn cold_stop(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), RuntimeError> {
        check_ctx(ctx)?;
        delay("fake::cold_stop::delay", ctx).await?;
        fail::fail_point!("fake::cold_stop", |a| Err(RuntimeError::Other(injected(a))));
        self.update(orb.id, |st| {
            if st.running {
                st.running = false;
                st.counters.cold_stops += 1;
            }
            Ok(())
        })?;
        crate::crash_point!("fake::cold_stop::after_effect");
        Ok(())
    }

    async fn discard_checkpoint(
        &self,
        orb: OrbId,
        checkpoint: &SleepCheckpointRef,
    ) -> Result<(), RuntimeError> {
        fail::fail_point!("fake::discard_checkpoint", |a| Err(RuntimeError::Other(
            injected(a)
        )));
        self.update(orb, |st| {
            if !st.consumed.contains(&checkpoint.generation) {
                st.consumed.push(checkpoint.generation);
            }
            Ok(())
        })?;
        crate::crash_point!("fake::discard_checkpoint::after_effect");
        Ok(())
    }

    async fn clone_disk(
        &self,
        orb: &OrbDefinition,
        key: &ProjectSnapshotKey,
        ctx: &OpContext,
    ) -> Result<ProjectSnapshotRef, RuntimeError> {
        check_ctx(ctx)?;
        delay("fake::clone_disk::delay", ctx).await?;
        fail::fail_point!("fake::clone_disk", |a| Err(RuntimeError::Other(injected(
            a
        ))));
        self.update(orb.id, |st| {
            if !st.disk || st.running {
                return Err(RuntimeError::Other(
                    "disk must exist and be stopped to clone".into(),
                ));
            }
            if !st.snapshots.contains(&key.0) {
                st.snapshots.push(key.0.clone());
                st.counters.clones += 1;
            }
            Ok(())
        })?;
        crate::crash_point!("fake::clone_disk::after_effect");
        Ok(ProjectSnapshotRef { key: key.clone() })
    }

    async fn delete(&self, orb: OrbId, ctx: &OpContext) -> Result<(), RuntimeError> {
        check_ctx(ctx)?;
        delay("fake::delete::delay", ctx).await?;
        fail::fail_point!("fake::delete", |a| Err(RuntimeError::Other(injected(a))));
        self.update(orb, |st| {
            let existed = st.disk || st.running;
            st.disk = false;
            st.running = false;
            st.checkpoints.clear();
            if existed {
                st.counters.deletes += 1;
            }
            Ok(())
        })?;
        crate::crash_point!("fake::delete::after_effect");
        Ok(())
    }

    fn guest(&self) -> &dyn GuestControl {
        self
    }
}

#[async_trait]
impl GuestControl for FakeRuntime {
    async fn bootstrap(
        &self,
        orb: &OrbDefinition,
        request: BootstrapRequest,
        ctx: &OpContext,
    ) -> Result<(), GuestError> {
        let cancel_err = |e: RuntimeError| match e {
            RuntimeError::DeadlineExceeded => GuestError::DeadlineExceeded,
            _ => GuestError::Cancelled,
        };
        check_ctx(ctx).map_err(cancel_err)?;
        delay("fake::bootstrap::delay", ctx)
            .await
            .map_err(cancel_err)?;
        fail::fail_point!("fake::bootstrap", |a| Err(GuestError::BootstrapFailed(
            injected(a)
        )));
        let names: Vec<String> = request.env.0.iter().map(|(n, _)| n.clone()).collect();
        self.update(orb.id, |st| {
            if !st.running {
                return Err(RuntimeError::Other("guest unreachable: not running".into()));
            }
            st.bootstraps.push(BootstrapRecord {
                generation: request.generation,
                cold_boot: request.cold_boot,
                env_names: names,
            });
            Ok(())
        })
        .map_err(|e| GuestError::Unreachable(e.to_string()))?;
        crate::crash_point!("fake::bootstrap::after_effect");
        Ok(())
    }
}
