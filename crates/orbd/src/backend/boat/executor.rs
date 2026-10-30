//! The Boat executor: durable acceptance, execution and terminal receipts, in the
//! coordinator process with its own store.
//!
//! A delivery is accepted once its receipt (command, orb, sealed guest env, pinned
//! deadlines) is committed; completion is reported until acknowledged and survives a
//! restart, which re-runs every unfinished operation. Operations of one orb run one at
//! a time. Every step reads the sandbox's state from Boat before it acts, so a re-run
//! continues where the last attempt stopped:
//!
//! * The orb's sandbox is found by its recorded id, else by its labels (a create whose
//!   response was lost); a create sends an idempotency key, so a retry cannot make a
//!   second sandbox either.
//! * Spawn records how it starts the sandbox (create or resume) before acting, so a
//!   re-run that finds it running reports that start, not `AlreadyRunning`.

use super::api::{ApiError, Client, CreateSandbox, Sandbox};
use crate::backend::{
    BackendEvent, CommandDelivery, CommandSecrets, LogChunk, LogStream, OperationStatus,
};
use crate::coordinator::db::Db;
use crate::model::{
    Action, ActionResult, Command, CommandFailure, CommandKey, CommandOutcome, ExecutionTarget,
    FailureCode, OperationId, OrbDefinition, OrbId, OrbState, ProcessDisposition,
    SnapshotDisposition, StableOrbState, StartKind, unix_ms_now,
};
use crate::secrets::{PermittedGuestEnv, SecretKey, env_file, redact};
use anyhow::Context;
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

const SCHEMA_VERSION: i64 = 1;
const SCHEMA: &str = r#"
CREATE TABLE receipts (
    operation_id TEXT PRIMARY KEY,
    orb_id TEXT NOT NULL,
    command_json TEXT NOT NULL,
    orb_json TEXT NOT NULL,
    -- Spawn only: the guest env, sealed with the coordinator's key (aad = operation id).
    sealed_env BLOB,
    work_deadline_ms INTEGER NOT NULL,
    cleanup_deadline_ms INTEGER NOT NULL,
    -- Spawn: how this operation started the sandbox, recorded before it acted.
    start_json TEXT,
    outcome_json TEXT,
    accepted_ms INTEGER NOT NULL,
    acked_ms INTEGER
);
-- Operations cancelled before acceptance; a later delivery of one is refused.
CREATE TABLE tombstones (
    operation_id TEXT PRIMARY KEY,
    created_ms INTEGER NOT NULL
);
-- Each orb's sandbox. `incarnation` counts replaced disks and is part of the labels and
-- idempotency key of its create; `sandbox_id` is bound once Boat returned it.
CREATE TABLE sandboxes (
    orb_id TEXT PRIMARY KEY,
    incarnation INTEGER NOT NULL,
    sandbox_id TEXT,
    deleted_ms INTEGER
);
"#;

/// Polling interval for sandbox state.
const POLL: Duration = Duration::from_secs(2);
/// How long `amp orb bootstrap` must stay up to count as started (it never exits on
/// success).
const BOOTSTRAP_SETTLE: Duration = Duration::from_secs(5);
/// A stop Boat has not acted on is sent again after this long.
const STOP_RESEND: Duration = Duration::from_secs(30);
/// Acknowledged receipts and tombstones are kept this long.
const RETAIN_MS: u64 = 3_600_000;

const ENV_PATH: &str = "/home/user/.config/amp/orbd-bootstrap.env";
/// Boat's base machine has no Amp CLI: install it as Amp's Boat example does, and
/// link it where Amp expects it.
const INSTALL_AMP: &str = r#"set -eu
chmod 600 /home/user/.config/amp/orbd-bootstrap.env
export PATH="$HOME/.local/bin:$HOME/.amp/bin:$PATH"
if ! command -v amp >/dev/null 2>&1; then
  curl -fsSL https://ampcode.com/install.sh | bash
fi
if [ ! -e /usr/local/bin/amp ]; then
  sudo -n ln -sf "$(command -v amp)" /usr/local/bin/amp || true
fi
amp --version
"#;
/// The credentials reach `amp orb bootstrap` from the env file, never a command line.
const START_BOOTSTRAP: &str = r#"export PATH="$HOME/.local/bin:$HOME/.amp/bin:$PATH"; set -a; . /home/user/.config/amp/orbd-bootstrap.env; set +a; exec amp orb bootstrap"#;

#[derive(Clone, Debug)]
struct Receipt {
    command: Command,
    orb: OrbDefinition,
    sealed_env: Option<Vec<u8>>,
    work_deadline_ms: u64,
    cleanup_deadline_ms: u64,
    start: Option<StartKind>,
    outcome: Option<CommandOutcome>,
    acked: bool,
}

#[derive(Debug)]
struct Binding {
    incarnation: u64,
    sandbox_id: Option<String>,
    deleted: bool,
}

fn js<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("serializes")
}

fn parse<T: for<'de> serde::Deserialize<'de>>(s: &str) -> anyhow::Result<T> {
    serde_json::from_str(s).context("corrupt boat receipt")
}

fn get_receipt(c: &Connection, op: OperationId) -> anyhow::Result<Option<Receipt>> {
    let row = c
        .query_row(
            "SELECT command_json, orb_json, sealed_env, work_deadline_ms, cleanup_deadline_ms,
                    start_json, outcome_json, acked_ms
             FROM receipts WHERE operation_id = ?1",
            [op.to_string()],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<Vec<u8>>>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, Option<String>>(6)?,
                    r.get::<_, Option<i64>>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((command, orb, sealed_env, work, cleanup, start, outcome, acked)) = row else {
        return Ok(None);
    };
    Ok(Some(Receipt {
        command: parse(&command)?,
        orb: parse(&orb)?,
        sealed_env,
        work_deadline_ms: work as u64,
        cleanup_deadline_ms: cleanup as u64,
        start: start.as_deref().map(parse).transpose()?,
        outcome: outcome.as_deref().map(parse).transpose()?,
        acked: acked.is_some(),
    }))
}

fn tombstoned(c: &Connection, op: OperationId) -> rusqlite::Result<bool> {
    c.query_row(
        "SELECT EXISTS (SELECT 1 FROM tombstones WHERE operation_id = ?1)",
        [op.to_string()],
        |r| r.get(0),
    )
}

/// The orb's sandbox row, created on first use.
fn binding(c: &Connection, orb: OrbId) -> rusqlite::Result<Binding> {
    c.execute(
        "INSERT OR IGNORE INTO sandboxes (orb_id, incarnation) VALUES (?1, 0)",
        [orb.to_string()],
    )?;
    c.query_row(
        "SELECT incarnation, sandbox_id, deleted_ms FROM sandboxes WHERE orb_id = ?1",
        [orb.to_string()],
        |r| {
            Ok(Binding {
                incarnation: r.get::<_, i64>(0)? as u64,
                sandbox_id: r.get(1)?,
                deleted: r.get::<_, Option<i64>>(2)?.is_some(),
            })
        },
    )
}

fn instant_at(unix_ms: u64) -> Instant {
    let now_ms = unix_ms_now();
    let now = Instant::now();
    if unix_ms >= now_ms {
        now + Duration::from_millis(unix_ms - now_ms)
    } else {
        now.checked_sub(Duration::from_millis(now_ms - unix_ms))
            .unwrap_or(now)
    }
}

/// Why an operation stopped before producing its own result.
enum Stop {
    /// Cancelled or past its work deadline.
    Interrupted,
    /// A Boat call failed for good; the result is this failure with the observed state.
    Failed(CommandFailure),
    /// Local failure (store); the operation stays unfinished and re-runs after restart.
    Fatal(anyhow::Error),
}

impl From<anyhow::Error> for Stop {
    fn from(e: anyhow::Error) -> Self {
        Self::Fatal(e)
    }
}

fn failed(code: FailureCode, message: impl Into<String>) -> Stop {
    Stop::Failed(CommandFailure {
        code,
        message: message.into(),
    })
}

fn api_failure(what: &str, e: &ApiError) -> CommandFailure {
    let code = match e.status() {
        Some(402 | 403 | 429) => FailureCode::QuotaExceeded,
        Some(s) if s >= 500 => FailureCode::BackendUnavailable,
        None => FailureCode::BackendUnavailable,
        Some(_) => FailureCode::BackendFailure,
    };
    CommandFailure {
        code,
        message: format!("{what}: {e}"),
    }
}

/// What a sandbox's Boat state means for the orb.
fn observed(sb: Option<&Sandbox>, bound: bool) -> OrbState {
    let Some(sb) = sb else {
        return if bound {
            OrbState::Faulted {
                last_known: StableOrbState::Running,
                reason: "its Boat sandbox no longer exists".into(),
            }
        } else {
            OrbState::known(StableOrbState::Uncreated)
        };
    };
    match sb.state.as_str() {
        "archived" => OrbState::known(StableOrbState::DiskOnlyStopped {
            reason: "Boat sandbox stopped: disk kept, processes lost".into(),
        }),
        "cancelled" => OrbState::known(StableOrbState::Uncreated),
        "error" => OrbState::Faulted {
            last_known: StableOrbState::Running,
            reason: format!(
                "Boat sandbox {} failed: {}",
                sb.id,
                sb.error.as_deref().unwrap_or("no reason given")
            ),
        },
        _ => OrbState::known(StableOrbState::Running),
    }
}

fn tail(stdout: &str, stderr: &str) -> String {
    let all = format!("{}\n{}", stdout.trim_end(), stderr.trim_end());
    let lines: Vec<&str> = all.trim().lines().collect();
    lines[lines.len().saturating_sub(20)..].join("\n")
}

/// One running operation.
struct Ctx {
    key: CommandKey,
    work_deadline: Instant,
    cleanup_deadline: Instant,
    cancel: CancellationToken,
    events: UnboundedSender<BackendEvent>,
    log_seq: AtomicU64,
}

impl Ctx {
    async fn bounded<T>(&self, f: impl Future<Output = T>) -> Result<T, Stop> {
        tokio::select! {
            _ = self.cancel.cancelled() => Err(Stop::Interrupted),
            _ = tokio::time::sleep_until(self.work_deadline.into()) => Err(Stop::Interrupted),
            v = f => Ok(v),
        }
    }

    async fn sleep(&self, d: Duration) -> Result<(), Stop> {
        self.bounded(tokio::time::sleep(d)).await
    }

    fn remaining_secs(&self) -> u64 {
        self.work_deadline
            .saturating_duration_since(Instant::now())
            .as_secs()
    }

    /// A line for Amp's setup log.
    fn log(&self, line: &str) {
        tracing::info!(operation = %self.key.operation_id, orb = %self.key.orb_id, "{line}");
        let _ = self.events.send(BackendEvent::Log {
            key: self.key,
            chunk: LogChunk {
                seq: self.log_seq.fetch_add(1, Ordering::Relaxed),
                stream: LogStream::Stdout,
                data: bytes::Bytes::from(format!("{line}\n")),
                dropped_before: 0,
            },
        });
    }
}

struct Finished {
    result: Result<ActionResult, CommandFailure>,
    observed: OrbState,
    processes: ProcessDisposition,
}

pub struct Executor {
    db: Db,
    api: Client,
    key: Arc<SecretKey>,
    auto_stop_secs: Option<u64>,
    events: UnboundedSender<BackendEvent>,
    /// Operations being executed, with their cancellation.
    running: Mutex<HashMap<OperationId, CancellationToken>>,
    orb_locks: Mutex<HashMap<OrbId, Arc<tokio::sync::Mutex<()>>>>,
}

impl Executor {
    pub fn open(
        path: &std::path::Path,
        api: Client,
        key: Arc<SecretKey>,
        auto_stop_secs: Option<u64>,
        events: UnboundedSender<BackendEvent>,
    ) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            db: Db::open_with(path, SCHEMA, SCHEMA_VERSION)?,
            api,
            key,
            auto_stop_secs,
            events,
            running: Mutex::default(),
            orb_locks: Mutex::default(),
        }))
    }

    fn emit(&self, ev: BackendEvent) {
        let _ = self.events.send(ev);
    }

    fn reject(&self, key: CommandKey, code: FailureCode, message: &str) {
        self.emit(BackendEvent::Rejected {
            key,
            failure: CommandFailure {
                code,
                message: message.into(),
            },
        });
    }

    /// Startup: report results the core may not have committed, re-run the rest.
    pub async fn recover(self: &Arc<Self>) -> anyhow::Result<()> {
        let (done, open) = self
            .db
            .call(|c| {
                let mut done = Vec::new();
                let mut open = Vec::new();
                let mut stmt = c.prepare(
                    "SELECT operation_id FROM receipts WHERE acked_ms IS NULL ORDER BY accepted_ms",
                )?;
                let ops: Vec<String> = stmt
                    .query_map([], |r| r.get(0))?
                    .collect::<rusqlite::Result<_>>()?;
                for op in ops {
                    let op = OperationId::parse(&op)?;
                    let Some(r) = get_receipt(c, op)? else {
                        continue;
                    };
                    match r.outcome {
                        Some(o) => done.push((r.command.key(), o)),
                        None => open.push(op),
                    }
                }
                Ok((done, open))
            })
            .await?;
        tracing::info!(
            unfinished = open.len(),
            unacknowledged = done.len(),
            "boat executor recovered"
        );
        for (key, outcome) in done {
            self.emit(BackendEvent::Completed { key, outcome });
        }
        for op in open {
            self.run(op);
        }
        Ok(())
    }

    pub async fn submit(self: &Arc<Self>, d: CommandDelivery) -> anyhow::Result<()> {
        let key = d.command.key();
        let op = key.operation_id;
        let sealed = match &d.secrets {
            CommandSecrets::Bootstrap(env) => {
                Some(self.key.seal(&env.to_json(), op.to_string().as_bytes()))
            }
            CommandSecrets::None => None,
        };
        if d.command.action == Action::Spawn && sealed.is_none() {
            self.reject(
                key,
                FailureCode::SecretUnavailable,
                "spawn without guest env",
            );
            return Ok(());
        }
        let now = unix_ms_now();
        let new = Receipt {
            command: d.command.clone(),
            orb: d.orb.clone(),
            sealed_env: sealed,
            work_deadline_ms: now.saturating_add(d.budget.work_remaining_ms),
            cleanup_deadline_ms: now.saturating_add(d.budget.cleanup_remaining_ms),
            start: None,
            outcome: None,
            acked: false,
        };
        // Accept unless cancelled first; one transaction, so cancel and accept never
        // both win.
        let (tomb, receipt) = self
            .db
            .call(move |c| {
                let tx = c.transaction()?;
                if tombstoned(&tx, op)? {
                    return Ok((true, None));
                }
                if let Some(r) = get_receipt(&tx, op)? {
                    return Ok((false, Some(r)));
                }
                tx.execute(
                    "INSERT INTO receipts (operation_id, orb_id, command_json, orb_json, sealed_env,
                         work_deadline_ms, cleanup_deadline_ms, accepted_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        op.to_string(),
                        new.orb.id.to_string(),
                        js(&new.command),
                        js(&new.orb),
                        new.sealed_env,
                        new.work_deadline_ms as i64,
                        new.cleanup_deadline_ms as i64,
                        now as i64,
                    ],
                )?;
                tx.commit()?;
                Ok((false, Some(new)))
            })
            .await?;
        let Some(r) = receipt else {
            debug_assert!(tomb);
            self.reject(key, FailureCode::Cancelled, "cancelled before acceptance");
            return Ok(());
        };
        if r.command.command_digest != d.command.command_digest {
            self.reject(
                key,
                FailureCode::OperationConflict,
                "operation redelivered with a different command",
            );
            return Ok(());
        }
        match r.outcome {
            Some(outcome) if !r.acked => self.emit(BackendEvent::Completed { key, outcome }),
            Some(_) => {}
            None => {
                self.emit(BackendEvent::Accepted { key });
                self.run(op);
            }
        }
        Ok(())
    }

    pub async fn cancel(&self, key: CommandKey) -> anyhow::Result<()> {
        let op = key.operation_id;
        if let Some(t) = self
            .running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&op)
        {
            t.cancel();
            return Ok(());
        }
        let now = unix_ms_now() as i64;
        self.db
            .call(move |c| {
                c.execute(
                    "INSERT OR IGNORE INTO tombstones (operation_id, created_ms)
                     SELECT ?1, ?2 WHERE NOT EXISTS
                         (SELECT 1 FROM receipts WHERE operation_id = ?1)",
                    params![op.to_string(), now],
                )?;
                Ok(())
            })
            .await
    }

    pub async fn status(&self, key: CommandKey) -> anyhow::Result<OperationStatus> {
        let op = key.operation_id;
        let (tomb, r) = self
            .db
            .call(move |c| Ok((tombstoned(c, op)?, get_receipt(c, op)?)))
            .await?;
        Ok(match (r, tomb) {
            (Some(r), _) => match r.outcome {
                Some(o) => OperationStatus::Completed(o),
                None => OperationStatus::Running,
            },
            (None, true) => OperationStatus::CancelledBeforeAccept,
            (None, false) => OperationStatus::NotAccepted,
        })
    }

    pub async fn acknowledge(&self, key: CommandKey) -> anyhow::Result<()> {
        let now = unix_ms_now() as i64;
        self.db
            .call(move |c| {
                c.execute(
                    "UPDATE receipts SET acked_ms = ?2
                     WHERE operation_id = ?1 AND outcome_json IS NOT NULL AND acked_ms IS NULL",
                    params![key.operation_id.to_string(), now],
                )?;
                let cutoff = now - RETAIN_MS as i64;
                c.execute("DELETE FROM receipts WHERE acked_ms < ?1", [cutoff])?;
                c.execute("DELETE FROM tombstones WHERE created_ms < ?1", [cutoff])?;
                Ok(())
            })
            .await
    }

    /// Executes `op` in the background unless it already runs.
    fn run(self: &Arc<Self>, op: OperationId) {
        let token = {
            let mut running = self.running.lock().unwrap_or_else(|e| e.into_inner());
            if running.contains_key(&op) {
                return;
            }
            running.entry(op).or_default().clone()
        };
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.execute(op, token).await {
                tracing::error!(operation = %op, error = %format!("{e:#}"), "boat operation left unfinished; it re-runs after restart");
            }
            this.running
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&op);
        });
    }

    async fn execute(&self, op: OperationId, cancel: CancellationToken) -> anyhow::Result<()> {
        let Some(r) = self.db.call(move |c| get_receipt(c, op)).await? else {
            return Ok(());
        };
        let key = r.command.key();
        if let Some(outcome) = r.outcome {
            if !r.acked {
                self.emit(BackendEvent::Completed { key, outcome });
            }
            return Ok(());
        }
        let lock = self
            .orb_locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(r.orb.id)
            .or_default()
            .clone();
        let _serial = lock.lock().await;
        let ctx = Ctx {
            key,
            work_deadline: instant_at(r.work_deadline_ms),
            cleanup_deadline: instant_at(r.cleanup_deadline_ms),
            cancel,
            events: self.events.clone(),
            // The core ignores chunks with seq 0.
            log_seq: AtomicU64::new(1),
        };
        let publish = matches!(
            r.command.action,
            Action::Pause {
                publish_snapshot: Some(_)
            }
        );
        let snapshot = if publish {
            SnapshotDisposition::NotPublished {
                reason: "Boat orbs have no project snapshots".into(),
            }
        } else {
            SnapshotDisposition::NotRequested
        };
        let result = match &r.command.action {
            Action::Spawn => self.spawn(&r, &ctx).await,
            Action::Pause { .. } => self.pause(&r, &ctx).await,
            Action::Delete => self.delete(&r, &ctx).await,
        };
        let outcome = match result {
            Ok(f) => CommandOutcome {
                result: f.result,
                observed: f.observed,
                processes: f.processes,
                snapshot,
            },
            Err(Stop::Fatal(e)) => return Err(e),
            Err(stop) => {
                let failure = match stop {
                    Stop::Failed(f) => f,
                    _ if ctx.cancel.is_cancelled() => CommandFailure {
                        code: FailureCode::Cancelled,
                        message: format!("{} cancelled", r.command.action.name()),
                    },
                    _ => CommandFailure {
                        code: FailureCode::DeadlineExceeded,
                        message: format!("{} did not finish in time", r.command.action.name()),
                    },
                };
                ctx.log(&format!("failed: {}", failure.message));
                CommandOutcome {
                    result: Err(failure),
                    observed: self.observe(r.orb.id, &ctx).await,
                    processes: ProcessDisposition::Unknown,
                    snapshot,
                }
            }
        };
        let o = outcome.clone();
        self.db
            .call(move |c| {
                c.execute(
                    "UPDATE receipts SET outcome_json = ?2
                     WHERE operation_id = ?1 AND outcome_json IS NULL",
                    params![op.to_string(), js(&o)],
                )?;
                Ok(())
            })
            .await?;
        self.emit(BackendEvent::Completed { key, outcome });
        Ok(())
    }

    /// Runs a Boat call, repeating it on transient errors until the work deadline.
    async fn retrying<T, F, Fut>(&self, ctx: &Ctx, mut f: F) -> Result<Result<T, ApiError>, Stop>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ApiError>>,
    {
        let mut delay = Duration::from_millis(500);
        loop {
            match ctx.bounded(f()).await? {
                Err(e) if e.transient() => {
                    tracing::debug!(operation = %ctx.key.operation_id, error = %e, "boat call failed; retrying");
                    ctx.sleep(delay).await?;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
                r => return Ok(r),
            }
        }
    }

    async fn call<T, F, Fut>(&self, ctx: &Ctx, what: &str, f: F) -> Result<T, Stop>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, ApiError>>,
    {
        self.retrying(ctx, f)
            .await?
            .map_err(|e| Stop::Failed(api_failure(what, &e)))
    }

    async fn binding(&self, orb: OrbId) -> Result<Binding, Stop> {
        Ok(self.db.call(move |c| Ok(binding(c, orb)?)).await?)
    }

    /// The orb's sandbox in its current incarnation: the bound one, else one a create
    /// left behind before its response was lost (which is then bound).
    async fn find(&self, orb: OrbId, b: &Binding, ctx: &Ctx) -> Result<Option<Sandbox>, Stop> {
        if let Some(id) = &b.sandbox_id {
            return self
                .call(ctx, "reading the Boat sandbox", || self.api.get(id))
                .await;
        }
        let labels = labels(orb, b.incarnation);
        let found = self
            .call(ctx, "looking up the Boat sandbox", || {
                self.api.list(&labels)
            })
            .await?;
        let Some(sb) = found.into_iter().find(|s| s.state != "cancelled") else {
            return Ok(None);
        };
        self.bind(orb, b.incarnation, &sb.id).await?;
        Ok(Some(sb))
    }

    async fn bind(&self, orb: OrbId, incarnation: u64, id: &str) -> Result<(), Stop> {
        let id = id.to_owned();
        self.db
            .call(move |c| {
                c.execute(
                    "UPDATE sandboxes SET sandbox_id = ?3 WHERE orb_id = ?1 AND incarnation = ?2",
                    params![orb.to_string(), incarnation as i64, id],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// The orb's disk is gone: its next sandbox is a new incarnation.
    async fn replace(&self, orb: OrbId) -> Result<(), Stop> {
        self.db
            .call(move |c| {
                c.execute(
                    "UPDATE sandboxes SET incarnation = incarnation + 1, sandbox_id = NULL
                     WHERE orb_id = ?1",
                    [orb.to_string()],
                )?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Records how this spawn starts the orb (the first record wins), before it acts.
    async fn record_start(
        &self,
        op: OperationId,
        start: &mut Option<StartKind>,
        kind: StartKind,
    ) -> Result<(), Stop> {
        if start.is_some() {
            return Ok(());
        }
        self.db
            .call(move |c| {
                c.execute(
                    "UPDATE receipts SET start_json = ?2 WHERE operation_id = ?1",
                    params![op.to_string(), js(&kind)],
                )?;
                Ok(())
            })
            .await?;
        *start = Some(kind);
        Ok(())
    }

    /// Best-effort state of the orb for a failed result, within the cleanup deadline.
    async fn observe(&self, orb: OrbId, ctx: &Ctx) -> OrbState {
        let b = match self.db.call(move |c| Ok(binding(c, orb)?)).await {
            Ok(b) => b,
            Err(e) => {
                return OrbState::Faulted {
                    last_known: StableOrbState::Running,
                    reason: format!("{e:#}"),
                };
            }
        };
        if b.deleted {
            return OrbState::known(StableOrbState::Deleted);
        }
        let deadline = ctx
            .cleanup_deadline
            .max(Instant::now() + Duration::from_secs(5));
        let looked = match &b.sandbox_id {
            Some(id) => tokio::time::timeout_at(deadline.into(), self.api.get(id)).await,
            None => {
                tokio::time::timeout_at(deadline.into(), self.api.list(&labels(orb, b.incarnation)))
                    .await
                    .map(|r| r.map(|v| v.into_iter().find(|s| s.state != "cancelled")))
            }
        };
        match looked {
            Ok(Ok(sb)) => observed(sb.as_ref(), b.sandbox_id.is_some()),
            Ok(Err(e)) => OrbState::Faulted {
                last_known: StableOrbState::Running,
                reason: format!("state unknown: {e}"),
            },
            Err(_) => OrbState::Faulted {
                last_known: StableOrbState::Running,
                reason: "state unknown: the Boat API did not answer".into(),
            },
        }
    }

    // --- spawn ----------------------------------------------------------------

    async fn spawn(&self, r: &Receipt, ctx: &Ctx) -> Result<Finished, Stop> {
        let op = r.command.operation_id;
        let ExecutionTarget::Provider { profile } = &r.orb.spec.template.target else {
            return Err(failed(
                FailureCode::InvalidRequest,
                "not a Boat execution target",
            ));
        };
        let env = r
            .sealed_env
            .as_ref()
            .and_then(|s| self.key.open(s, op.to_string().as_bytes()).ok())
            .and_then(|plain| PermittedGuestEnv::from_json(&plain).ok())
            .ok_or_else(|| {
                failed(
                    FailureCode::SecretUnavailable,
                    "sealed guest env unreadable",
                )
            })?;
        let (id, start) = self.start(r, &profile.0, ctx).await?;
        let processes = match start {
            StartKind::NewDisk => ProcessDisposition::NotApplicable,
            StartKind::AlreadyRunning => ProcessDisposition::Preserved,
            StartKind::ExistingDiskColdBoot | StartKind::MemoryRestore => ProcessDisposition::Lost,
        };
        let running = OrbState::known(StableOrbState::Running);
        let result = match self.bootstrap(&id, &env, ctx).await? {
            Ok(()) => Ok(ActionResult::Spawned {
                start,
                bootstrap_generation: r.command.generation,
            }),
            Err(message) => {
                ctx.log(&message);
                Err(CommandFailure {
                    code: FailureCode::GuestBootstrapFailed,
                    message,
                })
            }
        };
        Ok(Finished {
            result,
            observed: running,
            processes,
        })
    }

    /// Makes the orb's sandbox run: creates, resumes or waits for it.
    async fn start(&self, r: &Receipt, size: &str, ctx: &Ctx) -> Result<(String, StartKind), Stop> {
        let orb = r.orb.id;
        let op = r.command.operation_id;
        let mut start = r.start;
        let mut resumed = false;
        loop {
            let b = self.binding(orb).await?;
            if b.deleted {
                return Err(failed(FailureCode::OrbDeleted, "the orb was deleted"));
            }
            let Some(sb) = self.find(orb, &b, ctx).await? else {
                if b.sandbox_id.is_some() {
                    ctx.log("the orb's Boat sandbox no longer exists; creating a new one (its files are gone)");
                    self.replace(orb).await?;
                    continue;
                }
                self.record_start(op, &mut start, StartKind::NewDisk)
                    .await?;
                ctx.log(&format!("creating a {size} Boat sandbox"));
                let labels = labels(orb, b.incarnation);
                let mut labels_with_name = labels.clone();
                let amp_id = r.orb.identity.amp_orb_id.as_str();
                labels_with_name.push(("amp-orb", amp_id.to_owned()));
                let name: String = amp_id.chars().take(120).collect();
                let idempotency_key = format!("orbd-{orb}-{}", b.incarnation);
                let request = CreateSandbox {
                    size,
                    name: &name,
                    labels: &labels_with_name,
                    auto_stop_secs: self.auto_stop_secs,
                };
                let sb = self
                    .call(ctx, "creating the Boat sandbox", || {
                        self.api.create(&idempotency_key, &request)
                    })
                    .await?;
                self.bind(orb, b.incarnation, &sb.id).await?;
                ctx.log(&format!("created Boat sandbox {}", sb.id));
                continue;
            };
            match sb.state.as_str() {
                "ready" | "idle" | "running" => {
                    ctx.log(&format!("Boat sandbox {} is running", sb.id));
                    return Ok((sb.id, start.unwrap_or(StartKind::AlreadyRunning)));
                }
                "init" | "provisioning" | "provisioned" | "cloning" | "archiving" => {
                    // A start nobody recorded was made by an earlier, unfinished spawn.
                    if sb.state != "archiving" && start.is_none() {
                        self.record_start(op, &mut start, StartKind::ExistingDiskColdBoot)
                            .await?;
                    }
                    ctx.sleep(POLL).await?;
                }
                "archived" if sb.data_expired_at.is_some() => {
                    ctx.log(&format!(
                        "Boat deleted the disk of stopped sandbox {} (it keeps one for 14 days); \
                         replacing it with a new sandbox (the orb's files are gone)",
                        sb.id
                    ));
                    self.call(ctx, "deleting the expired Boat sandbox", || {
                        self.api.delete(&sb.id)
                    })
                    .await?;
                    self.replace(orb).await?;
                }
                "archived" | "error" if !resumed => {
                    if sb.state == "error" {
                        ctx.log(&format!(
                            "Boat sandbox {} is in error ({}); resuming it",
                            sb.id,
                            sb.error.as_deref().unwrap_or("no reason given")
                        ));
                    } else {
                        ctx.log(&format!("resuming Boat sandbox {}", sb.id));
                    }
                    self.record_start(op, &mut start, StartKind::ExistingDiskColdBoot)
                        .await?;
                    self.call(ctx, "resuming the Boat sandbox", || {
                        self.api.resume(&sb.id, self.auto_stop_secs)
                    })
                    .await?;
                    resumed = true;
                }
                "archived" => ctx.sleep(POLL).await?,
                "error" => {
                    return Err(failed(
                        FailureCode::BackendFailure,
                        format!(
                            "Boat sandbox {} failed to start: {}",
                            sb.id,
                            sb.error.as_deref().unwrap_or("no reason given")
                        ),
                    ));
                }
                "cancelled" => {
                    ctx.log("Boat cancelled the sandbox (no machine was free); creating another");
                    self.replace(orb).await?;
                }
                other => {
                    return Err(failed(
                        FailureCode::BackendFailure,
                        format!("Boat sandbox {} is in unknown state {other}", sb.id),
                    ));
                }
            }
        }
    }

    /// Delivers the guest env and starts `amp orb bootstrap`. `Ok(Err)` is a bootstrap
    /// failure (with redacted output); the sandbox keeps running.
    async fn bootstrap(
        &self,
        id: &str,
        env: &PermittedGuestEnv,
        ctx: &Ctx,
    ) -> Result<Result<(), String>, Stop> {
        let body = match env_file(env) {
            Ok(b) => b,
            Err(e) => return Ok(Err(e)),
        };
        self.call(ctx, "writing the bootstrap environment", || {
            self.api.write_file(id, ENV_PATH, &body)
        })
        .await?;
        ctx.log("installing the Amp CLI if needed");
        let out = self
            .call(ctx, "installing the Amp CLI", || {
                self.api.run(id, INSTALL_AMP, ctx.remaining_secs().min(300))
            })
            .await?;
        if out.exit_code != Some(0) {
            return Ok(Err(redact(
                &format!(
                    "installing the Amp CLI failed ({}):\n{}",
                    if out.timed_out {
                        "timed out".to_owned()
                    } else {
                        format!("exit {:?}", out.exit_code)
                    },
                    tail(&out.stdout, &out.stderr)
                ),
                env,
            )));
        }
        ctx.log(&format!(
            "Amp CLI {}",
            out.stdout.lines().last().unwrap_or_default().trim()
        ));
        ctx.log("starting amp orb bootstrap");
        let process = self
            .call(ctx, "starting amp orb bootstrap", || {
                self.api.start_detached(id, START_BOOTSTRAP)
            })
            .await?;
        ctx.sleep(BOOTSTRAP_SETTLE).await?;
        let st = self
            .call(ctx, "checking amp orb bootstrap", || {
                self.api.command_status(id, process)
            })
            .await?;
        if !st.running {
            return Ok(Err(redact(
                &format!(
                    "amp orb bootstrap exited (exit {:?}):\n{}",
                    st.exit_code,
                    tail(&st.stdout, &st.stderr)
                ),
                env,
            )));
        }
        ctx.log("amp orb bootstrap is running");
        Ok(Ok(()))
    }

    // --- pause ----------------------------------------------------------------

    async fn pause(&self, r: &Receipt, ctx: &Ctx) -> Result<Finished, Stop> {
        let orb = r.orb.id;
        let paused = |observed, processes| Finished {
            result: Ok(ActionResult::Paused { checkpoint: None }),
            observed,
            processes,
        };
        let b = self.binding(orb).await?;
        let bound = b.sandbox_id.is_some();
        let Some(mut sb) = self.find(orb, &b, ctx).await? else {
            return Ok(paused(
                observed(None, bound),
                ProcessDisposition::NotApplicable,
            ));
        };
        let mut stop_sent: Option<Instant> = None;
        loop {
            match sb.state.as_str() {
                "archived" => {
                    ctx.log(&format!("Boat sandbox {} stopped", sb.id));
                    return Ok(paused(observed(Some(&sb), true), ProcessDisposition::Lost));
                }
                "cancelled" => {
                    return Ok(paused(
                        observed(Some(&sb), true),
                        ProcessDisposition::NotApplicable,
                    ));
                }
                "archiving" => {}
                _ if stop_sent.is_none_or(|t| t.elapsed() >= STOP_RESEND) => {
                    ctx.log(&format!(
                        "stopping Boat sandbox {} (Boat keeps its disk, not its processes)",
                        sb.id
                    ));
                    self.call(ctx, "stopping the Boat sandbox", || self.api.stop(&sb.id))
                        .await?;
                    stop_sent = Some(Instant::now());
                }
                _ => {}
            }
            ctx.sleep(POLL).await?;
            let id = sb.id.clone();
            sb = match self
                .call(ctx, "reading the Boat sandbox", || self.api.get(&id))
                .await?
            {
                Some(s) => s,
                None => {
                    return Ok(paused(observed(None, true), ProcessDisposition::Lost));
                }
            };
        }
    }

    // --- delete ---------------------------------------------------------------

    async fn delete(&self, r: &Receipt, ctx: &Ctx) -> Result<Finished, Stop> {
        let orb = r.orb.id;
        let b = self.binding(orb).await?;
        // Every incarnation's sandbox, in case one outlived its replacement.
        let all = [("orbd-orb", orb.to_string())];
        let mut ids: Vec<String> = b.sandbox_id.into_iter().collect();
        for sb in self
            .call(ctx, "listing the orb's Boat sandboxes", || {
                self.api.list(&all)
            })
            .await?
        {
            if !ids.contains(&sb.id) {
                ids.push(sb.id);
            }
        }
        for id in &ids {
            ctx.log(&format!("deleting Boat sandbox {id}"));
            self.call(ctx, "deleting the Boat sandbox", || self.api.delete(id))
                .await?;
        }
        let now = unix_ms_now() as i64;
        self.db
            .call(move |c| {
                c.execute(
                    "UPDATE sandboxes SET deleted_ms = ?2 WHERE orb_id = ?1 AND deleted_ms IS NULL",
                    params![orb.to_string(), now],
                )?;
                Ok(())
            })
            .await?;
        Ok(Finished {
            result: Ok(ActionResult::Deleted),
            observed: OrbState::known(StableOrbState::Deleted),
            processes: ProcessDisposition::NotApplicable,
        })
    }
}

/// Labels that find one incarnation of an orb's sandbox.
fn labels(orb: OrbId, incarnation: u64) -> Vec<(&'static str, String)> {
    vec![
        ("orbd-orb", orb.to_string()),
        ("orbd-disk", incarnation.to_string()),
    ]
}
