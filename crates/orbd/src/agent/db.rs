//! Agent SQLite store, owned by one dedicated thread that runs closures sent over a
//! channel. All delivery decisions and result commits are single transactions.
//!
//! Tables: `meta` (highest controller epoch), `receipts` (one row per accepted
//! operation: command + orb definition, sealed guest env, pinned deadlines, progress
//! markers, terminal outcome, acked), `tombstones` (cancel-before-accept) and `orbs`
//! (highest accepted generation, created flag, last observed state, next checkpoint
//! generation, maintenance restore intent).

use crate::model::{
    Command, CommandFailure, CommandOutcome, FailureCode, OrbDefinition, OrbState, StableOrbState,
    StartKind,
};
use crate::model::{OperationId, OrbId, SleepCheckpointRef};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use std::path::Path;

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

#[derive(Clone)]
pub struct Db {
    tx: std::sync::mpsc::Sender<Job>,
}

impl Db {
    /// Opens (creating if needed) the database and starts the owning thread.
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let mut conn = Connection::open(path)?;
        init(&mut conn)?;
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("orbd-agent-db".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    job(&mut conn);
                }
            })?;
        Ok(Self { tx })
    }

    /// Runs `f` on the DB thread.
    pub async fn call<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> rusqlite::Result<R> + Send + 'static,
    ) -> anyhow::Result<R> {
        let (otx, orx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Box::new(move |c| {
                let _ = otx.send(f(c));
            }))
            .map_err(|_| anyhow::anyhow!("agent database thread has stopped"))?;
        Ok(orx
            .await
            .map_err(|_| anyhow::anyhow!("agent database thread has stopped"))??)
    }
}

/// Bump on any schema change; there is no upgrade path (greenfield).
const SCHEMA_VERSION: i64 = 2;

const SCHEMA: &str = "CREATE TABLE meta (
            key TEXT PRIMARY KEY, value TEXT NOT NULL);
         CREATE TABLE receipts (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_id TEXT NOT NULL UNIQUE,
            orb_id TEXT NOT NULL,
            generation INTEGER NOT NULL,
            command_digest TEXT NOT NULL,
            command_json TEXT NOT NULL,
            orb_json TEXT NOT NULL,
            sealed_env BLOB NOT NULL,
            work_deadline_unix_ms INTEGER NOT NULL,
            cleanup_deadline_unix_ms INTEGER NOT NULL,
            accepted_at_unix_ms INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('accepted','terminal')),
            progress_json TEXT NOT NULL DEFAULT '{}',
            outcome_json TEXT,
            completed_at_unix_ms INTEGER,
            acked INTEGER NOT NULL DEFAULT 0);
         CREATE INDEX receipts_status ON receipts(status, acked);
         CREATE TABLE tombstones (
            operation_id TEXT PRIMARY KEY,
            orb_id TEXT NOT NULL,
            recorded_at_unix_ms INTEGER NOT NULL);
         CREATE TABLE orbs (
            orb_id TEXT PRIMARY KEY,
            highest_generation INTEGER NOT NULL,
            created INTEGER NOT NULL DEFAULT 0,
            last_state_json TEXT,
            next_checkpoint_generation INTEGER NOT NULL DEFAULT 1,
            restore_json TEXT);
         CREATE TABLE project_snapshots (
            key TEXT PRIMARY KEY,
            published_at_unix_ms INTEGER NOT NULL);";

fn init(conn: &mut Connection) -> anyhow::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    let current: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    match current {
        SCHEMA_VERSION => Ok(()),
        0 => {
            let tx = conn.transaction()?;
            tx.execute_batch(SCHEMA)?;
            tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            tx.commit()?;
            Ok(())
        }
        v => anyhow::bail!(
            "agent database schema version {v} is not {SCHEMA_VERSION} (this orbd); orbd has \
             no upgrade path: delete the agent state directory to start over"
        ),
    }
}

// --- records ----------------------------------------------------------------

/// Durable step markers so a restarted executor knows what it already decided/did.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Progress {
    pub start: Option<StartPlan>,
    pub bootstrapped: bool,
    pub pause: Option<PausePlan>,
    pub checkpoint_failed: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartPlan {
    pub kind: StartKind,
    pub checkpoint: Option<SleepCheckpointRef>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PausePlan {
    Checkpoint { generation: u64 },
    ColdStop,
}

#[derive(Clone, Debug)]
pub struct Receipt {
    pub operation_id: OperationId,
    pub command: Command,
    pub orb: OrbDefinition,
    pub sealed_env: Vec<u8>,
    pub work_deadline_unix_ms: u64,
    pub cleanup_deadline_unix_ms: u64,
    pub terminal: bool,
    pub progress: Progress,
    pub outcome: Option<CommandOutcome>,
}

#[derive(Clone, Debug)]
pub struct OrbRow {
    pub orb_id: OrbId,
    pub highest_generation: u64,
    pub created: bool,
    pub last_state: Option<OrbState>,
}

fn json<T: Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("serializes")
}

fn from_json<T: for<'de> Deserialize<'de>>(s: &str) -> rusqlite::Result<T> {
    serde_json::from_str(s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}

const RECEIPT_COLS: &str = "command_json, orb_json, sealed_env, work_deadline_unix_ms, \
     cleanup_deadline_unix_ms, status, progress_json, outcome_json, operation_id";

fn receipt_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Receipt> {
    let outcome: Option<String> = r.get(7)?;
    let op: String = r.get(8)?;
    Ok(Receipt {
        operation_id: crate::model::wire::operation_id(&op).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e))
        })?,
        command: from_json(&r.get::<_, String>(0)?)?,
        orb: from_json(&r.get::<_, String>(1)?)?,
        sealed_env: r.get(2)?,
        work_deadline_unix_ms: r.get::<_, i64>(3)? as u64,
        cleanup_deadline_unix_ms: r.get::<_, i64>(4)? as u64,
        terminal: r.get::<_, String>(5)? == "terminal",
        progress: from_json(&r.get::<_, String>(6)?)?,
        outcome: outcome.map(|s| from_json(&s)).transpose()?,
    })
}

pub fn get_receipt(conn: &Connection, op: OperationId) -> rusqlite::Result<Option<Receipt>> {
    conn.query_row(
        &format!("SELECT {RECEIPT_COLS} FROM receipts WHERE operation_id = ?1"),
        [op.to_string()],
        receipt_from_row,
    )
    .optional()
}

/// Accepted-but-not-terminal receipts in acceptance order.
pub fn list_unfinished(conn: &Connection) -> rusqlite::Result<Vec<Receipt>> {
    let mut st = conn.prepare(&format!(
        "SELECT {RECEIPT_COLS} FROM receipts WHERE status = 'accepted' ORDER BY seq"
    ))?;
    st.query_map([], receipt_from_row)?.collect()
}

/// Terminal receipts whose completion has not been acknowledged.
pub fn list_unacked(conn: &Connection) -> rusqlite::Result<Vec<Receipt>> {
    let mut st = conn.prepare(&format!(
        "SELECT {RECEIPT_COLS} FROM receipts WHERE status = 'terminal' AND acked = 0 ORDER BY seq"
    ))?;
    st.query_map([], receipt_from_row)?.collect()
}

// --- meta / epoch -----------------------------------------------------------

pub fn highest_epoch(conn: &Connection) -> rusqlite::Result<u64> {
    let v: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'controller_epoch'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    Ok(v.and_then(|s| s.parse().ok()).unwrap_or(0))
}

fn set_epoch(tx: &Transaction<'_>, epoch: u64) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO meta(key, value) VALUES ('controller_epoch', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [epoch.to_string()],
    )?;
    Ok(())
}

/// Records `epoch` if higher than seen. Returns false when `epoch` is stale.
pub fn observe_epoch(conn: &mut Connection, epoch: u64) -> rusqlite::Result<bool> {
    let tx = conn.transaction()?;
    let cur = highest_epoch(&tx)?;
    if epoch > cur {
        set_epoch(&tx, epoch)?;
    }
    tx.commit()?;
    Ok(epoch >= cur)
}

// --- delivery decision ------------------------------------------------------

pub struct NewReceipt {
    pub command: Command,
    pub orb: OrbDefinition,
    pub sealed_env: Vec<u8>,
    pub work_deadline_unix_ms: u64,
    pub cleanup_deadline_unix_ms: u64,
    pub now_unix_ms: u64,
}

pub enum Decision {
    Rejected(CommandFailure),
    /// Known operation with the same digest: replay Accepted (+ Completed if terminal).
    Replay(Box<Receipt>),
    /// Newly and durably accepted.
    Accepted,
}

fn reject(code: FailureCode, msg: impl Into<String>) -> Decision {
    Decision::Rejected(CommandFailure {
        code,
        message: msg.into(),
    })
}

/// Steps 1–5 of delivery handling in one transaction.
pub fn decide_delivery(
    conn: &mut Connection,
    epoch: u64,
    new: NewReceipt,
) -> rusqlite::Result<Decision> {
    let tx = conn.transaction()?;
    let cur = highest_epoch(&tx)?;
    if epoch < cur {
        return Ok(reject(
            FailureCode::StaleController,
            format!("controller epoch {epoch} is below the highest seen ({cur})"),
        ));
    }
    if epoch > cur {
        set_epoch(&tx, epoch)?;
    }
    let op = new.command.operation_id;
    if let Some(existing) = get_receipt(&tx, op)? {
        tx.commit()?;
        if existing.command.command_digest != new.command.command_digest {
            return Ok(reject(
                FailureCode::OperationConflict,
                "operation id reused with a different command digest",
            ));
        }
        return Ok(Decision::Replay(Box::new(existing)));
    }
    let tomb: bool = tx
        .query_row(
            "SELECT 1 FROM tombstones WHERE operation_id = ?1",
            [op.to_string()],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);
    if tomb {
        tx.commit()?;
        return Ok(reject(
            FailureCode::Cancelled,
            "cancelled before acceptance",
        ));
    }
    let orb = new.command.orb_id.to_string();
    let highest: Option<i64> = tx
        .query_row(
            "SELECT highest_generation FROM orbs WHERE orb_id = ?1",
            [&orb],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(h) = highest
        && new.command.generation <= h as u64
    {
        tx.commit()?;
        return Ok(reject(
            FailureCode::StaleGeneration,
            format!(
                "generation {} is not above the highest accepted ({h})",
                new.command.generation
            ),
        ));
    }
    // Any accepted command supersedes a maintenance restore intent.
    tx.execute(
        "INSERT INTO orbs(orb_id, highest_generation) VALUES (?1, ?2)
         ON CONFLICT(orb_id) DO UPDATE SET highest_generation = excluded.highest_generation,
            restore_json = NULL",
        params![orb, new.command.generation as i64],
    )?;
    tx.execute(
        "INSERT INTO receipts(operation_id, orb_id, generation, command_digest, command_json,
            orb_json, sealed_env, work_deadline_unix_ms, cleanup_deadline_unix_ms,
            accepted_at_unix_ms, status)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'accepted')",
        params![
            op.to_string(),
            orb,
            new.command.generation as i64,
            new.command.command_digest.0,
            json(&new.command),
            json(&new.orb),
            new.sealed_env,
            new.work_deadline_unix_ms as i64,
            new.cleanup_deadline_unix_ms as i64,
            new.now_unix_ms as i64,
        ],
    )?;
    tx.commit()?;
    Ok(Decision::Accepted)
}

// --- cancel -----------------------------------------------------------------

pub enum CancelDecision {
    RecordedBeforeAccept,
    Cancelling,
    AlreadyTerminal,
}

pub fn cancel_operation(
    conn: &mut Connection,
    op: OperationId,
    orb: OrbId,
    now: u64,
) -> rusqlite::Result<CancelDecision> {
    let tx = conn.transaction()?;
    let status: Option<String> = tx
        .query_row(
            "SELECT status FROM receipts WHERE operation_id = ?1",
            [op.to_string()],
            |r| r.get(0),
        )
        .optional()?;
    let d = match status.as_deref() {
        Some("terminal") => CancelDecision::AlreadyTerminal,
        Some(_) => CancelDecision::Cancelling,
        None => {
            tx.execute(
                "INSERT OR IGNORE INTO tombstones(operation_id, orb_id, recorded_at_unix_ms)
                 VALUES (?1, ?2, ?3)",
                params![op.to_string(), orb.to_string(), now as i64],
            )?;
            CancelDecision::RecordedBeforeAccept
        }
    };
    tx.commit()?;
    Ok(d)
}

pub fn tombstone_orb(conn: &Connection, op: OperationId) -> rusqlite::Result<Option<OrbId>> {
    let v: Option<String> = conn
        .query_row(
            "SELECT orb_id FROM tombstones WHERE operation_id = ?1",
            [op.to_string()],
            |r| r.get(0),
        )
        .optional()?;
    Ok(v.and_then(|s| crate::model::wire::orb_id(&s).ok()))
}

// --- executor writes --------------------------------------------------------

pub fn save_progress(conn: &Connection, op: OperationId, p: &Progress) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE receipts SET progress_json = ?2 WHERE operation_id = ?1",
        params![op.to_string(), json(p)],
    )?;
    Ok(())
}

/// Durably records "this operation is about to create the orb's disk": sets the orb's
/// created flag and the start plan in one transaction.
pub fn begin_create(
    conn: &mut Connection,
    op: OperationId,
    orb: OrbId,
    progress: &Progress,
) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "UPDATE orbs SET created = 1 WHERE orb_id = ?1",
        [orb.to_string()],
    )?;
    save_progress(&tx, op, progress)?;
    tx.commit()
}

/// The orb has no disk after all (first start failed before creating anything).
pub fn clear_created(conn: &Connection, orb: OrbId) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE orbs SET created = 0 WHERE orb_id = ?1",
        [orb.to_string()],
    )?;
    Ok(())
}

/// Allocates the orb's next checkpoint generation and records it in the progress.
pub fn allocate_checkpoint_generation(
    conn: &mut Connection,
    op: OperationId,
    orb: OrbId,
    progress: &mut Progress,
) -> rusqlite::Result<u64> {
    let tx = conn.transaction()?;
    let g: i64 = tx.query_row(
        "SELECT next_checkpoint_generation FROM orbs WHERE orb_id = ?1",
        [orb.to_string()],
        |r| r.get(0),
    )?;
    tx.execute(
        "UPDATE orbs SET next_checkpoint_generation = ?2 WHERE orb_id = ?1",
        params![orb.to_string(), g + 1],
    )?;
    progress.pause = Some(PausePlan::Checkpoint {
        generation: g as u64,
    });
    save_progress(&tx, op, progress)?;
    tx.commit()?;
    Ok(g as u64)
}

/// Allocates the orb's next checkpoint generation outside any operation (agent drain).
pub fn allocate_maintenance_checkpoint(conn: &mut Connection, orb: OrbId) -> rusqlite::Result<u64> {
    let tx = conn.transaction()?;
    let g: i64 = tx.query_row(
        "SELECT next_checkpoint_generation FROM orbs WHERE orb_id = ?1",
        [orb.to_string()],
        |r| r.get(0),
    )?;
    tx.execute(
        "UPDATE orbs SET next_checkpoint_generation = ?2 WHERE orb_id = ?1",
        params![orb.to_string(), g + 1],
    )?;
    tx.commit()?;
    Ok(g as u64)
}

/// Restore the orb from `checkpoint` when the agent starts again, unless a command
/// with a generation above `generation` is accepted first.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreIntent {
    pub generation: u64,
    pub checkpoint: SleepCheckpointRef,
}

/// Records what a maintenance step observed, with the restore intent it leaves.
pub fn record_maintenance(
    conn: &Connection,
    orb: OrbId,
    state: &OrbState,
    intent: Option<&RestoreIntent>,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE orbs SET last_state_json = ?2, restore_json = ?3 WHERE orb_id = ?1",
        params![orb.to_string(), json(state), intent.map(json)],
    )?;
    Ok(())
}

/// Records a project snapshot published on this host (idempotent).
pub fn record_project_snapshot(
    conn: &Connection,
    key: &crate::model::ProjectSnapshotKey,
    now: u64,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO project_snapshots(key, published_at_unix_ms) VALUES (?1, ?2)",
        params![key.as_str(), now as i64],
    )?;
    Ok(())
}

/// Forgets a project snapshot deleted from this host.
pub fn forget_project_snapshot(conn: &Connection, key: &str) -> rusqlite::Result<()> {
    conn.execute("DELETE FROM project_snapshots WHERE key = ?1", params![key])?;
    Ok(())
}

pub fn list_project_snapshots(conn: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut st = conn.prepare("SELECT key FROM project_snapshots ORDER BY key")?;
    st.query_map([], |r| r.get(0))?.collect()
}

pub fn set_last_state(conn: &Connection, orb: OrbId, state: &OrbState) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE orbs SET last_state_json = ?2 WHERE orb_id = ?1",
        params![orb.to_string(), json(state)],
    )?;
    Ok(())
}

pub fn list_restore_intents(conn: &Connection) -> rusqlite::Result<Vec<(OrbRow, RestoreIntent)>> {
    let mut st = conn.prepare(&format!(
        "SELECT {ORB_COLS}, restore_json FROM orbs WHERE restore_json IS NOT NULL ORDER BY orb_id"
    ))?;
    st.query_map([], |r| {
        Ok((orb_from_row(r)?, from_json(&r.get::<_, String>(4)?)?))
    })?
    .collect()
}

pub fn complete(
    conn: &mut Connection,
    op: OperationId,
    orb: OrbId,
    outcome: &CommandOutcome,
    now: u64,
) -> rusqlite::Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "UPDATE receipts SET status = 'terminal', outcome_json = ?2, completed_at_unix_ms = ?3
         WHERE operation_id = ?1",
        params![op.to_string(), json(outcome), now as i64],
    )?;
    tx.execute(
        "UPDATE orbs SET last_state_json = ?2 WHERE orb_id = ?1",
        params![orb.to_string(), json(&outcome.observed)],
    )?;
    tx.commit()
}

pub fn ack(conn: &Connection, op: OperationId) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE receipts SET acked = 1 WHERE operation_id = ?1 AND status = 'terminal'",
        [op.to_string()],
    )?;
    Ok(())
}

// --- orbs -------------------------------------------------------------------

fn orb_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<OrbRow> {
    let id: String = r.get(0)?;
    let state: Option<String> = r.get(3)?;
    Ok(OrbRow {
        orb_id: crate::model::wire::orb_id(&id).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })?,
        highest_generation: r.get::<_, i64>(1)? as u64,
        created: r.get::<_, i64>(2)? != 0,
        last_state: state.map(|s| from_json(&s)).transpose()?,
    })
}

const ORB_COLS: &str = "orb_id, highest_generation, created, last_state_json";

pub fn get_orb(conn: &Connection, orb: OrbId) -> rusqlite::Result<Option<OrbRow>> {
    conn.query_row(
        &format!("SELECT {ORB_COLS} FROM orbs WHERE orb_id = ?1"),
        [orb.to_string()],
        orb_from_row,
    )
    .optional()
}

/// Orbs with `orb_id > after`, in id order.
pub fn list_orbs(conn: &Connection, after: &str, limit: usize) -> rusqlite::Result<Vec<OrbRow>> {
    let mut st = conn.prepare(&format!(
        "SELECT {ORB_COLS} FROM orbs WHERE orb_id > ?1 ORDER BY orb_id LIMIT ?2"
    ))?;
    st.query_map(params![after, limit as i64], orb_from_row)?
        .collect()
}

/// The most recent orb definition for every orb the agent has accepted work for.
pub fn list_orb_definitions(conn: &Connection) -> rusqlite::Result<Vec<OrbDefinition>> {
    let mut st = conn.prepare(
        "SELECT orb_json FROM receipts WHERE seq IN (SELECT MAX(seq) FROM receipts GROUP BY orb_id)",
    )?;
    st.query_map([], |r| from_json(&r.get::<_, String>(0)?))?
        .collect()
}

impl OrbRow {
    pub fn is_deleted(&self) -> bool {
        matches!(
            self.last_state.as_ref().map(OrbState::last_known),
            Some(StableOrbState::Deleted)
        )
    }
}
