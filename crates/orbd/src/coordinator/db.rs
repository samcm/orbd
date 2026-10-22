//! SQLite owned by one dedicated thread. Async code sends closures; no connection is
//! ever shared across tasks.

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use std::path::Path;
use tokio::sync::oneshot;

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

#[derive(Clone)]
pub struct Db {
    tx: std::sync::mpsc::Sender<Job>,
}

/// Schema migrations, applied in order; `PRAGMA user_version` records progress.
const MIGRATIONS: &[&str] = &[r#"
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);

CREATE TABLE orbs (
    id TEXT PRIMARY KEY,
    amp_endpoint TEXT NOT NULL,
    amp_orb_id TEXT NOT NULL,
    offering_id TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    spec_digest TEXT NOT NULL,
    location_json TEXT NOT NULL,
    last_issued_generation INTEGER NOT NULL,
    observation_json TEXT NOT NULL,
    observation_generation INTEGER NOT NULL,
    created_ms INTEGER NOT NULL,
    UNIQUE (amp_endpoint, amp_orb_id)
);
CREATE TRIGGER orbs_identity_immutable
BEFORE UPDATE OF id, amp_endpoint, amp_orb_id, offering_id, spec_json, spec_digest, location_json
ON orbs
BEGIN
    SELECT RAISE(ABORT, 'orb offering, spec and location are immutable');
END;

CREATE TABLE commands (
    operation_id TEXT PRIMARY KEY,
    orb_id TEXT NOT NULL REFERENCES orbs(id),
    generation INTEGER NOT NULL,
    action_json TEXT NOT NULL,
    command_digest TEXT NOT NULL,
    fingerprint TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'terminal', 'unknown')),
    outcome_json TEXT,
    created_ms INTEGER NOT NULL,
    submitted_ms INTEGER,
    accepted_ms INTEGER,
    terminal_ms INTEGER,
    work_deadline_ms INTEGER NOT NULL,
    cleanup_deadline_ms INTEGER NOT NULL,
    cancel_requested_ms INTEGER,
    UNIQUE (orb_id, generation)
);
CREATE UNIQUE INDEX one_open_command_per_orb ON commands (orb_id) WHERE status <> 'terminal';

CREATE TABLE requests (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    invocation_id TEXT NOT NULL UNIQUE,
    offering_id TEXT NOT NULL,
    amp_endpoint TEXT NOT NULL,
    amp_orb_id TEXT NOT NULL,
    action_json TEXT NOT NULL,
    fingerprint TEXT,
    status TEXT NOT NULL CHECK (status IN ('queued', 'active', 'finished')),
    start_ms INTEGER NOT NULL,
    work_deadline_ms INTEGER NOT NULL,
    cleanup_deadline_ms INTEGER NOT NULL,
    dispatch_deadline_ms INTEGER NOT NULL,
    operation_id TEXT,
    result_json TEXT
);
CREATE INDEX requests_queue ON requests (status, amp_endpoint, amp_orb_id, seq);
CREATE INDEX requests_operation ON requests (operation_id);

CREATE TABLE reservations (
    orb_id TEXT PRIMARY KEY REFERENCES orbs(id),
    backend_id TEXT NOT NULL,
    pool TEXT NOT NULL,
    vcpus INTEGER NOT NULL,
    memory_bytes INTEGER NOT NULL,
    disk_bytes INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('running', 'stopped'))
);
CREATE INDEX reservations_pool ON reservations (backend_id, pool);

CREATE TABLE delete_tombstones (
    amp_endpoint TEXT NOT NULL,
    amp_orb_id TEXT NOT NULL,
    orb_id TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    PRIMARY KEY (amp_endpoint, amp_orb_id)
);

-- Sealed guest env. owner is an operation id, or an invocation id while the request
-- is still queued. aad = owner.
CREATE TABLE secret_payloads (
    owner TEXT PRIMARY KEY,
    sealed BLOB NOT NULL,
    created_ms INTEGER NOT NULL
);
"#];

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let mut conn =
            Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        migrate(&mut conn)?;
        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("orbd-db".into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    job(&mut conn);
                }
            })?;
        Ok(Self { tx })
    }

    pub async fn call<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (otx, orx) = oneshot::channel();
        let job: Job = Box::new(move |c| {
            let _ = otx.send(f(c));
        });
        self.tx
            .send(job)
            .map_err(|_| anyhow::anyhow!("database thread has stopped"))?;
        orx.await
            .map_err(|_| anyhow::anyhow!("database thread dropped the request"))?
    }
}

fn migrate(conn: &mut Connection) -> Result<()> {
    let current: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if current as usize > MIGRATIONS.len() {
        bail!(
            "database schema version {current} is newer than this orbd supports ({})",
            MIGRATIONS.len()
        );
    }
    for (i, sql) in MIGRATIONS.iter().enumerate().skip(current as usize) {
        let tx = conn.transaction()?;
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", (i + 1) as i64)?;
        tx.commit()?;
    }
    Ok(())
}
