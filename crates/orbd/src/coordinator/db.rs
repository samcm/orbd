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

/// Bump on any schema change. There are no upgrade paths: a database with another
/// version is refused (delete the state directory to start over).
const SCHEMA_VERSION: i64 = 2;

const SCHEMA: &str = r#"
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
    -- Set when a deleted orb is replaced by a new incarnation with the same Amp id.
    retired_ms INTEGER
);
CREATE UNIQUE INDEX orbs_identity ON orbs (amp_endpoint, amp_orb_id) WHERE retired_ms IS NULL;
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

-- One per orb incarnation: once deleted, that incarnation (its disk, its location) can
-- never run again. A later spawn for the same Amp id creates a new incarnation.
CREATE TABLE delete_tombstones (
    amp_endpoint TEXT NOT NULL,
    amp_orb_id TEXT NOT NULL,
    orb_id TEXT PRIMARY KEY,
    created_ms INTEGER NOT NULL
);

-- Sealed guest env. owner is an operation id, or an invocation id while the request
-- is still queued. aad = owner.
CREATE TABLE secret_payloads (
    owner TEXT PRIMARY KEY,
    sealed BLOB NOT NULL,
    created_ms INTEGER NOT NULL
);

-- Agent registry: the source of truth for which agents may connect. Removed agents stay
-- as tombstones (their orbs keep referencing the id; the id is never reused).
CREATE TABLE agents (
    id TEXT PRIMARY KEY,
    -- SHA-256 (hex) of the agent's public key; certificates for any other key are refused.
    key_sha256 TEXT NOT NULL,
    cert_not_after_ms INTEGER NOT NULL,
    enrolled_ms INTEGER NOT NULL,
    removed_ms INTEGER
);

-- Single-use join tokens. Only a hash of the secret is stored.
CREATE TABLE join_tokens (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    secret_sha256 TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    expires_ms INTEGER NOT NULL,
    revoked_ms INTEGER,
    redeemed_ms INTEGER,
    -- Set on redemption, so a retry with the same CSR gets the same certificate.
    csr_sha256 TEXT,
    cert_der BLOB
);
CREATE INDEX join_tokens_agent ON join_tokens (agent_id);
"#;

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, SCHEMA, SCHEMA_VERSION)
    }

    /// Opens a database with its own schema (backend-private stores, e.g. Boat's).
    pub(crate) fn open_with(path: &Path, schema: &str, version: i64) -> Result<Self> {
        let mut conn =
            Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        migrate(&mut conn, path, schema, version)?;
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

fn migrate(conn: &mut Connection, path: &Path, schema: &str, version: i64) -> Result<()> {
    let current: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    match current {
        v if v == version => Ok(()),
        0 => {
            let tx = conn.transaction()?;
            tx.execute_batch(schema)?;
            tx.pragma_update(None, "user_version", version)?;
            tx.commit()?;
            Ok(())
        }
        v => bail!(
            "{}: schema version {v} is not {version} (this orbd); orbd has no upgrade path: \
             delete the coordinator state directory to start over",
            path.display()
        ),
    }
}
