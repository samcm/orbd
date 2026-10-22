//! Typed SQLite access. Every function runs on the DB thread.

use super::types::*;
use crate::model::*;
use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};

fn js<T: Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("serializable")
}

fn conv<E: std::error::Error + Send + Sync + 'static>(idx: usize, e: E) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, Box::new(e))
}

fn de<T: DeserializeOwned>(row: &Row, idx: usize) -> rusqlite::Result<T> {
    let s: String = row.get(idx)?;
    serde_json::from_str(&s).map_err(|e| conv(idx, e))
}

fn de_opt<T: DeserializeOwned>(row: &Row, idx: usize) -> rusqlite::Result<Option<T>> {
    let s: Option<String> = row.get(idx)?;
    s.map(|s| serde_json::from_str(&s).map_err(|e| conv(idx, e)))
        .transpose()
}

fn uuid_col<T>(
    row: &Row,
    idx: usize,
    f: impl FnOnce(&str) -> Result<T, uuid::Error>,
) -> rusqlite::Result<T> {
    let s: String = row.get(idx)?;
    f(&s).map_err(|e| conv(idx, e))
}

fn u(row: &Row, idx: usize) -> rusqlite::Result<u64> {
    Ok(row.get::<_, i64>(idx)? as u64)
}

fn u_opt(row: &Row, idx: usize) -> rusqlite::Result<Option<u64>> {
    Ok(row.get::<_, Option<i64>>(idx)?.map(|v| v as u64))
}

const ORB_COLS: &str = "id, amp_endpoint, amp_orb_id, offering_id, spec_json, spec_digest, \
                        location_json, last_issued_generation, observation_json";

fn orb_row(row: &Row) -> rusqlite::Result<OrbRecord> {
    Ok(OrbRecord {
        id: uuid_col(row, 0, OrbId::parse)?,
        identity: OrbIdentity {
            amp_endpoint: AmpEndpointId(row.get(1)?),
            amp_orb_id: AmpOrbId(row.get(2)?),
        },
        offering_id: OfferingId(row.get(3)?),
        spec: de(row, 4)?,
        spec_digest: SpecDigest(row.get(5)?),
        location: de(row, 6)?,
        last_issued_generation: u(row, 7)?,
        observation: de(row, 8)?,
    })
}

const CMD_COLS: &str = "operation_id, orb_id, generation, action_json, command_digest, fingerprint, \
                        status, outcome_json, submitted_ms, work_deadline_ms, cleanup_deadline_ms, \
                        cancel_requested_ms";

fn cmd_row(row: &Row) -> rusqlite::Result<CmdRow> {
    let status: String = row.get(6)?;
    Ok(CmdRow {
        operation_id: uuid_col(row, 0, OperationId::parse)?,
        orb_id: uuid_col(row, 1, OrbId::parse)?,
        generation: u(row, 2)?,
        action: de(row, 3)?,
        command_digest: row.get(4)?,
        fingerprint: row.get(5)?,
        status: CmdStatus::parse(&status).ok_or_else(|| conv(6, std::fmt::Error))?,
        outcome: de_opt(row, 7)?,
        submitted_ms: u_opt(row, 8)?,
        work_deadline_ms: u(row, 9)?,
        cleanup_deadline_ms: u(row, 10)?,
        cancel_requested_ms: u_opt(row, 11)?,
    })
}

const REQ_COLS: &str = "seq, invocation_id, offering_id, amp_endpoint, amp_orb_id, action_json, \
                        fingerprint, status, start_ms, work_deadline_ms, cleanup_deadline_ms, \
                        dispatch_deadline_ms, operation_id, result_json";

fn req_row(row: &Row) -> rusqlite::Result<Req> {
    let status: String = row.get(7)?;
    let op: Option<String> = row.get(12)?;
    Ok(Req {
        seq: row.get(0)?,
        invocation_id: uuid_col(row, 1, InvocationId::parse)?,
        offering_id: OfferingId(row.get(2)?),
        amp_endpoint: AmpEndpointId(row.get(3)?),
        amp_orb_id: AmpOrbId(row.get(4)?),
        action: de(row, 5)?,
        fingerprint: row.get(6)?,
        status: ReqStatus::parse(&status).ok_or_else(|| conv(7, std::fmt::Error))?,
        start_ms: u(row, 8)?,
        work_deadline_ms: u(row, 9)?,
        cleanup_deadline_ms: u(row, 10)?,
        dispatch_deadline_ms: u(row, 11)?,
        operation_id: op
            .map(|s| OperationId::parse(&s).map_err(|e| conv(12, e)))
            .transpose()?,
        result: de_opt(row, 13)?,
    })
}

// --- meta ----------------------------------------------------------------------------

/// Durably increments and returns the controller epoch.
pub fn bump_epoch(c: &mut Connection) -> Result<u64> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(
        "INSERT INTO meta(key, value) VALUES ('controller_epoch', 1)
         ON CONFLICT(key) DO UPDATE SET value = value + 1",
        [],
    )?;
    let v: i64 = tx.query_row(
        "SELECT value FROM meta WHERE key = 'controller_epoch'",
        [],
        |r| r.get(0),
    )?;
    tx.commit()?;
    Ok(v as u64)
}

// --- reads ---------------------------------------------------------------------------

pub fn get_orb_by_identity(c: &Connection, ep: &str, orb: &str) -> Result<Option<OrbRecord>> {
    Ok(c.query_row(
        &format!("SELECT {ORB_COLS} FROM orbs WHERE amp_endpoint = ?1 AND amp_orb_id = ?2"),
        params![ep, orb],
        orb_row,
    )
    .optional()?)
}

pub fn get_orb(c: &Connection, id: OrbId) -> Result<Option<OrbRecord>> {
    Ok(c.query_row(
        &format!("SELECT {ORB_COLS} FROM orbs WHERE id = ?1"),
        params![id.to_string()],
        orb_row,
    )
    .optional()?)
}

pub fn get_command(c: &Connection, op: OperationId) -> Result<Option<CmdRow>> {
    Ok(c.query_row(
        &format!("SELECT {CMD_COLS} FROM commands WHERE operation_id = ?1"),
        params![op.to_string()],
        cmd_row,
    )
    .optional()?)
}

pub fn open_command(c: &Connection, orb: OrbId) -> Result<Option<CmdRow>> {
    Ok(c.query_row(
        &format!("SELECT {CMD_COLS} FROM commands WHERE orb_id = ?1 AND status <> 'terminal'"),
        params![orb.to_string()],
        cmd_row,
    )
    .optional()?)
}

/// All non-terminal commands with their orb.
pub fn open_commands(c: &Connection) -> Result<Vec<(CmdRow, OrbRecord)>> {
    let mut st = c.prepare(&format!(
        "SELECT {CMD_COLS} FROM commands WHERE status <> 'terminal' ORDER BY created_ms"
    ))?;
    let cmds = st
        .query_map([], cmd_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = Vec::new();
    for cmd in cmds {
        match get_orb(c, cmd.orb_id)? {
            Some(orb) => out.push((cmd, orb)),
            None => bail!("command {} references a missing orb", cmd.operation_id),
        }
    }
    Ok(out)
}

pub fn is_tombstoned(c: &Connection, ep: &str, orb: &str) -> Result<bool> {
    Ok(c.query_row(
        "SELECT 1 FROM delete_tombstones WHERE amp_endpoint = ?1 AND amp_orb_id = ?2",
        params![ep, orb],
        |_| Ok(()),
    )
    .optional()?
    .is_some())
}

pub fn get_request(c: &Connection, inv: InvocationId) -> Result<Option<Req>> {
    Ok(c.query_row(
        &format!("SELECT {REQ_COLS} FROM requests WHERE invocation_id = ?1"),
        params![inv.to_string()],
        req_row,
    )
    .optional()?)
}

pub fn queued_for(c: &Connection, ep: &str, orb: &str) -> Result<Vec<Req>> {
    let mut st = c.prepare(&format!(
        "SELECT {REQ_COLS} FROM requests
         WHERE status = 'queued' AND amp_endpoint = ?1 AND amp_orb_id = ?2 ORDER BY seq"
    ))?;
    Ok(st
        .query_map(params![ep, orb], req_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn queue_keys(c: &Connection) -> Result<Vec<(String, String)>> {
    let mut st = c.prepare(
        "SELECT DISTINCT amp_endpoint, amp_orb_id FROM requests WHERE status = 'queued'",
    )?;
    Ok(st
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn queue_position(c: &Connection, req: &Req) -> Result<u32> {
    let n: i64 = c.query_row(
        "SELECT COUNT(*) FROM requests WHERE status = 'queued'
         AND amp_endpoint = ?1 AND amp_orb_id = ?2 AND seq < ?3",
        params![req.amp_endpoint.as_str(), req.amp_orb_id.as_str(), req.seq],
        |r| r.get(0),
    )?;
    Ok(n as u32)
}

pub fn expired_queued(c: &Connection, now: u64) -> Result<Vec<Req>> {
    let mut st = c.prepare(&format!(
        "SELECT {REQ_COLS} FROM requests WHERE status = 'queued' AND work_deadline_ms <= ?1"
    ))?;
    Ok(st
        .query_map(params![now as i64], req_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn active_past_dispatch(c: &Connection, now: u64) -> Result<Vec<Req>> {
    let mut st = c.prepare(&format!(
        "SELECT {REQ_COLS} FROM requests WHERE status = 'active' AND dispatch_deadline_ms <= ?1"
    ))?;
    Ok(st
        .query_map(params![now as i64], req_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn count_where(c: &Connection, sql: &str) -> Result<u64> {
    Ok(c.query_row(sql, [], |r| r.get::<_, i64>(0))? as u64)
}

/// Sum of reservations in a pool, optionally excluding one orb (its own current row).
pub fn pool_used(
    c: &Connection,
    backend: &str,
    pool: &str,
    exclude: Option<OrbId>,
) -> Result<Resources> {
    let mut st = c.prepare(
        "SELECT vcpus, memory_bytes, disk_bytes FROM reservations
         WHERE backend_id = ?1 AND pool = ?2 AND orb_id <> ?3",
    )?;
    let ex = exclude.map(|o| o.to_string()).unwrap_or_default();
    let rows = st.query_map(params![backend, pool, ex], |r| {
        Ok((r.get::<_, i64>(0)? as u32, u(r, 1)?, u(r, 2)?))
    })?;
    let mut sum = Resources::default();
    for r in rows {
        let (v, m, d) = r?;
        sum = Resources {
            vcpus: sum.vcpus.saturating_add(v),
            memory_bytes: sum.memory_bytes.saturating_add(m),
            disk_bytes: sum.disk_bytes.saturating_add(d),
        };
    }
    Ok(sum)
}

pub fn pool_memory_gauges(c: &Connection) -> Result<Vec<(String, u64)>> {
    let mut st = c.prepare("SELECT backend_id || '/' || pool, memory_bytes FROM reservations")?;
    let rows = st.query_map([], |r| Ok((r.get::<_, String>(0)?, u(r, 1)?)))?;
    let mut map = std::collections::BTreeMap::<String, u64>::new();
    for r in rows {
        let (k, m) = r?;
        let e = map.entry(k).or_default();
        *e = e.saturating_add(m);
    }
    Ok(map.into_iter().collect())
}

pub fn get_payload(c: &Connection, owner: &str) -> Result<Option<Vec<u8>>> {
    Ok(c.query_row(
        "SELECT sealed FROM secret_payloads WHERE owner = ?1",
        params![owner],
        |r| r.get(0),
    )
    .optional()?)
}

// --- writes --------------------------------------------------------------------------

pub fn insert_request(
    c: &mut Connection,
    req: &Req,
    sealed: Option<(String, Vec<u8>)>,
) -> Result<()> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(
        "INSERT INTO requests(invocation_id, offering_id, amp_endpoint, amp_orb_id, action_json,
            fingerprint, status, start_ms, work_deadline_ms, cleanup_deadline_ms,
            dispatch_deadline_ms, operation_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            req.invocation_id.to_string(),
            req.offering_id.as_str(),
            req.amp_endpoint.as_str(),
            req.amp_orb_id.as_str(),
            js(&req.action),
            req.fingerprint,
            req.status.as_str(),
            req.start_ms as i64,
            req.work_deadline_ms as i64,
            req.cleanup_deadline_ms as i64,
            req.dispatch_deadline_ms as i64,
            req.operation_id.map(|o| o.to_string()),
        ],
    )?;
    if let Some((owner, bytes)) = sealed {
        tx.execute(
            "INSERT INTO secret_payloads(owner, sealed, created_ms) VALUES (?1, ?2, ?3)",
            params![owner, bytes, unix_ms_now() as i64],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub fn finish_request(
    c: &mut Connection,
    inv: InvocationId,
    result: &StoredResult,
) -> Result<bool> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let n = tx.execute(
        "UPDATE requests SET status = 'finished', result_json = ?2
         WHERE invocation_id = ?1 AND status <> 'finished'",
        params![inv.to_string(), js(result)],
    )?;
    tx.execute(
        "DELETE FROM secret_payloads WHERE owner = ?1",
        params![inv.to_string()],
    )?;
    tx.commit()?;
    Ok(n > 0)
}

/// Marks a queued request as attached to an existing open command.
pub fn attach_request(c: &mut Connection, inv: InvocationId, op: OperationId) -> Result<()> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(
        "UPDATE requests SET status = 'active', operation_id = ?2 WHERE invocation_id = ?1",
        params![inv.to_string(), op.to_string()],
    )?;
    tx.execute(
        "DELETE FROM secret_payloads WHERE owner = ?1",
        params![inv.to_string()],
    )?;
    tx.commit()?;
    Ok(())
}

pub struct CreatePlan {
    pub invocation_id: InvocationId,
    pub new_orb: Option<OrbRecord>,
    pub orb_id: OrbId,
    /// Existing orb's generation before this command (compare-and-set).
    pub prev_generation: u64,
    pub generation: u64,
    pub operation_id: OperationId,
    pub action: Action,
    pub command_digest: String,
    pub fingerprint: Option<String>,
    pub work_deadline_ms: u64,
    pub cleanup_deadline_ms: u64,
    /// (backend, pool, resources) to reserve in full ('running').
    pub reservation: Option<(String, String, Resources)>,
    pub tombstone: Option<(String, String)>,
    /// Sealed guest env for the new operation (aad = operation id).
    pub sealed: Option<Vec<u8>>,
}

/// Command creation + generation bump + reservation + outbox intent in one transaction.
pub fn create_command(c: &mut Connection, p: &CreatePlan) -> Result<()> {
    let now = unix_ms_now() as i64;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(orb) = &p.new_orb {
        tx.execute(
            "INSERT INTO orbs(id, amp_endpoint, amp_orb_id, offering_id, spec_json, spec_digest,
                location_json, last_issued_generation, observation_json, observation_generation,
                created_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                orb.id.to_string(),
                orb.identity.amp_endpoint.as_str(),
                orb.identity.amp_orb_id.as_str(),
                orb.offering_id.as_str(),
                js(&orb.spec),
                orb.spec_digest.0,
                js(&orb.location),
                p.generation as i64,
                js(&orb.observation),
                orb.observation.generation as i64,
                now,
            ],
        )?;
    } else {
        let n = tx.execute(
            "UPDATE orbs SET last_issued_generation = ?2 WHERE id = ?1 AND last_issued_generation = ?3",
            params![p.orb_id.to_string(), p.generation as i64, p.prev_generation as i64],
        )?;
        if n != 1 {
            bail!("orb generation changed concurrently");
        }
    }
    tx.execute(
        "INSERT INTO commands(operation_id, orb_id, generation, action_json, command_digest,
            fingerprint, status, created_ms, work_deadline_ms, cleanup_deadline_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7, ?8, ?9)",
        params![
            p.operation_id.to_string(),
            p.orb_id.to_string(),
            p.generation as i64,
            js(&p.action),
            p.command_digest,
            p.fingerprint,
            now,
            p.work_deadline_ms as i64,
            p.cleanup_deadline_ms as i64,
        ],
    )?;
    if let Some((backend, pool, r)) = &p.reservation {
        tx.execute(
            "INSERT INTO reservations(orb_id, backend_id, pool, vcpus, memory_bytes, disk_bytes, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running')
             ON CONFLICT(orb_id) DO UPDATE SET backend_id = excluded.backend_id,
                pool = excluded.pool, vcpus = excluded.vcpus,
                memory_bytes = excluded.memory_bytes, disk_bytes = excluded.disk_bytes,
                kind = 'running'",
            params![
                p.orb_id.to_string(),
                backend,
                pool,
                r.vcpus as i64,
                r.memory_bytes as i64,
                r.disk_bytes as i64
            ],
        )?;
    }
    if let Some((ep, amp)) = &p.tombstone {
        tx.execute(
            "INSERT OR IGNORE INTO delete_tombstones(amp_endpoint, amp_orb_id, orb_id, created_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![ep, amp, p.orb_id.to_string(), now],
        )?;
    }
    if let Some(sealed) = &p.sealed {
        tx.execute(
            "INSERT INTO secret_payloads(owner, sealed, created_ms) VALUES (?1, ?2, ?3)",
            params![p.operation_id.to_string(), sealed, now],
        )?;
    }
    tx.execute(
        "DELETE FROM secret_payloads WHERE owner = ?1",
        params![p.invocation_id.to_string()],
    )?;
    tx.execute(
        "UPDATE requests SET status = 'active', operation_id = ?2 WHERE invocation_id = ?1",
        params![p.invocation_id.to_string(), p.operation_id.to_string()],
    )?;
    tx.commit()?;
    Ok(())
}

pub fn mark_submitted(c: &Connection, op: OperationId, now: u64) -> Result<()> {
    c.execute(
        "UPDATE commands SET submitted_ms = COALESCE(submitted_ms, ?2) WHERE operation_id = ?1",
        params![op.to_string(), now as i64],
    )?;
    Ok(())
}

pub fn mark_accepted(c: &Connection, op: OperationId, now: u64) -> Result<()> {
    c.execute(
        "UPDATE commands SET status = 'accepted', accepted_ms = COALESCE(accepted_ms, ?2)
         WHERE operation_id = ?1 AND status IN ('pending', 'unknown')",
        params![op.to_string(), now as i64],
    )?;
    Ok(())
}

pub fn mark_unknown(c: &Connection, op: OperationId) -> Result<()> {
    c.execute(
        "UPDATE commands SET status = 'unknown' WHERE operation_id = ?1 AND status IN ('pending', 'accepted')",
        params![op.to_string()],
    )?;
    Ok(())
}

pub fn mark_cancel_requested(c: &Connection, op: OperationId, now: u64) -> Result<()> {
    c.execute(
        "UPDATE commands SET cancel_requested_ms = COALESCE(cancel_requested_ms, ?2)
         WHERE operation_id = ?1",
        params![op.to_string(), now as i64],
    )?;
    Ok(())
}

pub enum Applied {
    /// Unknown operation or key mismatch.
    Unknown,
    AlreadyTerminal,
    Done(Box<Done>),
}

pub struct Done {
    pub cmd: CmdRow,
    pub orb: OrbRecord,
    pub finished: Vec<InvocationId>,
    pub results: Vec<StoredResult>,
}

/// Applies a terminal outcome in one transaction: command, observation, reservations,
/// sealed payload and the requests attached to the command.
pub fn apply_completion(
    c: &mut Connection,
    key: CommandKey,
    outcome: &CommandOutcome,
    now: u64,
) -> Result<Applied> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let Some(cmd) = get_command(&tx, key.operation_id)? else {
        return Ok(Applied::Unknown);
    };
    if cmd.orb_id != key.orb_id || cmd.generation != key.generation {
        return Ok(Applied::Unknown);
    }
    if cmd.status == CmdStatus::Terminal {
        return Ok(Applied::AlreadyTerminal);
    }
    tx.execute(
        "UPDATE commands SET status = 'terminal', outcome_json = ?2, terminal_ms = ?3
         WHERE operation_id = ?1",
        params![key.operation_id.to_string(), js(outcome), now as i64],
    )?;
    let Some(mut orb) = get_orb(&tx, cmd.orb_id)? else {
        bail!("command {} references a missing orb", key.operation_id);
    };
    // Never roll an observation back to an older generation.
    if cmd.generation >= orb.observation.generation {
        let obs = VersionedObservation {
            state: outcome.observed.clone(),
            generation: cmd.generation,
            observed_at_unix_ms: now,
        };
        tx.execute(
            "UPDATE orbs SET observation_json = ?2, observation_generation = ?3 WHERE id = ?1",
            params![orb.id.to_string(), js(&obs), cmd.generation as i64],
        )?;
        orb.observation = obs;
        let orb_id = orb.id.to_string();
        let deleted = matches!(outcome.result, Ok(ActionResult::Deleted));
        match &outcome.observed {
            _ if deleted => {
                tx.execute(
                    "DELETE FROM reservations WHERE orb_id = ?1",
                    params![orb_id],
                )?;
            }
            OrbState::Known {
                state: StableOrbState::Uncreated | StableOrbState::Deleted,
            } => {
                tx.execute(
                    "DELETE FROM reservations WHERE orb_id = ?1",
                    params![orb_id],
                )?;
            }
            OrbState::Known {
                state: StableOrbState::Sleeping { .. } | StableOrbState::DiskOnlyStopped { .. },
            } => {
                tx.execute(
                    "UPDATE reservations SET vcpus = 0, memory_bytes = 0, kind = 'stopped'
                     WHERE orb_id = ?1",
                    params![orb_id],
                )?;
            }
            // Running keeps everything; Faulted is conservative: keep.
            _ => {}
        }
    }
    tx.execute(
        "DELETE FROM secret_payloads WHERE owner = ?1",
        params![key.operation_id.to_string()],
    )?;
    let action = cmd.action.name();
    let result = StoredResult::from_outcome(action, key.operation_id, outcome);
    let mut finished = Vec::new();
    let mut results = Vec::new();
    {
        let mut st = tx.prepare(
            "SELECT invocation_id FROM requests WHERE operation_id = ?1 AND status = 'active'",
        )?;
        let invs: Vec<String> = st
            .query_map(params![key.operation_id.to_string()], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        drop(st);
        for inv in invs {
            tx.execute(
                "UPDATE requests SET status = 'finished', result_json = ?2 WHERE invocation_id = ?1",
                params![inv, js(&result)],
            )?;
            finished.push(InvocationId::parse(&inv)?);
            results.push(result.clone());
        }
    }
    let mut cmd = cmd;
    cmd.status = CmdStatus::Terminal;
    cmd.outcome = Some(outcome.clone());
    tx.commit()?;
    Ok(Applied::Done(Box::new(Done {
        cmd,
        orb,
        finished,
        results,
    })))
}

pub fn gc(c: &Connection, older_than_ms: u64) -> Result<()> {
    c.execute(
        "DELETE FROM requests WHERE status = 'finished' AND dispatch_deadline_ms < ?1",
        params![older_than_ms as i64],
    )?;
    Ok(())
}
