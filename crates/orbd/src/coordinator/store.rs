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
        &format!(
            "SELECT {ORB_COLS} FROM orbs WHERE amp_endpoint = ?1 AND amp_orb_id = ?2 \
             AND retired_ms IS NULL"
        ),
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

/// Sizes an orb's reservation to the state it was observed in: a stopped orb holds no
/// compute, an orb with no disk holds nothing; Running and Faulted keep everything.
fn reserve_for(c: &Connection, orb: OrbId, state: &OrbState) -> Result<()> {
    let orb_id = orb.to_string();
    match state {
        OrbState::Known {
            state: StableOrbState::Uncreated | StableOrbState::Deleted,
        } => {
            c.execute(
                "DELETE FROM reservations WHERE orb_id = ?1",
                params![orb_id],
            )?;
        }
        OrbState::Known {
            state: StableOrbState::Sleeping { .. } | StableOrbState::DiskOnlyStopped { .. },
        } => {
            c.execute(
                "UPDATE reservations SET vcpus = 0, memory_bytes = 0, disk_bytes = 0,
                    kind = 'stopped'
                 WHERE orb_id = ?1",
                params![orb_id],
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// Applies what the orb's host reports outside any command: only that an orb recorded
/// as running has stopped (its VM died with the host, or while the agent was down), or
/// that an orb recorded asleep has lost its memory checkpoint (its agent dropped it past
/// `ORBD_CHECKPOINT_MAX_AGE_HOURS`), only when no command is open for it and the report
/// is not older than the record. Returns the state it replaced, if it changed.
pub fn observe_stopped(
    c: &mut Connection,
    orb: OrbId,
    location: &Location,
    generation: u64,
    state: &OrbState,
    now: u64,
) -> Result<Option<OrbState>> {
    let (stopped, without_checkpoint) = match state {
        OrbState::Known {
            state: StableOrbState::Sleeping { .. },
        } => (true, false),
        OrbState::Known {
            state: StableOrbState::DiskOnlyStopped { .. },
        } => (true, true),
        _ => (false, false),
    };
    if !stopped {
        return Ok(None);
    }
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let Some(rec) = get_orb(&tx, orb)? else {
        return Ok(None);
    };
    let (running, asleep) = match &rec.observation.state {
        OrbState::Known {
            state: StableOrbState::Running,
        } => (true, false),
        OrbState::Known {
            state: StableOrbState::Sleeping { .. },
        } => (false, true),
        _ => (false, false),
    };
    let lost_checkpoint = asleep && without_checkpoint;
    if &rec.location != location
        || generation < rec.observation.generation
        || !(running || lost_checkpoint)
        || open_command(&tx, orb)?.is_some()
    {
        return Ok(None);
    }
    let obs = VersionedObservation {
        state: state.clone(),
        generation: rec.observation.generation,
        // An orb that lost its checkpoint has been stopped since it went to sleep: its
        // idle time (`idle_orb_ttl_days`, `--asleep-days`) goes on from then.
        observed_at_unix_ms: if lost_checkpoint {
            rec.observation.observed_at_unix_ms
        } else {
            now
        },
    };
    tx.execute(
        "UPDATE orbs SET observation_json = ?2 WHERE id = ?1",
        params![orb.to_string(), js(&obs)],
    )?;
    reserve_for(&tx, orb, state)?;
    tx.commit()?;
    Ok(Some(rec.observation.state))
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

/// Orbs asleep (or stopped) since before `cutoff_ms` with no open command, no pending
/// deletion and no request since the cutoff: what an idle reclaim deletes.
pub fn idle_orbs(c: &Connection, cutoff_ms: u64) -> Result<Vec<OrbRecord>> {
    let mut st = c.prepare(&format!(
        "SELECT {ORB_COLS} FROM orbs o WHERE retired_ms IS NULL
           AND NOT EXISTS (SELECT 1 FROM commands m
                           WHERE m.orb_id = o.id AND m.status <> 'terminal')
           AND NOT EXISTS (SELECT 1 FROM delete_tombstones t WHERE t.orb_id = o.id)
           AND NOT EXISTS (SELECT 1 FROM requests r
                           WHERE r.amp_endpoint = o.amp_endpoint
                             AND r.amp_orb_id = o.amp_orb_id
                             AND (r.status <> 'finished' OR r.start_ms >= ?1))"
    ))?;
    let orbs = st
        .query_map(params![cutoff_ms as i64], orb_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(orbs
        .into_iter()
        .filter(|o| {
            o.observation.observed_at_unix_ms < cutoff_ms
                && matches!(
                    o.observation.state,
                    OrbState::Known {
                        state: StableOrbState::Sleeping { .. }
                            | StableOrbState::DiskOnlyStopped { .. }
                    }
                )
        })
        .collect())
}

/// The room one pool keeps for waking the orbs asleep or stopped in it: the largest of
/// their wakes' needs, resource by resource, as `core::wake_demand` sizes each (growth
/// headroom up to `headroom`, plus the memory checkpoint of a memory-preserving orb,
/// less what `releases` reports its wake frees, for the checkpoint generation it sleeps
/// on). Aggregated here, not loaded orb by orb. Counts only orbs that may wake: known
/// asleep or stopped (not faulted) since `since_ms` or later, not retired, no delete
/// begun, and not `excluded` (archived).
pub fn wake_room(
    c: &Connection,
    backend: &str,
    pool: &str,
    headroom: u64,
    since_ms: u64,
    releases: &[(OrbId, crate::backend::WakeRelease)],
    excluded: &[OrbId],
) -> Result<Resources> {
    let releases = serde_json::to_string(
        &releases
            .iter()
            .map(|(id, r)| (id.to_string(), r.checkpoint_generation, r.bytes))
            .collect::<Vec<_>>(),
    )?;
    let excluded =
        serde_json::to_string(&excluded.iter().map(|id| id.to_string()).collect::<Vec<_>>())?;
    Ok(c.query_row(
        "WITH rel(id, gen, bytes) AS (
             SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]'),
                    json_extract(value, '$[2]')
             FROM json_each(?5)),
         w AS (
             SELECT json_extract(o.spec_json, '$.template.resources.vcpus') AS vcpus,
                    json_extract(o.spec_json, '$.template.resources.memory_bytes') AS mem,
                    MIN(json_extract(o.spec_json, '$.template.resources.disk_bytes'), ?3)
                        AS growth,
                    CASE WHEN json_extract(o.spec_json, '$.template.pause_policy.mode')
                              = 'memory_preserving'
                         THEN json_extract(o.spec_json, '$.template.resources.memory_bytes')
                         ELSE 0 END AS ckpt,
                    CASE WHEN json_extract(o.observation_json, '$.state.state.kind')
                              = 'sleeping'
                          AND rel.gen = json_extract(o.observation_json,
                                                     '$.state.state.checkpoint.generation')
                         THEN rel.bytes ELSE 0 END AS freed
             FROM orbs o LEFT JOIN rel ON rel.id = o.id
             WHERE o.retired_ms IS NULL
               AND o.id IN (SELECT orb_id FROM reservations
                            WHERE backend_id = ?1 AND pool = ?2 AND kind = 'stopped')
               AND json_extract(o.observation_json, '$.state.kind') = 'known'
               AND json_extract(o.observation_json, '$.state.state.kind')
                   IN ('sleeping', 'disk_only_stopped')
               AND json_extract(o.observation_json, '$.observed_at_unix_ms') >= ?4
               AND NOT EXISTS (SELECT 1 FROM delete_tombstones t WHERE t.orb_id = o.id)
               AND o.id NOT IN (SELECT value FROM json_each(?6)))
         SELECT COALESCE(MAX(vcpus), 0), COALESCE(MAX(mem), 0),
                COALESCE(MAX(growth + ckpt - MIN(freed, ckpt)), 0)
         FROM w",
        params![
            backend,
            pool,
            headroom as i64,
            since_ms as i64,
            releases,
            excluded
        ],
        |r| {
            Ok(Resources {
                vcpus: u(r, 0)?.min(u32::MAX as u64) as u32,
                memory_bytes: u(r, 1)?,
                disk_bytes: u(r, 2)?,
            })
        },
    )?)
}

/// How an orb that never started was refused ([`unstarted`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Unstarted {
    /// How its last command failed.
    pub last_failure: Option<FailureCode>,
    /// How many of its latest commands in a row its host refused for want of room
    /// (memory, disk or vCPUs).
    pub capacity_refusals: u32,
}

const CAPACITY_CODES: [FailureCode; 3] = [
    FailureCode::InsufficientMemory,
    FailureCode::InsufficientDisk,
    FailureCode::QuotaExceeded,
];

/// Whether `orb` never started, and how it was refused: known uncreated by its last
/// command (`generation`, nothing issued since, none open), none of its commands ever
/// started or paused it, not retired, no delete begun. Its host said nothing of it
/// exists there. None if not such an orb.
pub fn unstarted(c: &Connection, orb: OrbId, generation: u64) -> Result<Option<Unstarted>> {
    let Some(rec) = get_orb_by_id_live(c, orb)? else {
        return Ok(None);
    };
    let unstarted = rec.last_issued_generation == generation
        && rec.observation.generation == generation
        && rec.observation.state
            == OrbState::Known {
                state: StableOrbState::Uncreated,
            }
        && open_command(c, orb)?.is_none()
        && last_active(c, orb)?.is_none()
        && !is_tombstoned(c, orb)?;
    if !unstarted {
        return Ok(None);
    }
    let failures: Vec<Option<FailureCode>> = orb_commands(c, orb, 16)?
        .into_iter()
        .map(|h| h.outcome.and_then(|o| o.result.err()).map(|f| f.code))
        .collect();
    let capacity_refusals = failures
        .iter()
        .take_while(|f| f.is_some_and(|code| CAPACITY_CODES.contains(&code)))
        .count() as u32;
    Ok(Some(Unstarted {
        last_failure: failures.first().copied().flatten(),
        capacity_refusals,
    }))
}

/// The hosts that refused an Amp orb before it ever started, since `since_ms`: where its
/// incarnations retired for a new placement were ([`CreatePlan::retire`]), newest first.
pub fn refused_hosts(
    c: &Connection,
    ep: &str,
    amp_orb: &str,
    since_ms: u64,
) -> Result<Vec<Location>> {
    let mut st = c.prepare(
        "SELECT o.location_json FROM orbs o
         WHERE o.amp_endpoint = ?1 AND o.amp_orb_id = ?2 AND o.retired_ms >= ?3
           AND EXISTS (SELECT 1 FROM delete_tombstones t WHERE t.orb_id = o.id)
           AND NOT EXISTS (SELECT 1 FROM commands m WHERE m.orb_id = o.id
                             AND m.status = 'terminal'
                             AND json_extract(m.action_json, '$.kind') IN ('spawn', 'pause')
                             AND json_type(m.outcome_json, '$.result.Ok') IS NOT NULL)
         ORDER BY o.retired_ms DESC",
    )?;
    let rows = st.query_map(params![ep, amp_orb, since_ms as i64], |r| de(r, 0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Removes the rows of incarnations retired for a new placement before `before_ms`
/// (orb, commands, tombstone, reservation) once nothing is left to do for them: their
/// old host deleted them, answered that it cannot run them, or was sent every delete
/// it gets ([`retired_undeleted`]). Returns how many it removed.
pub fn prune_retired_unstarted(
    c: &mut Connection,
    before_ms: u64,
    max_deletes: u32,
) -> Result<usize> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let ids: Vec<String> = {
        let mut st = tx.prepare(
            "SELECT o.id FROM orbs o
             WHERE o.retired_ms IS NOT NULL AND o.retired_ms < ?1
               AND EXISTS (SELECT 1 FROM delete_tombstones t WHERE t.orb_id = o.id)
               AND NOT EXISTS (SELECT 1 FROM commands m WHERE m.orb_id = o.id
                                 AND m.status <> 'terminal')
               AND NOT EXISTS (SELECT 1 FROM commands m WHERE m.orb_id = o.id
                                 AND m.status = 'terminal'
                                 AND json_extract(m.action_json, '$.kind') IN ('spawn', 'pause')
                                 AND json_type(m.outcome_json, '$.result.Ok') IS NOT NULL)
               AND (COALESCE(json_extract(o.observation_json, '$.state.state.kind'), '')
                        = 'deleted'
                    OR EXISTS (SELECT 1 FROM commands m WHERE m.orb_id = o.id
                                 AND m.status = 'terminal'
                                 AND json_extract(m.action_json, '$.kind') = 'delete'
                                 AND json_extract(m.outcome_json, '$.result.Err.code')
                                     = 'unsupported_capability')
                    OR (SELECT COUNT(*) FROM commands m WHERE m.orb_id = o.id
                          AND json_extract(m.action_json, '$.kind') = 'delete') >= ?2)",
        )?;
        st.query_map(params![before_ms as i64, max_deletes as i64], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    for id in &ids {
        tx.execute("DELETE FROM commands WHERE orb_id = ?1", params![id])?;
        tx.execute("DELETE FROM reservations WHERE orb_id = ?1", params![id])?;
        tx.execute(
            "DELETE FROM delete_tombstones WHERE orb_id = ?1",
            params![id],
        )?;
        tx.execute("DELETE FROM orbs WHERE id = ?1", params![id])?;
    }
    tx.commit()?;
    Ok(ids.len())
}

/// A retired orb its old host is still to delete ([`retired_undeleted`]).
#[derive(Clone, Debug)]
pub struct RetiredUndeleted {
    pub id: OrbId,
    pub amp_orb_id: String,
    pub location: Location,
    pub last_issued_generation: u64,
    pub spec_digest: SpecDigest,
    pub target: ExecutionTarget,
    pub resources: Resources,
    /// Deletes sent for it so far, and when the last one was created.
    pub attempts: u32,
    pub last_attempt_ms: Option<u64>,
}

/// Retired orbs whose old host is still to be told to delete them: orbs that never
/// started, replaced by a new incarnation elsewhere ([`CreatePlan::retire`]). Not those
/// deleted, with a command open, or whose host answered a delete that it cannot run
/// them (`UnsupportedCapability`: an agent without their runtime, which created nothing
/// of them). Filtered here, only the fields a delete needs.
pub fn retired_undeleted(c: &Connection) -> Result<Vec<RetiredUndeleted>> {
    let mut st = c.prepare(
        "SELECT o.id, o.amp_orb_id, o.location_json, o.last_issued_generation, o.spec_digest,
                json_extract(o.spec_json, '$.template.target'),
                json_extract(o.spec_json, '$.template.resources'),
                (SELECT COUNT(*) FROM commands m WHERE m.orb_id = o.id
                   AND json_extract(m.action_json, '$.kind') = 'delete'),
                (SELECT MAX(created_ms) FROM commands m WHERE m.orb_id = o.id
                   AND json_extract(m.action_json, '$.kind') = 'delete')
         FROM orbs o
         WHERE o.retired_ms IS NOT NULL
           AND EXISTS (SELECT 1 FROM delete_tombstones t WHERE t.orb_id = o.id)
           AND COALESCE(json_extract(o.observation_json, '$.state.state.kind'), '') <> 'deleted'
           AND NOT EXISTS (SELECT 1 FROM commands m
                           WHERE m.orb_id = o.id AND m.status <> 'terminal')
           AND NOT EXISTS (SELECT 1 FROM commands m
                           WHERE m.orb_id = o.id AND m.status = 'terminal'
                             AND json_extract(m.action_json, '$.kind') = 'delete'
                             AND json_extract(m.outcome_json, '$.result.Err.code')
                                 = 'unsupported_capability')",
    )?;
    let rows = st.query_map([], |r| {
        Ok(RetiredUndeleted {
            id: uuid_col(r, 0, OrbId::parse)?,
            amp_orb_id: r.get(1)?,
            location: de(r, 2)?,
            last_issued_generation: u(r, 3)?,
            spec_digest: SpecDigest(r.get(4)?),
            target: de(r, 5)?,
            resources: de(r, 6)?,
            attempts: u(r, 7)?.min(u32::MAX as u64) as u32,
            last_attempt_ms: u_opt(r, 8)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// The orb, unless it is retired.
fn get_orb_by_id_live(c: &Connection, id: OrbId) -> Result<Option<OrbRecord>> {
    Ok(c.query_row(
        &format!("SELECT {ORB_COLS} FROM orbs WHERE id = ?1 AND retired_ms IS NULL"),
        params![id.to_string()],
        orb_row,
    )
    .optional()?)
}

/// Retires a deleted orb (or one that never started, [`CreatePlan::retire`]) so a new
/// incarnation can take its Amp id.
pub fn retire_orb(c: &Connection, orb: OrbId) -> Result<()> {
    c.execute(
        "UPDATE orbs SET retired_ms = ?2 WHERE id = ?1 AND retired_ms IS NULL",
        params![orb.to_string(), crate::model::unix_ms_now() as i64],
    )?;
    Ok(())
}

pub fn is_tombstoned(c: &Connection, orb: OrbId) -> Result<bool> {
    Ok(c.query_row(
        "SELECT 1 FROM delete_tombstones WHERE orb_id = ?1",
        params![orb.to_string()],
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

/// What the reservations of one pool hold.
#[derive(Clone, Copy, Debug, Default)]
pub struct PoolUse {
    /// Summed over running orbs only: sleeping orbs hold no vCPUs or memory, and their
    /// disk is already allocated (it is in the agent's free space).
    pub held: Resources,
    pub running: u32,
    pub orbs: u32,
}

/// Rows from before disk headroom: a stopped row may still carry its orb's nominal disk
/// size, so only running rows count.
const POOL_USE_SQL: &str = "SELECT
        COALESCE(SUM(CASE WHEN kind = 'running' THEN vcpus ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN kind = 'running' THEN memory_bytes ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN kind = 'running' THEN disk_bytes ELSE 0 END), 0),
        COALESCE(SUM(kind = 'running'), 0),
        COUNT(*)
     FROM reservations";

fn pool_use_row(r: &Row, off: usize) -> rusqlite::Result<PoolUse> {
    Ok(PoolUse {
        held: Resources {
            vcpus: u(r, off)?.min(u32::MAX as u64) as u32,
            memory_bytes: u(r, off + 1)?,
            disk_bytes: u(r, off + 2)?,
        },
        running: u(r, off + 3)? as u32,
        orbs: u(r, off + 4)? as u32,
    })
}

/// Reservations in a pool, optionally excluding one orb (its own current row).
pub fn pool_used(
    c: &Connection,
    backend: &str,
    pool: &str,
    exclude: Option<OrbId>,
) -> Result<PoolUse> {
    let ex = exclude.map(|o| o.to_string()).unwrap_or_default();
    Ok(c.query_row(
        &format!("{POOL_USE_SQL} WHERE backend_id = ?1 AND pool = ?2 AND orb_id <> ?3"),
        params![backend, pool, ex],
        |r| pool_use_row(r, 0),
    )?)
}

/// Every pool of `backend` that holds reservations.
pub fn pools(c: &Connection, backend: &str) -> Result<Vec<(String, PoolUse)>> {
    let mut st = c.prepare(&format!(
        "{} WHERE backend_id = ?1 GROUP BY pool",
        POOL_USE_SQL.replacen("SELECT", "SELECT pool,", 1)
    ))?;
    let rows = st.query_map(params![backend], |r| Ok((r.get(0)?, pool_use_row(r, 1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

pub fn pool_gauges(c: &Connection) -> Result<Vec<(String, PoolUse)>> {
    let mut st = c.prepare(&format!(
        "{} GROUP BY backend_id, pool",
        POOL_USE_SQL.replacen("SELECT", "SELECT backend_id || '/' || pool,", 1)
    ))?;
    let rows = st.query_map([], |r| Ok((r.get(0)?, pool_use_row(r, 1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
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
    /// (backend, pool, resources) to reserve as 'running' (disk is the orb's headroom).
    pub reservation: Option<(String, String, Resources)>,
    pub tombstone: Option<(String, String)>,
    /// Sealed guest env for the new operation (aad = operation id).
    pub sealed: Option<Vec<u8>>,
    /// (orb, its last issued generation): an orb that never started ([`unstarted`]),
    /// retired in the same transaction, before `new_orb` takes its Amp id, with a
    /// delete tombstone so its old host is told to delete it (see
    /// [`retired_undeleted`]). Refused if it is no longer such an orb.
    pub retire: Option<(OrbId, u64)>,
}

/// Command creation + generation bump + reservation + outbox intent in one transaction.
pub fn create_command(c: &mut Connection, p: &CreatePlan) -> Result<()> {
    let now = unix_ms_now() as i64;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some((old, generation)) = p.retire {
        let Some(new) = &p.new_orb else {
            bail!("only a new orb replaces an orb that never started");
        };
        if unstarted(&tx, old, generation)?.is_none() {
            bail!("orb {old} changed concurrently");
        }
        retire_orb(&tx, old)?;
        tx.execute(
            "INSERT OR IGNORE INTO delete_tombstones(amp_endpoint, amp_orb_id, orb_id, created_ms)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                new.identity.amp_endpoint.as_str(),
                new.identity.amp_orb_id.as_str(),
                old.to_string(),
                now
            ],
        )?;
        tx.execute(
            "DELETE FROM reservations WHERE orb_id = ?1",
            params![old.to_string()],
        )?;
    }
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
        if matches!(outcome.result, Ok(ActionResult::Deleted)) {
            tx.execute(
                "DELETE FROM reservations WHERE orb_id = ?1",
                params![orb.id.to_string()],
            )?;
        } else {
            reserve_for(&tx, orb.id, &outcome.observed)?;
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

// --- operator views (`orbd orb`, `orbd status`) --------------------------------------

/// A command as operators see it.
#[derive(Clone, Debug)]
pub struct CmdHistory {
    pub operation_id: OperationId,
    pub generation: u64,
    pub action: Action,
    pub status: String,
    pub outcome: Option<CommandOutcome>,
    pub created_ms: u64,
    pub accepted_ms: Option<u64>,
    pub terminal_ms: Option<u64>,
}

const HIST_COLS: &str = "operation_id, generation, action_json, status, outcome_json, \
                         created_ms, accepted_ms, terminal_ms";

fn hist_row(row: &Row) -> rusqlite::Result<CmdHistory> {
    Ok(CmdHistory {
        operation_id: uuid_col(row, 0, OperationId::parse)?,
        generation: u(row, 1)?,
        action: de(row, 2)?,
        status: row.get(3)?,
        outcome: de_opt(row, 4)?,
        created_ms: u(row, 5)?,
        accepted_ms: u_opt(row, 6)?,
        terminal_ms: u_opt(row, 7)?,
    })
}

/// The latest `limit` commands of one orb, latest first (served by its
/// `(orb_id, generation)` index).
pub fn orb_commands(c: &Connection, orb: OrbId, limit: u32) -> Result<Vec<CmdHistory>> {
    let mut st = c.prepare(&format!(
        "SELECT {HIST_COLS} FROM commands WHERE orb_id = ?1 ORDER BY generation DESC LIMIT ?2"
    ))?;
    Ok(st
        .query_map(params![orb.to_string(), limit], hist_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Every request the coordinator holds (finished ones are kept for about an hour),
/// oldest first.
pub fn all_requests(c: &Connection) -> Result<Vec<Req>> {
    let mut st = c.prepare(&format!("SELECT {REQ_COLS} FROM requests ORDER BY seq"))?;
    Ok(st
        .query_map([], req_row)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Orbs that exist, oldest first, with when each was created and whether a delete of
/// it has started (tombstoned): those not deleted yet are listed too, so a delete that
/// fails or hangs stays visible.
pub fn live_orbs_created(c: &Connection) -> Result<Vec<(OrbRecord, u64, bool)>> {
    let mut st = c.prepare(&format!(
        "SELECT {ORB_COLS}, created_ms,
                EXISTS (SELECT 1 FROM delete_tombstones t WHERE t.orb_id = o.id)
         FROM orbs o WHERE retired_ms IS NULL ORDER BY created_ms"
    ))?;
    let rows = st
        .query_map([], |r| Ok((orb_row(r)?, u(r, 9)?, r.get::<_, bool>(10)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows
        .into_iter()
        .filter(|(o, _, tomb)| {
            !*tomb || !matches!(o.observation.state.last_known(), StableOrbState::Deleted)
        })
        .collect())
}

/// When the orb last spawned or paused successfully (indexed by orb).
pub fn last_active(c: &Connection, orb: OrbId) -> Result<Option<u64>> {
    Ok(c.query_row(
        "SELECT terminal_ms FROM commands
         WHERE orb_id = ?1 AND status = 'terminal'
           AND json_extract(action_json, '$.kind') IN ('spawn', 'pause')
           AND json_type(outcome_json, '$.result.Ok') IS NOT NULL
         ORDER BY generation DESC LIMIT 1",
        params![orb.to_string()],
        |r| u_opt(r, 0),
    )
    .optional()?
    .flatten())
}

/// What each orb's running reservation holds (an orb admitted to start, or running).
pub fn running_reservations(c: &Connection) -> Result<std::collections::HashMap<OrbId, Resources>> {
    let mut st = c.prepare(
        "SELECT orb_id, vcpus, memory_bytes, disk_bytes FROM reservations WHERE kind = 'running'",
    )?;
    let rows = st.query_map([], |r| {
        Ok((
            uuid_col(r, 0, OrbId::parse)?,
            Resources {
                vcpus: u(r, 1)?.min(u32::MAX as u64) as u32,
                memory_bytes: u(r, 2)?,
                disk_bytes: u(r, 3)?,
            },
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fleet(agent: &str) -> Location {
        Location::Fleet {
            backend_id: BackendId::new("fleet"),
            agent_id: AgentId::new(agent),
        }
    }

    /// An orb on agent-a, recorded `state` by its generation 5 command at
    /// `observed_at`.
    fn orb(c: &Connection, state: &OrbState, observed_at: u64) -> OrbId {
        let id = OrbId::generate();
        let spec = OrbSpec {
            template: SpecTemplate {
                target: ExecutionTarget::Fleet {
                    runtime: RuntimeKind::Fake,
                },
                resources: Resources {
                    vcpus: 2,
                    memory_bytes: 2 * GIB,
                    disk_bytes: 4 * GIB,
                },
                requirements: CapabilityRequirements {
                    release_compute_when_paused: true,
                    nested_kvm: NestedKvmRequirement::NotRequired,
                    project_snapshots: SnapshotRequirement::NotRequired,
                    arch: Arch::Amd64,
                    egress: None,
                },
                pause_policy: PausePolicy::MemoryPreserving {
                    on_failure: CheckpointFailurePolicy::ColdStop,
                },
            },
            initial_disk: InitialDiskSource::Image {
                image_ref: "image".into(),
            },
        };
        let obs = VersionedObservation {
            state: state.clone(),
            generation: 5,
            observed_at_unix_ms: observed_at,
        };
        c.execute(
            "INSERT INTO orbs(id, amp_endpoint, amp_orb_id, offering_id, spec_json, spec_digest,
                location_json, last_issued_generation, observation_json, observation_generation,
                created_ms)
             VALUES (?1, 'amp', ?2, 'offering', ?3, ?4, ?5, 5, ?6, 5, 0)",
            params![
                id.to_string(),
                format!("amp-{id}"),
                js(&spec),
                spec.digest().0,
                js(&fleet("agent-a")),
                js(&obs)
            ],
        )
        .unwrap();
        id
    }

    /// What a host reports outside any command is applied only to an orb recorded
    /// running (it stopped: stopped since now) or asleep that has lost its checkpoint
    /// (stopped since it went to sleep), from the orb's own location, not older than the
    /// record, and while no command is open for it.
    #[test]
    fn observe_stopped_applies_a_stop_or_a_lost_checkpoint_only() {
        let mut c = crate::coordinator::db::test_connection();
        let (here, then, now) = (fleet("agent-a"), 1_000_000, 2_000_000);
        let asleep = OrbState::known(StableOrbState::Sleeping {
            checkpoint: SleepCheckpointRef { generation: 3 },
        });
        let stopped = OrbState::known(StableOrbState::DiskOnlyStopped {
            reason: "memory checkpoint dropped".into(),
        });
        let running = OrbState::known(StableOrbState::Running);
        let observed = |c: &Connection, id| get_orb(c, id).unwrap().unwrap().observation;

        let a = orb(&c, &asleep, then);
        let other = fleet("agent-b");
        assert_eq!(
            observe_stopped(&mut c, a, &other, 5, &stopped, now).unwrap(),
            None,
            "another location"
        );
        assert_eq!(
            observe_stopped(&mut c, a, &here, 4, &stopped, now).unwrap(),
            None,
            "older than the record"
        );
        c.execute(
            "INSERT INTO commands(operation_id, orb_id, generation, action_json, command_digest,
                status, created_ms, work_deadline_ms, cleanup_deadline_ms)
             VALUES (?1, ?2, 6, ?3, 'd', 'accepted', 0, 0, 0)",
            params![
                OperationId::generate().to_string(),
                a.to_string(),
                js(&Action::Spawn)
            ],
        )
        .unwrap();
        assert_eq!(
            observe_stopped(&mut c, a, &here, 6, &stopped, now).unwrap(),
            None,
            "a command is open"
        );
        c.execute("UPDATE commands SET status = 'terminal'", [])
            .unwrap();
        assert_eq!(
            observe_stopped(&mut c, a, &here, 6, &asleep, now).unwrap(),
            None,
            "asleep is no news"
        );
        assert_eq!(
            observe_stopped(&mut c, a, &here, 6, &running, now).unwrap(),
            None,
            "only stops are applied"
        );
        assert_eq!(observed(&c, a).state, asleep, "nothing applied so far");
        assert_eq!(
            observe_stopped(&mut c, a, &here, 6, &stopped, now).unwrap(),
            Some(asleep.clone()),
            "asleep, then lost its checkpoint"
        );
        let o = observed(&c, a);
        assert_eq!(
            (o.state, o.generation, o.observed_at_unix_ms),
            (stopped.clone(), 5, then),
            "stopped since it went to sleep"
        );

        let r = orb(&c, &running, then);
        assert_eq!(
            observe_stopped(&mut c, r, &here, 5, &stopped, now).unwrap(),
            Some(running.clone())
        );
        let o = observed(&c, r);
        assert_eq!(
            (o.state, o.observed_at_unix_ms),
            (stopped.clone(), now),
            "stopped since now"
        );

        let faulted = OrbState::Faulted {
            last_known: StableOrbState::Running,
            reason: "inspect failed".into(),
        };
        let f = orb(&c, &faulted, then);
        assert_eq!(
            observe_stopped(&mut c, f, &here, 5, &stopped, now).unwrap(),
            None,
            "faulted"
        );
        assert_eq!(observed(&c, f).state, faulted);
    }

    /// A command of `orb` at `generation`, terminal with `result` and `observed`.
    fn command(
        c: &Connection,
        orb: OrbId,
        generation: u64,
        action: &Action,
        result: Result<ActionResult, CommandFailure>,
        observed: &OrbState,
    ) {
        let outcome = CommandOutcome {
            result,
            observed: observed.clone(),
            processes: ProcessDisposition::NotApplicable,
            snapshot: SnapshotDisposition::NotRequested,
        };
        c.execute(
            "INSERT INTO commands(operation_id, orb_id, generation, action_json, command_digest,
                status, outcome_json, created_ms, terminal_ms, work_deadline_ms,
                cleanup_deadline_ms)
             VALUES (?1, ?2, ?3, ?4, 'd', 'terminal', ?5, 0, 1, 0, 0)",
            params![
                OperationId::generate().to_string(),
                orb.to_string(),
                generation as i64,
                js(action),
                js(&outcome)
            ],
        )
        .unwrap();
    }

    fn refused() -> Result<ActionResult, CommandFailure> {
        Err(CommandFailure {
            code: FailureCode::InsufficientMemory,
            message: "agent capacity exceeded".into(),
        })
    }

    /// An orb never started while its last command (nothing issued since, none open)
    /// left it known uncreated, none ever started or paused it and no delete has begun:
    /// with how that command failed.
    #[test]
    fn orbs_that_never_started_are_told_apart_with_their_last_failure() {
        let c = crate::coordinator::db::test_connection();
        let uncreated = OrbState::known(StableOrbState::Uncreated);
        let u = orb(&c, &uncreated, 0);
        command(&c, u, 3, &Action::Spawn, refused(), &uncreated);
        let not_ready = Err(CommandFailure {
            code: FailureCode::ImageNotReady,
            message: "preparing".into(),
        });
        command(&c, u, 4, &Action::Spawn, not_ready, &uncreated);
        command(&c, u, 5, &Action::Spawn, refused(), &uncreated);
        assert_eq!(
            unstarted(&c, u, 5).unwrap(),
            Some(Unstarted {
                last_failure: Some(FailureCode::InsufficientMemory),
                capacity_refusals: 1,
            }),
            "refusals for want of room count only in a row"
        );
        command(&c, u, 6, &Action::Spawn, refused(), &uncreated);
        c.execute(
            "UPDATE orbs SET last_issued_generation = 6, observation_generation = 6,
                observation_json = json_set(observation_json, '$.generation', 6) WHERE id = ?1",
            params![u.to_string()],
        )
        .unwrap();
        assert_eq!(unstarted(&c, u, 6).unwrap().unwrap().capacity_refusals, 2);
        assert_eq!(
            unstarted(&c, u, 5).unwrap(),
            None,
            "a newer generation was issued"
        );

        // It started once (a spawn succeeded): it has a disk somewhere.
        let s = orb(&c, &uncreated, 0);
        let spawned = Ok(ActionResult::Spawned {
            start: StartKind::NewDisk,
            bootstrap_generation: 4,
        });
        command(
            &c,
            s,
            4,
            &Action::Spawn,
            spawned,
            &OrbState::known(StableOrbState::Running),
        );
        command(&c, s, 5, &Action::Spawn, refused(), &uncreated);
        assert_eq!(unstarted(&c, s, 5).unwrap(), None, "started before");

        for state in [
            OrbState::known(StableOrbState::Sleeping {
                checkpoint: SleepCheckpointRef { generation: 3 },
            }),
            OrbState::known(StableOrbState::DiskOnlyStopped {
                reason: "stopped".into(),
            }),
            OrbState::Faulted {
                last_known: StableOrbState::Uncreated,
                reason: "disk is missing".into(),
            },
        ] {
            let o = orb(&c, &state, 0);
            assert_eq!(unstarted(&c, o, 5).unwrap(), None, "{state:?}");
        }

        let o = orb(&c, &uncreated, 0);
        c.execute(
            "INSERT INTO commands(operation_id, orb_id, generation, action_json, command_digest,
                status, created_ms, work_deadline_ms, cleanup_deadline_ms)
             VALUES (?1, ?2, 5, ?3, 'd', 'accepted', 0, 0, 0)",
            params![
                OperationId::generate().to_string(),
                o.to_string(),
                js(&Action::Spawn)
            ],
        )
        .unwrap();
        assert_eq!(unstarted(&c, o, 5).unwrap(), None, "a command is open");

        let d = orb(&c, &uncreated, 0);
        c.execute(
            "INSERT INTO delete_tombstones(amp_endpoint, amp_orb_id, orb_id, created_ms)
             VALUES ('amp', ?1, ?2, 0)",
            params![format!("amp-{d}"), d.to_string()],
        )
        .unwrap();
        assert_eq!(unstarted(&c, d, 5).unwrap(), None, "being deleted");
    }

    /// A new incarnation of an orb that never started retires it in the command's own
    /// transaction (its Amp id is free only once the new one takes it), marks it for its
    /// old host to delete, and is refused, changing nothing, if the orb is no longer
    /// such an orb. Once that delete is done, nothing is left to clean up.
    #[test]
    fn a_new_incarnation_retires_an_orb_that_never_started_atomically() {
        let mut c = crate::coordinator::db::test_connection();
        let uncreated = OrbState::known(StableOrbState::Uncreated);
        let old = orb(&c, &uncreated, 0);
        command(&c, old, 5, &Action::Spawn, refused(), &uncreated);
        let rec = get_orb(&c, old).unwrap().unwrap();
        let plan = |retire: (OrbId, u64)| {
            let mut new = rec.clone();
            new.id = OrbId::generate();
            new.location = fleet("agent-b");
            new.last_issued_generation = 1;
            CreatePlan {
                invocation_id: InvocationId::generate(),
                orb_id: new.id,
                new_orb: Some(new),
                prev_generation: 0,
                generation: 1,
                operation_id: OperationId::generate(),
                action: Action::Spawn,
                command_digest: "d".into(),
                fingerprint: None,
                work_deadline_ms: 0,
                cleanup_deadline_ms: 0,
                reservation: None,
                tombstone: None,
                sealed: None,
                retire: Some(retire),
            }
        };
        let identity = |c: &Connection| {
            get_orb_by_identity(c, "amp", &format!("amp-{old}"))
                .unwrap()
                .map(|o| o.id)
        };

        // Stale: a newer generation was issued meanwhile.
        assert!(create_command(&mut c, &plan((old, 4))).is_err());
        assert_eq!(identity(&c), Some(old), "nothing changed");
        assert!(retired_undeleted(&c).unwrap().is_empty());

        let p = plan((old, 5));
        create_command(&mut c, &p).unwrap();
        assert_eq!(
            identity(&c),
            Some(p.orb_id),
            "the new incarnation has the Amp id"
        );
        assert!(is_tombstoned(&c, old).unwrap());
        assert!(!is_tombstoned(&c, p.orb_id).unwrap());
        let pending: Vec<OrbId> = retired_undeleted(&c)
            .unwrap()
            .iter()
            .map(|o| o.id)
            .collect();
        assert_eq!(pending, vec![old]);
        assert!(
            create_command(&mut c, &plan((old, 5))).is_err(),
            "retired once"
        );

        // A delete that failed (its host was busy): still to do, the try counted.
        command(
            &c,
            old,
            6,
            &Action::Delete,
            Err(CommandFailure {
                code: FailureCode::BackendFailure,
                message: "busy".into(),
            }),
            &uncreated,
        );
        let pending = retired_undeleted(&c).unwrap();
        assert_eq!(
            pending
                .iter()
                .map(|o| (o.id, o.attempts, o.last_attempt_ms))
                .collect::<Vec<_>>(),
            vec![(old, 1, Some(0))]
        );
        // Its old host deleted it.
        command(
            &c,
            old,
            7,
            &Action::Delete,
            Ok(ActionResult::Deleted),
            &OrbState::known(StableOrbState::Deleted),
        );
        c.execute(
            "UPDATE orbs SET observation_json = ?2, last_issued_generation = 7 WHERE id = ?1",
            params![
                old.to_string(),
                js(&VersionedObservation {
                    state: OrbState::known(StableOrbState::Deleted),
                    generation: 7,
                    observed_at_unix_ms: 0,
                })
            ],
        )
        .unwrap();
        assert!(retired_undeleted(&c).unwrap().is_empty());

        // A host that cannot run an orb (no runtime for it there) created nothing of it:
        // its answer to the delete ends the cleanup.
        let other = orb(&c, &uncreated, 0);
        command(&c, other, 5, &Action::Spawn, refused(), &uncreated);
        let rec = get_orb(&c, other).unwrap().unwrap();
        let mut p = plan((other, 5));
        p.new_orb.as_mut().unwrap().identity = rec.identity.clone();
        create_command(&mut c, &p).unwrap();
        assert_eq!(retired_undeleted(&c).unwrap().len(), 1);
        command(
            &c,
            other,
            6,
            &Action::Delete,
            Err(CommandFailure {
                code: FailureCode::UnsupportedCapability,
                message: "this agent has no runtime for the orb's execution target".into(),
            }),
            &uncreated,
        );
        assert!(retired_undeleted(&c).unwrap().is_empty());
    }

    /// An orb with `resources`, memory-preserving or not, recorded `state` at
    /// `observed_at`, with a `kind` reservation in `pool`.
    fn sized_orb(
        c: &Connection,
        resources: Resources,
        memory_preserving: bool,
        state: &OrbState,
        observed_at: u64,
        pool: &str,
        kind: &str,
    ) -> OrbRecord {
        let id = orb(c, state, observed_at);
        let mut rec = get_orb(c, id).unwrap().unwrap();
        rec.spec.template.resources = resources;
        if !memory_preserving {
            rec.spec.template.pause_policy = PausePolicy::DiskOnly;
        }
        c.execute_batch("DROP TRIGGER orbs_identity_immutable").ok();
        c.execute(
            "UPDATE orbs SET spec_json = ?2 WHERE id = ?1",
            params![id.to_string(), js(&rec.spec)],
        )
        .unwrap();
        c.execute(
            "INSERT INTO reservations(orb_id, backend_id, pool, vcpus, memory_bytes, disk_bytes,
                kind) VALUES (?1, 'fleet', ?2, 0, 0, 0, ?3)",
            params![id.to_string(), pool, kind],
        )
        .unwrap();
        rec
    }

    /// A pool keeps room for the largest wake among the orbs that may wake there, per
    /// resource and net of what their host reports their wakes free (for the generation
    /// they sleep on), computed by the database exactly as `wake_demand` sizes each
    /// wake; not for orbs running, being deleted, faulted, retired, asleep before
    /// `since`, archived, or in another pool.
    #[test]
    fn wake_room_is_the_largest_wake_that_may_come_per_resource() {
        let c = crate::coordinator::db::test_connection();
        let policy = crate::coordinator::core::Policy {
            running_orb_disk_headroom: 10 * GIB,
            idle_orb_ttl: None,
        };
        let res = |vcpus, mem_gib: u64, disk_gib: u64| Resources {
            vcpus,
            memory_bytes: mem_gib * GIB,
            disk_bytes: disk_gib * GIB,
        };
        let asleep = |g| {
            OrbState::known(StableOrbState::Sleeping {
                checkpoint: SleepCheckpointRef { generation: g },
            })
        };
        let stopped = OrbState::known(StableOrbState::DiskOnlyStopped {
            reason: "stopped".into(),
        });
        let (since, recent) = (1_000, 2_000);
        let big = sized_orb(
            &c,
            res(8, 32, 100),
            true,
            &asleep(9),
            recent,
            "a",
            "stopped",
        );
        let small = sized_orb(&c, res(2, 4, 50), true, &stopped, recent, "a", "stopped");
        let disk_only = sized_orb(&c, res(1, 1, 5), false, &stopped, recent, "a", "stopped");
        // None of these may wake here soon.
        let huge = res(32, 128, 500);
        sized_orb(&c, huge, true, &asleep(1), recent, "a", "running");
        sized_orb(&c, huge, true, &asleep(1), recent, "b", "stopped");
        sized_orb(&c, huge, true, &asleep(1), since - 1, "a", "stopped");
        let faulted = OrbState::Faulted {
            last_known: StableOrbState::Sleeping {
                checkpoint: SleepCheckpointRef { generation: 1 },
            },
            reason: "inspect failed".into(),
        };
        sized_orb(&c, huge, true, &faulted, recent, "a", "stopped");
        let tomb = sized_orb(&c, huge, true, &asleep(1), recent, "a", "stopped");
        c.execute(
            "INSERT INTO delete_tombstones(amp_endpoint, amp_orb_id, orb_id, created_ms)
             VALUES ('amp', ?1, ?2, 0)",
            params![format!("amp-{}", tomb.id), tomb.id.to_string()],
        )
        .unwrap();
        let retired = sized_orb(&c, huge, true, &asleep(1), recent, "a", "stopped");
        retire_orb(&c, retired.id).unwrap();
        let archived = sized_orb(&c, huge, true, &stopped, recent, "a", "stopped");

        let freed = |g, gib: u64| crate::backend::WakeRelease {
            checkpoint_generation: g,
            bytes: gib * GIB,
        };
        let room = |releases: &[(OrbId, crate::backend::WakeRelease)]| {
            wake_room(&c, "fleet", "a", 10 * GIB, since, releases, &[archived.id]).unwrap()
        };
        // The big one held in RAM: its wake frees its checkpoint room; the small one
        // stopped without a checkpoint needs its growth and its next checkpoint.
        let releases = [(big.id, freed(9, 32))];
        assert_eq!(room(&releases), res(8, 32, 14));
        // As `wake_demand` would size each one.
        let by_rust = [&big, &small, &disk_only]
            .iter()
            .map(|o| {
                let r = releases.iter().find(|(id, _)| *id == o.id).map(|(_, r)| *r);
                crate::coordinator::core::wake_demand(
                    &policy,
                    &o.spec.template,
                    &o.observation.state,
                    r,
                )
            })
            .fold(Resources::default(), |a, n| Resources {
                vcpus: a.vcpus.max(n.vcpus),
                memory_bytes: a.memory_bytes.max(n.memory_bytes),
                disk_bytes: a.disk_bytes.max(n.disk_bytes),
            });
        assert_eq!(room(&releases), by_rust);
        // Nothing reported for it (a checkpoint on disk a Firecracker VM keeps mapped),
        // or for another generation: its whole headroom.
        assert_eq!(room(&[]), res(8, 32, 42));
        assert_eq!(room(&[(big.id, freed(8, 32))]), res(8, 32, 42));
        assert_eq!(
            wake_room(&c, "fleet", "empty", 10 * GIB, since, &[], &[]).unwrap(),
            Resources::default()
        );
    }

    /// The hosts that refused an Amp orb before it started are where its incarnations
    /// retired for a new placement were, lately; not where an incarnation that ran was
    /// (deleted and replaced), nor long ago.
    #[test]
    fn refused_hosts_are_where_incarnations_that_never_started_were() {
        let c = crate::coordinator::db::test_connection();
        c.execute_batch("DROP TRIGGER orbs_identity_immutable")
            .unwrap();
        let uncreated = OrbState::known(StableOrbState::Uncreated);
        let retire_at = |id: OrbId, at: u64, agent: &str| {
            c.execute(
                "UPDATE orbs SET retired_ms = ?2, location_json = ?3, amp_orb_id = 'amp-x'
                 WHERE id = ?1",
                params![id.to_string(), at as i64, js(&fleet(agent))],
            )
            .unwrap();
            c.execute(
                "INSERT INTO delete_tombstones(amp_endpoint, amp_orb_id, orb_id, created_ms)
                 VALUES ('amp', 'amp-x', ?1, 0)",
                params![id.to_string()],
            )
            .unwrap();
        };
        let a = orb(&c, &uncreated, 0);
        retire_at(a, 5_000, "agent-a");
        let b = orb(&c, &uncreated, 0);
        retire_at(b, 6_000, "agent-b");
        let long_ago = orb(&c, &uncreated, 0);
        retire_at(long_ago, 100, "agent-c");
        let ran = orb(&c, &OrbState::known(StableOrbState::Deleted), 0);
        let spawned = Ok(ActionResult::Spawned {
            start: StartKind::NewDisk,
            bootstrap_generation: 1,
        });
        command(
            &c,
            ran,
            1,
            &Action::Spawn,
            spawned,
            &OrbState::known(StableOrbState::Running),
        );
        retire_at(ran, 7_000, "agent-d");
        assert_eq!(
            refused_hosts(&c, "amp", "amp-x", 1_000).unwrap(),
            vec![fleet("agent-b"), fleet("agent-a")]
        );
        assert!(refused_hosts(&c, "amp", "amp-y", 0).unwrap().is_empty());
    }

    /// The rows of an incarnation retired for a new placement go once nothing is left to
    /// do for it and it is old enough: its old host deleted it, cannot run it, or was
    /// sent every delete it gets. Not before, not one that ran, not a live orb.
    #[test]
    fn retired_incarnations_are_pruned_once_settled() {
        let mut c = crate::coordinator::db::test_connection();
        let uncreated = OrbState::known(StableOrbState::Uncreated);
        let deleted = OrbState::known(StableOrbState::Deleted);
        let retired = |c: &Connection, state: &OrbState, at: u64| {
            let id = orb(c, state, 0);
            c.execute(
                "UPDATE orbs SET retired_ms = ?2 WHERE id = ?1",
                params![id.to_string(), at as i64],
            )
            .unwrap();
            c.execute(
                "INSERT INTO delete_tombstones(amp_endpoint, amp_orb_id, orb_id, created_ms)
                 VALUES ('amp', ?1, ?2, 0)",
                params![format!("amp-{id}"), id.to_string()],
            )
            .unwrap();
            id
        };
        let busy = || {
            Err(CommandFailure {
                code: FailureCode::BackendFailure,
                message: "busy".into(),
            })
        };
        let done = retired(&c, &deleted, 100);
        command(&c, done, 5, &Action::Spawn, refused(), &uncreated);
        command(
            &c,
            done,
            6,
            &Action::Delete,
            Ok(ActionResult::Deleted),
            &deleted,
        );
        let unsupported = retired(&c, &uncreated, 100);
        let no_runtime = Err(CommandFailure {
            code: FailureCode::UnsupportedCapability,
            message: "no runtime".into(),
        });
        command(&c, unsupported, 6, &Action::Delete, no_runtime, &uncreated);
        let gave_up = retired(&c, &uncreated, 100);
        command(&c, gave_up, 6, &Action::Delete, busy(), &uncreated);
        command(&c, gave_up, 7, &Action::Delete, busy(), &uncreated);
        let pending = retired(&c, &uncreated, 100);
        command(&c, pending, 6, &Action::Delete, busy(), &uncreated);
        let young = retired(&c, &deleted, 5_000);
        let ran = retired(&c, &deleted, 100);
        let spawned = Ok(ActionResult::Spawned {
            start: StartKind::NewDisk,
            bootstrap_generation: 1,
        });
        command(
            &c,
            ran,
            4,
            &Action::Spawn,
            spawned,
            &OrbState::known(StableOrbState::Running),
        );
        let live = orb(&c, &uncreated, 0);

        assert_eq!(prune_retired_unstarted(&mut c, 1_000, 2).unwrap(), 3);
        let left = |c: &Connection, id: OrbId| get_orb(c, id).unwrap().is_some();
        for id in [done, unsupported, gave_up] {
            assert!(!left(&c, id));
            assert!(orb_commands(&c, id, 10).unwrap().is_empty());
            assert!(!is_tombstoned(&c, id).unwrap());
        }
        for id in [pending, young, ran, live] {
            assert!(left(&c, id));
        }
    }
}
