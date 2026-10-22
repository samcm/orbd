//! Agent registry and join tokens in the coordinator database (tables `agents`,
//! `join_tokens`). The database is the source of truth; the backend keeps a cache of
//! `agents` in its in-memory state, updated only after a commit.

use crate::model::unix_ms_now;
use crate::pki::{self, Ca};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use tonic::Status;

pub struct AgentRow {
    pub id: String,
    pub key_sha256: String,
    pub cert_not_after_ms: u64,
    pub enrolled_ms: u64,
    pub removed: bool,
}

pub fn load(c: &Connection) -> Result<Vec<AgentRow>> {
    let mut st = c.prepare(
        "SELECT id, key_sha256, cert_not_after_ms, enrolled_ms, removed_ms IS NOT NULL
         FROM agents",
    )?;
    let rows = st.query_map([], |r| {
        Ok(AgentRow {
            id: r.get(0)?,
            key_sha256: r.get(1)?,
            cert_not_after_ms: r.get::<_, i64>(2)? as u64,
            enrolled_ms: r.get::<_, i64>(3)? as u64,
            removed: r.get(4)?,
        })
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}

fn registered(c: &Connection, agent_id: &str) -> rusqlite::Result<bool> {
    c.query_row("SELECT 1 FROM agents WHERE id = ?1", [agent_id], |_| Ok(()))
        .optional()
        .map(|r| r.is_some())
}

/// Records a new token. Refused for any id already registered, removed ones included
/// (their orbs still reference the id).
pub fn create_token(
    c: &Connection,
    token: &pki::JoinToken,
    expires_ms: u64,
) -> Result<Result<(), Status>> {
    if registered(c, &token.agent_id)? {
        return Ok(Err(Status::already_exists(format!(
            "agent {} is already registered; a new host needs a new agent id",
            token.agent_id
        ))));
    }
    c.execute(
        "INSERT INTO join_tokens (id, agent_id, secret_sha256, created_ms, expires_ms)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            token.token_id,
            token.agent_id,
            pki::sha256_hex(&token.secret),
            unix_ms_now() as i64,
            expires_ms as i64
        ],
    )?;
    Ok(Ok(()))
}

pub struct Enrolled {
    pub row: AgentRow,
    pub cert_der: Vec<u8>,
    /// False when this was a retry that returned the certificate issued before.
    pub new: bool,
}

/// Redeems a token for a certificate in one transaction: token consumption, the agent
/// record (with its key) and the issued certificate commit together. A retry with the
/// same CSR returns the same certificate; any other CSR is refused.
pub fn redeem(
    c: &mut Connection,
    ca: &Ca,
    token_id: &str,
    secret: &[u8],
    csr_der: &[u8],
) -> Result<Result<Enrolled, Status>> {
    let denied = || Ok(Err(Status::permission_denied("join token is not valid")));
    let tx = c.transaction()?;
    #[allow(clippy::type_complexity)]
    let row: Option<(String, String, i64, Option<i64>, Option<i64>, Option<String>, Option<Vec<u8>>)> = tx
        .query_row(
            "SELECT agent_id, secret_sha256, expires_ms, revoked_ms, redeemed_ms, csr_sha256, cert_der
             FROM join_tokens WHERE id = ?1",
            [token_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?)),
        )
        .optional()?;
    let Some((agent_id, secret_sha, expires, revoked, redeemed, csr_sha, cert)) = row else {
        return denied();
    };
    if pki::sha256_hex(secret) != secret_sha || revoked.is_some() {
        return denied();
    }
    if redeemed.is_some() {
        // Same CSR: the agent lost our response; hand out the same certificate.
        return match (csr_sha, cert) {
            (Some(h), Some(cert)) if h == pki::sha256_hex(csr_der) => {
                let row = load(&tx)?.into_iter().find(|a| a.id == agent_id);
                match row {
                    Some(row) if !row.removed => Ok(Ok(Enrolled {
                        row,
                        cert_der: cert,
                        new: false,
                    })),
                    _ => denied(),
                }
            }
            _ => Ok(Err(Status::permission_denied(
                "join token was already used",
            ))),
        };
    }
    let now = unix_ms_now();
    if now as i64 >= expires {
        return Ok(Err(Status::permission_denied("join token has expired")));
    }
    if registered(&tx, &agent_id)? {
        return Ok(Err(Status::already_exists(format!(
            "agent {agent_id} is already enrolled"
        ))));
    }
    let (cert_der, key_sha256) = match ca.issue_agent(&agent_id, csr_der) {
        Ok(v) => v,
        Err(e) => return Ok(Err(Status::invalid_argument(e.to_string()))),
    };
    let info = pki::cert_info(&cert_der)?;
    tx.execute(
        "UPDATE join_tokens SET redeemed_ms = ?2, csr_sha256 = ?3, cert_der = ?4 WHERE id = ?1",
        params![token_id, now as i64, pki::sha256_hex(csr_der), cert_der],
    )?;
    tx.execute(
        "INSERT INTO agents (id, key_sha256, cert_not_after_ms, enrolled_ms)
         VALUES (?1, ?2, ?3, ?4)",
        params![agent_id, key_sha256, info.not_after_ms as i64, now as i64],
    )?;
    tx.commit()?;
    Ok(Ok(Enrolled {
        row: AgentRow {
            id: agent_id,
            key_sha256,
            cert_not_after_ms: info.not_after_ms,
            enrolled_ms: now,
            removed: false,
        },
        cert_der,
        new: true,
    }))
}

/// Records a renewed certificate, if the agent is still active with the same key.
pub fn renewed(
    c: &Connection,
    agent_id: &str,
    key_sha256: &str,
    not_after_ms: u64,
) -> Result<bool> {
    Ok(c.execute(
        "UPDATE agents SET cert_not_after_ms = ?3
         WHERE id = ?1 AND key_sha256 = ?2 AND removed_ms IS NULL",
        params![agent_id, key_sha256, not_after_ms as i64],
    )? == 1)
}

/// Removes an agent for good and revokes its outstanding tokens. Returns false if
/// neither an agent nor a token exists for the id.
pub fn remove(c: &mut Connection, agent_id: &str) -> Result<bool> {
    let now = unix_ms_now() as i64;
    let tx = c.transaction()?;
    let agents = tx.execute(
        "UPDATE agents SET removed_ms = ?2 WHERE id = ?1 AND removed_ms IS NULL",
        params![agent_id, now],
    )?;
    let tokens = tx.execute(
        "UPDATE join_tokens SET revoked_ms = ?2
         WHERE agent_id = ?1 AND redeemed_ms IS NULL AND revoked_ms IS NULL",
        params![agent_id, now],
    )?;
    let known = agents + tokens > 0 || registered(&tx, agent_id)?;
    tx.commit()?;
    Ok(known)
}
