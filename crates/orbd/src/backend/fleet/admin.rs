//! `orbd.control.v1.Admin` (local unix socket): join tokens and the agent registry.

use super::{Inner, registry};
use crate::coordinator::store::{self, PoolUse};
use crate::model::{AgentId, GIB, unix_ms_now};
use crate::pki::{self, JoinToken};
use orbd_proto::control::v1 as pb;
use orbd_proto::control::v1::admin_server::Admin;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};

pub const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(3600);

pub struct AdminService(pub(super) Arc<Inner>);

impl AdminService {
    pub fn into_server(self) -> pb::admin_server::AdminServer<Self> {
        pb::admin_server::AdminServer::new(self)
    }
}

fn internal(e: anyhow::Error) -> Status {
    tracing::error!(error = %format!("{e:#}"), "admin request failed");
    Status::internal(format!("{e:#}"))
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn create_join_token(
        &self,
        request: Request<pb::CreateJoinTokenRequest>,
    ) -> Result<Response<pb::CreateJoinTokenResponse>, Status> {
        let r = request.into_inner();
        pki::validate_agent_id(&r.agent_id).map_err(Status::invalid_argument)?;
        let ttl = match r.ttl_secs {
            0 => DEFAULT_TOKEN_TTL,
            s => Duration::from_secs(s),
        };
        let expires = unix_ms_now().saturating_add(ttl.as_millis() as u64);
        let token = JoinToken::new(&self.0.public_url, &self.0.ca.cert_der, &r.agent_id);
        let encoded = token.encode();
        self.0
            .db
            .call(move |c| registry::create_token(c, &token, expires))
            .await
            .map_err(internal)??;
        tracing::info!(agent = %r.agent_id, ttl_secs = ttl.as_secs(), "join token created");
        Ok(Response::new(pb::CreateJoinTokenResponse {
            token: encoded,
            expires_unix_ms: expires,
        }))
    }

    async fn list_agents(
        &self,
        _: Request<pb::ListAgentsRequest>,
    ) -> Result<Response<pb::ListAgentsResponse>, Status> {
        let backend = self.0.id.to_string();
        let pools: std::collections::HashMap<String, PoolUse> = self
            .0
            .db
            .call(move |c| store::pools(c, &backend))
            .await
            .map_err(internal)?
            .into_iter()
            .collect();
        let st = self.0.lock();
        let agents = st
            .agents
            .iter()
            .map(|(id, s)| {
                let used = pools.get(id.as_str()).copied().unwrap_or_default();
                let disk = s.heartbeat.as_ref().map(|(hb, _)| super::disk_report(hb));
                let d = disk.unwrap_or_default();
                pb::AgentInfo {
                    agent_id: id.to_string(),
                    removed: s.removed,
                    connected: s.session.is_some(),
                    enrolled_unix_ms: s.enrolled_ms,
                    certificate_expires_unix_ms: s.cert_not_after_ms,
                    capacity: s.hello.as_ref().and_then(|h| h.capacity),
                    disk_reported: disk.is_some(),
                    disk_free_bytes: d.free_bytes,
                    disk_total_bytes: d.total_bytes,
                    disk_allocated_bytes: d.allocated_bytes,
                    disk_reserve_bytes: d.reserve_bytes,
                    reserved: Some(used.held.into()),
                    orbs: used.orbs,
                    running_orbs: used.running,
                }
            })
            .collect();
        Ok(Response::new(pb::ListAgentsResponse { agents }))
    }

    async fn remove_agent(
        &self,
        request: Request<pb::RemoveAgentRequest>,
    ) -> Result<Response<pb::RemoveAgentResponse>, Status> {
        let id = AgentId::new(request.into_inner().agent_id);
        if !self.0.remove(&id).await.map_err(internal)? {
            return Err(Status::not_found(format!(
                "no agent or join token for {id}"
            )));
        }
        Ok(Response::new(pb::RemoveAgentResponse {}))
    }
}

// --- CLI (`orbd join-token`, `orbd agent list|remove`) ---------------------------

async fn client(
    socket: &std::path::Path,
) -> anyhow::Result<pb::admin_client::AdminClient<tonic::transport::Channel>> {
    use anyhow::Context;
    let channel = crate::amp::dispatch::connect(socket)
        .await
        .with_context(|| {
            format!(
                "connecting to {} (is the coordinator running?)",
                socket.display()
            )
        })?;
    Ok(pb::admin_client::AdminClient::new(channel))
}

fn status(s: Status) -> anyhow::Error {
    anyhow::anyhow!("{}", s.message())
}

/// Creates a join token; returns it as one line.
pub async fn cli_join_token(
    socket: &std::path::Path,
    agent_id: &str,
    ttl: Duration,
) -> anyhow::Result<String> {
    let r = client(socket)
        .await?
        .create_join_token(pb::CreateJoinTokenRequest {
            agent_id: agent_id.to_owned(),
            ttl_secs: ttl.as_secs().max(1),
        })
        .await
        .map_err(status)?;
    Ok(r.into_inner().token)
}

fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / GIB as f64)
}

/// One line per agent: id, state, orbs, reserved/capacity, disk, certificate expiry.
pub async fn cli_list(socket: &std::path::Path) -> anyhow::Result<String> {
    let r = client(socket)
        .await?
        .list_agents(pb::ListAgentsRequest {})
        .await
        .map_err(status)?;
    let now = unix_ms_now();
    let mut out = String::new();
    for a in r.into_inner().agents {
        let state = match (a.removed, a.connected) {
            (true, _) => "removed",
            (false, true) => "connected",
            (false, false) => "disconnected",
        };
        let days = a.certificate_expires_unix_ms.saturating_sub(now) / 86_400_000;
        let cap = a.capacity.unwrap_or_default();
        let held = a.reserved.unwrap_or_default();
        let disk = if a.disk_reported {
            let of = if a.disk_total_bytes > 0 {
                format!(" of {}", gib(a.disk_total_bytes))
            } else {
                String::new()
            };
            format!(
                "disk {} free{of} ({} kept free), {} allocated, {} reserved, limit {}",
                gib(a.disk_free_bytes),
                gib(a.disk_reserve_bytes),
                gib(a.disk_allocated_bytes),
                gib(held.disk_bytes),
                gib(cap.disk_bytes)
            )
        } else {
            "disk not reported".into()
        };
        out.push_str(&format!(
            "{}\t{state}\t{} orbs ({} running)\tvCPUs {}/{}\tmemory {}/{}\t{disk}\t\
             certificate expires in {days}d\n",
            a.agent_id,
            a.orbs,
            a.running_orbs,
            held.vcpus,
            cap.vcpus,
            gib(held.memory_bytes),
            gib(cap.memory_bytes),
        ));
    }
    Ok(out)
}

pub async fn cli_remove(socket: &std::path::Path, agent_id: &str) -> anyhow::Result<()> {
    client(socket)
        .await?
        .remove_agent(pb::RemoveAgentRequest {
            agent_id: agent_id.to_owned(),
        })
        .await
        .map_err(status)?;
    Ok(())
}
