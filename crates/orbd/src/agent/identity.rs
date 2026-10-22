//! The agent's identity: enrollment with a join token, the mTLS credentials it yields,
//! and certificate renewal.
//!
//! Files live in `<state_dir>/identity/` (0700, files 0600): `enrollment.json` (agent id
//! and coordinator URL), `ca.pem`, `agent.key`, `agent.csr` and, once enrolled,
//! `agent.pem`. The key and CSR are written before the CSR is sent, so a retry after a
//! lost response presents the same CSR and gets the same certificate.

use super::Shared;
use crate::model::AgentId;
use crate::pki::{self, JoinToken};
use anyhow::{Context, Result, bail};
use orbd_proto::control::v1 as pb;
use orbd_proto::control::v1::agent_control_client::AgentControlClient;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity as TlsIdentity};

const RENEW_RETRY: Duration = Duration::from_secs(600);

#[derive(Serialize, Deserialize)]
struct Enrollment {
    agent_id: String,
    coordinator_url: String,
}

pub(crate) struct Identity {
    pub agent_id: AgentId,
    pub coordinator_url: String,
    dir: PathBuf,
}

impl Identity {
    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// Loads the enrolled identity, or enrolls with `join`. Returns `None` if shut down
    /// while waiting for the coordinator.
    pub async fn establish(
        state_dir: &Path,
        join: Option<&str>,
        shutdown: &CancellationToken,
    ) -> Result<Option<Self>> {
        let dir = state_dir.join("identity");
        let meta: Option<Enrollment> = match std::fs::read(dir.join("enrollment.json")) {
            Ok(b) => Some(serde_json::from_slice(&b).context("reading enrollment.json")?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e).context("reading enrollment.json"),
        };
        let token = join.map(JoinToken::decode).transpose()?;
        if let Some(m) = &meta
            && dir.join("agent.pem").exists()
        {
            if let Some(t) = &token {
                tracing::info!(
                    agent = %m.agent_id,
                    token_agent = %t.agent_id,
                    "already enrolled; ignoring the join token"
                );
            }
            return Ok(Some(Self {
                agent_id: AgentId::new(m.agent_id.clone()),
                coordinator_url: m.coordinator_url.clone(),
                dir,
            }));
        }
        let Some(token) = token else {
            bail!(
                "this agent is not enrolled: start it with ORBD_JOIN=<token> \
                 (run `orbd join-token <agent-id>` on the coordinator)"
            );
        };
        match &meta {
            // An unfinished enrollment (key and CSR persisted, no certificate): retry it.
            Some(m) => {
                if m.agent_id != token.agent_id || m.coordinator_url != token.coordinator_url {
                    bail!(
                        "{} holds an unfinished enrollment as {} at {}; use a token for that \
                         agent, or delete the directory to start over",
                        dir.display(),
                        m.agent_id,
                        m.coordinator_url
                    );
                }
            }
            None => {
                pki::private_dir(&dir)?;
                let (key, csr) = pki::new_agent_key(&token.agent_id)?;
                pki::write_private(&dir.join("agent.key"), key.as_bytes())?;
                pki::write_private(&dir.join("agent.csr"), &csr)?;
                let meta = Enrollment {
                    agent_id: token.agent_id.clone(),
                    coordinator_url: token.coordinator_url.clone(),
                };
                pki::write_private(&dir.join("enrollment.json"), &serde_json::to_vec(&meta)?)?;
            }
        }
        let ca_pem = pki::der_to_pem("CERTIFICATE", &token.ca_der);
        pki::write_private(&dir.join("ca.pem"), ca_pem.as_bytes())?;
        let id = Self {
            agent_id: AgentId::new(token.agent_id.clone()),
            coordinator_url: token.coordinator_url.clone(),
            dir,
        };
        let csr = std::fs::read(id.path("agent.csr")).context("reading agent.csr")?;
        let request = pb::EnrollRequest {
            token_id: token.token_id.clone(),
            secret: token.secret.to_vec(),
            csr_der: csr,
        };
        let mut delay = Duration::from_secs(1);
        let chain = loop {
            let attempt = async {
                let channel = id.channel(None).await?;
                let r = AgentControlClient::new(channel)
                    .enroll(request.clone())
                    .await?;
                anyhow::Ok(r.into_inner().certificate_chain)
            };
            let err = tokio::select! {
                _ = shutdown.cancelled() => return Ok(None),
                r = attempt => match r {
                    Ok(chain) => break chain,
                    Err(e) => e,
                },
            };
            // The coordinator refused the token: retrying cannot help.
            if let Some(s) = err.downcast_ref::<tonic::Status>()
                && !matches!(
                    s.code(),
                    tonic::Code::Unavailable | tonic::Code::ResourceExhausted
                )
            {
                bail!("enrollment as {} refused: {}", id.agent_id, s.message());
            }
            tracing::warn!(error = %format!("{err:#}"), "enrollment failed; retrying");
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(None),
                _ = tokio::time::sleep(delay) => {}
            }
            delay = (delay * 2).min(Duration::from_secs(30));
        };
        id.install(&chain, &request.csr_der)?;
        tracing::info!(agent = %id.agent_id, "enrolled");
        Ok(Some(id))
    }

    /// Checks an issued chain against our id and key and saves the leaf.
    fn install(&self, chain: &[Vec<u8>], csr: &[u8]) -> Result<()> {
        let leaf = chain.first().context("empty certificate chain")?;
        let info = pki::cert_info(leaf)?;
        if !info.names.iter().any(|n| n == self.agent_id.as_str())
            || info.key_sha256 != pki::csr_key_sha256(csr)?
        {
            bail!("coordinator issued a certificate for another identity");
        }
        pki::write_private(
            &self.path("agent.pem"),
            pki::der_to_pem("CERTIFICATE", leaf).as_bytes(),
        )
    }

    /// A TLS channel to the coordinator, verifying it against our CA and the URL host;
    /// presenting `identity` (cert PEM, key PEM) when given.
    async fn channel(&self, identity: Option<(Vec<u8>, Vec<u8>)>) -> Result<Channel> {
        let ca = tokio::fs::read(self.path("ca.pem"))
            .await
            .context("reading ca.pem")?;
        let host = pki::url_host(&self.coordinator_url).map_err(anyhow::Error::msg)?;
        let mut tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca))
            .domain_name(host);
        if let Some((cert, key)) = identity {
            tls = tls.identity(TlsIdentity::from_pem(cert, key));
        }
        Ok(Endpoint::from_shared(self.coordinator_url.clone())?
            .tls_config(tls)?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(10))
            .keep_alive_while_idle(true)
            .connect()
            .await?)
    }

    /// An mTLS channel with the current certificate.
    pub async fn mtls_channel(&self) -> Result<Channel> {
        let cert = tokio::fs::read(self.path("agent.pem"))
            .await
            .context("reading agent.pem")?;
        let key = tokio::fs::read(self.path("agent.key"))
            .await
            .context("reading agent.key")?;
        self.channel(Some((cert, key))).await
    }

    /// Two thirds into the certificate's life, ± 1/18 of it (about day 60 ± 5 of 90).
    fn renew_at_ms(&self) -> Result<u64> {
        let pem = std::fs::read_to_string(self.path("agent.pem"))?;
        let der = pki::pem_body(&pem)?;
        let info = pki::cert_info(&der)?;
        let life = info.not_after_ms.saturating_sub(info.not_before_ms);
        let spread = life / 9;
        let jitter = rand::random::<u64>() % (spread + 1);
        Ok(info.not_before_ms + life * 2 / 3 - spread / 2 + jitter)
    }

    async fn renew(&self) -> Result<()> {
        let key = std::fs::read_to_string(self.path("agent.key"))?;
        let csr = pki::agent_csr(&key, self.agent_id.as_str())?;
        let mut client = AgentControlClient::new(self.mtls_channel().await?);
        let chain = client
            .renew(pb::RenewRequest {
                csr_der: csr.clone(),
            })
            .await?
            .into_inner()
            .certificate_chain;
        self.install(&chain, &csr)
    }
}

/// Renews the certificate around day 60, then drops the session so it reconnects with
/// the new one. Failures are retried every [`RENEW_RETRY`].
pub(crate) async fn renew_loop(sh: Arc<Shared>) {
    let id = &sh.identity;
    let sleep = |d: Duration| {
        let shutdown = sh.shutdown.clone();
        async move {
            tokio::select! {
                _ = shutdown.cancelled() => false,
                _ = tokio::time::sleep(d) => true,
            }
        }
    };
    loop {
        let at = match id.renew_at_ms() {
            Ok(at) => at,
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "reading agent certificate");
                if !sleep(RENEW_RETRY).await {
                    return;
                }
                continue;
            }
        };
        let wait = Duration::from_millis(at.saturating_sub(crate::model::unix_ms_now()));
        if !sleep(wait).await {
            return;
        }
        match id.renew().await {
            Ok(()) => {
                tracing::info!("agent certificate renewed; reconnecting");
                if let Some(s) = sh
                    .session
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                {
                    s.kill.cancel();
                }
            }
            Err(e) => {
                tracing::warn!(error = %format!("{e:#}"), "certificate renewal failed; retrying");
                if !sleep(RENEW_RETRY).await {
                    return;
                }
            }
        }
    }
}
