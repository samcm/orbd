//! The `AgentControl` gRPC server on TLS from the built-in CA.
//!
//! Client certificates are optional at the TLS layer (enrolling agents have none yet)
//! but always chain-verified when presented. `Session` and `Renew` then require one
//! naming an active agent with its registered key; `Enroll` is the only call without.

use super::{AgentSlot, Inner, MISSED_HEARTBEATS, registry};
use crate::config::coordinator::CoordinatorConfig;
use crate::model::{AgentId, unix_ms_now};
use crate::pki::{self, CertInfo};
use anyhow::Context;
use futures::Stream;
use orbd_proto::control::v1 as pb;
use orbd_proto::control::v1::agent_control_server::{AgentControl, AgentControlServer};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tonic::transport::Server;
use tonic::transport::server::Connected;
use tonic::{Request, Response, Status, Streaming};

const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HANDSHAKES: usize = 256;
/// Server certificates last [`pki::LEAF_LIFETIME`]; a fresh one is issued at every start
/// and then this often.
const SERVER_CERT_ROTATION: Duration = Duration::from_secs(30 * 24 * 3600);
/// Enrollment attempts allowed per [`ENROLL_WINDOW`] (all clients together).
const ENROLL_BURST: usize = 30;
const ENROLL_WINDOW: Duration = Duration::from_secs(60);
const MAX_CSR_BYTES: usize = 4096;

fn tls_config(inner: &Inner, host: &str) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(inner.ca.cert_der.clone()))?;
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .allow_unauthenticated()
    .build()?;
    let (chain, key) = inner.ca.issue_server(host)?;
    let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            chain.into_iter().map(CertificateDer::from).collect(),
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)),
        )?;
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(cfg))
}

pub(super) async fn serve(inner: Arc<Inner>, config: &CoordinatorConfig) -> anyhow::Result<()> {
    let host = pki::url_host(&config.public_url).map_err(anyhow::Error::msg)?;
    let tls = Arc::new(RwLock::new(tls_config(&inner, &host)?));
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("binding AgentControl listener {}", config.listen))?;
    let shutdown = inner.shutdown.clone();

    // Server certificate rotation.
    {
        let (tls, inner, shutdown) = (tls.clone(), inner.clone(), shutdown.clone());
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(SERVER_CERT_ROTATION) => {}
                }
                match tls_config(&inner, &host) {
                    Ok(cfg) => *tls.write().unwrap_or_else(|e| e.into_inner()) = cfg,
                    Err(e) => tracing::error!(error = %e, "renewing server certificate"),
                }
            }
        });
    }

    // TLS handshakes happen here (bounded, with a timeout); tonic sees finished streams.
    let (conn_tx, conn_rx) = tokio::sync::mpsc::channel::<std::io::Result<AgentConn>>(64);
    {
        let shutdown = shutdown.clone();
        let permits = Arc::new(tokio::sync::Semaphore::new(MAX_HANDSHAKES));
        tokio::spawn(async move {
            loop {
                let (tcp, _) = tokio::select! {
                    _ = shutdown.cancelled() => return,
                    a = listener.accept() => match a {
                        Ok(a) => a,
                        Err(e) => {
                            tracing::warn!(error = %e, "accepting AgentControl connection");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    },
                };
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    continue; // overloaded: drop the connection
                };
                let _ = tcp.set_nodelay(true);
                let acceptor = tokio_rustls::TlsAcceptor::from(
                    tls.read().unwrap_or_else(|e| e.into_inner()).clone(),
                );
                let conn_tx = conn_tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                        Ok(Ok(stream)) => {
                            let peer = stream
                                .get_ref()
                                .1
                                .peer_certificates()
                                .and_then(|c| c.first())
                                .map(|c| Arc::new(c.to_vec()));
                            let _ = conn_tx.send(Ok(AgentConn { stream, peer })).await;
                        }
                        Ok(Err(e)) => tracing::debug!(error = %e, "TLS handshake failed"),
                        Err(_) => tracing::debug!("TLS handshake timed out"),
                    }
                });
            }
        });
    }

    let router = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .add_service(AgentControlServer::new(Service {
            inner,
            enrollments: Mutex::default(),
        }));
    tracing::info!(listen = %config.listen, "AgentControl listening (TLS)");
    tokio::spawn(async move {
        if let Err(e) = router
            .serve_with_incoming_shutdown(ReceiverStream::new(conn_rx), shutdown.cancelled_owned())
            .await
        {
            tracing::error!(error = %e, "AgentControl server terminated");
        }
    });
    Ok(())
}

/// A TLS connection and the (chain-verified) client certificate it presented, if any.
struct AgentConn {
    stream: tokio_rustls::server::TlsStream<TcpStream>,
    peer: Option<Arc<Vec<u8>>>,
}

#[derive(Clone)]
struct PeerCert(Option<Arc<Vec<u8>>>);

impl Connected for AgentConn {
    type ConnectInfo = PeerCert;
    fn connect_info(&self) -> PeerCert {
        PeerCert(self.peer.clone())
    }
}

impl AsyncRead for AgentConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for AgentConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

struct Service {
    inner: Arc<Inner>,
    /// Recent enrollment attempts, for rate limiting.
    enrollments: Mutex<VecDeque<Instant>>,
}

/// The verified client certificate of `request`, and the agent id it names.
fn client_cert<T>(request: &Request<T>) -> Result<(String, CertInfo), Status> {
    let der = request
        .extensions()
        .get::<PeerCert>()
        .and_then(|p| p.0.clone())
        .ok_or_else(|| Status::unauthenticated("client certificate required"))?;
    let info = pki::cert_info(&der)
        .map_err(|_| Status::unauthenticated("unparseable client certificate"))?;
    let id = info
        .names
        .first()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("client certificate names no agent"))?;
    Ok((id, info))
}

fn internal(e: anyhow::Error) -> Status {
    tracing::error!(error = %format!("{e:#}"), "AgentControl request failed");
    Status::internal("internal error")
}

fn denied(agent: &str, why: &str) -> Status {
    tracing::warn!(agent_id = %agent.escape_debug(), reason = why, "rejecting agent");
    metrics::counter!("orbd_fleet_auth_rejected_total").increment(1);
    Status::permission_denied(format!("agent {agent}: {why}"))
}

impl Service {
    fn admit_enrollment(&self) -> Result<(), Status> {
        let mut recent = self.enrollments.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        while recent
            .front()
            .is_some_and(|t| now.duration_since(*t) > ENROLL_WINDOW)
        {
            recent.pop_front();
        }
        if recent.len() >= ENROLL_BURST {
            return Err(Status::resource_exhausted(
                "too many enrollments; retry later",
            ));
        }
        recent.push_back(now);
        Ok(())
    }
}

type SessionStream = Pin<Box<dyn Stream<Item = Result<pb::CoordinatorEvent, Status>> + Send>>;

#[tonic::async_trait]
impl AgentControl for Service {
    type SessionStream = SessionStream;

    async fn session(
        &self,
        request: Request<Streaming<pb::AgentEvent>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let inner = &self.inner;
        let (cert_agent, cert) = client_cert(&request)?;
        let mut inbound = request.into_inner();
        let first = tokio::time::timeout(HELLO_TIMEOUT, inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("no Hello"))??
            .ok_or_else(|| Status::invalid_argument("stream ended before Hello"))?;
        let Some(pb::agent_event::Event::Hello(hello)) = first.event else {
            return Err(Status::invalid_argument("first message must be Hello"));
        };
        if cert.not_after_ms <= unix_ms_now() {
            return Err(denied(&cert_agent, "certificate expired"));
        }
        if hello.agent_id != cert_agent {
            return Err(denied(&hello.agent_id, "certificate names another agent"));
        }
        if !hello
            .protocol_versions
            .contains(&orbd_proto::CONTROL_PROTOCOL_VERSION)
        {
            return Err(Status::failed_precondition(format!(
                "unsupported protocol versions {:?}; need {}",
                hello.protocol_versions,
                orbd_proto::CONTROL_PROTOCOL_VERSION
            )));
        }
        let agent = AgentId::new(cert_agent);
        let Some((sid, cancel, rx)) = inner.open_session(&agent, &cert.key_sha256, hello) else {
            return Err(denied(agent.as_str(), "not an active agent with this key"));
        };
        metrics::counter!("orbd_fleet_messages_total", "type" => "hello").increment(1);
        tracing::info!(%agent, session = %sid, "agent session opened");

        // A session never outlives the certificate that opened it.
        let expires = Duration::from_millis(cert.not_after_ms.saturating_sub(unix_ms_now()));
        let reader = inner.clone();
        tokio::spawn(async move {
            let limit = reader.heartbeat_interval * MISSED_HEARTBEATS;
            let expired = tokio::time::sleep(expires);
            tokio::pin!(expired);
            let reason = loop {
                let msg = tokio::select! {
                    _ = cancel.cancelled() => break "session closed".to_owned(),
                    _ = &mut expired => break "certificate expired".to_owned(),
                    m = tokio::time::timeout(limit, inbound.message()) => m,
                };
                match msg {
                    Err(_) => break format!("missed {MISSED_HEARTBEATS} heartbeats"),
                    Ok(Ok(Some(ev))) => {
                        if !reader.handle_event(&agent, sid, ev) {
                            return; // replaced; nothing to clean up
                        }
                    }
                    Ok(Ok(None)) => break "stream ended".to_owned(),
                    Ok(Err(e)) => break format!("stream error: {}", e.code()),
                }
            };
            reader.end_session(&agent, sid, reason);
        });
        Ok(Response::new(Box::pin(
            UnboundedReceiverStream::new(rx).map(Ok),
        )))
    }

    async fn enroll(
        &self,
        request: Request<pb::EnrollRequest>,
    ) -> Result<Response<pb::EnrollResponse>, Status> {
        self.admit_enrollment()?;
        let r = request.into_inner();
        if r.csr_der.len() > MAX_CSR_BYTES || r.token_id.len() > 64 || r.secret.len() != 32 {
            return Err(Status::invalid_argument("malformed enrollment request"));
        }
        let inner = self.inner.clone();
        let enrolled = self
            .inner
            .db
            .call(move |c| registry::redeem(c, &inner.ca, &r.token_id, &r.secret, &r.csr_der))
            .await
            .map_err(internal)?
            .inspect_err(|s| {
                tracing::warn!(reason = s.message(), "enrollment refused");
                metrics::counter!("orbd_fleet_auth_rejected_total").increment(1);
            })?;
        let agent_id = enrolled.row.id.clone();
        if enrolled.new {
            tracing::info!(agent = %agent_id, "agent enrolled");
            let mut st = self.inner.lock();
            st.agents
                .insert(AgentId::new(agent_id.clone()), AgentSlot::new(enrolled.row));
        }
        Ok(Response::new(pb::EnrollResponse {
            agent_id,
            certificate_chain: vec![enrolled.cert_der, self.inner.ca.cert_der.clone()],
        }))
    }

    async fn renew(
        &self,
        request: Request<pb::RenewRequest>,
    ) -> Result<Response<pb::RenewResponse>, Status> {
        let (agent, cert) = client_cert(&request)?;
        let csr = request.into_inner().csr_der;
        if csr.len() > MAX_CSR_BYTES {
            return Err(Status::invalid_argument("CSR too large"));
        }
        let csr_key =
            pki::csr_key_sha256(&csr).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let active = {
            let st = self.inner.lock();
            st.agents
                .get(&AgentId::new(agent.clone()))
                .is_some_and(|s| {
                    !s.removed && s.key_sha256 == cert.key_sha256 && s.key_sha256 == csr_key
                })
        };
        if !active {
            return Err(denied(&agent, "not an active agent with this key"));
        }
        let (der, _) = self.inner.ca.issue_agent(&agent, &csr).map_err(internal)?;
        let not_after = pki::cert_info(&der).map_err(internal)?.not_after_ms;
        let (id, key) = (agent.clone(), csr_key.clone());
        let ok = self
            .inner
            .db
            .call(move |c| registry::renewed(c, &id, &key, not_after))
            .await
            .map_err(internal)?;
        if !ok {
            return Err(denied(&agent, "removed during renewal"));
        }
        if let Some(s) = self
            .inner
            .lock()
            .agents
            .get_mut(&AgentId::new(agent.clone()))
        {
            s.cert_not_after_ms = not_after;
        }
        tracing::info!(%agent, "agent certificate renewed");
        Ok(Response::new(pb::RenewResponse {
            certificate_chain: vec![der, self.inner.ca.cert_der.clone()],
        }))
    }
}
