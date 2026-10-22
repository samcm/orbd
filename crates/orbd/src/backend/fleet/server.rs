//! The mTLS `AgentControl` gRPC server.

use super::{Inner, MISSED_HEARTBEATS};
use crate::config::coordinator::FleetBackendConfig;
use crate::model::AgentId;
use anyhow::Context;
use futures::Stream;
use orbd_proto::control::v1 as pb;
use orbd_proto::control::v1::agent_control_server::{AgentControl, AgentControlServer};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{TcpListenerStream, UnboundedReceiverStream};
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn serve(inner: Arc<Inner>, config: &FleetBackendConfig) -> anyhow::Result<()> {
    let read = |p: &std::path::Path| {
        std::fs::read(p).with_context(|| format!("reading TLS file {}", p.display()))
    };
    let cert = read(&config.tls.certificate_file)?;
    let key = read(&config.tls.private_key_file)?;
    let ca = read(&config.tls.ca_file)?;
    // `client_ca_root` makes client certificates mandatory.
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(cert, key))
        .client_ca_root(Certificate::from_pem(ca));
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("binding AgentControl listener {}", config.listen))?;
    let shutdown = inner.shutdown.clone();
    let router = Server::builder()
        .tls_config(tls)
        .context("configuring AgentControl TLS")?
        .http2_keepalive_interval(Some(Duration::from_secs(10)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .add_service(AgentControlServer::new(Service { inner }));
    tracing::info!(listen = %config.listen, "AgentControl listening (mTLS)");
    tokio::spawn(async move {
        if let Err(e) = router
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                shutdown.cancelled_owned(),
            )
            .await
        {
            tracing::error!(error = %e, "AgentControl server terminated");
        }
    });
    Ok(())
}

struct Service {
    inner: Arc<Inner>,
}

/// Names a client certificate vouches for: subject CNs and DNS SANs of the leaf.
fn certificate_names(request: &Request<Streaming<pb::AgentEvent>>) -> Result<Vec<String>, Status> {
    use x509_parser::extensions::GeneralName;
    let certs = request
        .peer_certs()
        .ok_or_else(|| Status::unauthenticated("client certificate required"))?;
    let leaf = certs
        .first()
        .ok_or_else(|| Status::unauthenticated("empty client certificate chain"))?;
    let (_, cert) = x509_parser::parse_x509_certificate(leaf.as_ref())
        .map_err(|_| Status::unauthenticated("unparseable client certificate"))?;
    let mut names: Vec<String> = cert
        .subject()
        .iter_common_name()
        .filter_map(|cn| cn.as_str().ok().map(str::to_owned))
        .collect();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for n in &san.value.general_names {
            if let GeneralName::DNSName(d) = n {
                names.push((*d).to_owned());
            }
        }
    }
    Ok(names)
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
        let names = certificate_names(&request)?;
        let mut inbound = request.into_inner();
        let first = tokio::time::timeout(HELLO_TIMEOUT, inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("no Hello"))??
            .ok_or_else(|| Status::invalid_argument("stream ended before Hello"))?;
        let Some(pb::agent_event::Event::Hello(hello)) = first.event else {
            return Err(Status::invalid_argument("first message must be Hello"));
        };
        let agent_id = hello.agent_id.clone();
        if !names.iter().any(|n| n == &agent_id) || !inner.authorised.contains(&agent_id) {
            tracing::warn!(agent_id = %agent_id.escape_debug(), "rejecting agent: identity not authorised");
            metrics::counter!("orbd_fleet_auth_rejected_total").increment(1);
            return Err(Status::permission_denied(
                "certificate does not authorise this agent id",
            ));
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
        let agent = AgentId::new(agent_id);
        metrics::counter!("orbd_fleet_messages_total", "type" => "hello").increment(1);
        let (sid, cancel, rx) = inner.open_session(&agent, hello);
        tracing::info!(%agent, session = %sid, "agent session opened");

        let reader = inner.clone();
        tokio::spawn(async move {
            let limit = reader.heartbeat_interval * MISSED_HEARTBEATS;
            let reason = loop {
                let msg = tokio::select! {
                    _ = cancel.cancelled() => break "session closed".to_owned(),
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
}
