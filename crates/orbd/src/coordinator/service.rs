//! `orbd.control.v1.Dispatch` on the local unix socket: validates requests and hands them
//! to the coordinator core. The core owns everything after validation.

use super::core::{Exec, Msg, ValidatedRequest};
use super::types::{ReqAction, Status, StoredResult};
use crate::model::{AmpOrbId, InvocationId, Offering, OfferingId};
use crate::secrets::{PermittedGuestEnv, SecretKey};
use orbd_proto::control::v1 as pb;
use orbd_proto::control::v1::dispatch_server::Dispatch;
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response};

pub struct DispatchService {
    pub core: mpsc::Sender<Msg>,
    pub offerings: Arc<HashMap<OfferingId, Offering>>,
    /// Each provider with orb types: its orb types (offerings), the default first.
    pub providers: Arc<HashMap<String, Vec<OfferingId>>>,
    pub key: Arc<SecretKey>,
}

/// Why a request was not handed to the core: `Invalid` (the default) blames the job,
/// so the Amp CLI that ran it; `Failed` blames orbd's configuration.
struct Rejection(Status, String);

impl<S: Into<String>> From<S> for Rejection {
    fn from(msg: S) -> Self {
        Self(Status::Invalid, msg.into())
    }
}

const MAX_FIELD: usize = 512;

fn short(name: &str, v: &str) -> Result<(), String> {
    if v.len() > MAX_FIELD {
        return Err(format!("{name} is too long"));
    }
    Ok(())
}

impl DispatchService {
    /// The offering a new orb of this request is placed as (an existing orb keeps its
    /// own). A provider's spawn names it as `ORB_TYPE`; without one, and for a pause or
    /// delete, it is the provider's first orb type.
    fn offering(&self, r: &pb::DispatchRequest) -> Result<&Offering, Rejection> {
        let get = |id: &OfferingId| self.offerings.get(id).ok_or("unknown offering");
        if r.provider_id.is_empty() {
            return Ok(get(&OfferingId::new(r.offering_id.as_str()))?);
        }
        if !r.offering_id.is_empty() {
            return Err("a job names a provider or an offering, not both".into());
        }
        let types = self
            .providers
            .get(&r.provider_id)
            .ok_or("unknown provider")?;
        let spawn = matches!(r.action, Some(pb::dispatch_request::Action::Spawn(_)));
        if !spawn || r.orb_type.is_empty() {
            return Ok(get(&types[0])?);
        }
        match types.iter().find(|t| t.as_str() == r.orb_type) {
            Some(t) => Ok(get(t)?),
            None => Err(Rejection(
                Status::Failed,
                format!(
                    "orb type {:?} is not one of provider {}'s orb_types ({}); name each orb type in Amp after one",
                    r.orb_type,
                    r.provider_id,
                    types
                        .iter()
                        .map(|t| t.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )),
        }
    }

    fn validate(&self, r: pb::DispatchRequest) -> Result<(ValidatedRequest, u64), Rejection> {
        let invocation_id =
            InvocationId::parse(&r.invocation_id).map_err(|_| "invalid invocation_id")?;
        let offering = self.offering(&r)?;
        let amp_orb_id =
            AmpOrbId::parse(&r.amp_orb_id).map_err(|_| "invalid ORB_ID".to_string())?;
        let mut env = PermittedGuestEnv::default();
        let action = match r.action {
            Some(pb::dispatch_request::Action::Spawn(s)) => {
                if s.image_ref.is_empty() {
                    return Err("ORB_IMAGE is required for spawn".into());
                }
                short("ORB_IMAGE", &s.image_ref)?;
                if let Some(id) = &s.project_snapshot_id {
                    short("ORB_SNAPSHOT_ID", id)?;
                }
                env = PermittedGuestEnv(
                    s.guest_env
                        .into_iter()
                        .map(|e| (e.name, SecretString::from(e.value)))
                        .collect(),
                );
                env.validate()?;
                ReqAction::Spawn {
                    image_ref: s.image_ref,
                    snapshot_id: s.project_snapshot_id.filter(|s| !s.is_empty()),
                }
            }
            Some(pb::dispatch_request::Action::Pause(p)) => {
                if let Some(id) = &p.project_snapshot_id {
                    short("ORB_SNAPSHOT_ID", id)?;
                }
                ReqAction::Pause {
                    snapshot_id: p.project_snapshot_id.filter(|s| !s.is_empty()),
                }
            }
            Some(pb::dispatch_request::Action::Delete(_)) => ReqAction::Delete,
            None => return Err("missing action".into()),
        };
        let fingerprint = match &action {
            ReqAction::Spawn {
                image_ref,
                snapshot_id,
            } => {
                let mut pairs: Vec<(&str, &str)> = env
                    .0
                    .iter()
                    .map(|(n, v)| (n.as_str(), v.expose_secret()))
                    .collect();
                pairs.sort();
                let snap = snapshot_id
                    .as_deref()
                    .map(|s| format!("s:{s}"))
                    .unwrap_or_else(|| "n:".into());
                let mut parts: Vec<&[u8]> = vec![
                    b"spawn-v1",
                    offering.id.as_str().as_bytes(),
                    amp_orb_id.as_str().as_bytes(),
                    image_ref.as_bytes(),
                    snap.as_bytes(),
                ];
                for (n, v) in &pairs {
                    parts.push(n.as_bytes());
                    parts.push(v.as_bytes());
                }
                Some(self.key.fingerprint(&parts))
            }
            _ => None,
        };
        Ok((
            ValidatedRequest {
                invocation_id,
                offering_id: offering.id.clone(),
                amp_endpoint: offering.amp_endpoint.clone(),
                amp_orb_id,
                action,
                env,
                fingerprint,
                remaining_budget_ms: r.remaining_budget_ms,
            },
            r.resume_after_log_seq,
        ))
    }
}

#[tonic::async_trait]
impl Dispatch for DispatchService {
    type ExecuteStream = ReceiverStream<Result<pb::DispatchEvent, tonic::Status>>;

    async fn execute(
        &self,
        request: Request<pb::DispatchRequest>,
    ) -> Result<Response<Self::ExecuteStream>, tonic::Status> {
        let (tx, rx) = mpsc::channel(256);
        match self.validate(request.into_inner()) {
            Err(Rejection(status, msg)) => {
                metrics::counter!("orbd_dispatch_requests_total", "action" => "unknown", "status" => status.label())
                    .increment(1);
                let res = StoredResult::plain(status, msg);
                let _ = tx
                    .send(Ok(pb::DispatchEvent {
                        event: Some(pb::dispatch_event::Event::Result(res.to_pb())),
                    }))
                    .await;
            }
            Ok((req, resume)) => {
                self.core
                    .send(Msg::Execute(Box::new(Exec {
                        req,
                        resume_after_log_seq: resume,
                        tx,
                    })))
                    .await
                    .map_err(|_| tonic::Status::unavailable("coordinator is shutting down"))?;
            }
        }
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
