//! `orbd.control.v1.Orbs` on the admin socket, and the `orbd orb list|delete` CLI: the
//! coordinator's orbs, and deleting one whose thread is gone (Amp never deletes the
//! orbs of archived threads).

use super::core::{Msg, state_label};
use super::db::Db;
use super::store;
use crate::model::{AmpOrbId, Location, unix_ms_now};
use orbd_proto::control::v1 as pb;
use orbd_proto::control::v1::dispatch_event::Event;
use orbd_proto::control::v1::orbs_server::Orbs;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

pub struct OrbsService {
    pub(super) core: mpsc::Sender<Msg>,
    pub(super) db: Db,
}

#[tonic::async_trait]
impl Orbs for OrbsService {
    async fn list_orbs(
        &self,
        _: Request<pb::ListOrbsRequest>,
    ) -> Result<Response<pb::ListOrbsResponse>, Status> {
        let orbs = self
            .db
            .call(|c| store::live_orbs(c))
            .await
            .map_err(|e| Status::internal(format!("{e:#}")))?;
        let orbs = orbs
            .into_iter()
            .map(|o| pb::OrbInfo {
                amp_orb_id: o.identity.amp_orb_id.to_string(),
                offering_id: o.offering_id.to_string(),
                agent_id: match &o.location {
                    Location::Fleet { agent_id, .. } => agent_id.to_string(),
                    Location::External { .. } => String::new(),
                },
                state: state_label(&o.observation.state),
                observed_unix_ms: o.observation.observed_at_unix_ms,
            })
            .collect();
        Ok(Response::new(pb::ListOrbsResponse { orbs }))
    }

    async fn delete_orb(
        &self,
        request: Request<pb::DeleteOrbRequest>,
    ) -> Result<Response<pb::DeleteOrbResponse>, Status> {
        let r = request.into_inner();
        let amp_orb_id = AmpOrbId::parse(&r.amp_orb_id)
            .map_err(|_| Status::invalid_argument("invalid orb id"))?;
        let (tx, mut rx) = mpsc::channel(64);
        self.core
            .send(Msg::DeleteAsleep {
                amp_orb_id,
                min_asleep: Duration::from_secs(r.min_asleep_secs),
                tx,
            })
            .await
            .map_err(|_| Status::unavailable("coordinator is shutting down"))?;
        while let Some(ev) = rx.recv().await {
            if let Some(Event::Result(res)) = ev?.event {
                return match pb::DispatchStatus::try_from(res.status) {
                    Ok(pb::DispatchStatus::Succeeded) => Ok(Response::new(pb::DeleteOrbResponse {
                        message: res.message,
                    })),
                    Ok(pb::DispatchStatus::Unavailable) => Err(Status::unavailable(res.message)),
                    _ => Err(Status::aborted(res.message)),
                };
            }
        }
        Err(Status::unavailable(
            "the coordinator stopped before the delete finished",
        ))
    }
}

// --- CLI ---------------------------------------------------------------------------

async fn client(
    socket: &std::path::Path,
) -> anyhow::Result<pb::orbs_client::OrbsClient<tonic::transport::Channel>> {
    use anyhow::Context;
    let channel = crate::amp::dispatch::connect(socket)
        .await
        .with_context(|| {
            format!(
                "connecting to {} (is the coordinator running?)",
                socket.display()
            )
        })?;
    Ok(pb::orbs_client::OrbsClient::new(channel))
}

fn age(ms: u64) -> String {
    let m = ms / 60_000;
    match m {
        0..60 => format!("{m}m"),
        60..1440 => format!("{}h", m / 60),
        _ => format!("{}d", m / 1440),
    }
}

/// One line per orb: Amp orb id, state and for how long, agent, offering.
pub async fn cli_list(socket: &std::path::Path) -> anyhow::Result<String> {
    let r = client(socket)
        .await?
        .list_orbs(pb::ListOrbsRequest {})
        .await
        .map_err(|s| anyhow::anyhow!("{}", s.message()))?;
    let now = unix_ms_now();
    let mut out = String::new();
    for o in r.into_inner().orbs {
        out.push_str(&format!(
            "{}\t{} for {}\t{}\t{}\n",
            o.amp_orb_id,
            o.state,
            age(now.saturating_sub(o.observed_unix_ms)),
            o.agent_id,
            o.offering_id
        ));
    }
    Ok(out)
}

/// Deletes one orb if it has been asleep, with no job, for `min_asleep`.
pub async fn cli_delete(
    socket: &std::path::Path,
    amp_orb_id: &str,
    min_asleep: Duration,
) -> anyhow::Result<String> {
    let r = client(socket)
        .await?
        .delete_orb(pb::DeleteOrbRequest {
            amp_orb_id: amp_orb_id.to_owned(),
            min_asleep_secs: min_asleep.as_secs(),
        })
        .await
        .map_err(|s| anyhow::anyhow!("{amp_orb_id} not deleted: {}", s.message()))?;
    Ok(format!("{amp_orb_id} deleted: {}", r.into_inner().message))
}
