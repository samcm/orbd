//! Coordinator core: admission, durable request queue, command journal and outbox,
//! result application, recovery, and the local `Dispatch` service. Backend-neutral:
//! everything backend-specific lives behind [`crate::backend::ComputeBackend`].
//!
//! * `db` — SQLite on one dedicated thread, schema migrations.
//! * `store` — typed queries and the multi-statement transactions.
//! * `core` — the single actor owning admission, queueing, delivery and results.
//! * `service` — the tonic `Dispatch` service (validation only).

mod core;
mod db;
mod logs;
mod service;
mod store;
mod types;

use crate::backend::{BackendHandle, ComputeBackend};
use crate::config::coordinator::{BackendConfig, CoordinatorConfig};
use crate::model::{BackendId, Offering, OfferingId};
use crate::secrets::SecretKey;
use anyhow::{Context, Result, bail};
use futures::StreamExt;
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Exclusive process lock; held for the life of the coordinator.
fn lock_state_dir(dir: &std::path::Path) -> Result<nix::fcntl::Flock<std::fs::File>> {
    let path = dir.join("coordinator.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock).map_err(|(_, e)| {
        anyhow::anyhow!(
            "another coordinator holds {} ({e}); refusing to start",
            path.display()
        )
    })
}

/// Runs the coordinator until `shutdown` is cancelled.
pub async fn run(config: CoordinatorConfig, shutdown: CancellationToken) -> Result<()> {
    let state_dir = config.coordinator.state_dir.clone();
    std::fs::create_dir_all(&state_dir)
        .with_context(|| format!("creating {}", state_dir.display()))?;
    let _ = std::fs::set_permissions(&state_dir, std::fs::Permissions::from_mode(0o700));
    let _lock = lock_state_dir(&state_dir)?;

    let key = Arc::new(SecretKey::load(&config.coordinator.secret_key_file)?);
    let db = db::Db::open(&config.db_path())?;
    let epoch = db.call(store::bump_epoch).await?;
    tracing::info!(epoch, "coordinator controller epoch");

    if let Some(listen) = config.coordinator.metrics_listen {
        crate::telemetry::init_metrics(listen).context("starting metrics listener")?;
    }

    let mut offerings: HashMap<OfferingId, Offering> = HashMap::new();
    for o in &config.offerings {
        offerings.insert(o.id.clone(), o.to_offering()?);
    }

    // Start backends.
    let (core_tx, core_rx) = mpsc::channel::<core::Msg>(4096);
    let mut backends: HashMap<BackendId, Arc<dyn ComputeBackend>> = HashMap::new();
    for b in &config.backends {
        let handle: BackendHandle = match b {
            BackendConfig::AgentFleet(cfg) => {
                crate::backend::fleet::FleetBackend::start(cfg, epoch, shutdown.clone())
                    .await
                    .with_context(|| format!("starting backend {}", cfg.id))?
            }
        };
        let BackendHandle {
            backend,
            mut events,
        } = handle;
        backends.insert(b.id().clone(), backend);
        let tx = core_tx.clone();
        tokio::spawn(async move {
            while let Some(ev) = events.next().await {
                if tx.send(core::Msg::Backend(ev)).await.is_err() {
                    break;
                }
            }
        });
    }

    // Every offering must be satisfiable by its backend's capabilities.
    for (id, offering) in &offerings {
        let Some(backend) = backends.get(&offering.backend_id) else {
            bail!(
                "offering {id}: backend {} was not started",
                offering.backend_id
            );
        };
        let caps = backend
            .capabilities(&offering.spec.target)
            .map_err(|e| anyhow::anyhow!("offering {id}: {e}"))?;
        let mismatches = caps.check(&offering.spec);
        if !mismatches.is_empty() {
            let list: Vec<String> = mismatches.iter().map(|m| m.to_string()).collect();
            bail!(
                "offering {id} does not match backend capabilities: {}",
                list.join("; ")
            );
        }
    }

    let offerings_arc = Arc::new(offerings.clone());
    let mut core = core::Core::new(db, key.clone(), epoch, offerings, backends, core_tx.clone());
    core.recover().await.context("recovering commands")?;

    // Serve dispatch.
    let socket = config.coordinator.dispatch_socket.clone();
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::remove_file(&socket) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("removing stale {}", socket.display())),
    }
    let listener = tokio::net::UnixListener::bind(&socket)
        .with_context(|| format!("binding {}", socket.display()))?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;

    let core_task = tokio::spawn(core.run(core_rx, shutdown.clone()));
    let service = service::DispatchService {
        core: core_tx,
        offerings: offerings_arc,
        key,
    };
    let stop = shutdown.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(orbd_proto::control::v1::dispatch_server::DispatchServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                stop.cancelled(),
            )
            .await
    });
    tracing::info!(socket = %socket.display(), "dispatch socket ready");

    // Providers poll Amp only after recovery and with the socket accepting.
    let providers = crate::amp::provider::spawn_all(&config, &socket, shutdown.clone())?;

    shutdown.cancelled().await;
    let _ = server.await;
    let _ = core_task.await;
    let _ = providers.await;
    let _ = std::fs::remove_file(&socket);
    Ok(())
}
