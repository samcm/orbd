//! Coordinator core: admission, durable request queue, command journal and outbox,
//! result application, recovery, and the local `Dispatch` service. Backend-neutral:
//! everything backend-specific lives behind [`crate::backend::ComputeBackend`].
//!
//! * `db` — SQLite on one dedicated thread. One schema version and no upgrade paths: a
//!   database from another orbd version is refused (delete the state directory).
//! * `store` — typed queries and the multi-statement transactions.
//! * `core` — the single actor owning admission, queueing, delivery and results.
//! * `service` — the tonic `Dispatch` service (validation only).

mod core;
pub(crate) mod db;
mod logs;
mod service;
pub(crate) mod store;
mod types;

use crate::backend::{BackendHandle, ComputeBackend};
use crate::config::coordinator::CoordinatorConfig;
use crate::model::{BackendId, GIB, Offering, OfferingId};
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

/// Serves a gRPC service on a fresh unix socket (mode 0600) until `shutdown`.
fn serve_unix<S>(
    socket: &std::path::Path,
    service: S,
    shutdown: CancellationToken,
) -> Result<tokio::task::JoinHandle<()>>
where
    S: tower::Service<
            tonic::codegen::http::Request<tonic::body::Body>,
            Response = tonic::codegen::http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + tonic::server::NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    match std::fs::remove_file(socket) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("removing stale {}", socket.display())),
    }
    let listener = tokio::net::UnixListener::bind(socket)
        .with_context(|| format!("binding {}", socket.display()))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    let path = socket.to_owned();
    Ok(tokio::spawn(async move {
        let r = tonic::transport::Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                shutdown.cancelled(),
            )
            .await;
        if let Err(e) = r {
            tracing::error!(socket = %path.display(), error = %e, "local server failed");
        }
        let _ = std::fs::remove_file(&path);
    }))
}

/// Runs the coordinator until `shutdown` is cancelled.
pub async fn run(config: CoordinatorConfig, shutdown: CancellationToken) -> Result<()> {
    let state_dir = config.state_dir.clone();
    crate::pki::private_dir(&state_dir)?;
    let _lock = lock_state_dir(&state_dir)?;

    // Keys and the CA are created on first start only; afterwards they must exist.
    let first_init = !config.db_path().exists();
    let key = Arc::new(SecretKey::load_or_create(
        &state_dir.join("secret.key"),
        first_init,
    )?);
    let ca = crate::pki::Ca::load_or_create(&state_dir.join("pki"), first_init)?;
    let db = db::Db::open(&config.db_path())?;
    let epoch = db.call(store::bump_epoch).await?;
    tracing::info!(epoch, "coordinator controller epoch");

    if let Some(listen) = config.metrics_listen {
        crate::telemetry::init_metrics(listen).context("starting metrics listener")?;
    }

    let mut offerings: HashMap<OfferingId, Offering> = HashMap::new();
    for o in &config.offerings {
        offerings.insert(o.id.clone(), o.to_offering()?);
    }

    // Start the agent fleet.
    let (core_tx, core_rx) = mpsc::channel::<core::Msg>(4096);
    let mut backends: HashMap<BackendId, Arc<dyn ComputeBackend>> = HashMap::new();
    let (handle, admin) = crate::backend::fleet::FleetBackend::start(
        &config,
        ca,
        db.clone(),
        epoch,
        shutdown.clone(),
    )
    .await
    .context("starting agent fleet")?;
    let BackendHandle {
        backend,
        mut events,
    } = handle;
    backends.insert(backend.id().clone(), backend);
    let tx = core_tx.clone();
    tokio::spawn(async move {
        while let Some(ev) = events.next().await {
            if tx.send(core::Msg::Backend(ev)).await.is_err() {
                break;
            }
        }
    });

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
    let policy = core::Policy {
        running_orb_disk_headroom: config.running_orb_disk_headroom_gib.saturating_mul(GIB),
        idle_orb_ttl: (config.idle_orb_ttl_days > 0).then(|| {
            std::time::Duration::from_secs(config.idle_orb_ttl_days.saturating_mul(86_400))
        }),
    };
    tracing::info!(
        running_orb_disk_headroom_gib = config.running_orb_disk_headroom_gib,
        idle_orb_ttl_days = config.idle_orb_ttl_days,
        "placement policy"
    );
    let mut core = core::Core::new(
        db,
        key.clone(),
        epoch,
        offerings,
        backends,
        core_tx.clone(),
        policy,
    );
    core.recover().await.context("recovering commands")?;

    let core_task = tokio::spawn(core.run(core_rx, shutdown.clone()));
    let socket = config.dispatch_socket();
    let dispatch = serve_unix(
        &socket,
        orbd_proto::control::v1::dispatch_server::DispatchServer::new(service::DispatchService {
            core: core_tx,
            offerings: offerings_arc,
            key,
        }),
        shutdown.clone(),
    )?;
    let admin = serve_unix(
        &config.admin_socket(),
        admin.into_server(),
        shutdown.clone(),
    )?;
    tracing::info!(socket = %socket.display(), "dispatch socket ready");

    // Providers poll Amp only after recovery and with the socket accepting.
    let providers = crate::amp::provider::spawn_all(&config, &socket, shutdown.clone())?;

    shutdown.cancelled().await;
    let _ = dispatch.await;
    let _ = admin.await;
    let _ = core_task.await;
    let _ = providers.await;
    Ok(())
}
