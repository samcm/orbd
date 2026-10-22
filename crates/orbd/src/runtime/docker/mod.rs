//! Docker runtime: one systemd container per orb, from the Amp orb image.
//!
//! A routing/test runtime: pause stops the container, so processes are lost (no
//! memory-preserving sleep), and Docker gives orbs no `/dev/kvm`. Containers are named
//! from the internal orb id and labelled as orbd-managed; nothing secret is ever put in
//! their configuration (credentials go in over exec stdin, see [`guest`]).

mod guest;

use super::{
    GuestControl, LogStream, OpContext, Runtime, RuntimeCapabilities, RuntimeError,
    RuntimeObservation, StartSource,
};
use crate::config::agent::DockerRuntimeConfig;
use crate::model::{
    InitialDiskSource, OrbDefinition, OrbId, ProjectSnapshotKey, ProjectSnapshotRef, RuntimeKind,
    SleepCheckpointRef,
};
use async_trait::async_trait;
use bollard::Docker;
use bollard::errors::Error as DockerError;
use bollard::models::{
    ContainerCreateBody, ContainerInspectResponse, ContainerStateStatusEnum, DeviceMapping,
    HostConfig, HostConfigCgroupnsModeEnum,
};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, RemoveContainerOptionsBuilder,
    StopContainerOptionsBuilder,
};
use futures::StreamExt;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const LABEL_MANAGED: &str = "orbd.managed";
const LABEL_ORB_ID: &str = "orbd.orb-id";
const LABEL_SPEC_DIGEST: &str = "orbd.spec-digest";
const LABEL_AMP_ORB_ID: &str = "orbd.amp-orb-id";
/// systemd's clean shutdown signal, and how long `docker stop` waits for it.
const STOP_SIGNAL: &str = "SIGRTMIN+3";
const STOP_TIMEOUT_SECS: i64 = 30;
const PIDS_LIMIT: i64 = 16384;

pub struct DockerRuntime {
    docker: Docker,
    config: DockerRuntimeConfig,
    version: String,
    pulls: Pulls,
}

impl DockerRuntime {
    /// Connects to dockerd, negotiates the API version and starts pulls of missing
    /// `prewarm_images`.
    pub async fn connect(config: DockerRuntimeConfig) -> Result<Self, RuntimeError> {
        let docker = match &config.docker_host {
            Some(host) => Docker::connect_with_host(host),
            None => Docker::connect_with_local_defaults(),
        }
        .map_err(docker_err)?
        // Every call is bounded by the operation deadline instead.
        .with_timeout(Duration::from_secs(3600))
        .negotiate_version()
        .await
        .map_err(docker_err)?;
        let v = docker.version().await.map_err(docker_err)?;
        let version = format!(
            "docker {} (api {})",
            v.version.unwrap_or_default(),
            v.api_version.unwrap_or_default()
        );
        let rt = Self {
            docker,
            config,
            version,
            pulls: Pulls::default(),
        };
        for image in &rt.config.prewarm_images {
            match rt.docker.inspect_image(image).await {
                Ok(_) => {}
                Err(e) if is_not_found(&e) => {
                    rt.pulls.ensure(&rt.docker, image);
                }
                Err(e) => return Err(docker_err(e)),
            }
        }
        Ok(rt)
    }

    async fn find(&self, orb: OrbId) -> Result<Option<ContainerInspectResponse>, RuntimeError> {
        match self
            .docker
            .inspect_container(&container_name(orb), None)
            .await
        {
            Ok(c) => Ok(Some(c)),
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(docker_err(e)),
        }
    }

    async fn observe(&self, orb: OrbId) -> Result<RuntimeObservation, RuntimeError> {
        Ok(match self.find(orb).await? {
            None => RuntimeObservation::Absent,
            Some(c) => observation(orb, &c),
        })
    }

    async fn ensure_image(&self, image: &str, ctx: &OpContext) -> Result<(), RuntimeError> {
        match bounded(ctx, self.docker.inspect_image(image)).await? {
            Ok(_) => Ok(()),
            Err(e) if is_not_found(&e) => {
                let msg = self.pulls.ensure(&self.docker, image);
                log(ctx, &msg);
                Err(RuntimeError::ImageNotReady(msg))
            }
            Err(e) => Err(docker_err(e)),
        }
    }

    async fn create(
        &self,
        orb: &OrbDefinition,
        image: &str,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        let res = orb.spec.template.resources;
        let labels = HashMap::from([
            (LABEL_MANAGED.to_owned(), "true".to_owned()),
            (LABEL_ORB_ID.to_owned(), orb.id.to_string()),
            (LABEL_SPEC_DIGEST.to_owned(), orb.spec_digest.0.clone()),
            (
                LABEL_AMP_ORB_ID.to_owned(),
                orb.identity.amp_orb_id.0.clone(),
            ),
        ]);
        let tmpfs = HashMap::from([
            (
                "/run".to_owned(),
                "rw,nosuid,nodev,exec,mode=755".to_owned(),
            ),
            (
                "/run/lock".to_owned(),
                "rw,nosuid,nodev,noexec,mode=1777".to_owned(),
            ),
        ]);
        let body = ContainerCreateBody {
            image: Some(image.to_owned()),
            hostname: Some(hostname(orb)),
            labels: Some(labels),
            // Boot the image's systemd, which runs the orb's services like an Amp-hosted
            // orb. `container=docker` tells systemd it is in a container.
            user: Some("root".to_owned()),
            entrypoint: Some(vec!["/lib/systemd/systemd".to_owned()]),
            env: Some(vec!["container=docker".to_owned()]),
            stop_signal: Some(STOP_SIGNAL.to_owned()),
            stop_timeout: Some(STOP_TIMEOUT_SECS),
            host_config: Some(HostConfig {
                nano_cpus: Some(i64::from(res.vcpus) * 1_000_000_000),
                memory: Some(i64_of(res.memory_bytes)?),
                memory_swap: Some(i64_of(res.memory_bytes)?),
                pids_limit: Some(PIDS_LIMIT),
                // systemd needs its own writable cgroup subtree; no CAP_SYS_ADMIN.
                cgroupns_mode: Some(HostConfigCgroupnsModeEnum::PRIVATE),
                security_opt: Some(vec!["writable-cgroups=true".to_owned()]),
                tmpfs: Some(tmpfs),
                // Tailscale in the orb needs a tun device.
                cap_add: Some(vec!["NET_ADMIN".to_owned()]),
                devices: Some(vec![DeviceMapping {
                    path_on_host: Some("/dev/net/tun".to_owned()),
                    path_in_container: Some("/dev/net/tun".to_owned()),
                    cgroup_permissions: Some("rwm".to_owned()),
                }]),
                // Explicit: the host's resolv.conf may point at a local stub resolver.
                dns: Some(self.config.dns_servers.clone()),
                network_mode: self.config.network.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let opts = CreateContainerOptionsBuilder::default()
            .name(&container_name(orb.id))
            .build();
        log(
            ctx,
            &format!("creating container {}", container_name(orb.id)),
        );
        match bounded(ctx, self.docker.create_container(Some(opts), body)).await? {
            Ok(_) => Ok(()),
            // Created by a concurrent or interrupted attempt; the caller re-inspects.
            Err(DockerError::DockerResponseServerError {
                status_code: 409, ..
            }) => Ok(()),
            Err(e) => Err(docker_err(e)),
        }
    }

    async fn start_existing(&self, orb: OrbId, ctx: &OpContext) -> Result<(), RuntimeError> {
        let name = container_name(orb);
        log(ctx, &format!("starting container {name}"));
        match bounded(ctx, self.docker.start_container(&name, None)).await? {
            // 304: already started.
            Ok(())
            | Err(DockerError::DockerResponseServerError {
                status_code: 304, ..
            }) => {}
            Err(e) => return Err(docker_err(e)),
        }
        let c = bounded(ctx, self.find(orb))
            .await??
            .ok_or_else(|| RuntimeError::Other(format!("{name} vanished after start")))?;
        match observation(orb, &c) {
            RuntimeObservation::Running => {
                log(ctx, &format!("container {name} is running"));
                Ok(())
            }
            other => {
                let state = c.state.unwrap_or_default();
                Err(RuntimeError::Other(format!(
                    "{name} is not running after start ({other:?}, exit code {:?}, error {:?})",
                    state.exit_code, state.error
                )))
            }
        }
    }
}

/// Local image repository for project snapshots.
const SNAPSHOT_REPO: &str = "orbd-project-snapshot";

fn snapshot_image(key: &ProjectSnapshotKey) -> String {
    format!("{SNAPSHOT_REPO}:{}", key.as_str())
}

#[async_trait]
impl Runtime for DockerRuntime {
    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities {
            kind: RuntimeKind::Docker,
            version: self.version.clone(),
            memory_preserving_sleep: false,
            nested_kvm: false,
            nested_state_qualified: false,
            project_snapshots: true,
        }
    }

    async fn inspect(&self, orb: OrbId) -> Result<RuntimeObservation, RuntimeError> {
        self.observe(orb).await
    }

    async fn start(
        &self,
        orb: &OrbDefinition,
        source: StartSource,
        ctx: &OpContext,
    ) -> Result<(), RuntimeError> {
        let image = match source {
            StartSource::NewDisk(InitialDiskSource::Image { image_ref }) => Some(image_ref),
            StartSource::NewDisk(InitialDiskSource::ProjectSnapshot { snapshot: r, .. }) => {
                let image = snapshot_image(&r.key);
                match bounded(ctx, self.docker.inspect_image(&image)).await? {
                    Ok(_) => Some(image),
                    Err(e) if is_not_found(&e) => {
                        return Err(RuntimeError::NotFound(format!(
                            "project snapshot {} is not on this host",
                            r.key
                        )));
                    }
                    Err(e) => return Err(docker_err(e)),
                }
            }
            StartSource::MemoryCheckpoint(_) => {
                return Err(RuntimeError::Unsupported(
                    "docker runtime has no memory checkpoints".into(),
                ));
            }
            StartSource::ExistingDiskColdBoot => None,
        };
        let existing = bounded(ctx, self.find(orb.id)).await??;
        let existing = match (existing, &image) {
            (None, None) => {
                return Err(RuntimeError::NotFound(format!(
                    "no container {} to boot",
                    container_name(orb.id)
                )));
            }
            (None, Some(image)) => {
                if !image.starts_with(SNAPSHOT_REPO) {
                    self.ensure_image(image, ctx).await?;
                }
                self.create(orb, image, ctx).await?;
                bounded(ctx, self.find(orb.id)).await??.ok_or_else(|| {
                    RuntimeError::Other(format!("{} vanished after create", container_name(orb.id)))
                })?
            }
            (Some(c), _) => c,
        };
        let observed = observation(orb.id, &existing);
        if let RuntimeObservation::Inconsistent { reason } = observed {
            return Err(RuntimeError::Other(reason));
        }
        if label(&existing, LABEL_SPEC_DIGEST) != Some(orb.spec_digest.0.as_str()) {
            return Err(RuntimeError::Other(format!(
                "{} was created for a different spec",
                container_name(orb.id)
            )));
        }
        match observed {
            RuntimeObservation::Running => Ok(()),
            _ => self.start_existing(orb.id, ctx).await,
        }
    }

    async fn checkpoint_and_stop(
        &self,
        _orb: &OrbDefinition,
        _checkpoint_generation: u64,
        _ctx: &OpContext,
    ) -> Result<SleepCheckpointRef, RuntimeError> {
        Err(RuntimeError::Unsupported(
            "docker runtime has no memory-preserving sleep".into(),
        ))
    }

    async fn cold_stop(&self, orb: &OrbDefinition, ctx: &OpContext) -> Result<(), RuntimeError> {
        let name = container_name(orb.id);
        match bounded(ctx, self.observe(orb.id)).await?? {
            RuntimeObservation::Absent => {
                return Err(RuntimeError::NotFound(format!("no container {name}")));
            }
            RuntimeObservation::Stopped { .. } => return Ok(()),
            RuntimeObservation::Running | RuntimeObservation::Inconsistent { .. } => {}
        }
        // Leave a little of the budget for docker's SIGKILL after the grace period.
        let left = ctx.work_deadline.saturating_duration_since(Instant::now());
        let grace = (left.as_secs() as i64 - 5).clamp(1, STOP_TIMEOUT_SECS);
        log(ctx, &format!("stopping container {name} ({grace}s grace)"));
        let opts = StopContainerOptionsBuilder::default()
            .t(grace as i32)
            .build();
        match bounded(ctx, self.docker.stop_container(&name, Some(opts))).await? {
            Ok(())
            | Err(DockerError::DockerResponseServerError {
                status_code: 304, ..
            }) => {}
            Err(e) if is_not_found(&e) => {
                return Err(RuntimeError::NotFound(format!("no container {name}")));
            }
            Err(e) => return Err(docker_err(e)),
        }
        match bounded(ctx, self.observe(orb.id)).await?? {
            RuntimeObservation::Stopped { .. } => {
                log(ctx, &format!("container {name} stopped"));
                Ok(())
            }
            other => Err(RuntimeError::Other(format!(
                "{name} did not stop: {other:?}"
            ))),
        }
    }

    async fn discard_checkpoint(
        &self,
        _orb: OrbId,
        _checkpoint: &SleepCheckpointRef,
    ) -> Result<(), RuntimeError> {
        Err(RuntimeError::Unsupported(
            "docker runtime has no memory checkpoints".into(),
        ))
    }

    /// Commits the stopped container to a local image `orbd-project-snapshot:<key>`.
    /// The image is agent-local and secret-bearing (it holds the orb's files, including
    /// its last bootstrap env); orbs started from it are re-bootstrapped with fresh
    /// credentials.
    async fn clone_disk(
        &self,
        orb: &OrbDefinition,
        key: &ProjectSnapshotKey,
        ctx: &OpContext,
    ) -> Result<ProjectSnapshotRef, RuntimeError> {
        let image = snapshot_image(key);
        let done = ProjectSnapshotRef { key: key.clone() };
        if bounded(ctx, self.docker.inspect_image(&image))
            .await?
            .is_ok()
        {
            return Ok(done);
        }
        match bounded(ctx, self.observe(orb.id)).await?? {
            RuntimeObservation::Stopped { .. } => {}
            other => {
                return Err(RuntimeError::Other(format!(
                    "snapshot needs a stopped orb, found {other:?}"
                )));
            }
        }
        log(ctx, &format!("committing project snapshot {key}"));
        let opts = bollard::query_parameters::CommitContainerOptionsBuilder::new()
            .container(&container_name(orb.id))
            .repo(SNAPSHOT_REPO)
            .tag(key.as_str())
            .build();
        bounded(
            ctx,
            self.docker
                .commit_container(opts, bollard::models::ContainerConfig::default()),
        )
        .await?
        .map_err(docker_err)?;
        log(ctx, &format!("project snapshot {key} published"));
        Ok(done)
    }

    async fn delete(&self, orb: OrbId, ctx: &OpContext) -> Result<(), RuntimeError> {
        let name = container_name(orb);
        if let Some(c) = bounded(ctx, self.find(orb)).await?? {
            if label(&c, LABEL_ORB_ID) != Some(orb.to_string().as_str()) {
                return Err(RuntimeError::Other(format!(
                    "refusing to remove {name}: not managed by orbd for this orb"
                )));
            }
            log(ctx, &format!("removing container {name} and its volumes"));
            let opts = RemoveContainerOptionsBuilder::default()
                .force(true)
                .v(true)
                .build();
            match bounded(ctx, self.docker.remove_container(&name, Some(opts))).await? {
                Ok(()) => {}
                Err(e) if is_not_found(&e) => {}
                Err(e) => return Err(docker_err(e)),
            }
        }
        Ok(())
    }

    fn guest(&self) -> &dyn GuestControl {
        self
    }
}

/// Background image pulls: a 6 GB pull does not fit an operation budget, so a missing
/// image starts a pull and the operation fails with `ImageNotReady` until it lands.
#[derive(Clone, Default)]
struct Pulls(Arc<Mutex<HashMap<String, PullState>>>);

enum PullState {
    InProgress { started: Instant, status: String },
    Failed(String),
}

impl Pulls {
    /// Starts a pull unless one is running; returns a progress message.
    fn ensure(&self, docker: &Docker, image: &str) -> String {
        let mut pulls = self.0.lock().expect("pull state lock");
        let previous = match pulls.get(image) {
            Some(PullState::InProgress { started, status }) => {
                return format!(
                    "pulling {image} ({}s, {status})",
                    started.elapsed().as_secs()
                );
            }
            Some(PullState::Failed(e)) => format!("; previous pull failed: {e}"),
            None => String::new(),
        };
        pulls.insert(
            image.to_owned(),
            PullState::InProgress {
                started: Instant::now(),
                status: "starting".into(),
            },
        );
        drop(pulls);
        tracing::info!(image, "pulling image");
        let (docker, image_owned, state) = (docker.clone(), image.to_owned(), self.clone());
        tokio::spawn(async move {
            let opts = CreateImageOptionsBuilder::default()
                .from_image(&image_owned)
                .build();
            let mut stream = docker.create_image(Some(opts), None, None);
            let mut result = Ok(());
            while let Some(item) = stream.next().await {
                match item {
                    Ok(info) => {
                        if let Some(s) = info.status
                            && let Some(PullState::InProgress { status, .. }) = state
                                .0
                                .lock()
                                .expect("pull state lock")
                                .get_mut(&image_owned)
                        {
                            *status = s;
                        }
                    }
                    Err(e) => {
                        result = Err(e.to_string());
                        break;
                    }
                }
            }
            let mut pulls = state.0.lock().expect("pull state lock");
            match result {
                Ok(()) => {
                    tracing::info!(image = %image_owned, "pulled image");
                    pulls.remove(&image_owned);
                }
                Err(e) => {
                    tracing::warn!(image = %image_owned, error = %e, "image pull failed");
                    pulls.insert(image_owned, PullState::Failed(e));
                }
            }
        });
        format!("image {image} is not present; started pulling it{previous}")
    }
}

pub(crate) fn container_name(orb: OrbId) -> String {
    format!("orbd-{orb}")
}

/// RFC 1123 label from Amp's orb id (`[A-Za-z0-9._-]`), for prompts and logs.
fn hostname(orb: &OrbDefinition) -> String {
    let h: String = orb
        .identity
        .amp_orb_id
        .0
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .take(63)
        .collect();
    let h = h.trim_matches('-');
    if h.is_empty() { "orb".into() } else { h.into() }
}

fn label<'a>(c: &'a ContainerInspectResponse, key: &str) -> Option<&'a str> {
    c.config
        .as_ref()?
        .labels
        .as_ref()?
        .get(key)
        .map(String::as_str)
}

fn observation(orb: OrbId, c: &ContainerInspectResponse) -> RuntimeObservation {
    if label(c, LABEL_ORB_ID) != Some(orb.to_string().as_str()) {
        return RuntimeObservation::Inconsistent {
            reason: format!(
                "container {} exists but is not managed by orbd for this orb",
                container_name(orb)
            ),
        };
    }
    let status = c.state.as_ref().and_then(|s| s.status);
    match status {
        Some(ContainerStateStatusEnum::RUNNING) => RuntimeObservation::Running,
        Some(ContainerStateStatusEnum::CREATED | ContainerStateStatusEnum::EXITED) => {
            RuntimeObservation::Stopped { checkpoint: None }
        }
        other => RuntimeObservation::Inconsistent {
            reason: format!(
                "container {} is {}",
                container_name(orb),
                other.map_or("in an unknown state".into(), |s| s.to_string())
            ),
        },
    }
}

fn is_not_found(e: &DockerError) -> bool {
    matches!(
        e,
        DockerError::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

fn docker_err(e: DockerError) -> RuntimeError {
    RuntimeError::Other(format!("docker: {e}"))
}

fn i64_of(v: u64) -> Result<i64, RuntimeError> {
    i64::try_from(v).map_err(|_| RuntimeError::Other(format!("{v} overflows i64")))
}

/// Streams a progress line to the operation log. Advisory: dropped if the sink is full.
fn log(ctx: &OpContext, line: &str) {
    tracing::debug!(operation = %ctx.operation_id, "{line}");
    let _ = ctx
        .log
        .try_send((LogStream::Stdout, bytes::Bytes::from(format!("{line}\n"))));
}

#[derive(Debug)]
enum Interrupted {
    Cancelled,
    Deadline,
}

impl From<Interrupted> for RuntimeError {
    fn from(i: Interrupted) -> Self {
        match i {
            Interrupted::Cancelled => Self::Cancelled,
            Interrupted::Deadline => Self::DeadlineExceeded,
        }
    }
}

/// Runs `fut` until it finishes, `ctx.cancel` fires or the work deadline passes.
async fn bounded<F: Future>(ctx: &OpContext, fut: F) -> Result<F::Output, Interrupted> {
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(Interrupted::Cancelled),
        _ = tokio::time::sleep_until(ctx.work_deadline.into()) => Err(Interrupted::Deadline),
        out = fut => Ok(out),
    }
}
