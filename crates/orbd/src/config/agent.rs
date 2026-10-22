//! Agent configuration (`orbd agent --config agent.toml`).

use super::{ConfigError, ResourcesConfig, TlsFiles, load_toml};
use crate::model::AgentId;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub agent: AgentSection,
    #[serde(default)]
    pub runtime: RuntimesConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSection {
    /// Must match the identity in this agent's client certificate.
    pub id: AgentId,
    /// e.g. `https://coordinator.example:7443`.
    pub coordinator_url: String,
    /// TLS server name to verify; defaults to the URL host.
    pub coordinator_server_name: Option<String>,
    pub tls: TlsFiles,
    /// Holds `agent.sqlite`, images, orb disks, checkpoints, snapshots.
    pub state_dir: PathBuf,
    /// Sockets, locks, process metadata.
    pub run_dir: PathBuf,
    /// 64 hex chars; seals accepted guest env for crash recovery.
    pub secret_key_file: PathBuf,
    /// Ceiling for orbs on this host (configured guest RAM + overhead, disk).
    pub capacity: ResourcesConfig,
    pub metrics_listen: Option<SocketAddr>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimesConfig {
    pub fake: Option<FakeRuntimeConfig>,
    pub docker: Option<DockerRuntimeConfig>,
}

/// Test runtime with durable fake state. Never enable in production.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeRuntimeConfig {
    /// Advertise memory-preserving sleep (to exercise that path).
    #[serde(default)]
    pub memory_preserving_sleep: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DockerRuntimeConfig {
    /// Docker API endpoint; defaults to the local socket.
    pub docker_host: Option<String>,
    /// DNS servers written into orbs. Explicit, because copying the host's
    /// resolv.conf breaks orbs whose host resolver is a local stub (e.g. Tailscale).
    pub dns_servers: Vec<String>,
    /// Docker network for orb containers; defaults to Docker's bridge.
    pub network: Option<String>,
    /// Images pulled in the background at startup when missing, so the first spawn
    /// does not fail with ImageNotReady while a multi-GB pull runs.
    #[serde(default)]
    pub prewarm_images: Vec<String>,
}

impl AgentConfig {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let cfg: Self = load_toml(path)?;
        cfg.agent.capacity.to_resources()?;
        if cfg.runtime.fake.is_none() && cfg.runtime.docker.is_none() {
            return Err(ConfigError::Invalid("no runtime configured".into()));
        }
        if let Some(d) = &cfg.runtime.docker
            && d.dns_servers.is_empty()
        {
            return Err(ConfigError::Invalid(
                "runtime.docker.dns_servers must not be empty".into(),
            ));
        }
        Ok(cfg)
    }

    pub fn db_path(&self) -> PathBuf {
        self.agent.state_dir.join("agent.sqlite")
    }
}
