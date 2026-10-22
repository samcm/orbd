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
    /// Image preparation for VM runtimes (OCI image → ext4 root disk).
    pub images: Option<ImagesConfig>,
}

/// `[images]`: turns Amp's OCI image into a bootable root disk with orbd's guest
/// integration. Prepared images live under `<state_dir>/images/`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImagesConfig {
    /// Static `orbd-guest` binary (x86_64 musl) installed into every prepared image.
    pub guest_helper: PathBuf,
    /// `umoci` binary; must be the pinned version (see `orbd doctor`).
    #[serde(default = "default_umoci")]
    pub umoci: PathBuf,
    #[serde(default = "default_mkfs_ext4")]
    pub mkfs_ext4: PathBuf,
    /// Images prepared in the background at startup, so the first spawn does not fail
    /// with ImageNotReady while a multi-GB image is pulled and converted.
    #[serde(default)]
    pub prewarm: Vec<String>,
    /// How often a tag (e.g. `:latest`) is resolved again; the previously prepared
    /// image keeps serving until the new digest is ready.
    #[serde(default = "default_refresh_secs")]
    pub refresh_interval_secs: u64,
}

fn default_umoci() -> PathBuf {
    "umoci".into()
}

fn default_mkfs_ext4() -> PathBuf {
    "mkfs.ext4".into()
}

fn default_refresh_secs() -> u64 {
    3600
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

#[derive(Clone, Debug, Deserialize)]
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
