//! Coordinator configuration (`orbd coordinator --config coordinator.toml`).

use super::{ConfigError, ResourcesConfig, TlsFiles, load_toml};
use crate::model::{
    AgentId, AmpEndpointId, BackendId, CapabilityRequirements, CheckpointFailurePolicy,
    ExecutionTarget, NestedKvmRequirement, Offering, OfferingId, PausePolicy, RuntimeKind,
    SnapshotRequirement, SnapshotScope, SpecTemplate,
};
use serde::Deserialize;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorConfig {
    pub coordinator: CoordinatorSection,
    #[serde(rename = "amp_endpoint", default)]
    pub amp_endpoints: Vec<AmpEndpointConfig>,
    #[serde(rename = "backend", default)]
    pub backends: Vec<BackendConfig>,
    #[serde(rename = "offering", default)]
    pub offerings: Vec<OfferingConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorSection {
    /// Holds `coordinator.sqlite`, its process lock and the generated dispatch wrapper.
    pub state_dir: PathBuf,
    /// Local unix socket for `orbd dispatch`.
    pub dispatch_socket: PathBuf,
    /// 64 hex chars; seals persisted guest env and keys spawn fingerprints.
    pub secret_key_file: PathBuf,
    /// Prometheus listener, e.g. `127.0.0.1:9464`. Disabled when absent.
    pub metrics_listen: Option<SocketAddr>,
    /// Amp CLI used for `amp orb provider` children.
    #[serde(default = "default_amp_cli")]
    pub amp_cli: PathBuf,
}

fn default_amp_cli() -> PathBuf {
    PathBuf::from("amp")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AmpEndpointConfig {
    pub id: AmpEndpointId,
    pub url: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BackendConfig {
    AgentFleet(FleetBackendConfig),
}

impl BackendConfig {
    pub fn id(&self) -> &BackendId {
        match self {
            Self::AgentFleet(f) => &f.id,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetBackendConfig {
    pub id: BackendId,
    /// AgentControl gRPC listener (mTLS).
    pub listen: SocketAddr,
    pub tls: TlsFiles,
    /// Agents allowed to connect. Each agent's certificate must name its id
    /// (subject CN or a DNS SAN).
    #[serde(rename = "agent", default)]
    pub agents: Vec<AuthorizedAgent>,
    /// Heartbeat interval sent to agents in `Welcome`.
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_interval_ms: u64,
}

fn default_heartbeat_ms() -> u64 {
    5_000
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizedAgent {
    pub id: AgentId,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfferingConfig {
    pub id: OfferingId,
    pub amp_endpoint: AmpEndpointId,
    /// `AMP_ORB_PROVIDER_TOKEN` for this offering's `amp orb provider` child.
    /// When absent the offering is served for local dispatch only (tests).
    pub provider_token_file: Option<PathBuf>,
    pub backend: BackendId,
    /// Fleet runtime.
    pub runtime: RuntimeKind,
    pub resources: ResourcesConfig,
    pub pause: PauseConfig,
    #[serde(default)]
    pub require: RequireConfig,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum PauseConfig {
    DiskOnly,
    MemoryPreserving {
        on_checkpoint_failure: CheckpointFailurePolicy,
    },
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequireConfig {
    #[serde(default)]
    pub release_compute_when_paused: bool,
    #[serde(default)]
    pub nested_kvm: NestedKvmRequirement,
    #[serde(default)]
    pub project_snapshots: Option<SnapshotScope>,
}

impl OfferingConfig {
    pub fn to_offering(&self) -> Result<Offering, ConfigError> {
        Ok(Offering {
            id: self.id.clone(),
            amp_endpoint: self.amp_endpoint.clone(),
            backend_id: self.backend.clone(),
            spec: SpecTemplate {
                target: ExecutionTarget::Fleet {
                    runtime: self.runtime,
                },
                resources: self.resources.to_resources()?,
                requirements: CapabilityRequirements {
                    release_compute_when_paused: self.require.release_compute_when_paused,
                    nested_kvm: self.require.nested_kvm,
                    project_snapshots: match self.require.project_snapshots {
                        None => SnapshotRequirement::NotRequired,
                        Some(scope) => SnapshotRequirement::Required { scope },
                    },
                },
                pause_policy: match self.pause {
                    PauseConfig::DiskOnly => PausePolicy::DiskOnly,
                    PauseConfig::MemoryPreserving {
                        on_checkpoint_failure,
                    } => PausePolicy::MemoryPreserving {
                        on_failure: on_checkpoint_failure,
                    },
                },
            },
        })
    }
}

impl CoordinatorConfig {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let cfg: Self = load_toml(path)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Structural validation. Capability validation against backends happens when the
    /// backends are constructed (see `coordinator`).
    pub fn validate(&self) -> Result<(), ConfigError> {
        let inv = |m: String| Err(ConfigError::Invalid(m));
        let mut seen = HashSet::new();
        for e in &self.amp_endpoints {
            if !seen.insert(e.id.as_str()) {
                return inv(format!("duplicate amp_endpoint {}", e.id));
            }
        }
        let mut backends = HashSet::new();
        for b in &self.backends {
            if !backends.insert(b.id().as_str()) {
                return inv(format!("duplicate backend {}", b.id()));
            }
        }
        let mut offerings = HashSet::new();
        for o in &self.offerings {
            if !offerings.insert(o.id.as_str()) {
                return inv(format!("duplicate offering {}", o.id));
            }
            if !seen.contains(o.amp_endpoint.as_str()) {
                return inv(format!(
                    "offering {}: unknown amp_endpoint {}",
                    o.id, o.amp_endpoint
                ));
            }
            if !backends.contains(o.backend.as_str()) {
                return inv(format!("offering {}: unknown backend {}", o.id, o.backend));
            }
            let r = o.resources;
            if r.vcpus == 0 || r.memory_mib == 0 || r.disk_gib == 0 {
                return inv(format!("offering {}: resources must be non-zero", o.id));
            }
            o.to_offering()?;
        }
        Ok(())
    }

    pub fn offering(&self, id: &str) -> Option<&OfferingConfig> {
        self.offerings.iter().find(|o| o.id.as_str() == id)
    }

    pub fn db_path(&self) -> PathBuf {
        self.coordinator.state_dir.join("coordinator.sqlite")
    }
}
