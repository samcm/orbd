//! Coordinator configuration (`orbd coordinator`, default `/etc/orbd/orbd.yaml`).
//!
//! Only `public_url` and `offerings` are required:
//!
//! ```yaml
//! public_url: https://coordinator.example:7443
//! offerings:
//!   - id: qemu-4vcpu-8gb
//!     runtime: qemu
//!     resources: { vcpus: 4, memory_mib: 8192, disk_gib: 40 }
//!     provider_token_file: /etc/orbd/tokens/qemu-4vcpu-8gb
//! ```
//!
//! The coordinator serves one Amp endpoint and one agent fleet; pause policy and
//! capability requirements follow from each offering's runtime.

use super::{ConfigError, ResourcesConfig, load_yaml};
use crate::model::{
    AmpEndpointId, BackendId, CapabilityRequirements, CheckpointFailurePolicy, ExecutionTarget,
    NestedKvmRequirement, Offering, OfferingId, PausePolicy, RuntimeKind, SnapshotRequirement,
    SpecTemplate,
};
use serde::Deserialize;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Id of the single Amp endpoint (orb identity is scoped by it).
pub const AMP_ENDPOINT: &str = "amp";
/// Id of the single agent-fleet backend (recorded in every orb's location).
pub const FLEET_BACKEND: &str = "fleet";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinatorConfig {
    /// URL agents dial, e.g. `https://coordinator.example:7443`. Join tokens carry it, and
    /// its host is the name on the coordinator's certificate.
    pub public_url: String,
    /// AgentControl listener (TLS).
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    /// Database, keys, CA and the local dispatch/admin sockets.
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    #[serde(default = "default_amp_url")]
    pub amp_url: String,
    /// Prometheus listener, e.g. `127.0.0.1:9464`. Disabled when absent.
    pub metrics_listen: Option<SocketAddr>,
    /// Heartbeat interval sent to agents in `Welcome`.
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_interval_ms: u64,
    /// Executable the generated `orbd-dispatch` wrapper runs as `<exe> dispatch`.
    /// Defaults to the running orbd binary; set it to a stable path (e.g.
    /// `/usr/local/bin/orbd`) if the binary is replaced in place on upgrade.
    pub dispatch_exe: Option<PathBuf>,
    /// Amp CLI used for `amp orb provider` children.
    #[serde(default = "default_amp_cli")]
    pub amp_cli: PathBuf,
    /// How often to run `amp update` for `amp_cli` (seconds; 0 disables). Providers are
    /// restarted onto a new version between jobs. The CLI never self-updates.
    #[serde(default = "default_amp_cli_update_interval")]
    pub amp_cli_update_interval_secs: u64,
    /// Disk held free on its host for each running orb to grow into (capped at the
    /// orb's disk size), on top of its next memory checkpoint. Sleeping orbs hold none.
    #[serde(default = "default_running_orb_disk_headroom_gib")]
    pub running_orb_disk_headroom_gib: u64,
    /// Orbs asleep longer than this many days are deleted (0 never deletes). Amp sends
    /// no delete when a thread is archived, only a pause, so this is what frees the
    /// disks of abandoned orbs. An orb woken after it was deleted starts fresh from the
    /// image: its files and processes are gone and Amp runs setup again.
    #[serde(default = "default_idle_orb_ttl_days")]
    pub idle_orb_ttl_days: u64,
    pub offerings: Vec<OfferingConfig>,
}

fn default_running_orb_disk_headroom_gib() -> u64 {
    10
}

fn default_idle_orb_ttl_days() -> u64 {
    30
}

fn default_listen() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 7443))
}

pub(crate) fn default_state_dir() -> PathBuf {
    PathBuf::from("/var/lib/orbd")
}

fn default_amp_url() -> String {
    "https://ampcode.com".into()
}

fn default_heartbeat_ms() -> u64 {
    5_000
}

fn default_amp_cli_update_interval() -> u64 {
    6 * 60 * 60
}

fn default_amp_cli() -> PathBuf {
    PathBuf::from("amp")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfferingConfig {
    pub id: OfferingId,
    /// `AMP_ORB_PROVIDER_TOKEN` for this offering's `amp orb provider` child.
    /// When absent the offering is served for local dispatch only (tests).
    pub provider_token_file: Option<PathBuf>,
    pub runtime: RuntimeKind,
    pub resources: ResourcesConfig,
}

impl OfferingConfig {
    /// Pause policy and requirements are what the runtime is for: VM runtimes keep
    /// memory across sleep (falling back to a cold stop) and QEMU gives orbs `/dev/kvm`;
    /// Docker sleeps to disk. Every fleet runtime releases compute while paused.
    pub fn to_offering(&self) -> Result<Offering, ConfigError> {
        let pause_policy = match self.runtime {
            RuntimeKind::Docker => PausePolicy::DiskOnly,
            RuntimeKind::Qemu | RuntimeKind::Firecracker | RuntimeKind::Fake => {
                PausePolicy::MemoryPreserving {
                    on_failure: CheckpointFailurePolicy::ColdStop,
                }
            }
        };
        let nested_kvm = match self.runtime {
            RuntimeKind::Qemu => NestedKvmRequirement::GuestDevice,
            _ => NestedKvmRequirement::NotRequired,
        };
        Ok(Offering {
            id: self.id.clone(),
            amp_endpoint: AmpEndpointId::new(AMP_ENDPOINT),
            backend_id: BackendId::new(FLEET_BACKEND),
            spec: SpecTemplate {
                target: ExecutionTarget::Fleet {
                    runtime: self.runtime,
                },
                resources: self.resources.to_resources()?,
                requirements: CapabilityRequirements {
                    release_compute_when_paused: true,
                    nested_kvm,
                    project_snapshots: SnapshotRequirement::NotRequired,
                },
                pause_policy,
            },
        })
    }
}

impl CoordinatorConfig {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let cfg: Self = load_yaml(path)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Structural validation. Capability validation against the backend happens when
    /// it is constructed (see `coordinator`).
    pub fn validate(&self) -> Result<(), ConfigError> {
        let inv = |m: String| Err(ConfigError::Invalid(m));
        crate::pki::url_host(&self.public_url)
            .map_err(|e| ConfigError::Invalid(format!("public_url: {e}")))?;
        if self.heartbeat_interval_ms == 0 {
            return inv("heartbeat_interval_ms must be > 0".into());
        }
        let mut offerings = HashSet::new();
        for o in &self.offerings {
            if !offerings.insert(o.id.as_str()) {
                return inv(format!("duplicate offering {}", o.id));
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
        self.state_dir.join("coordinator.sqlite")
    }

    /// Local socket for `orbd dispatch`.
    pub fn dispatch_socket(&self) -> PathBuf {
        self.state_dir.join("dispatch.sock")
    }

    /// Local socket for `orbd join-token` and `orbd agent list/remove`.
    pub fn admin_socket(&self) -> PathBuf {
        self.state_dir.join("admin.sock")
    }
}
