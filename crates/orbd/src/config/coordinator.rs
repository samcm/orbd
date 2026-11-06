//! Coordinator configuration (`orbd coordinator`, default `/etc/orbd/orbd.yaml`).
//!
//! Only `public_url` and `offerings` are required; `providers` serve them to Amp:
//!
//! ```yaml
//! public_url: https://coordinator.example:7443
//! offerings:
//!   - id: qemu-4vcpu-8gb
//!     runtime: qemu
//!     resources: { vcpus: 4, memory_mib: 8192, disk_gib: 40 }
//! providers:
//!   - id: homelab
//!     token_file: /etc/orbd/tokens/homelab
//!     orb_types: [qemu-4vcpu-8gb]
//! ```
//!
//! The coordinator serves one Amp endpoint, one agent fleet and optionally Boat
//! (`boat:`, offerings with `runtime: boat`); pause policy and capability requirements
//! follow from each offering's runtime.

use super::{ConfigError, ResourcesConfig, load_yaml};
use crate::model::{
    AmpEndpointId, BackendId, BackendProfileId, CapabilityRequirements, CheckpointFailurePolicy,
    ExecutionTarget, NestedKvmRequirement, Offering, OfferingId, PausePolicy, RuntimeKind,
    SnapshotRequirement, SpecTemplate,
};
use serde::Deserialize;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Id of the single Amp endpoint (orb identity is scoped by it).
pub const AMP_ENDPOINT: &str = "amp";
/// Id of the single agent-fleet backend (recorded in every orb's location).
pub const FLEET_BACKEND: &str = "fleet";
/// Id of the Boat backend (recorded in the location of every Boat orb).
pub const BOAT_BACKEND: &str = "boat";

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
    /// Boat (boat.dev) sandboxes for offerings with `runtime: boat`.
    pub boat: Option<BoatConfig>,
    pub offerings: Vec<OfferingConfig>,
    /// Amp orb providers whose orb types are offerings: one token serves them all.
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
}

/// One Amp orb provider with orb types. Each orb type in Amp is named after the
/// offering it runs on; Amp passes the thread's type to every spawn as `ORB_TYPE`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    /// Names the provider in logs and metrics; distinct from every offering id.
    pub id: String,
    /// `AMP_ORB_PROVIDER_TOKEN` for this provider's `amp orb provider` child.
    pub token_file: PathBuf,
    /// The offerings, named as orb types in Amp. The first is used when a spawn has no
    /// `ORB_TYPE` (the provider has no orb types in Amp).
    pub orb_types: Vec<OfferingId>,
}

/// `boat:`: runs orbs as Boat sandboxes, driven by the coordinator over Boat's API.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoatConfig {
    /// File holding the Boat API key (`boat_...`).
    pub api_key_file: PathBuf,
    /// What running Boat orbs may hold together (sleeping ones hold nothing): the
    /// spending cap. Keep it within the plan's "sandboxes at once".
    pub capacity: BoatCapacity,
    /// Boat stops a sandbox this long after it was created or resumed, in case Amp
    /// never pauses it (0 never stops it). The free trial allows at most 7200.
    #[serde(default = "default_boat_auto_stop")]
    pub auto_stop_secs: u64,
    #[serde(default = "default_boat_api_url")]
    pub api_url: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoatCapacity {
    pub vcpus: u32,
    pub memory_mib: u64,
}

fn default_boat_auto_stop() -> u64 {
    24 * 60 * 60
}

fn default_boat_api_url() -> String {
    "https://boat.dev/api/v1".into()
}

/// Where an offering's orbs run: a fleet runtime on the agents, or Boat.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OfferingRuntime {
    Fake,
    Docker,
    Qemu,
    Firecracker,
    Boat,
}

impl OfferingRuntime {
    fn fleet(self) -> Option<RuntimeKind> {
        match self {
            Self::Fake => Some(RuntimeKind::Fake),
            Self::Docker => Some(RuntimeKind::Docker),
            Self::Qemu => Some(RuntimeKind::Qemu),
            Self::Firecracker => Some(RuntimeKind::Firecracker),
            Self::Boat => None,
        }
    }
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
    pub runtime: OfferingRuntime,
    /// For `runtime: boat`, the vCPUs and memory of a Boat size (see
    /// [`crate::backend::boat::SIZES`]).
    pub resources: ResourcesConfig,
    /// Offerings that take a new orb, in order, when no host of this one can. Each must
    /// be like-for-like: `/dev/kvm` in the orb on both or neither, and at least these
    /// vCPUs and memory. Spill targets' own spill_to lists are not followed.
    #[serde(default)]
    pub spill_to: Vec<OfferingId>,
}

impl OfferingConfig {
    /// Pause policy and requirements are what the runtime is for: VM runtimes keep
    /// memory across sleep (falling back to a cold stop) and QEMU gives orbs `/dev/kvm`;
    /// Docker and Boat sleep to disk. Every runtime releases compute while paused.
    pub fn to_offering(&self) -> Result<Offering, ConfigError> {
        let pause_policy = match self.runtime {
            OfferingRuntime::Docker | OfferingRuntime::Boat => PausePolicy::DiskOnly,
            OfferingRuntime::Qemu | OfferingRuntime::Firecracker | OfferingRuntime::Fake => {
                PausePolicy::MemoryPreserving {
                    on_failure: CheckpointFailurePolicy::ColdStop,
                }
            }
        };
        let nested_kvm = match self.runtime {
            OfferingRuntime::Qemu => NestedKvmRequirement::GuestDevice,
            _ => NestedKvmRequirement::NotRequired,
        };
        let (backend, target) = match self.runtime.fleet() {
            Some(runtime) => (FLEET_BACKEND, ExecutionTarget::Fleet { runtime }),
            None => {
                let r = self.resources;
                let size =
                    crate::backend::boat::size_for(r.vcpus, r.memory_mib).ok_or_else(|| {
                        ConfigError::Invalid(format!(
                            "offering {}: Boat has no size with {} vCPUs and {} MiB; use one of {}",
                            self.id,
                            r.vcpus,
                            r.memory_mib,
                            crate::backend::boat::describe_sizes()
                        ))
                    })?;
                (
                    BOAT_BACKEND,
                    ExecutionTarget::Provider {
                        profile: BackendProfileId::new(size.name),
                    },
                )
            }
        };
        Ok(Offering {
            id: self.id.clone(),
            amp_endpoint: AmpEndpointId::new(AMP_ENDPOINT),
            backend_id: BackendId::new(backend),
            spill_to: self.spill_to.clone(),
            spec: SpecTemplate {
                target,
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

/// Why an orb of `from` cannot run as `to`, or None if `to` is like-for-like: `/dev/kvm`
/// in the orb on both or neither, and at least as much compute. Sleep may differ: an
/// orb spilled to Boat keeps its disk across a pause but not its processes.
fn unlike(from: &SpecTemplate, to: &SpecTemplate) -> Option<String> {
    let kvm = |s: &SpecTemplate| s.requirements.nested_kvm != NestedKvmRequirement::NotRequired;
    let (a, b) = (from.resources, to.resources);
    if kvm(from) != kvm(to) {
        Some(format!(
            "one gives orbs /dev/kvm and the other does not ({} vs {})",
            if kvm(from) { "kvm" } else { "no kvm" },
            if kvm(to) { "kvm" } else { "no kvm" }
        ))
    } else if b.vcpus < a.vcpus || b.memory_bytes < a.memory_bytes {
        Some(format!(
            "it is smaller ({} vCPUs / {} MiB vs {} / {})",
            b.vcpus,
            b.memory_bytes / crate::model::MIB,
            a.vcpus,
            a.memory_bytes / crate::model::MIB
        ))
    } else {
        None
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
            if o.runtime == OfferingRuntime::Boat && self.boat.is_none() {
                return inv(format!(
                    "offering {}: runtime boat needs a `boat:` section",
                    o.id
                ));
            }
        }
        for o in &self.offerings {
            let from = o.to_offering()?;
            for (i, id) in o.spill_to.iter().enumerate() {
                if o.spill_to[..i].contains(id) {
                    return inv(format!("offering {}: spill_to lists {id} twice", o.id));
                }
                let Some(t) = self.offering(id.as_str()) else {
                    return inv(format!(
                        "offering {}: spill_to {id} is not an offering",
                        o.id
                    ));
                };
                if t.id == o.id {
                    return inv(format!("offering {}: spill_to names itself", o.id));
                }
                if let Some(why) = unlike(&from.spec, &t.to_offering()?.spec) {
                    return inv(format!("offering {}: cannot spill to {id}: {why}", o.id));
                }
            }
        }
        if let Some(b) = &self.boat
            && (b.capacity.vcpus == 0 || b.capacity.memory_mib == 0)
        {
            return inv("boat.capacity must be non-zero".into());
        }
        let mut providers = HashSet::new();
        for p in &self.providers {
            if offerings.contains(p.id.as_str()) || !providers.insert(p.id.as_str()) {
                return inv(format!(
                    "provider {}: the id is already an offering or provider",
                    p.id
                ));
            }
            if p.orb_types.is_empty() {
                return inv(format!("provider {}: orb_types is empty", p.id));
            }
            for (i, t) in p.orb_types.iter().enumerate() {
                if p.orb_types[..i].contains(t) {
                    return inv(format!("provider {}: orb_types lists {t} twice", p.id));
                }
                if self.offering(t.as_str()).is_none() {
                    return inv(format!(
                        "provider {}: orb type {t} is not an offering",
                        p.id
                    ));
                }
            }
        }
        // Amp splits one token's jobs between all the providers polling with it.
        let mut tokens = HashSet::new();
        let token_files = self
            .offerings
            .iter()
            .filter_map(|o| o.provider_token_file.as_ref());
        for t in token_files.chain(self.providers.iter().map(|p| &p.token_file)) {
            if !tokens.insert(t) {
                return inv(format!(
                    "provider token file {} is used twice; each provider needs its own token",
                    t.display()
                ));
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn offering(id: &str, runtime: OfferingRuntime, vcpus: u32, memory_mib: u64) -> Offering {
        OfferingConfig {
            id: OfferingId::new(id),
            provider_token_file: None,
            runtime,
            resources: ResourcesConfig {
                vcpus,
                memory_mib,
                disk_gib: 20,
            },
            spill_to: vec![],
        }
        .to_offering()
        .unwrap()
    }

    #[test]
    fn provider_orb_types_must_be_offerings_with_their_own_token() {
        let check = |providers: &str| {
            let yaml = format!(
                "public_url: https://c.example:7443\n\
                 offerings:\n  - id: qemu-4vcpu-8gb\n    runtime: qemu\n    \
                 resources: {{ vcpus: 4, memory_mib: 8192, disk_gib: 40 }}\n    \
                 provider_token_file: /t/qemu\n\
                 providers:\n{providers}"
            );
            serde_saphyr::from_str::<CoordinatorConfig>(&yaml)
                .unwrap()
                .validate()
                .err()
                .map(|e| e.to_string())
        };
        let ok = "  - { id: homelab, token_file: /t/homelab, orb_types: [qemu-4vcpu-8gb] }\n";
        assert_eq!(check(ok), None);
        let unknown = "  - { id: homelab, token_file: /t/homelab, orb_types: [gpu] }\n";
        assert!(
            check(unknown)
                .unwrap()
                .contains("orb type gpu is not an offering")
        );
        let shared = "  - { id: homelab, token_file: /t/qemu, orb_types: [qemu-4vcpu-8gb] }\n";
        assert!(check(shared).unwrap().contains("/t/qemu is used twice"));
    }

    #[test]
    fn spill_targets_must_be_like_for_like() {
        use OfferingRuntime::*;
        let fc = offering("fc", Firecracker, 1, 2048);
        let qemu = offering("qemu", Qemu, 2, 4096);
        let boat = offering("boat", Boat, 2, 4096);
        // No /dev/kvm on either side, and Boat is at least as big.
        assert_eq!(unlike(&fc.spec, &boat.spec), None);
        // QEMU orbs have /dev/kvm; Boat sandboxes do not.
        let why = unlike(&qemu.spec, &boat.spec).unwrap();
        assert!(why.contains("/dev/kvm"), "{why}");
        // Smaller is never a spill target.
        let big_fc = offering("fc-big", Firecracker, 4, 8192);
        assert!(
            unlike(&big_fc.spec, &boat.spec)
                .unwrap()
                .contains("smaller")
        );
    }
}
