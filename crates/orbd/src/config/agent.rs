//! Agent configuration. Agents need no file: identity comes from the join token,
//! runtime and capacity are detected, and [`AgentSection`] is filled from flags/env.
//! The optional YAML file (`orbd agent --config agent.yaml`) holds advanced runtime and
//! image settings.

use super::{ConfigError, load_yaml};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    /// From flags/env, never from the file.
    #[serde(skip)]
    pub agent: AgentSection,
    #[serde(default)]
    pub runtime: RuntimesConfig,
    /// Image preparation for VM runtimes (OCI image → ext4 root disk). Defaults to the
    /// tools baked into orbd's container image.
    #[serde(default = "default_images")]
    pub images: Option<ImagesConfig>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            agent: AgentSection::default(),
            runtime: RuntimesConfig::default(),
            images: default_images(),
        }
    }
}

fn default_images() -> Option<ImagesConfig> {
    Some(ImagesConfig {
        guest_helper: default_guest_helper(),
        umoci: default_umoci(),
        mkfs_ext4: default_mkfs_ext4(),
        prewarm: Vec::new(),
        refresh_interval_secs: default_refresh_secs(),
    })
}

/// Amp's orb image; detected runtimes pull it at startup.
pub const AMP_ORB_IMAGE: &str = "us-central1-docker.pkg.dev/amp-prod-0/amp-orb/amp-orb:latest";

/// Where orbd's container image installs the guest kernel and `orbd-guest`.
pub const DEFAULT_GUEST_KERNEL: &str = "/usr/share/orbd/bzImage";
pub const DEFAULT_GUEST_HELPER: &str = "/usr/share/orbd/orbd-guest";

/// `[images]`: turns Amp's OCI image into a bootable root disk with orbd's guest
/// integration. Prepared images live under `<state_dir>/images/`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImagesConfig {
    /// Static `orbd-guest` binary (x86_64 musl) installed into every prepared image.
    #[serde(default = "default_guest_helper")]
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

fn default_guest_helper() -> PathBuf {
    DEFAULT_GUEST_HELPER.into()
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

#[derive(Debug, Default)]
pub struct AgentSection {
    /// Holds `agent.sqlite`, the agent's identity, images, orb disks, checkpoints.
    pub state_dir: PathBuf,
    /// Sockets, locks, process metadata (`<state_dir>/run`).
    pub run_dir: PathBuf,
    /// Seals accepted guest env for crash recovery; generated on first start.
    pub secret_key_file: PathBuf,
    /// Overrides for the detected capacity ceiling.
    pub capacity: CapacityOverrides,
    /// DNS servers written into orbs. Never the host's resolvers: a local stub such as
    /// Tailscale's 100.100.100.100 is unreachable from orbs.
    pub dns_servers: Vec<String>,
    pub metrics_listen: Option<SocketAddr>,
}

pub const DEFAULT_DNS: [&str; 2] = ["1.1.1.1", "9.9.9.9"];

impl AgentSection {
    pub fn new(state_dir: PathBuf) -> Self {
        Self {
            run_dir: state_dir.join("run"),
            secret_key_file: state_dir.join("secret.key"),
            state_dir,
            capacity: CapacityOverrides::default(),
            dns_servers: DEFAULT_DNS.map(String::from).to_vec(),
            metrics_listen: None,
        }
    }
}

/// Per-field overrides of the detected capacity.
#[derive(Clone, Copy, Debug, Default)]
pub struct CapacityOverrides {
    pub vcpus: Option<u32>,
    pub memory_mib: Option<u64>,
    pub disk_gib: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimesConfig {
    pub fake: Option<FakeRuntimeConfig>,
    pub docker: Option<DockerRuntimeConfig>,
    pub qemu: Option<QemuRuntimeConfig>,
}

/// QEMU q35 VMs with nested KVM and memory-preserving sleep. Every field has a default
/// matching orbd's container image, so a zero-config agent needs none of them.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QemuRuntimeConfig {
    /// `qemu-system-x86_64`; a bare name is looked up on PATH.
    #[serde(default = "default_qemu")]
    pub qemu: PathBuf,
    #[serde(default = "default_qemu_img")]
    pub qemu_img: PathBuf,
    /// Guest kernel (bzImage, everything built in). Its digest is pinned in each
    /// checkpoint: a replaced kernel makes sleeping orbs cold-boot on their next spawn.
    #[serde(default = "default_kernel")]
    pub kernel: PathBuf,
    /// Delegated cgroup v2 directory; each QEMU runs in `<cgroup_parent>/orbd-<orb>`,
    /// outside the agent's cgroup, so stopping the agent service leaves VMs running.
    pub cgroup_parent: Option<PathBuf>,
    /// Prebuilt raw ext4 root disks by image reference, used instead of the image
    /// pipeline (development and KVM tests).
    #[serde(default)]
    pub static_images: Vec<QemuStaticImage>,
    /// DNS servers delivered to guests (from `agent.dns_servers`).
    #[serde(skip)]
    pub dns_servers: Vec<String>,
}

impl Default for QemuRuntimeConfig {
    fn default() -> Self {
        Self {
            qemu: default_qemu(),
            qemu_img: default_qemu_img(),
            kernel: default_kernel(),
            cgroup_parent: None,
            static_images: Vec::new(),
            dns_servers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QemuStaticImage {
    pub image_ref: String,
    pub rootfs: PathBuf,
}

fn default_qemu() -> PathBuf {
    "qemu-system-x86_64".into()
}
fn default_qemu_img() -> PathBuf {
    "qemu-img".into()
}
fn default_kernel() -> PathBuf {
    DEFAULT_GUEST_KERNEL.into()
}

/// Test runtime with durable fake state. Never enable in production.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeRuntimeConfig {
    /// Advertise memory-preserving sleep (to exercise that path).
    #[serde(default)]
    pub memory_preserving_sleep: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DockerRuntimeConfig {
    /// Docker API endpoint; defaults to the local socket.
    pub docker_host: Option<String>,
    /// DNS servers written into orbs (from `agent.dns_servers`).
    #[serde(skip)]
    pub dns_servers: Vec<String>,
    /// Docker network for orb containers; defaults to Docker's bridge.
    pub network: Option<String>,
    /// Images pulled in the background at startup when missing, so the first spawn
    /// does not fail with ImageNotReady while a multi-GB pull runs.
    #[serde(default)]
    pub prewarm_images: Vec<String>,
}

impl AgentConfig {
    /// Reads the optional advanced-settings file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        load_yaml(path)
    }

    pub fn db_path(&self) -> PathBuf {
        self.agent.state_dir.join("agent.sqlite")
    }
}
