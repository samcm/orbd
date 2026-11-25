//! Agent configuration. Agents need no file: identity comes from the join token,
//! runtime and capacity are detected, and [`AgentSection`] is filled from flags/env.
//! The optional YAML file (`orbd agent --config agent.yaml`) holds advanced runtime and
//! image settings.

use super::{ConfigError, load_yaml};
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddr};
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
        builder: default_builder(),
        prewarm: default_prewarm(),
        refresh_interval_secs: default_refresh_secs(),
        static_images: Vec::new(),
        allow: ImageAllow::default(),
        sandbox: ImageSandbox::default(),
    })
}

fn default_prewarm() -> Vec<String> {
    vec![AMP_ORB_IMAGE.into()]
}

/// Amp's orb image; agents prepare it at startup unless configured otherwise.
pub const AMP_ORB_IMAGE: &str = "us-central1-docker.pkg.dev/amp-prod-0/amp-orb/amp-orb:latest";

/// Where orbd's container image installs the guest kernel and `orbd-guest` (on macOS,
/// where `/usr/share` is read-only: `/usr/local/share/orbd`, kernel `Image`).
pub const DEFAULT_GUEST_KERNEL: &str = if cfg!(target_os = "macos") {
    "/usr/local/share/orbd/Image"
} else {
    "/usr/share/orbd/bzImage"
};
pub const DEFAULT_GUEST_HELPER: &str = if cfg!(target_os = "macos") {
    "/usr/local/share/orbd/orbd-guest"
} else {
    "/usr/share/orbd/orbd-guest"
};

/// The image builder macOS agents use by default (see [`ImagesConfig::builder`]); built
/// from `guest/builder/Dockerfile`.
pub const DEFAULT_IMAGE_BUILDER: &str = "orbd-image-builder:umoci-0.6.0";

/// `[images]`: turns Amp's OCI image into a bootable root disk with orbd's guest
/// integration. Prepared images live under `<state_dir>/images/`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImagesConfig {
    /// Static `orbd-guest` binary (musl, the host's architecture) installed into every
    /// prepared image.
    #[serde(default = "default_guest_helper")]
    pub guest_helper: PathBuf,
    /// `umoci` binary; must be the pinned version (see `orbd doctor`).
    #[serde(default = "default_umoci")]
    pub umoci: PathBuf,
    #[serde(default = "default_mkfs_ext4")]
    pub mkfs_ext4: PathBuf,
    /// Docker image that runs umoci and mkfs.ext4 for each build instead of the host
    /// (`docker run`, no network, only the build's work directory shared); `umoci` and
    /// `mkfs_ext4` are then unused. Default on macOS: [`DEFAULT_IMAGE_BUILDER`] (built
    /// from `guest/builder/Dockerfile`, never pulled); elsewhere none.
    #[serde(default = "default_builder")]
    pub builder: Option<String>,
    /// Images prepared in the background at startup, so the first spawn does not fail
    /// with ImageNotReady while a multi-GB image is pulled and converted. Defaults to
    /// Amp's orb image.
    #[serde(default = "default_prewarm")]
    pub prewarm: Vec<String>,
    /// How often a tag (e.g. `:latest`) is resolved again; the previously prepared
    /// image keeps serving until the new digest is ready.
    #[serde(default = "default_refresh_secs")]
    pub refresh_interval_secs: u64,
    /// Prebuilt raw ext4 root disks by image reference, used instead of preparing
    /// images (development and KVM tests).
    #[serde(default)]
    pub static_images: Vec<StaticImage>,
    /// Images that may be prepared, prewarmed ones included (from `agent.image_allow`).
    #[serde(skip)]
    pub allow: ImageAllow,
    /// How umoci, du and mkfs.ext4 run without a builder (from `agent.image_sandbox`).
    #[serde(skip)]
    pub sandbox: ImageSandbox,
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

fn default_builder() -> Option<String> {
    cfg!(target_os = "macos").then(|| DEFAULT_IMAGE_BUILDER.into())
}

fn default_refresh_secs() -> u64 {
    3600
}

#[derive(Debug)]
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
    /// The network VM orbs see.
    pub guest_network: GuestNetwork,
    /// Which private destinations VM orbs may reach (none by default).
    pub egress: Egress,
    pub metrics_listen: Option<SocketAddr>,
    /// Overrides the coordinator URL from enrollment (`ORBD_COORDINATOR_URL`).
    pub coordinator_url: Option<String>,
    /// New orbs come here only when no other agent fits them (`ORBD_OVERFLOW`).
    pub overflow: bool,
    /// Memory checkpoints of orbs asleep longer than this are dropped, so they cold-boot
    /// on their next wake (`ORBD_CHECKPOINT_MAX_AGE_HOURS`; None: kept for as long as
    /// the orb sleeps).
    pub checkpoint_max_age: Option<std::time::Duration>,
    /// A sleep's memory stays in RAM this long before its checkpoint is written, so a
    /// wake within it resumes in place and writes nothing (`ORBD_SLEEP_HOLD_MINUTES`;
    /// None: written at once).
    pub sleep_hold: Option<std::time::Duration>,
    /// The disk of an orb stopped (without a checkpoint) and unused this long is
    /// compressed until its next wake (`ORBD_ARCHIVE_AFTER_HOURS`; None: never).
    pub archive_after: Option<std::time::Duration>,
    /// Whether VMMs on Linux run jailed (`ORBD_VMM_JAIL`).
    pub vmm_jail: JailMode,
    /// Jailed VMMs run as `vmm_jail_uid_base + <index>` (`ORBD_VMM_JAIL_UID_BASE`).
    pub vmm_jail_uid_base: u32,
    /// Never use the Docker runtime (`ORBD_VM_ONLY`, on by default): detection fails
    /// unless a VM runtime is usable, and a configured or recorded Docker runtime is
    /// refused.
    pub vm_only: bool,
    /// Run no orb with `/dev/kvm` (unless `ORBD_NESTED_KVM=true`): advertise no nested
    /// KVM and refuse to start an orb whose type has `kvm`, whatever the coordinator sends.
    pub refuse_nested_kvm: bool,
    /// macOS: start QEMU orbs without a verified pf fence (`ORBD_ALLOW_UNFENCED_EGRESS`);
    /// they then reach whatever the Mac can, its LAN included.
    pub allow_unfenced_egress: bool,
    /// Images this agent prepares and runs (`ORBD_IMAGE_ALLOW`; empty: any).
    pub image_allow: ImageAllow,
    /// How image preparation runs its tools over image content (`ORBD_IMAGE_SANDBOX`,
    /// `ORBD_IMAGE_SANDBOX_UID_BASE`).
    pub image_sandbox: ImageSandbox,
}

/// `ORBD_VMM_JAIL`: whether QEMU and Firecracker run jailed on Linux (each VMM under its
/// orb's own uid, with no capabilities, in a mount namespace with only its orb's files;
/// see `runtime::vm::jail`). macOS QEMU always runs in its Seatbelt sandbox.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum JailMode {
    /// The default: a Linux VM agent runs as root and jails every VMM, or does not start.
    #[default]
    On,
    /// VMMs run as the agent's user, unjailed: unsafe, for development only.
    Off,
}

impl std::str::FromStr for JailMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "on" | "true" | "1" | "" => Ok(Self::On),
            "off" | "false" | "0" => Ok(Self::Off),
            other => Err(format!("{other:?}: expected on or off")),
        }
    }
}

impl std::fmt::Display for JailMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::On => "on",
            Self::Off => "off",
        })
    }
}

/// Default first uid and gid of jailed VMMs (1.4e9 and up are image builds' user
/// namespaces).
pub const DEFAULT_JAIL_UID_BASE: u32 = 1_300_000_000;

/// The jail Linux VMMs run in: orb `n`'s VMM (n from 1) gets uid and gid `uid_base + n`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VmmJail {
    pub uid_base: u32,
}

pub const DEFAULT_DNS: [&str; 2] = ["1.1.1.1", "9.9.9.9"];

/// The private IPv4 network a VM orb sees (every VM has its own): the guest is host `.15`
/// and its gateway host `.2`, as with QEMU's user networking. Fixed per orb when it is
/// created, so changing it affects only new orbs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GuestNetwork {
    network: Ipv4Addr,
    prefix: u8,
}

impl GuestNetwork {
    pub fn prefix(&self) -> u8 {
        self.prefix
    }

    pub fn gateway(&self) -> Ipv4Addr {
        self.host(2)
    }

    pub fn guest(&self) -> Ipv4Addr {
        self.host(15)
    }

    fn host(&self, n: u32) -> Ipv4Addr {
        Ipv4Addr::from(u32::from(self.network) + n)
    }

    fn mask(prefix: u8) -> u32 {
        u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0)
    }

    fn overlaps(&self, other: Ipv4Addr, other_prefix: u8) -> bool {
        let mask = Self::mask(self.prefix.min(other_prefix));
        u32::from(self.network) & mask == u32::from(other) & mask
    }
}

impl Default for GuestNetwork {
    fn default() -> Self {
        Self {
            network: Ipv4Addr::new(10, 0, 2, 0),
            prefix: 24,
        }
    }
}

impl std::fmt::Display for GuestNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

impl std::str::FromStr for GuestNetwork {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let (addr, prefix) = s
            .split_once('/')
            .ok_or_else(|| format!("{s}: expected a network such as 10.0.2.0/24"))?;
        let network: Ipv4Addr = addr.parse().map_err(|e| format!("{s}: {e}"))?;
        let prefix: u8 = prefix.parse().map_err(|e| format!("{s}: {e}"))?;
        // `.15` must be a host address, not the broadcast address.
        if prefix > 27 {
            return Err(format!("{s}: the prefix must be /27 or shorter"));
        }
        if u32::from(network) & !Self::mask(prefix) != 0 {
            return Err(format!("{s}: host bits are set"));
        }
        let net = Self { network, prefix };
        // Link-local carries orbd's own transit addresses (and cloud metadata).
        for (reserved, len) in [
            (Ipv4Addr::new(0, 0, 0, 0), 8),
            (Ipv4Addr::new(127, 0, 0, 0), 8),
            (Ipv4Addr::new(169, 254, 0, 0), 16),
            (Ipv4Addr::new(224, 0, 0, 0), 3),
        ] {
            if net.overlaps(reserved, len) {
                return Err(format!("{s}: overlaps reserved {reserved}/{len}"));
            }
        }
        Ok(net)
    }
}

/// Which private destinations VM orbs on Linux may reach. By default they get the
/// internet only: private, shared (a tailnet's 100.64.0.0/10), loopback, multicast,
/// documentation and reserved IPv4 destinations are dropped for their traffic, except
/// what `allow` names and the orbs' DNS servers (port 53). Cloud metadata endpoints,
/// IPv6 and the agent itself stay blocked whatever this says, and the agent's own
/// traffic is never affected. Docker orbs and macOS orbs (fenced by pf instead, see
/// `QemuRuntimeConfig::network_group`) are not filtered by orbd.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Egress {
    /// Private destinations orbs may reach anyway (`ORBD_EGRESS_ALLOW`).
    pub allow: Vec<EgressRule>,
    /// Log every new connection an orb starts, with the verdict (`ORBD_FLOW_LOG`; see
    /// `runtime::vm::flowlog`).
    pub flow_log: bool,
}

impl Egress {
    /// From `ORBD_EGRESS_ALLOW` entries (empty ones are skipped).
    pub fn new(allow: &[String]) -> Result<Self, String> {
        let allow = allow
            .iter()
            .map(|e| e.trim())
            .filter(|e| !e.is_empty())
            .map(str::parse)
            .collect::<Result<Vec<EgressRule>, String>>()?;
        Ok(Self {
            allow,
            flow_log: false,
        })
    }
}

pub use crate::model::egress::{EgressProto, EgressRule};

/// Which images an agent prepares and runs (`ORBD_IMAGE_ALLOW`), checked on the agent
/// before an image is resolved or pulled, for every spawn of a new disk and every
/// prewarmed image. Empty allows any image. Each pattern is one of
///
/// * `<repository>`: any tag or digest of it, e.g. `ghcr.io/acme/orb`;
/// * `<repository>:<tag>`: that tag only (a reference that also pins a digest is
///   pulled by its digest, so it needs a digest or repository pattern);
/// * `<repository>@<digest>`: that manifest only;
/// * `<registry>/<prefix>*`: every repository whose name starts with it, e.g.
///   `us-central1-docker.pkg.dev/amp-prod-0/amp-orb/*`.
///
/// References are compared in full (`debian` is `docker.io/library/debian:latest`, a
/// reference without tag is `:latest`), the image as Amp names it (before arm64's
/// `-arm64` repository).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImageAllow(Vec<ImagePattern>);

#[derive(Clone, Debug, PartialEq, Eq)]
enum ImagePattern {
    /// `registry/repository`
    Repository(String),
    Tag(String, String),
    Digest(String, String),
    /// `registry/` and the start of a repository name.
    Prefix(String),
}

impl std::fmt::Display for ImagePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Repository(r) => write!(f, "{r}"),
            Self::Tag(r, t) => write!(f, "{r}:{t}"),
            Self::Digest(r, d) => write!(f, "{r}@{d}"),
            Self::Prefix(p) => write!(f, "{p}*"),
        }
    }
}

impl std::str::FromStr for ImagePattern {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let bad = |why: &str| {
            format!(
                "{s:?}: {why} (expected <repository>, <repository>:<tag>, \
                 <repository>@<digest> or <registry>/<prefix>*)"
            )
        };
        let s = s.trim();
        if let Some(prefix) = s.strip_suffix('*') {
            if prefix.is_empty() {
                return Err(bad(
                    "`*` alone would allow every image: leave ORBD_IMAGE_ALLOW empty",
                ));
            }
            let (registry, rest) = prefix
                .split_once('/')
                .filter(|(r, _)| r.contains(['.', ':']) || *r == "localhost")
                .ok_or_else(|| bad("a prefix starts with a registry, e.g. ghcr.io/acme/*"))?;
            if !rest
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "._-/".contains(c))
            {
                return Err(bad(
                    "a prefix names repositories: lowercase, no tag, digest or inner `*`",
                ));
            }
            let registry = if registry == "index.docker.io" {
                "docker.io"
            } else {
                registry
            };
            return Ok(Self::Prefix(format!("{registry}/{rest}")));
        }
        if s.contains('*') {
            return Err(bad("`*` only ends a prefix"));
        }
        let r: oci_client::Reference = s.parse().map_err(|e| bad(&format!("{e}")))?;
        let repo = format!("{}/{}", r.registry(), r.repository());
        let tagged = s.rsplit('/').next().is_some_and(|last| last.contains(':'));
        Ok(match (r.digest(), r.tag()) {
            (Some(d), _) => Self::Digest(repo, d.to_owned()),
            (None, Some(t)) if tagged => Self::Tag(repo, t.to_owned()),
            _ => Self::Repository(repo),
        })
    }
}

impl ImagePattern {
    fn matches(&self, r: &oci_client::Reference) -> bool {
        let repo = format!("{}/{}", r.registry(), r.repository());
        match self {
            Self::Repository(x) => repo == *x,
            Self::Tag(x, t) => repo == *x && r.digest().is_none() && r.tag() == Some(t),
            Self::Digest(x, d) => repo == *x && r.digest() == Some(d),
            Self::Prefix(p) => repo.starts_with(p),
        }
    }
}

impl ImageAllow {
    /// From `ORBD_IMAGE_ALLOW` entries (empty ones are skipped).
    pub fn new(entries: &[String]) -> Result<Self, String> {
        entries
            .iter()
            .filter(|e| !e.trim().is_empty())
            .map(|e| e.parse())
            .collect::<Result<_, _>>()
            .map(Self)
    }

    /// Any image is allowed.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `image_ref` (as Amp gives it) may be prepared and run here; the error
    /// names the setting that refused it.
    pub fn check(&self, image_ref: &str) -> Result<(), String> {
        if self.0.is_empty() {
            return Ok(());
        }
        let allowed = image_ref
            .parse::<oci_client::Reference>()
            .is_ok_and(|r| self.0.iter().any(|p| p.matches(&r)));
        if allowed {
            Ok(())
        } else {
            Err(format!(
                "image {image_ref} is not allowed on this agent (ORBD_IMAGE_ALLOW)"
            ))
        }
    }
}

impl std::fmt::Display for ImageAllow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, p) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{p}")?;
        }
        Ok(())
    }
}

/// First of the host ids the image sandbox maps by default: far above regular users and
/// clear of the per-VM ids (1_300_000_000 and up).
pub const DEFAULT_IMAGE_SANDBOX_UID_BASE: u32 = 1_400_000_000;
/// Ids the image sandbox maps: 0..65535 inside.
pub const IMAGE_SANDBOX_IDS: u32 = 65_536;

/// How a Linux agent without a builder image runs umoci, du and mkfs.ext4 over image
/// content (`ORBD_IMAGE_SANDBOX`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageSandbox {
    /// As root of a user namespace whose ids 0..65535 are the host's `uid_base..`, with
    /// nothing of the host but the tools and the build's own files
    /// (`ORBD_IMAGE_SANDBOX_UID_BASE`; see `image::sandbox`).
    On { uid_base: u32 },
    /// As real root (`sudo -n` when the agent is not root): orbd before the sandbox.
    /// Unsafe: whoever sets an orb's image then feeds root-run tar and ext4 tooling.
    Off,
}

impl Default for ImageSandbox {
    fn default() -> Self {
        Self::On {
            uid_base: DEFAULT_IMAGE_SANDBOX_UID_BASE,
        }
    }
}

impl ImageSandbox {
    /// From `ORBD_IMAGE_SANDBOX` (on or off) and `ORBD_IMAGE_SANDBOX_UID_BASE`: the base
    /// must leave ids below 65536 (host root, system and regular users) unmapped and fit
    /// all 65536 ids below 2^32-1.
    pub fn new(on: bool, uid_base: u32) -> Result<Self, String> {
        if !on {
            return Ok(Self::Off);
        }
        if !(IMAGE_SANDBOX_IDS..=u32::MAX - IMAGE_SANDBOX_IDS).contains(&uid_base) {
            return Err(format!(
                "ORBD_IMAGE_SANDBOX_UID_BASE={uid_base}: must be between {IMAGE_SANDBOX_IDS} and {}",
                u32::MAX - IMAGE_SANDBOX_IDS
            ));
        }
        Ok(Self::On { uid_base })
    }

    pub fn uid_base(&self) -> Option<u32> {
        match self {
            Self::On { uid_base } => Some(*uid_base),
            Self::Off => None,
        }
    }
}

impl Default for AgentSection {
    /// [`AgentSection::new`]'s settings (VM-only, no nested KVM, VMMs jailed) with no
    /// paths.
    fn default() -> Self {
        Self {
            run_dir: PathBuf::new(),
            secret_key_file: PathBuf::new(),
            ..Self::new(PathBuf::new())
        }
    }
}

impl AgentSection {
    pub fn new(state_dir: PathBuf) -> Self {
        Self {
            run_dir: state_dir.join("run"),
            secret_key_file: state_dir.join("secret.key"),
            state_dir,
            capacity: CapacityOverrides::default(),
            dns_servers: DEFAULT_DNS.map(String::from).to_vec(),
            guest_network: GuestNetwork::default(),
            egress: Egress::default(),
            metrics_listen: None,
            coordinator_url: None,
            overflow: false,
            checkpoint_max_age: None,
            sleep_hold: Self::sleep_hold_minutes(None),
            archive_after: Self::archive_after_hours(None),
            vmm_jail: JailMode::On,
            vmm_jail_uid_base: DEFAULT_JAIL_UID_BASE,
            vm_only: true,
            refuse_nested_kvm: true,
            allow_unfenced_egress: false,
            image_allow: ImageAllow::default(),
            image_sandbox: ImageSandbox::default(),
        }
    }

    /// `ORBD_CHECKPOINT_MAX_AGE_HOURS`: unset or 0 keeps checkpoints (the default).
    pub fn checkpoint_max_age_hours(hours: Option<u64>) -> Option<std::time::Duration> {
        hours
            .filter(|&h| h > 0)
            .map(|h| std::time::Duration::from_secs(h.saturating_mul(3600)))
    }

    /// `ORBD_SLEEP_HOLD_MINUTES`: unset is [`DEFAULT_SLEEP_HOLD_MINUTES`], 0 off.
    pub fn sleep_hold_minutes(minutes: Option<u64>) -> Option<std::time::Duration> {
        Some(minutes.unwrap_or(DEFAULT_SLEEP_HOLD_MINUTES))
            .filter(|&m| m > 0)
            .map(|m| std::time::Duration::from_secs(m.saturating_mul(60)))
    }

    /// `ORBD_ARCHIVE_AFTER_HOURS`: unset is [`DEFAULT_ARCHIVE_AFTER_HOURS`], 0 off.
    pub fn archive_after_hours(hours: Option<u64>) -> Option<std::time::Duration> {
        Some(hours.unwrap_or(DEFAULT_ARCHIVE_AFTER_HOURS))
            .filter(|&h| h > 0)
            .map(|h| std::time::Duration::from_secs(h.saturating_mul(3600)))
    }
}

/// How long a sleep stays in RAM before its checkpoint is written, by default. Most
/// wakes come within minutes of the sleep; one within this writes nothing.
pub const DEFAULT_SLEEP_HOLD_MINUTES: u64 = 20;

/// How long an orb stays stopped and unused before its disk is archived, by default.
/// Orbs stopped this long rarely come back, and one that does pays a restore.
pub const DEFAULT_ARCHIVE_AFTER_HOURS: u64 = 24;

/// Per-field overrides of the detected capacity.
#[derive(Clone, Copy, Debug, Default)]
pub struct CapacityOverrides {
    pub vcpus: Option<u32>,
    pub memory_mib: Option<u64>,
    /// Limit on what orbd allocates under the state directory (default: the
    /// filesystem's size).
    pub disk_gib: Option<u64>,
    /// Free space to keep on the state directory's filesystem (default: max(10 GiB,
    /// 5% of it)).
    pub disk_reserve_gib: Option<u64>,
    /// Memory to keep available on the host (default: max(4 GiB, 10% of it)).
    pub memory_reserve_gib: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimesConfig {
    pub fake: Option<FakeRuntimeConfig>,
    pub docker: Option<DockerRuntimeConfig>,
    pub qemu: Option<QemuRuntimeConfig>,
    pub firecracker: Option<FirecrackerRuntimeConfig>,
}

/// QEMU VMs with memory-preserving sleep (q35 with nested KVM on Linux, `virt` under
/// Hypervisor.framework on Apple Silicon). Every field has a default matching orbd's
/// container image (or a macOS install), so a zero-config agent needs none of them.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QemuRuntimeConfig {
    /// `qemu-system-x86_64` (`qemu-system-aarch64` on arm64); a bare name is looked up
    /// on PATH.
    #[serde(default = "default_qemu")]
    pub qemu: PathBuf,
    #[serde(default = "default_qemu_img")]
    pub qemu_img: PathBuf,
    /// Guest kernel (bzImage, or arm64 `Image`; everything built in). Its digest is pinned
    /// in each checkpoint: a replaced kernel makes sleeping orbs cold-boot on their next
    /// spawn.
    #[serde(default = "default_kernel")]
    pub kernel: PathBuf,
    /// Delegated cgroup v2 directory; each QEMU runs in `<cgroup_parent>/orbd-<orb>`,
    /// outside the agent's cgroup, so stopping the agent service leaves VMs running.
    pub cgroup_parent: Option<PathBuf>,
    /// macOS: a group QEMU runs with, through `orbd-netgroup` (on PATH, setgid to it):
    /// pf `group` rules then confine orb traffic without touching the agent's own. The
    /// runtime refuses to start while it is not setgid, and orbs start only while pf
    /// verifiably blocks the group (see `runtime::qemu::fence`), unless
    /// `ORBD_ALLOW_UNFENCED_EGRESS` is set. Ignored, with a warning, on Linux, where QEMU
    /// orbs have their own filtered network namespaces.
    pub network_group: Option<String>,
    /// DNS servers delivered to guests (from `agent.dns_servers`).
    #[serde(skip)]
    pub dns_servers: Vec<String>,
    /// Network for new orbs (from `agent.guest_network`).
    #[serde(skip)]
    pub guest_network: GuestNetwork,
    /// Private destinations orbs with their own network namespace may reach (from
    /// `agent.egress`; Linux).
    #[serde(skip)]
    pub egress: Egress,
    /// The jail QEMU runs in (from `agent.vmm_jail*`; Linux); None runs it unjailed.
    #[serde(skip)]
    pub jail: Option<VmmJail>,
    /// Start no orb whose type has `kvm` (from `agent.refuse_nested_kvm`).
    #[serde(skip, default = "refuse_nested_kvm")]
    pub refuse_nested_kvm: bool,
    /// macOS: start orbs without a verified egress fence (from
    /// `agent.allow_unfenced_egress`).
    #[serde(skip)]
    pub allow_unfenced_egress: bool,
}

/// Agents allow no nested KVM unless `ORBD_NESTED_KVM=true`.
fn refuse_nested_kvm() -> bool {
    true
}

impl Default for QemuRuntimeConfig {
    fn default() -> Self {
        Self {
            qemu: default_qemu(),
            qemu_img: default_qemu_img(),
            kernel: default_kernel(),
            cgroup_parent: None,
            network_group: None,
            dns_servers: Vec::new(),
            guest_network: GuestNetwork::default(),
            egress: Egress::default(),
            jail: None,
            refuse_nested_kvm: refuse_nested_kvm(),
            allow_unfenced_egress: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticImage {
    pub image_ref: String,
    pub rootfs: PathBuf,
}

/// Firecracker microVMs: fast wake from lazily loaded memory, no `/dev/kvm` inside.
/// Every field has a default matching orbd's container image.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FirecrackerRuntimeConfig {
    /// `firecracker`; a bare name is looked up on PATH.
    #[serde(default = "default_firecracker")]
    pub firecracker: PathBuf,
    /// Guest kernel, the same one QEMU boots. Only cold boots read it.
    #[serde(default = "default_kernel")]
    pub kernel: PathBuf,
    /// As for QEMU: each VMM runs in `<cgroup_parent>/orbd-<orb>`.
    pub cgroup_parent: Option<PathBuf>,
    /// DNS servers delivered to guests (from `agent.dns_servers`).
    #[serde(skip)]
    pub dns_servers: Vec<String>,
    /// Network for new orbs (from `agent.guest_network`).
    #[serde(skip)]
    pub guest_network: GuestNetwork,
    /// Private destinations orbs may reach (from `agent.egress`).
    #[serde(skip)]
    pub egress: Egress,
    /// The jail Firecracker runs in (from `agent.vmm_jail*`); None runs it unjailed.
    #[serde(skip)]
    pub jail: Option<VmmJail>,
    /// Free disk the agent keeps (its disk reserve): archives and restores of orbs'
    /// disks never take the filesystem below it.
    #[serde(skip)]
    pub disk_reserve: u64,
}

impl Default for FirecrackerRuntimeConfig {
    fn default() -> Self {
        Self {
            firecracker: default_firecracker(),
            kernel: default_kernel(),
            cgroup_parent: None,
            dns_servers: Vec::new(),
            guest_network: GuestNetwork::default(),
            egress: Egress::default(),
            jail: None,
            disk_reserve: 0,
        }
    }
}

fn default_firecracker() -> PathBuf {
    "firecracker".into()
}

fn default_qemu() -> PathBuf {
    orbd_qemu::probe::QEMU_SYSTEM.into()
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
    /// Report this filesystem instead of the state directory's real one, so tests do
    /// not depend on the host's free space.
    pub disk: Option<FakeDisk>,
    /// The architecture it reports (default amd64, whatever the host's), to test
    /// placement by architecture.
    #[serde(default)]
    pub arch: crate::model::Arch,
    /// Advertise restricted egress (enforcing nothing), to test placement and refusal of
    /// orb types with `egress`. Off by default.
    #[serde(default)]
    pub egress_policy: bool,
    /// Keep a restored checkpoint's blocks allocated until the orb's next sleep, as
    /// Firecracker does, instead of freeing them at the wake, as QEMU does. Off by
    /// default.
    #[serde(default)]
    pub restore_keeps_checkpoint: bool,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeDisk {
    pub free_gib: u64,
    pub total_gib: u64,
}

#[derive(Clone, Debug, Deserialize)]
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
    /// does not fail with ImageNotReady while a multi-GB pull runs. Defaults to Amp's
    /// orb image.
    #[serde(default = "default_prewarm")]
    pub prewarm_images: Vec<String>,
}

impl Default for DockerRuntimeConfig {
    fn default() -> Self {
        Self {
            docker_host: None,
            dns_servers: Vec::new(),
            network: None,
            prewarm_images: default_prewarm(),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::{
        AgentSection, Egress, EgressProto, EgressRule, GuestNetwork, ImageAllow, ImageSandbox,
        JailMode,
    };
    use std::time::Duration;

    /// Checkpoints are kept for as long as an orb sleeps unless a max age is set; 0
    /// (e.g. compose's `${VAR:-0}`) means unset.
    #[test]
    fn sleep_holds_default_on_and_zero_turns_them_off() {
        let s = AgentSection::new("/s".into());
        assert_eq!(s.sleep_hold, Some(Duration::from_secs(20 * 60)));
        assert_eq!(AgentSection::sleep_hold_minutes(Some(0)), None);
        assert_eq!(
            AgentSection::sleep_hold_minutes(Some(5)),
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn archives_default_on_and_zero_turns_them_off() {
        let s = AgentSection::new("/s".into());
        assert_eq!(s.archive_after, Some(Duration::from_secs(24 * 3600)));
        assert_eq!(AgentSection::archive_after_hours(Some(0)), None);
        assert_eq!(
            AgentSection::archive_after_hours(Some(72)),
            Some(Duration::from_secs(72 * 3600))
        );
    }

    #[test]
    fn checkpoint_max_age_is_off_unless_set() {
        assert_eq!(AgentSection::default().checkpoint_max_age, None);
        assert_eq!(AgentSection::new("/s".into()).checkpoint_max_age, None);
        assert_eq!(AgentSection::checkpoint_max_age_hours(None), None);
        assert_eq!(AgentSection::checkpoint_max_age_hours(Some(0)), None);
        assert_eq!(
            AgentSection::checkpoint_max_age_hours(Some(12)),
            Some(Duration::from_secs(12 * 3600))
        );
        assert!(AgentSection::checkpoint_max_age_hours(Some(u64::MAX)).is_some());
    }

    #[test]
    fn egress_rules_parse_and_print() {
        for (input, printed, proto, prefix, ports) in [
            (
                "udp:192.168.2.201:41641",
                "udp:192.168.2.201:41641",
                EgressProto::Udp,
                32,
                Some((41641, 41641)),
            ),
            (
                "tcp:10.0.0.0/8:8000-8100",
                "tcp:10.0.0.0/8:8000-8100",
                EgressProto::Tcp,
                8,
                Some((8000, 8100)),
            ),
            (
                "any:192.168.2.0/24",
                "any:192.168.2.0/24",
                EgressProto::Any,
                24,
                None,
            ),
            (
                " TCP:100.64.0.0/10:443 ",
                "tcp:100.64.0.0/10:443",
                EgressProto::Tcp,
                10,
                Some((443, 443)),
            ),
            (
                "any:172.16.0.0/12:*",
                "any:172.16.0.0/12",
                EgressProto::Any,
                12,
                None,
            ),
        ] {
            let r: EgressRule = input.parse().unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(
                (r.proto, r.prefix, r.ports),
                (proto, prefix, ports),
                "{input}"
            );
            assert_eq!(r.to_string(), printed);
            assert_eq!(printed.parse::<EgressRule>().unwrap(), r, "round trip");
        }
        for bad in [
            "",
            "192.168.2.201:41641",    // no protocol
            "icmp:192.168.2.201",     // unknown protocol
            "*:192.168.2.201",        // `any`, not `*`
            "udp:192.168.2.201:1:2",  // too many parts
            "udp:192.168.2.1/24:53",  // host bits
            "udp:192.168.2.0/33",     // prefix
            "tcp:*:443",              // every private address
            "any:0.0.0.0/0",          // the same
            "any:96.0.0.0/3",         // shorter than /8
            "tcp:169.254.169.254:80", // link-local: cloud metadata
            "any:169.0.0.0/8",        // contains link-local
            "udp:host.lan:53",        // names: addresses only
            "udp:fd00::1:53",         // IPv4 only
            "tcp:10.0.0.1:0",         // port 0
            "tcp:10.0.0.1:90-80",     // range backwards
            "tcp:10.0.0.1:70000",     // out of range
        ] {
            assert!(bad.parse::<EgressRule>().is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn egress_lists_skip_empty_entries() {
        let list = |s: &[&str]| s.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        // `ORBD_EGRESS_ALLOW=` and a trailing comma.
        assert_eq!(Egress::new(&list(&[""])).unwrap(), Egress::default());
        let e = Egress::new(&list(&["udp:10.0.0.1:53", " "])).unwrap();
        assert_eq!(e.allow.len(), 1);
        assert!(Egress::new(&list(&["udp:10.0.0.1:53", "bad"])).is_err());
    }

    fn allow(entries: &[&str]) -> ImageAllow {
        let entries: Vec<String> = entries.iter().map(|s| s.to_string()).collect();
        ImageAllow::new(&entries).unwrap_or_else(|e| panic!("{entries:?}: {e}"))
    }

    const AMP: &str = "us-central1-docker.pkg.dev/amp-prod-0/amp-orb/amp-orb";
    const DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// An empty list (the default, `ORBD_IMAGE_ALLOW=` and stray commas) allows anything,
    /// even what is no valid reference (the registry refuses that later).
    #[test]
    fn empty_image_allowlist_allows_any_image() {
        assert!(ImageAllow::default().is_empty());
        for a in [allow(&[]), allow(&["", " "])] {
            assert!(a.is_empty());
            for r in ["debian", "ghcr.io/x/y:z", "Not A Ref"] {
                assert!(a.check(r).is_ok(), "{r}");
            }
        }
    }

    #[test]
    fn image_allowlist_patterns() {
        let latest = format!("{AMP}:latest");
        let pinned = format!("{AMP}@{DIGEST}");
        let tag_and_digest = format!("{AMP}:latest@{DIGEST}");
        let other_tag = format!("{AMP}:canary");
        let arm = format!("{AMP}-arm64:latest");
        let sibling = "us-central1-docker.pkg.dev/amp-prod-0/amp-orb/other:1";
        let elsewhere = "us-central1-docker.pkg.dev/amp-prod-0/elsewhere/amp-orb:latest";
        let all = [
            latest.as_str(),
            AMP, // implicit :latest
            &pinned,
            &tag_and_digest,
            &other_tag,
            &arm,
            sibling,
            elsewhere,
        ];
        let check = |a: &ImageAllow, allowed: &[&str]| {
            for r in all {
                assert_eq!(a.check(r).is_ok(), allowed.contains(&r), "{a} with {r}");
            }
        };
        // A repository: any tag or digest of it, not other repositories.
        check(
            &allow(&[AMP]),
            &[&latest, AMP, &pinned, &tag_and_digest, &other_tag],
        );
        // A tag: that tag only; a digest pins the manifest whatever the tag says.
        check(&allow(&[&latest]), &[&latest, AMP]);
        check(&allow(&[&pinned]), &[&pinned, &tag_and_digest]);
        // A prefix: every repository under it.
        let prefix = "us-central1-docker.pkg.dev/amp-prod-0/amp-orb/*";
        check(
            &allow(&[prefix]),
            &[
                &latest,
                AMP,
                &pinned,
                &tag_and_digest,
                &other_tag,
                &arm,
                sibling,
            ],
        );
        check(
            &allow(&[&format!("{AMP}*")]),
            &[&latest, AMP, &pinned, &tag_and_digest, &other_tag, &arm],
        );
        // Several patterns, with whitespace and an empty entry.
        check(
            &allow(&[" ghcr.io/acme/orb ", "", &other_tag]),
            &[&other_tag],
        );
    }

    /// References and patterns are compared in full form: Docker Hub's defaults apply.
    #[test]
    fn image_allowlist_normalizes_references() {
        let a = allow(&[
            "debian:13",
            "docker.io/library/alpine",
            "index.docker.io/acme/*",
        ]);
        for ok in [
            "debian:13",
            "docker.io/library/debian:13",
            "index.docker.io/library/debian:13",
            "alpine",
            "alpine:3",
            "acme/orb:1",
            "docker.io/acme/orb",
        ] {
            assert!(a.check(ok).is_ok(), "{ok}");
        }
        for refused in [
            "debian",
            "debian:12",
            "ghcr.io/library/alpine",
            "acmecorp/orb",
        ] {
            let e = a.check(refused).unwrap_err();
            assert!(e.contains("ORBD_IMAGE_ALLOW") && e.contains(refused), "{e}");
        }
        assert_eq!(
            a.to_string(),
            "docker.io/library/debian:13,docker.io/library/alpine,docker.io/acme/*"
        );
        // Not a reference at all: refused when there is a list.
        assert!(a.check("Not A Ref").is_err());
        assert!(
            allow(&["localhost:5000/*"])
                .check("localhost:5000/x/y:1")
                .is_ok()
        );
    }

    #[test]
    fn bad_image_patterns_are_refused() {
        for bad in [
            "*",                         // everything: leave the setting empty
            "acme/*",                    // a prefix needs a registry
            "debian*",                   // the same
            "ghcr.io/acme/orb:*",        // no tags in prefixes
            "ghcr.io/acme/*/orb",        // `*` only at the end
            "ghcr.io/Acme/*",            // repository names are lowercase
            "ghcr.io/Acme/orb",          // not a reference (uppercase)
            "ghcr.io/acme/orb@sha256:1", // bad digest
        ] {
            assert!(
                ImageAllow::new(&[bad.to_string()]).is_err(),
                "{bad:?} parsed"
            );
        }
    }

    #[test]
    fn image_sandbox_settings() {
        assert_eq!(
            ImageSandbox::default(),
            ImageSandbox::On {
                uid_base: 1_400_000_000
            }
        );
        assert_eq!(
            AgentSection::new("/s".into()).image_sandbox,
            ImageSandbox::default()
        );
        assert_eq!(ImageSandbox::new(false, 0), Ok(ImageSandbox::Off));
        assert_eq!(ImageSandbox::Off.uid_base(), None);
        assert_eq!(
            ImageSandbox::new(true, 200_000).unwrap().uid_base(),
            Some(200_000)
        );
        // Never host root or ordinary users, and all 65536 ids below 2^32-1.
        for bad in [0, 1000, 65_535, u32::MAX - 65_535, u32::MAX] {
            assert!(ImageSandbox::new(true, bad).is_err(), "{bad}");
        }
        assert!(ImageSandbox::new(true, u32::MAX - 65_536).is_ok());
    }

    #[test]
    fn jail_mode_parsing() {
        for (s, m) in [
            ("", JailMode::On),
            ("on", JailMode::On),
            ("TRUE", JailMode::On),
            ("1", JailMode::On),
            ("off", JailMode::Off),
            ("false", JailMode::Off),
            (" 0 ", JailMode::Off),
        ] {
            assert_eq!(s.parse::<JailMode>().unwrap(), m, "{s:?}");
        }
        assert!("maybe".parse::<JailMode>().is_err());
        assert!("auto".parse::<JailMode>().is_err());
        assert_eq!(JailMode::default(), JailMode::On);
        assert_eq!(
            JailMode::Off.to_string().parse::<JailMode>(),
            Ok(JailMode::Off)
        );
    }

    #[test]
    fn guest_network_parsing() {
        let n: GuestNetwork = "172.31.0.0/16".parse().unwrap();
        assert_eq!(n.to_string(), "172.31.0.0/16");
        assert_eq!(n.gateway().to_string(), "172.31.0.2");
        assert_eq!(n.guest().to_string(), "172.31.0.15");
        assert_eq!(GuestNetwork::default().to_string(), "10.0.2.0/24");
        for bad in [
            "10.0.2.0",       // no prefix
            "10.0.2.1/24",    // host bits
            "10.0.2.0/28",    // .15 would be broadcast
            "169.254.8.0/24", // orbd's transit network
            "160.0.0.0/4",    // contains link-local
            "127.1.0.0/16",   // loopback
            "10.0.2.0/33",
        ] {
            assert!(bad.parse::<GuestNetwork>().is_err(), "{bad}");
        }
        assert!("10.0.2.0/27".parse::<GuestNetwork>().is_ok());
    }
}
