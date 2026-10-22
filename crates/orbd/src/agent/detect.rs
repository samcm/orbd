//! Runtime and capacity detection, for agents started without runtime configuration.
//!
//! Detection probes function, not file existence: usable KVM and QEMU select QEMU,
//! otherwise a Docker daemon that answers selects Docker. The first choice is recorded
//! in `<state_dir>/runtime`; later starts use it or fail, so an agent never abandons its
//! orbs because detection changed.

use crate::config::agent::{AMP_ORB_IMAGE, AgentConfig, DockerRuntimeConfig};
use crate::model::{GIB, MIB, Resources, RuntimeKind};
use anyhow::{Context, Result, bail};
use std::time::Duration;

const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

fn qemu_usable() -> Result<(), String> {
    crate::doctor::kvm_usable()?;
    let out = std::process::Command::new("qemu-system-x86_64")
        .arg("--version")
        .output()
        .map_err(|e| format!("qemu-system-x86_64: {e}"))?;
    if !out.status.success() {
        return Err(format!("qemu-system-x86_64 --version: {}", out.status));
    }
    Ok(())
}

async fn docker(config: &DockerRuntimeConfig) -> Result<bollard::Docker, String> {
    let docker = match &config.docker_host {
        Some(h) => bollard::Docker::connect_with_host(h),
        None => bollard::Docker::connect_with_local_defaults(),
    }
    .map_err(|e| e.to_string())?;
    match tokio::time::timeout(PROBE_TIMEOUT, docker.ping()).await {
        Ok(Ok(_)) => Ok(docker),
        Ok(Err(e)) => Err(format!("docker: {e}")),
        Err(_) => Err("docker: no answer within 5s".into()),
    }
}

async fn usable(kind: RuntimeKind) -> Result<(), String> {
    match kind {
        RuntimeKind::Qemu => qemu_usable(),
        RuntimeKind::Docker => docker(&DockerRuntimeConfig::default()).await.map(|_| ()),
        other => Err(format!("{} is never detected", other.as_str())),
    }
}

/// Fills in a runtime when the config names none, and the agent's DNS servers.
pub(crate) async fn runtime(config: &mut AgentConfig) -> Result<()> {
    let r = &mut config.runtime;
    if r.fake.is_none() && r.docker.is_none() {
        let record = config.agent.state_dir.join("runtime");
        let kind = match std::fs::read_to_string(&record) {
            Ok(s) => {
                let kind = parse_kind(s.trim())
                    .with_context(|| format!("unrecognised runtime in {}", record.display()))?;
                if let Err(why) = usable(kind).await {
                    bail!(
                        "this agent runs {} orbs but {} is not usable: {why}",
                        kind.as_str(),
                        kind.as_str()
                    );
                }
                kind
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let kind = detect().await?;
                crate::pki::write_private(&record, kind.as_str().as_bytes())?;
                kind
            }
            Err(e) => return Err(e).context(format!("reading {}", record.display())),
        };
        tracing::info!(runtime = kind.as_str(), "using detected runtime");
        match kind {
            RuntimeKind::Docker => {
                r.docker = Some(DockerRuntimeConfig {
                    prewarm_images: vec![AMP_ORB_IMAGE.into()],
                    ..Default::default()
                })
            }
            other => bail!("the {} runtime is not in this build", other.as_str()),
        }
    }
    if let Some(d) = &mut r.docker {
        d.dns_servers = config.agent.dns_servers.clone();
    }
    Ok(())
}

fn parse_kind(s: &str) -> Option<RuntimeKind> {
    [RuntimeKind::Qemu, RuntimeKind::Docker]
        .into_iter()
        .find(|k| k.as_str() == s)
}

async fn detect() -> Result<RuntimeKind> {
    let qemu = qemu_usable();
    // The QEMU runtime is not in this build yet; when it lands, `Ok` here returns Qemu.
    if qemu.is_ok() {
        tracing::warn!("KVM and QEMU are usable but this build has no QEMU runtime");
    }
    match usable(RuntimeKind::Docker).await {
        Ok(()) => Ok(RuntimeKind::Docker),
        Err(docker) => bail!(
            "no usable runtime: QEMU needs /dev/kvm and qemu-system-x86_64 ({}); Docker needs \
             its socket, e.g. -v /var/run/docker.sock:/var/run/docker.sock ({docker})",
            qemu.err().unwrap_or_else(|| "usable".into())
        ),
    }
}

/// The capacity ceiling: detected resources minus a reserve for the host, with
/// per-field overrides. Docker orbs are siblings on the Docker host, so its CPUs and
/// memory count; otherwise this process's effective (cgroup-limited) resources do.
pub(crate) async fn capacity(config: &AgentConfig) -> Result<Resources> {
    let o = config.agent.capacity;
    let docker_only = config.runtime.fake.is_none() && config.runtime.docker.is_some();
    let (cpus, memory) = match (&config.runtime.docker, o.vcpus.zip(o.memory_mib)) {
        (_, Some(_)) => (0, 0), // both overridden
        (Some(d), None) if docker_only => {
            let info = docker(d)
                .await
                .map_err(anyhow::Error::msg)?
                .info()
                .await
                .context("docker info")?;
            (
                info.ncpu.unwrap_or(0).max(0) as u64,
                info.mem_total.unwrap_or(0).max(0) as u64,
            )
        }
        _ => (
            std::thread::available_parallelism().map_or(1, |n| n.get() as u64),
            local_memory()?,
        ),
    };
    let disk = nix::sys::statvfs::statvfs(&config.agent.state_dir)
        .map(|s| s.blocks_available() * s.fragment_size())
        .with_context(|| format!("statvfs {}", config.agent.state_dir.display()))?;
    let c = Resources {
        vcpus: o
            .vcpus
            .unwrap_or_else(|| cpus.saturating_sub(1).min(u32::MAX as u64) as u32),
        memory_bytes: o
            .memory_mib
            .map(|m| m.saturating_mul(MIB))
            .unwrap_or_else(|| memory.saturating_sub((2 * GIB).max(memory / 10))),
        disk_bytes: o
            .disk_gib
            .map(|g| g.saturating_mul(GIB))
            .unwrap_or_else(|| disk.saturating_sub((10 * GIB).max(disk / 10))),
    };
    if c.vcpus == 0 || c.memory_bytes == 0 || c.disk_bytes == 0 {
        bail!(
            "no capacity for orbs: {} vCPUs, {} MiB memory, {} GiB disk after reserving 1 CPU, \
             max(2 GiB, 10%) memory and max(10 GiB, 10%) of {}; set ORBD_CAPACITY_VCPUS, \
             ORBD_CAPACITY_MEMORY_MIB or ORBD_CAPACITY_DISK_GIB",
            c.vcpus,
            c.memory_bytes / MIB,
            c.disk_bytes / GIB,
            config.agent.state_dir.display()
        );
    }
    tracing::info!(
        vcpus = c.vcpus,
        memory_mib = c.memory_bytes / MIB,
        disk_gib = c.disk_bytes / GIB,
        "capacity ceiling"
    );
    Ok(c)
}

/// Physical memory, capped by this process's cgroup limit (v2 or v1).
fn local_memory() -> Result<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").context("reading /proc/meminfo")?;
    let total = meminfo
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .context("no MemTotal in /proc/meminfo")?
        * 1024;
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    let v2 = cgroup
        .lines()
        .find_map(|l| l.strip_prefix("0::"))
        .map(|p| format!("/sys/fs/cgroup{}/memory.max", p.trim_end_matches('/')));
    let limit = [
        v2.as_deref().unwrap_or("/sys/fs/cgroup/memory.max"),
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .filter_map(|p| std::fs::read_to_string(p).ok())
    .find_map(|v| v.trim().parse::<u64>().ok());
    Ok(limit.map_or(total, |l| l.min(total)))
}
