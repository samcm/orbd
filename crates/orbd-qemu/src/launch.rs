//! Launching QEMU: the guest-visible machine ([`LaunchConfig`], recorded with every
//! launch and in every checkpoint, and reused verbatim to load one), the host-side
//! plumbing around it, and the supervised start of the process.
//!
//! Every launch gets a unique name, `<prefix>.<nonce>` ([`name`]): it is QEMU's `-name`,
//! the marker [`Process::adopt`] looks for in `/proc/<pid>/cmdline`, and what
//! `query-name` reports, so a QMP socket is known to belong to this launch.

use crate::qmp::Qmp;
use orbd_vm::process::{self, CpuIdentity, KernelIdentity, LaunchKind, Process, VmmIdentity};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// QEMU's `-cpu` unless the caller names a model: the host CPU, which ties a checkpoint
/// to hosts with the same CPU. A named model (e.g. `EPYC-Rome-v4`) restores on any host
/// that can provide it.
pub const DEFAULT_CPU_MODEL: &str = "host";

/// Name of the virtio-serial port the guest agent serves; its host end is
/// [`HostFiles::guest_sock`].
pub const GUEST_PORT: &str = "org.orbd.control";

/// Everything that defines the guest-visible machine of one launch. A checkpoint's
/// manifest stores the config it was saved from; loading relaunches with it verbatim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchConfig {
    pub qemu: VmmIdentity,
    /// The CPU the guest was given (for [`DEFAULT_CPU_MODEL`], the host's).
    pub cpu: CpuIdentity,
    pub kernel: KernelIdentity,
    /// Machine, CPU, memory, kernel and device arguments with fixed ids. Host-side
    /// plumbing (chardev backends, block files, sockets, pidfile, name) is not included.
    pub machine_args: Vec<String>,
}

pub type LaunchRecord = process::LaunchRecord<LaunchConfig>;

/// The guest-visible machine: a q35 KVM VM with a direct-boot kernel, a serial console,
/// the guest agent's virtio-serial port, one virtio-blk disk (node `disk0`), the
/// caller's network devices and further arguments, and a free-page-reporting balloon.
pub struct Machine<'a> {
    /// Versioned machine type ([`q35_machine`](crate::probe::q35_machine)).
    pub machine_type: &'a str,
    /// QEMU `-cpu` model ([`DEFAULT_CPU_MODEL`] or a named model).
    pub cpu_model: &'a str,
    pub vcpus: u32,
    pub memory_mib: u64,
    pub kernel: &'a Path,
    pub kernel_cmdline: &'a str,
    /// Network device arguments (`-netdev`/`-device`), with fixed ids.
    pub net_args: Vec<String>,
    /// Further guest-visible arguments, e.g. `-device vmgenid,id=vmgenid0,guid=auto` or
    /// `-rtc clock=vm`; devices with fixed ids.
    pub extra_args: Vec<String>,
}

impl Machine<'_> {
    /// [`LaunchConfig::machine_args`] for this machine.
    pub fn args(&self) -> Vec<String> {
        let mut a: Vec<String> = [
            "-machine",
            &format!("{},accel=kvm", self.machine_type),
            "-cpu",
            self.cpu_model,
            "-smp",
            &self.vcpus.to_string(),
            "-m",
            &format!("{}M", self.memory_mib),
            "-kernel",
            &self.kernel.display().to_string(),
            "-append",
            self.kernel_cmdline,
            "-serial",
            "chardev:serial0",
            "-device",
            "virtio-serial-pci,id=guestbus",
            "-device",
            &format!(
                "virtserialport,id=guestctl,bus=guestbus.0,nr=1,chardev=guest,name={GUEST_PORT}"
            ),
            "-device",
            "virtio-blk-pci,id=disk0dev,drive=disk0",
        ]
        .map(String::from)
        .into();
        a.extend(self.net_args.iter().cloned());
        a.extend(self.extra_args.iter().cloned());
        a.extend(
            [
                "-device",
                "virtio-balloon-pci,id=balloon0,free-page-reporting=on",
            ]
            .map(String::from),
        );
        a
    }
}

/// Host-side files of one launch.
pub struct HostFiles<'a> {
    pub pidfile: &'a Path,
    /// QMP monitor socket (QEMU listens).
    pub qmp: &'a Path,
    /// Host end of [`GUEST_PORT`] (QEMU listens).
    pub guest_sock: &'a Path,
    /// The guest's serial console, appended to.
    pub console_log: &'a Path,
    /// qcow2 disk behind node `disk0`.
    pub disk: &'a Path,
}

/// The launch name for `record`: `<prefix>.<nonce>`.
pub fn name<C>(prefix: &str, record: &process::LaunchRecord<C>) -> String {
    format!("{prefix}.{}", record.nonce)
}

/// Host-side plumbing for one launch (not part of the guest-visible machine). A load
/// starts paused, waiting for `migrate-incoming`.
pub fn host_args(
    name: &str,
    files: &HostFiles<'_>,
    direct_io: bool,
    kind: LaunchKind,
) -> Vec<String> {
    let esc = |p: &Path| p.display().to_string().replace(',', ",,");
    let file = serde_json::json!({
        "driver": "file",
        "filename": files.disk,
        "aio": if direct_io { "native" } else { "threads" },
        "cache": { "direct": direct_io },
    });
    let blockdev = serde_json::json!({
        "driver": "qcow2",
        "node-name": "disk0",
        "discard": "unmap",
        "cache": { "direct": direct_io },
        "file": file,
    });
    let mut a = vec![
        "-name".into(),
        format!("guest={name},debug-threads=on"),
        "-nodefaults".into(),
        "-no-user-config".into(),
        "-nographic".into(),
        "-sandbox".into(),
        "on,obsolete=deny,elevateprivileges=deny,spawn=deny,resourcecontrol=deny".into(),
        "-pidfile".into(),
        files.pidfile.display().to_string(),
        "-chardev".into(),
        format!("socket,id=qmp,path={},server=on,wait=off", esc(files.qmp)),
        "-mon".into(),
        "chardev=qmp,mode=control".into(),
        "-chardev".into(),
        format!(
            "socket,id=guest,path={},server=on,wait=off",
            esc(files.guest_sock)
        ),
        "-chardev".into(),
        format!("file,id=serial0,path={},append=on", esc(files.console_log)),
        "-blockdev".into(),
        blockdev.to_string(),
    ];
    if let LaunchKind::Restore { .. } = kind {
        a.extend(["-S", "-incoming", "defer"].map(String::from));
    }
    a
}

pub struct Launch<'a> {
    /// Kind and machine of this launch ([`LaunchRecord::new`]).
    pub record: LaunchRecord,
    /// Launch names are `<name_prefix>.<nonce>`.
    pub name_prefix: &'a str,
    pub files: HostFiles<'a>,
    /// Disk I/O bypasses the page cache ([`direct_io`](crate::probe::direct_io)).
    pub direct_io: bool,
    /// Receives QEMU's stdout and stderr.
    pub log: &'a Path,
    /// The durable launch record (`launch.json`).
    pub record_path: &'a Path,
    /// Cgroup v2 directory to move QEMU into before exec.
    pub cgroup: Option<PathBuf>,
    /// Network namespace (`/run/netns/<name>`) QEMU runs in.
    pub netns: Option<PathBuf>,
}

/// Records the launch and starts QEMU in its own session. Returns once the process
/// exists; [`wait_ready`] waits for its monitor.
pub fn spawn(l: Launch<'_>) -> std::io::Result<(Process, LaunchRecord)> {
    let mut args = host_args(
        &name(l.name_prefix, &l.record),
        &l.files,
        l.direct_io,
        l.record.kind,
    );
    args.extend(l.record.config.machine_args.iter().cloned());
    process::spawn(process::Spawn {
        program: &l.record.config.qemu.path.clone(),
        args,
        stdout: l.log,
        stderr: l.log,
        record_path: l.record_path,
        record: l.record,
        cgroup: l.cgroup,
        netns: l.netns,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum ReadyError {
    #[error("QEMU exited during start")]
    Exited,
    #[error("QEMU did not open its QMP socket in time")]
    Timeout,
}

/// Waits until QEMU's monitor at `qmp` answers as the launch `name`, polling every
/// `poll`. Does not kill the process on failure.
pub async fn wait_ready(
    proc: &Process,
    qmp: &Path,
    name: &str,
    timeout: Duration,
    poll: Duration,
) -> Result<(), ReadyError> {
    let ready_by = Instant::now() + timeout;
    loop {
        if !proc.is_alive() {
            return Err(ReadyError::Exited);
        }
        if let Ok(q) = Qmp::connect(qmp, Duration::from_secs(1)).await
            && q.name().await.ok().flatten().as_deref() == Some(name)
        {
            return Ok(());
        }
        if Instant::now() >= ready_by {
            return Err(ReadyError::Timeout);
        }
        tokio::time::sleep(poll).await;
    }
}
