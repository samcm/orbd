//! QEMU process supervision: a one-shot launcher (fork → new session → optional cgroup
//! move → exec of a foreground QEMU), durable launch records, and validated adoption of
//! QEMUs that outlived an earlier agent process.
//!
//! QEMU is never `-daemonize`d and never a long-lived child that the agent must keep
//! reaping: the spawned `Child` handle is dropped at once (tokio reaps it in the
//! background if it exits while this agent lives) and every later wait goes through a
//! pidfd, the same for launched and adopted processes. A process is accepted as an
//! orb's QEMU only if the host boot id, `/proc/<pid>` start time and the launch nonce in
//! its argv match the launch record (and, for QMP users, `query-name`).
//!
//! Production placement: QEMUs must not live in the agent service's cgroup, or a
//! service stop kills every orb. Under systemd use `KillMode=process` or set
//! `runtime.qemu.cgroup_parent` to a delegated cgroup v2 directory; the launcher moves
//! each QEMU into `<cgroup_parent>/orbd-<orb>` before exec.

use super::fsutil::{read_json, write_json_atomic};
use nix::libc;
use serde::{Deserialize, Serialize};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Identity of the QEMU binary (recorded in manifests; restore requires the same version).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QemuIdentity {
    pub path: PathBuf,
    pub version: String,
    pub sha256: String,
}

/// Host CPU as `-cpu host` sees it (restore requires the same model and flags).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuIdentity {
    pub model: String,
    pub flags_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelIdentity {
    pub path: PathBuf,
    pub sha256: String,
}

/// Everything that defines the guest-visible machine of one launch. A checkpoint's
/// manifest stores the config it was saved from; restore relaunches with it verbatim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchConfig {
    pub qemu: QemuIdentity,
    pub cpu: CpuIdentity,
    pub kernel: KernelIdentity,
    /// Machine, CPU, memory, kernel and device arguments with fixed ids. Host-side
    /// plumbing (chardev backends, block files, sockets, pidfile, name) is not included.
    pub machine_args: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum LaunchKind {
    /// Boots the kernel; the guest runs from the first instruction.
    Boot,
    /// `-S -incoming defer`, loading this checkpoint generation.
    Restore { generation: u64 },
}

/// Durable pre-launch intent (`<run>/qemu/<orb>/launch.json`), written before exec and
/// completed with pid and start time after it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LaunchRecord {
    pub nonce: String,
    pub boot_id: String,
    pub kind: LaunchKind,
    pub config: LaunchConfig,
    pub pid: Option<i32>,
    pub start_time: Option<u64>,
}

impl LaunchRecord {
    /// The `-name` value carrying the nonce; matched in `/proc/<pid>/cmdline` and QMP.
    pub fn name(&self, orb: &str) -> String {
        format!("orbd-{orb}.{}", self.nonce)
    }
}

pub fn read_record(path: &Path) -> Option<LaunchRecord> {
    read_json(path).ok().flatten()
}

pub fn host_boot_id() -> String {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map(|s| s.trim().to_owned())
        .unwrap_or_default()
}

/// Start time (clock ticks since boot) from `/proc/<pid>/stat`.
pub fn start_time(pid: i32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = &stat[stat.rfind(')')? + 1..];
    // Fields after the comm: state is field 3; starttime is field 22.
    rest.split_whitespace().nth(19)?.parse().ok()
}

/// A validated, live QEMU process, held through a pidfd (no pid-reuse races).
pub struct Process {
    pub pid: i32,
    pidfd: OwnedFd,
}

impl Process {
    fn open(pid: i32) -> Option<Self> {
        // SAFETY: plain syscall; a valid fd is owned below.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            return None;
        }
        Some(Self {
            pid,
            // SAFETY: the kernel returned a new fd that nothing else owns.
            pidfd: unsafe { OwnedFd::from_raw_fd(fd as i32) },
        })
    }

    /// Adopts the QEMU described by `record`, if it is still the same process.
    pub fn adopt(record: &LaunchRecord, orb: &str, pidfile: &Path) -> Option<Self> {
        if record.boot_id != host_boot_id() {
            return None;
        }
        let pid = record.pid.or_else(|| {
            std::fs::read_to_string(pidfile)
                .ok()
                .and_then(|s| s.trim().parse().ok())
        })?;
        let p = Self::open(pid)?;
        // Validate after opening the pidfd, so the checks are about this process.
        let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
        let marker = format!("guest={},", record.name(orb));
        let has_marker = cmdline
            .split(|b| *b == 0)
            .any(|a| a.starts_with(marker.as_bytes()));
        if !has_marker {
            return None;
        }
        if let Some(st) = record.start_time
            && start_time(pid) != Some(st)
        {
            return None;
        }
        p.is_alive().then_some(p)
    }

    pub fn is_alive(&self) -> bool {
        let mut pfd = libc::pollfd {
            fd: self.pidfd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd, zero timeout.
        let r = unsafe { libc::poll(&mut pfd, 1, 0) };
        r == 0
    }

    /// Waits until the process exits or `deadline` passes; true if it exited.
    pub async fn wait_exit(&self, deadline: Instant) -> bool {
        let Ok(fd) = self.pidfd.try_clone() else {
            return !self.is_alive();
        };
        // SAFETY: the AsyncFd owns `fd`, which stays open (and the same pidfd) until it drops.
        let registered = unsafe {
            tokio::io::unix::AsyncFd::register_with_interest(fd, tokio::io::Interest::READABLE)
        };
        let Ok(afd) = registered else {
            return !self.is_alive();
        };
        tokio::select! {
            _ = afd.readable() => true,
            _ = tokio::time::sleep_until(deadline.into()) => !self.is_alive(),
        }
    }

    pub fn kill(&self) {
        // SAFETY: signals exactly the process behind the pidfd.
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.pidfd.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            );
        }
    }

    /// SIGKILL and wait (bounded) for the exit.
    pub async fn kill_and_wait(&self, timeout: Duration) -> bool {
        self.kill();
        self.wait_exit(Instant::now() + timeout).await
    }
}

pub struct Spawn<'a> {
    pub qemu: &'a Path,
    pub args: Vec<String>,
    pub log_file: &'a Path,
    pub record_path: &'a Path,
    pub record: LaunchRecord,
    /// Cgroup v2 directory to move QEMU into before exec.
    pub cgroup: Option<PathBuf>,
}

/// Records the launch intent, then forks and execs QEMU in its own session. Returns once
/// the process exists (not once QEMU is ready).
pub fn spawn(s: Spawn<'_>) -> std::io::Result<(Process, LaunchRecord)> {
    let mut record = s.record;
    write_json_atomic(s.record_path, &record)?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(s.log_file)?;
    let procs = match &s.cgroup {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            Some(
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(dir.join("cgroup.procs"))?,
            )
        }
        None => None,
    };
    let procs_fd = procs.as_ref().map(|f| f.as_raw_fd());
    let mut cmd = std::process::Command::new(s.qemu);
    cmd.args(&s.args)
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    // SAFETY: only async-signal-safe calls (setsid, write) between fork and exec.
    unsafe {
        cmd.pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if let Some(fd) = procs_fd
                && libc::write(fd, b"0\n".as_ptr().cast(), 2) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = cmd.spawn()?;
    let pid = child.id() as i32;
    let proc = Process::open(pid).ok_or_else(|| std::io::Error::other("pidfd_open failed"))?;
    // Not waited here: a std Child never reaps on drop, so reap it on a detached thread
    // only while this agent lives; afterwards init (or the subreaper) does.
    std::thread::spawn(move || {
        let mut child = child;
        let _ = child.wait();
    });
    record.pid = Some(pid);
    record.start_time = start_time(pid);
    write_json_atomic(s.record_path, &record)?;
    Ok((proc, record))
}
