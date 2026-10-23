//! VMM process supervision: a one-shot launcher (fork → new session → optional cgroup
//! move → optional network namespace → exec of a foreground VMM), durable launch
//! records, and validated adoption of VMMs that outlived an earlier supervisor process.
//!
//! A VMM is never daemonized and never a long-lived child that the supervisor must keep
//! reaping: the spawned `Child` handle is dropped at once (reaped in the background if
//! it exits while the supervisor lives) and every later wait goes through a pidfd, the
//! same for launched and adopted processes. A process is accepted as a VM's VMM only if
//! the host boot id, `/proc/<pid>` start time and the launch marker in its argv match
//! the launch record (and the VMM's own API then reports the same marker).
//!
//! Production placement: VMMs must not live in the supervisor service's cgroup, or a
//! service stop kills every VM. Under systemd use `KillMode=process` or pass a
//! delegated cgroup v2 directory as [`Spawn::cgroup`]; the launcher moves the VMM into
//! it before exec.

use crate::fsutil::{read_json, write_json_atomic};
use nix::libc;
use serde::{Deserialize, Serialize};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Binary of a VMM (recorded in manifests; restore requires the same binary).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VmmIdentity {
    pub path: PathBuf,
    pub version: String,
    pub sha256: String,
}

/// Host CPU as the guest sees it (restore requires the same model and flags).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CpuIdentity {
    pub model: String,
    pub flags_sha256: String,
}

impl CpuIdentity {
    pub fn host() -> Self {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let field = |name: &str| {
            cpuinfo
                .lines()
                .find(|l| l.starts_with(name))
                .and_then(|l| l.split_once(':'))
                .map(|(_, v)| v.trim().to_owned())
                .unwrap_or_default()
        };
        use sha2::{Digest, Sha256};
        Self {
            model: field("model name"),
            flags_sha256: hex::encode(Sha256::digest(field("flags").as_bytes())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelIdentity {
    pub path: PathBuf,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum LaunchKind {
    /// Boots the kernel; the guest runs from the first instruction.
    Boot,
    /// Loads this checkpoint generation with vCPUs stopped.
    Restore { generation: u64 },
}

/// Durable pre-launch intent (a `launch.json` per VM), written before exec and
/// completed with pid and start time after it. `config` is the guest-visible machine
/// definition.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LaunchRecord<C> {
    pub nonce: String,
    pub boot_id: String,
    pub kind: LaunchKind,
    pub config: C,
    pub pid: Option<i32>,
    pub start_time: Option<u64>,
}

impl<C> LaunchRecord<C> {
    pub fn new(kind: LaunchKind, config: C) -> Self {
        Self {
            nonce: uuid::Uuid::new_v4().simple().to_string(),
            boot_id: host_boot_id(),
            kind,
            config,
            pid: None,
            start_time: None,
        }
    }
}

pub fn read_record<C: serde::de::DeserializeOwned>(path: &Path) -> Option<LaunchRecord<C>> {
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

/// A validated, live VMM process, held through a pidfd (no pid-reuse races).
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

    /// Adopts the VMM described by `record` and recognised by `marker` (a unique
    /// string in its argv), if it is still the same process. Without a recorded pid
    /// (the supervisor died right after exec) the pid comes from `pidfile` or a `/proc` scan.
    pub fn adopt<C>(
        record: &LaunchRecord<C>,
        marker: &str,
        pidfile: Option<&Path>,
    ) -> Option<Self> {
        if record.boot_id != host_boot_id() {
            return None;
        }
        let pid = record
            .pid
            .or_else(|| {
                std::fs::read_to_string(pidfile?)
                    .ok()
                    .and_then(|s| s.trim().parse().ok())
            })
            .or_else(|| find_by_marker(marker))?;
        let p = Self::open(pid)?;
        // Validate after opening the pidfd, so the checks are about this process.
        if !has_marker(pid, marker) {
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

fn has_marker(pid: i32, marker: &str) -> bool {
    std::fs::read(format!("/proc/{pid}/cmdline")).is_ok_and(|cmdline| {
        cmdline
            .split(|b| *b == 0)
            .any(|a| String::from_utf8_lossy(a).contains(marker))
    })
}

fn find_by_marker(marker: &str) -> Option<i32> {
    std::fs::read_dir("/proc")
        .ok()?
        .flatten()
        .filter_map(|e| e.file_name().to_str()?.parse::<i32>().ok())
        .find(|pid| has_marker(*pid, marker))
}

pub struct Spawn<'a, C> {
    pub program: &'a Path,
    pub args: Vec<String>,
    /// Receives the VMM's stdout.
    pub stdout: &'a Path,
    pub stderr: &'a Path,
    pub record_path: &'a Path,
    pub record: LaunchRecord<C>,
    /// Cgroup v2 directory to move the VMM into before exec.
    pub cgroup: Option<PathBuf>,
    /// Network namespace (`/run/netns/<name>`) the VMM runs in.
    pub netns: Option<PathBuf>,
}

/// Records the launch intent, then forks and execs the VMM in its own session. Returns
/// once the process exists (not once it is ready).
pub fn spawn<C: Serialize>(s: Spawn<'_, C>) -> std::io::Result<(Process, LaunchRecord<C>)> {
    let mut record = s.record;
    write_json_atomic(s.record_path, &record)?;
    let open_log = |p: &Path| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
    };
    let stdout = open_log(s.stdout)?;
    let stderr = open_log(s.stderr)?;
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
    let netns = s.netns.as_deref().map(std::fs::File::open).transpose()?;
    let procs_fd = procs.as_ref().map(|f| f.as_raw_fd());
    let netns_fd = netns.as_ref().map(|f| f.as_raw_fd());
    let mut cmd = std::process::Command::new(s.program);
    cmd.args(&s.args)
        .stdin(std::process::Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    // SAFETY: only async-signal-safe calls (setsid, write, setns) between fork and exec.
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
            if let Some(fd) = netns_fd
                && libc::setns(fd, libc::CLONE_NEWNET) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = cmd.spawn()?;
    let pid = child.id() as i32;
    fail::fail_point!("vm::launch::after_exec", |_| std::process::abort());
    let proc = Process::open(pid).ok_or_else(|| std::io::Error::other("pidfd_open failed"))?;
    // Not waited here: a std Child never reaps on drop, so reap it on a detached thread
    // only while this supervisor lives; afterwards init (or the subreaper) does.
    std::thread::spawn(move || {
        let mut child = child;
        let _ = child.wait();
    });
    record.pid = Some(pid);
    record.start_time = start_time(pid);
    write_json_atomic(s.record_path, &record)?;
    Ok((proc, record))
}
