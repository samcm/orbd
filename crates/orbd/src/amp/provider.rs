//! Supervises one `amp orb provider` child per offering that has a provider token.
//! Each child gets only its own token, `ORBD_OFFERING_ID` and `ORBD_DISPATCH_SOCKET`,
//! and runs the generated `orbd-dispatch` wrapper for every job.
//!
//! Update checks: the Amp CLI self-updates by default. Children run with
//! `AMP_SKIP_UPDATE_CHECK=1` (documented override that disables all update checks) and
//! with `--settings-file` pointing at a generated file containing
//! `"amp.updates.mode": "disabled"`, so the CLI never updates itself mid-job. Instead
//! the coordinator runs `amp update` on a schedule (`amp_cli_update_interval_secs`);
//! after an update each provider child is restarted once it has no job running
//! (bounded wait). A job claimed by a restarted child is not lost: Amp re-hands it
//! to a provider about 27 s after its claimer dies.

use super::dispatch::{ENV_DISPATCH_SOCKET, ENV_OFFERING_ID};
use crate::config::coordinator::CoordinatorConfig;
use anyhow::{Context, anyhow};
use secrecy::{ExposeSecret, SecretString};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;

const BACKOFF_START: Duration = Duration::from_secs(1);
const BACKOFF_CAP: Duration = Duration::from_secs(60);
const STABLE_AFTER: Duration = Duration::from_secs(60);
const TERM_GRACE: Duration = Duration::from_secs(5);
/// After an update, how long to wait for a child to be between jobs before restarting it
/// anyway (an interrupted job is re-handed by Amp).
const UPDATE_RESTART_MAX_WAIT: Duration = Duration::from_secs(15 * 60);
const UPDATE_TIMEOUT: Duration = Duration::from_secs(300);

/// Parent environment variables a provider child may inherit. Everything else
/// (notably any other offering's token) is dropped.
const ENV_ALLOW: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "LANG",
    "LC_ALL",
    "TZ",
    "TMPDIR",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NO_PROXY",
    "https_proxy",
    "http_proxy",
    "no_proxy",
];

struct ChildSpec {
    offering: String,
    amp_cli: PathBuf,
    amp_url: String,
    token_file: PathBuf,
    wrapper: PathBuf,
    settings: PathBuf,
    socket: PathBuf,
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

fn write_atomic(path: &Path, content: &str, mode: u32) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(mode)
        .open(&tmp)
        .with_context(|| format!("create {}", tmp.display()))?;
    f.write_all(content.as_bytes())?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path).with_context(|| format!("install {}", path.display()))?;
    Ok(())
}

/// Starts supervising provider children for every offering with `provider_token_file`.
/// Call only after recovery and backend reconciliation, so no job arrives early.
/// The returned task ends after `shutdown` once all children have exited.
pub fn spawn_all(
    config: &CoordinatorConfig,
    dispatch_socket: &Path,
    shutdown: CancellationToken,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let offerings: Vec<_> = config
        .offerings
        .iter()
        .filter(|o| o.provider_token_file.is_some())
        .collect();
    if offerings.is_empty() {
        return Ok(tokio::spawn(async move { shutdown.cancelled().await }));
    }
    let bin_dir = config.coordinator.state_dir.join("bin");
    std::fs::create_dir_all(&bin_dir)?;
    let wrapper = bin_dir.join("orbd-dispatch");
    let exe = match &config.coordinator.dispatch_exe {
        Some(p) => p.clone(),
        None => std::env::current_exe().context("locate orbd executable")?,
    };
    let exe = exe
        .to_str()
        .ok_or_else(|| anyhow!("orbd executable path is not UTF-8"))?;
    write_atomic(
        &wrapper,
        &format!("#!/bin/sh\nexec {} dispatch \"$@\"\n", shell_quote(exe)),
        0o755,
    )?;
    let settings = config.coordinator.state_dir.join("amp-settings.json");
    write_atomic(
        &settings,
        "{\n  \"amp.updates.mode\": \"disabled\"\n}\n",
        0o600,
    )?;

    let mut specs = Vec::new();
    for o in offerings {
        let endpoint = config
            .amp_endpoints
            .iter()
            .find(|e| e.id == o.amp_endpoint)
            .ok_or_else(|| anyhow!("offering {}: unknown amp_endpoint", o.id))?;
        specs.push(ChildSpec {
            offering: o.id.as_str().to_owned(),
            amp_cli: config.coordinator.amp_cli.clone(),
            amp_url: endpoint.url.clone(),
            token_file: o.provider_token_file.clone().expect("filtered"),
            wrapper: wrapper.clone(),
            settings: settings.clone(),
            socket: dispatch_socket.to_owned(),
        });
    }
    let interval = config.coordinator.amp_cli_update_interval_secs;
    let (updated_tx, updated_rx) = tokio::sync::watch::channel(0u64);
    let probe = specs[0].clone_for_cli();
    Ok(tokio::spawn(async move {
        // Check once before the first child starts, so a stale CLI never polls.
        let mut tasks = tokio::task::JoinSet::new();
        if interval > 0 {
            update_cli(&probe).await;
        }
        record_version(&probe).await;
        for spec in specs {
            tasks.spawn(supervise(spec, updated_rx.clone(), shutdown.clone()));
        }
        if interval > 0 {
            let shutdown = shutdown.clone();
            tasks.spawn(async move {
                let mut generation = 0;
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(interval)) => {}
                        _ = shutdown.cancelled() => return,
                    }
                    if update_cli(&probe).await {
                        record_version(&probe).await;
                        generation += 1;
                        let _ = updated_tx.send(generation);
                    }
                }
            });
        }
        while tasks.join_next().await.is_some() {}
    }))
}

impl ChildSpec {
    /// The fields needed to run the CLI itself (no token, no offering).
    fn clone_for_cli(&self) -> ChildSpec {
        ChildSpec {
            offering: String::new(),
            amp_cli: self.amp_cli.clone(),
            amp_url: self.amp_url.clone(),
            token_file: PathBuf::new(),
            wrapper: PathBuf::new(),
            settings: self.settings.clone(),
            socket: PathBuf::new(),
        }
    }
}

/// Runs `amp update`; returns true when a new version was installed. Records the result
/// in `orbd_amp_cli_update_last_result{result}` and `..._last_run_timestamp_seconds`.
async fn update_cli(spec: &ChildSpec) -> bool {
    let mut cmd = base_command(spec);
    cmd.args(["update", "--porcelain"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = tokio::time::timeout(UPDATE_TIMEOUT, cmd.output()).await;
    let (result, updated) = match out {
        Ok(Ok(o)) if o.status.success() => {
            let stdout = String::from_utf8_lossy(&o.stdout);
            let last = stdout
                .lines()
                .rev()
                .find(|l| !l.trim().is_empty())
                .unwrap_or("");
            if let Some(v) = last.trim().strip_prefix("updated ") {
                tracing::info!(version = v, "amp cli updated");
                ("updated", true)
            } else {
                tracing::debug!("amp cli is up to date");
                ("up_to_date", false)
            }
        }
        Ok(Ok(o)) => {
            let err = String::from_utf8_lossy(&o.stderr);
            let tail: Vec<&str> = err.lines().rev().take(3).collect();
            tracing::warn!(status = %o.status, stderr = ?tail, "amp update failed");
            ("failed", false)
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "amp update could not run");
            ("failed", false)
        }
        Err(_) => {
            tracing::warn!("amp update timed out");
            ("failed", false)
        }
    };
    for r in ["updated", "up_to_date", "failed"] {
        metrics::gauge!("orbd_amp_cli_update_last_result", "result" => r).set(if r == result {
            1.0
        } else {
            0.0
        });
    }
    metrics::gauge!("orbd_amp_cli_update_last_run_timestamp_seconds")
        .set(crate::model::unix_ms_now() as f64 / 1000.0);
    metrics::counter!("orbd_amp_cli_updates_total", "result" => result).increment(1);
    updated
}

/// Publishes the installed CLI version as `orbd_amp_cli_info{version} 1`.
async fn record_version(spec: &ChildSpec) {
    static LAST: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
    let Some(version) = cli_version(spec).await else {
        return;
    };
    tracing::info!(amp_cli = %spec.amp_cli.display(), %version, "amp cli version");
    let mut last = LAST.lock().unwrap();
    if let Some(old) = last.replace(version.clone())
        && old != version
    {
        metrics::gauge!("orbd_amp_cli_info", "version" => old).set(0.0);
    }
    metrics::gauge!("orbd_amp_cli_info", "version" => version).set(1.0);
}

/// True while any process other than the provider itself is in its process group,
/// i.e. a dispatch job is running.
fn job_running(child: &Child) -> bool {
    let Some(pgid) = child.id() else { return false };
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return false;
    };
    dir.flatten().any(|e| {
        let Some(pid) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else {
            return false;
        };
        if pid == pgid {
            return false;
        }
        let Ok(stat) = std::fs::read_to_string(e.path().join("stat")) else {
            return false;
        };
        // Fields after the parenthesised comm: state ppid pgrp ...
        stat.rsplit_once(')')
            .and_then(|(_, rest)| rest.split_whitespace().nth(2)?.parse::<u32>().ok())
            == Some(pgid)
    })
}

fn base_command(spec: &ChildSpec) -> Command {
    let mut cmd = Command::new(&spec.amp_cli);
    cmd.env_clear();
    for name in ENV_ALLOW {
        if let Some(v) = std::env::var_os(name) {
            cmd.env(name, v);
        }
    }
    cmd.env("AMP_SKIP_UPDATE_CHECK", "1")
        .arg("--settings-file")
        .arg(&spec.settings)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    cmd
}

async fn cli_version(spec: &ChildSpec) -> Option<String> {
    let mut cmd = base_command(spec);
    cmd.arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    match tokio::time::timeout(Duration::from_secs(20), cmd.output()).await {
        Ok(Ok(out)) if out.status.success() => {
            let text = String::from_utf8_lossy(&out.stdout);
            text.split_whitespace().next().map(str::to_owned)
        }
        Ok(Ok(out)) => {
            tracing::warn!(status = %out.status, "amp --version failed");
            None
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "amp --version failed");
            None
        }
        Err(_) => {
            tracing::warn!("amp --version timed out");
            None
        }
    }
}

fn read_token(path: &Path) -> anyhow::Result<SecretString> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read provider token {}", path.display()))?;
    let t = raw.trim();
    if t.is_empty() {
        return Err(anyhow!("provider token file {} is empty", path.display()));
    }
    Ok(SecretString::from(t.to_owned()))
}

fn forward_lines(
    offering: String,
    stream: &'static str,
    reader: impl AsyncRead + Unpin + Send + 'static,
) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::info!(offering = %offering, stream, "amp provider: {line}");
        }
    });
}

fn signal_group(child: &Child, sig: nix::sys::signal::Signal) {
    if let Some(pid) = child.id() {
        let _ = nix::sys::signal::killpg(nix::unistd::Pid::from_raw(pid as i32), sig);
    }
}

enum Exit {
    Shutdown,
    /// Restarting onto an updated CLI.
    Update,
}

async fn stop_child(child: &mut Child) {
    use nix::sys::signal::Signal;
    signal_group(child, Signal::SIGTERM);
    if tokio::time::timeout(TERM_GRACE, child.wait())
        .await
        .is_err()
    {
        signal_group(child, Signal::SIGKILL);
        let _ = child.wait().await;
    }
    // Reap stragglers left in the group (e.g. a dispatch job).
    signal_group(child, Signal::SIGKILL);
}

async fn run_once(
    spec: &ChildSpec,
    updated: &mut tokio::sync::watch::Receiver<u64>,
    shutdown: &CancellationToken,
) -> anyhow::Result<Exit> {
    let token = read_token(&spec.token_file)?;
    let mut cmd = base_command(spec);
    cmd.env("AMP_ORB_PROVIDER_TOKEN", token.expose_secret())
        .env(ENV_OFFERING_ID, &spec.offering)
        .env(ENV_DISPATCH_SOCKET, &spec.socket)
        .arg("orb")
        .arg("provider")
        .arg("--amp-url")
        .arg(&spec.amp_url)
        .arg(&spec.wrapper)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawn {}", spec.amp_cli.display()))?;
    drop(token);
    tracing::info!(offering = %spec.offering, pid = ?child.id(), "amp provider started");
    metrics::gauge!("orbd_provider_up", "offering" => spec.offering.clone()).set(1.0);
    if let Some(o) = child.stdout.take() {
        forward_lines(spec.offering.clone(), "stdout", o);
    }
    if let Some(e) = child.stderr.take() {
        forward_lines(spec.offering.clone(), "stderr", e);
    }
    updated.mark_unchanged();
    let mut update_due: Option<tokio::time::Instant> = None;
    let mut idle_poll = tokio::time::interval(Duration::from_secs(1));
    let result = loop {
        tokio::select! {
            status = child.wait() => {
                break Err(anyhow!("amp provider exited: {}", status?));
            }
            _ = shutdown.cancelled() => {
                stop_child(&mut child).await;
                break Ok(Exit::Shutdown);
            }
            Ok(()) = updated.changed(), if update_due.is_none() => {
                tracing::info!(offering = %spec.offering, "amp cli updated; restarting provider between jobs");
                update_due = Some(tokio::time::Instant::now() + UPDATE_RESTART_MAX_WAIT);
            }
            _ = idle_poll.tick(), if update_due.is_some() => {
                let busy = job_running(&child);
                if !busy || tokio::time::Instant::now() >= update_due.unwrap() {
                    if busy {
                        tracing::warn!(offering = %spec.offering, "job still running; restarting anyway (Amp re-hands it)");
                    }
                    stop_child(&mut child).await;
                    break Ok(Exit::Update);
                }
            }
        }
    };
    metrics::gauge!("orbd_provider_up", "offering" => spec.offering.clone()).set(0.0);
    result
}

async fn supervise(
    spec: ChildSpec,
    mut updated: tokio::sync::watch::Receiver<u64>,
    shutdown: CancellationToken,
) {
    let mut backoff = BACKOFF_START;
    while !shutdown.is_cancelled() {
        let started = tokio::time::Instant::now();
        match run_once(&spec, &mut updated, &shutdown).await {
            Ok(Exit::Shutdown) => break,
            Ok(Exit::Update) => {
                tracing::info!(offering = %spec.offering, "restarting amp provider on the updated cli");
                backoff = BACKOFF_START;
                continue;
            }
            Err(e) => tracing::warn!(offering = %spec.offering, error = %e, "amp provider stopped"),
        }
        if started.elapsed() >= STABLE_AFTER {
            backoff = BACKOFF_START;
        }
        metrics::counter!("orbd_provider_restarts_total", "offering" => spec.offering.clone())
            .increment(1);
        tracing::info!(offering = %spec.offering, delay = ?backoff, "restarting amp provider");
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.cancelled() => break,
        }
        backoff = (backoff * 2).min(BACKOFF_CAP);
    }
    metrics::gauge!("orbd_provider_up", "offering" => spec.offering.clone()).set(0.0);
}
