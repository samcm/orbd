//! Supervises one `amp orb provider` child per offering that has a provider token.
//! Each child gets only its own token, `ORBD_OFFERING_ID` and `ORBD_DISPATCH_SOCKET`,
//! and runs the generated `orbd-dispatch` wrapper for every job.
//!
//! Update checks: the Amp CLI self-updates by default. Children run with
//! `AMP_SKIP_UPDATE_CHECK=1` (documented override that disables all update checks) and
//! with `--settings-file` pointing at a generated file containing
//! `"amp.updates.mode": "disabled"`, so the installed binary is the one that runs.

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
    let exe = std::env::current_exe().context("locate orbd executable")?;
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
    Ok(tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for spec in specs {
            tasks.spawn(supervise(spec, shutdown.clone()));
        }
        while tasks.join_next().await.is_some() {}
    }))
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

async fn log_version(spec: &ChildSpec) {
    let mut cmd = base_command(spec);
    cmd.arg("--version")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    match tokio::time::timeout(Duration::from_secs(20), cmd.output()).await {
        Ok(Ok(out)) => tracing::info!(
            offering = %spec.offering,
            amp_cli = %spec.amp_cli.display(),
            version = %String::from_utf8_lossy(&out.stdout).trim(),
            "amp cli version"
        ),
        Ok(Err(e)) => tracing::warn!(offering = %spec.offering, error = %e, "amp --version failed"),
        Err(_) => tracing::warn!(offering = %spec.offering, "amp --version timed out"),
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

async fn run_once(spec: &ChildSpec, shutdown: &CancellationToken) -> anyhow::Result<()> {
    use nix::sys::signal::Signal;
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
    let result = tokio::select! {
        status = child.wait() => {
            let status = status?;
            Err(anyhow!("amp provider exited: {status}"))
        }
        _ = shutdown.cancelled() => {
            signal_group(&child, Signal::SIGTERM);
            if tokio::time::timeout(TERM_GRACE, child.wait()).await.is_err() {
                signal_group(&child, Signal::SIGKILL);
                let _ = child.wait().await;
            }
            // Reap stragglers left in the group (e.g. a dispatch job).
            signal_group(&child, Signal::SIGKILL);
            Ok(())
        }
    };
    metrics::gauge!("orbd_provider_up", "offering" => spec.offering.clone()).set(0.0);
    result
}

async fn supervise(spec: ChildSpec, shutdown: CancellationToken) {
    log_version(&spec).await;
    let mut backoff = BACKOFF_START;
    while !shutdown.is_cancelled() {
        let started = tokio::time::Instant::now();
        match run_once(&spec, &shutdown).await {
            Ok(()) => break,
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
