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
//!
//! Probation: a new CLI is unproven until a spawn it ran succeeds; then a copy is kept
//! as the known-good CLI (`<state>/bin/amp-good`). If jobs on two different orbs show it
//! breaks orbd's dispatch contract first (see `dispatch::CLI_CHECK`), the providers go
//! back to the known-good copy and that version is never run again; the next release
//! gets its own probation. State lives in `<state>/amp-cli.json`.

use super::dispatch::{CLI_CHECK, ENV_DISPATCH_SOCKET, ENV_OFFERING_ID};
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
/// Bad jobs, on this many different orbs and before any good spawn, fail a CLI's
/// probation. One orb that keeps failing is more likely the orb than the CLI.
const PROBATION_BAD_ORBS: usize = 2;
/// Versions remembered as bad (oldest dropped first).
const MAX_BLOCKED: usize = 20;

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
    /// The CLI `amp update` maintains; children run [`Active::path`].
    amp_cli: PathBuf,
    amp_url: String,
    token_file: PathBuf,
    wrapper: PathBuf,
    settings: PathBuf,
    socket: PathBuf,
    verdicts: tokio::sync::mpsc::UnboundedSender<Verdict>,
}

/// The CLI provider children run. A new generation restarts them between jobs.
#[derive(Clone, Debug)]
struct Active {
    generation: u64,
    path: PathBuf,
}

/// A `cli-check` line from a dispatch job, tagged with the CLI generation that ran it.
#[derive(Debug)]
struct Verdict {
    generation: u64,
    good: bool,
    orb: String,
}

/// Persisted probation state.
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct CliState {
    /// Version of `<state>/bin/amp-good`.
    good: Option<String>,
    /// Versions that failed probation, oldest first.
    #[serde(default)]
    blocked: Vec<String>,
}

fn parse_cli_check(line: &str) -> Option<(bool, String)> {
    let rest = line
        .strip_prefix("orbd dispatch: ")?
        .strip_prefix(CLI_CHECK)?;
    let mut it = rest.split_whitespace();
    let good = match it.next()? {
        "good" => true,
        "bad" => false,
        _ => return None,
    };
    Some((good, it.next().unwrap_or("").to_owned()))
}

/// What the probation controller does with one verdict.
#[derive(Debug, PartialEq)]
enum Decision {
    None,
    Promote,
    RollBack,
}

struct Probation {
    generation: u64,
    version: String,
    bad_orbs: std::collections::BTreeSet<String>,
}

impl Probation {
    fn judge(&mut self, v: &Verdict) -> Decision {
        if v.generation != self.generation {
            return Decision::None;
        }
        if v.good {
            return Decision::Promote;
        }
        self.bad_orbs.insert(v.orb.clone());
        if self.bad_orbs.len() >= PROBATION_BAD_ORBS {
            Decision::RollBack
        } else {
            Decision::None
        }
    }
}

/// `p` itself when it names a path, else its first match on PATH.
fn resolve(p: &Path) -> Option<PathBuf> {
    if p.components().count() > 1 {
        return Some(p.to_owned());
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|d| d.join(p))
        .find(|c| c.is_file())
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
    let bin_dir = config.state_dir.join("bin");
    std::fs::create_dir_all(&bin_dir)?;
    let wrapper = bin_dir.join("orbd-dispatch");
    let exe = match &config.dispatch_exe {
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
    let settings = config.state_dir.join("amp-settings.json");
    write_atomic(
        &settings,
        "{\n  \"amp.updates.mode\": \"disabled\"\n}\n",
        0o600,
    )?;

    let (verdict_tx, verdict_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut specs = Vec::new();
    for o in offerings {
        specs.push(ChildSpec {
            offering: o.id.as_str().to_owned(),
            amp_cli: config.amp_cli.clone(),
            amp_url: config.amp_url.clone(),
            token_file: o.provider_token_file.clone().expect("filtered"),
            wrapper: wrapper.clone(),
            settings: settings.clone(),
            socket: dispatch_socket.to_owned(),
            verdicts: verdict_tx.clone(),
        });
    }
    drop(verdict_tx);
    let interval = config.amp_cli_update_interval_secs;
    let probe = specs[0].clone_for_cli();
    let ctl = CliControl {
        good_bin: bin_dir.join("amp-good"),
        state_file: config.state_dir.join("amp-cli.json"),
    };
    Ok(tokio::spawn(async move {
        // Check once before the first child starts, so a stale CLI never polls.
        if interval > 0 {
            update_cli(&probe).await;
        }
        record_version(&probe).await;
        let mut state = ctl.load();
        let mut probation = None;
        let first = ctl.choose(&probe, &mut state, 0, &mut probation).await;
        let (active_tx, active_rx) = tokio::sync::watch::channel(first);
        let mut tasks = tokio::task::JoinSet::new();
        for spec in specs {
            tasks.spawn(supervise(spec, active_rx.clone(), shutdown.clone()));
        }
        let shutdown2 = shutdown.clone();
        tasks.spawn(async move {
            let mut verdicts = verdict_rx;
            let mut tick = tokio::time::interval(Duration::from_secs(interval.max(1)));
            tick.reset();
            loop {
                tokio::select! {
                    _ = tick.tick(), if interval > 0 => {
                        if update_cli(&probe).await {
                            record_version(&probe).await;
                            let generation = active_tx.borrow().generation + 1;
                            let next = ctl.choose(&probe, &mut state, generation, &mut probation).await;
                            let same = next.path == active_tx.borrow().path;
                            if !same || probation.is_some() {
                                let _ = active_tx.send(next);
                            }
                        }
                    }
                    Some(v) = verdicts.recv() => {
                        let Some(p) = probation.as_mut() else { continue };
                        match p.judge(&v) {
                            Decision::None => {}
                            Decision::Promote => {
                                let p = probation.take().expect("on probation");
                                ctl.promote(&probe, &mut state, &p.version).await;
                            }
                            Decision::RollBack => {
                                let p = probation.take().expect("on probation");
                                let generation = active_tx.borrow().generation + 1;
                                if let Some(next) = ctl.roll_back(&mut state, &p, generation) {
                                    let _ = active_tx.send(next);
                                }
                            }
                        }
                    }
                    _ = shutdown2.cancelled() => return,
                }
            }
        });
        while tasks.join_next().await.is_some() {}
    }))
}

struct CliControl {
    good_bin: PathBuf,
    state_file: PathBuf,
}

impl CliControl {
    fn load(&self) -> CliState {
        std::fs::read(&self.state_file)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default()
    }

    fn save(&self, state: &CliState) {
        let json = serde_json::to_string_pretty(state).expect("serializable");
        if let Err(e) = write_atomic(&self.state_file, &json, 0o600) {
            tracing::warn!(error = %e, "saving amp cli state");
        }
    }

    fn good(&self, state: &CliState, generation: u64) -> Option<Active> {
        (state.good.is_some() && self.good_bin.is_file()).then(|| Active {
            generation,
            path: self.good_bin.clone(),
        })
    }

    /// The CLI to run after `amp update` left `probe.amp_cli` at some version: that one
    /// on probation unless it is blocked or already the known-good version.
    async fn choose(
        &self,
        probe: &ChildSpec,
        state: &mut CliState,
        generation: u64,
        probation: &mut Option<Probation>,
    ) -> Active {
        let upstream = Active {
            generation,
            path: probe.amp_cli.clone(),
        };
        let Some(version) = cli_version(probe).await else {
            return self.good(state, generation).unwrap_or(upstream);
        };
        if state.blocked.contains(&version) {
            *probation = None;
            if let Some(good) = self.good(state, generation) {
                tracing::warn!(%version, good = ?state.good, "amp cli failed probation before; running the known-good cli");
                return good;
            }
            tracing::error!(%version, "amp cli failed probation before, but there is no known-good cli; running it");
            return upstream;
        }
        if state.good.as_deref() == Some(version.as_str()) && self.good_bin.is_file() {
            *probation = None;
        } else {
            tracing::info!(%version, "amp cli on probation until a spawn succeeds");
            *probation = Some(Probation {
                generation,
                version,
                bad_orbs: Default::default(),
            });
        }
        upstream
    }

    async fn promote(&self, probe: &ChildSpec, state: &mut CliState, version: &str) {
        let Some(src) = resolve(&probe.amp_cli) else {
            tracing::warn!(amp_cli = %probe.amp_cli.display(), "cannot locate the amp cli to keep it");
            return;
        };
        let tmp = self.good_bin.with_extension("tmp");
        let copied = async {
            tokio::fs::copy(&src, &tmp).await?;
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755)).await?;
            tokio::fs::rename(&tmp, &self.good_bin).await
        };
        if let Err(e) = copied.await {
            tracing::warn!(error = %e, "keeping the proven amp cli");
            return;
        }
        state.good = Some(version.to_owned());
        self.save(state);
        tracing::info!(
            version,
            "amp cli passed probation; kept as the known-good cli"
        );
        metrics::counter!("orbd_amp_cli_probation_total", "result" => "promoted").increment(1);
    }

    fn roll_back(&self, state: &mut CliState, p: &Probation, generation: u64) -> Option<Active> {
        state.blocked.retain(|v| v != &p.version);
        state.blocked.push(p.version.clone());
        if state.blocked.len() > MAX_BLOCKED {
            state.blocked.remove(0);
        }
        self.save(state);
        metrics::counter!("orbd_amp_cli_probation_total", "result" => "rolled_back").increment(1);
        match self.good(state, generation) {
            Some(good) => {
                tracing::warn!(version = %p.version, bad_orbs = ?p.bad_orbs, good = ?state.good, "amp cli broke dispatch on probation; rolling back to the known-good cli");
                Some(good)
            }
            None => {
                tracing::error!(version = %p.version, bad_orbs = ?p.bad_orbs, "amp cli broke dispatch on probation, and there is no known-good cli to roll back to");
                None
            }
        }
    }
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
            verdicts: self.verdicts.clone(),
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
    base_command_at(&spec.amp_cli, spec)
}

fn base_command_at(cli: &Path, spec: &ChildSpec) -> Command {
    let mut cmd = Command::new(cli);
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
    generation: u64,
    verdicts: tokio::sync::mpsc::UnboundedSender<Verdict>,
) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some((good, orb)) = parse_cli_check(&line) {
                let _ = verdicts.send(Verdict {
                    generation,
                    good,
                    orb,
                });
                continue;
            }
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
    updated: &mut tokio::sync::watch::Receiver<Active>,
    shutdown: &CancellationToken,
) -> anyhow::Result<Exit> {
    let active = updated.borrow_and_update().clone();
    let token = read_token(&spec.token_file)?;
    let mut cmd = base_command_at(&active.path, spec);
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
        .with_context(|| format!("spawn {}", active.path.display()))?;
    drop(token);
    tracing::info!(offering = %spec.offering, pid = ?child.id(), cli = %active.path.display(), "amp provider started");
    metrics::gauge!("orbd_provider_up", "offering" => spec.offering.clone()).set(1.0);
    if let Some(o) = child.stdout.take() {
        forward_lines(
            spec.offering.clone(),
            "stdout",
            o,
            active.generation,
            spec.verdicts.clone(),
        );
    }
    if let Some(e) = child.stderr.take() {
        forward_lines(
            spec.offering.clone(),
            "stderr",
            e,
            active.generation,
            spec.verdicts.clone(),
        );
    }
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
                tracing::info!(offering = %spec.offering, "amp cli changed; restarting provider between jobs");
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
    mut updated: tokio::sync::watch::Receiver<Active>,
    shutdown: CancellationToken,
) {
    let mut backoff = BACKOFF_START;
    while !shutdown.is_cancelled() {
        let started = tokio::time::Instant::now();
        match run_once(&spec, &mut updated, &shutdown).await {
            Ok(Exit::Shutdown) => break,
            Ok(Exit::Update) => {
                tracing::info!(offering = %spec.offering, "restarting amp provider on the new cli");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probation_rolls_back_only_on_bad_jobs_from_two_orbs_of_its_generation() {
        let v = |generation, good, orb: &str| Verdict {
            generation,
            good,
            orb: orb.into(),
        };
        let mut p = Probation {
            generation: 3,
            version: "new".into(),
            bad_orbs: Default::default(),
        };
        assert_eq!(
            p.judge(&v(2, false, "amp-a")),
            Decision::None,
            "old cli's job"
        );
        assert_eq!(
            p.judge(&v(2, false, "amp-b")),
            Decision::None,
            "old cli's job"
        );
        assert_eq!(p.judge(&v(3, false, "amp-a")), Decision::None);
        assert_eq!(
            p.judge(&v(3, false, "amp-a")),
            Decision::None,
            "same orb again"
        );
        assert_eq!(p.judge(&v(3, false, "amp-b")), Decision::RollBack);
        assert_eq!(p.judge(&v(3, true, "amp-c")), Decision::Promote);
        assert_eq!(
            parse_cli_check("orbd dispatch: cli-check bad amp-x"),
            Some((false, "amp-x".into()))
        );
        assert_eq!(
            parse_cli_check("orbd dispatch: result Failed: spawn failed"),
            None
        );
    }
}
