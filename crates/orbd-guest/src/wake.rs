//! `Wake`: clock, network/DNS, credentials, and (re)starting Amp's bootstrap. Mirrors
//! the Docker runtime's exec-based bootstrap (`crates/orbd/src/runtime/docker/guest.rs`).

use crate::cmd::{self, check, run};
use crate::files::{create_dirs_owned, user_ids, write_atomic};
use crate::proto::{EnvVar, Resync, Wake};
use std::path::Path;
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

/// The image's unit: runs `amp orb bootstrap` for the life of the orb; its ExecStartPre
/// updates the orb's amp CLI on every start.
const BOOTSTRAP_UNIT: &str = "amp-headless-bootstrap.service";
const HEADLESS_UNIT: &str = "amp-headless.service";
/// Transient unit used only when the image lacks [`BOOTSTRAP_UNIT`].
const FALLBACK_UNIT: &str = "orbd-amp-bootstrap.service";
const ORB_USER: &str = "user";
const ENV_DIR: &str = "/home/user/.config/amp";
const ENV_FILE: &str = "/home/user/.config/amp/headless-bootstrap.env";
/// Amp's units carry a drop-in `ConditionPathExists=` on this file, so after a cold boot
/// they stay down until fresh credentials arrive. /run is tmpfs: a cold boot clears it, a
/// memory restore keeps it.
pub const CREDENTIALS_READY: &str = "/run/orbd-guest/credentials-ready";
/// How long the bootstrap unit must stay up (same process, no restarts) to count as
/// started. `amp orb bootstrap` never exits on success.
const BOOTSTRAP_SETTLE: Duration = Duration::from_secs(5);
const SYSTEMD_BOOT_WAIT: Duration = Duration::from_secs(90);
const SYSTEMCTL: Duration = Duration::from_secs(60);

pub fn wake(w: &Wake) -> Result<(), String> {
    log!(
        "wake: generation {} ({} boot)",
        w.generation,
        if w.cold_boot { "cold" } else { "warm" }
    );
    if w.current_time_unix_ns > 0 {
        step_clock(w.current_time_unix_ns)?;
    }
    if let Some(net) = &w.network {
        crate::net::apply(net)?;
    }
    let body = env_file(&w.permitted_env)?;
    let redact = |s: &str| redact(s, &w.permitted_env);

    if w.cold_boot {
        // amp-headless.service is enabled after first setup; it must not keep (or come
        // up with) the env from before this boot. The bootstrap starts it again.
        let out = run(&["systemctl", "stop", HEADLESS_UNIT], SYSTEMCTL)?;
        if !out.ok {
            log!("stopping {HEADLESS_UNIT}: {}", out.stderr.trim());
        }
    }

    let (uid, gid) = user_ids(ORB_USER).map_err(|e| format!("user {ORB_USER}: {e}"))?;
    create_dirs_owned(Path::new(ENV_DIR), (uid, gid)).map_err(|e| format!("{ENV_DIR}: {e}"))?;
    write_atomic(Path::new(ENV_FILE), &body, 0o600, Some((uid, gid)))
        .map_err(|e| format!("writing {ENV_FILE}: {e}"))?;
    log!("wrote {ENV_FILE}");
    std::fs::create_dir_all("/run/orbd-guest").map_err(|e| e.to_string())?;
    std::fs::write(CREDENTIALS_READY, b"").map_err(|e| format!("{CREDENTIALS_READY}: {e}"))?;

    wait_for_systemd();

    let unit = if has_unit(BOOTSTRAP_UNIT)? {
        log!("restarting {BOOTSTRAP_UNIT}");
        let out = run(&["systemctl", "restart", BOOTSTRAP_UNIT], SYSTEMCTL)?;
        if !out.ok {
            let what = format!(
                "systemctl restart {BOOTSTRAP_UNIT} failed: {}",
                out.stderr.trim()
            );
            return Err(redact(&failed(BOOTSTRAP_UNIT, &what)));
        }
        BOOTSTRAP_UNIT
    } else {
        log!(
            "image has no {BOOTSTRAP_UNIT}; falling back to `amp orb bootstrap` ({FALLBACK_UNIT})"
        );
        let _ = run(&["systemctl", "stop", FALLBACK_UNIT], SYSTEMCTL);
        let _ = run(&["systemctl", "reset-failed", FALLBACK_UNIT], SYSTEMCTL);
        let env_file = format!("--property=EnvironmentFile={ENV_FILE}");
        let out = run(
            &[
                "systemd-run",
                "--unit",
                FALLBACK_UNIT,
                "--uid=user",
                "--gid=user",
                "--property=Restart=on-failure",
                &env_file,
                "/bin/sh",
                "-lc",
                "exec amp orb bootstrap",
            ],
            SYSTEMCTL,
        )?;
        if !out.ok {
            let what = format!("systemd-run {FALLBACK_UNIT} failed: {}", out.stderr.trim());
            return Err(redact(&failed(FALLBACK_UNIT, &what)));
        }
        FALLBACK_UNIT
    };

    let first = UnitState::get(unit)?;
    let settled_at = Instant::now() + BOOTSTRAP_SETTLE;
    let mut state = first.clone();
    while state.running() && state.main_pid == first.main_pid && state.restarts == first.restarts {
        if Instant::now() >= settled_at {
            log!("{unit} is running");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
        state = UnitState::get(unit)?;
    }
    let what = format!(
        "{unit} did not stay running ({}/{}, restarts {})",
        state.active, state.sub, state.restarts
    );
    Err(redact(&failed(unit, &what)))
}

/// `Resync`: clock and network only, after a restore no spawn asked for.
pub fn resync(r: &Resync) -> Result<(), String> {
    log!("resync");
    if r.current_time_unix_ns > 0 {
        step_clock(r.current_time_unix_ns)?;
    }
    if let Some(net) = &r.network {
        crate::net::apply(net)?;
    }
    Ok(())
}

fn step_clock(unix_ns: i64) -> Result<(), String> {
    let ts = libc::timespec {
        tv_sec: unix_ns.div_euclid(1_000_000_000) as _,
        tv_nsec: unix_ns.rem_euclid(1_000_000_000) as _,
    };
    let before = std::time::SystemTime::now();
    // SAFETY: valid timespec.
    if unsafe { libc::clock_settime(libc::CLOCK_REALTIME, &ts) } < 0 {
        return Err(format!(
            "clock_settime: {}",
            std::io::Error::last_os_error()
        ));
    }
    let skew_ms = (unix_ns / 1_000_000)
        - before
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
    log!("clock: stepped by {skew_ms} ms");
    Ok(())
}

/// Waits (bounded) until boot finished, so the restart is not queued behind boot jobs.
fn wait_for_systemd() {
    match run(
        &["systemctl", "is-system-running", "--wait"],
        SYSTEMD_BOOT_WAIT,
    ) {
        Ok(out) => {
            let state = out.stdout.trim().to_owned();
            if state == "degraded" {
                let failed = run(
                    &["systemctl", "--failed", "--plain", "--no-legend"],
                    SYSTEMCTL,
                )
                .map(|o| o.stdout)
                .unwrap_or_default();
                log!("systemd is degraded; failed units:\n{}", failed.trim_end());
            } else {
                log!("systemd is {state}");
            }
        }
        Err(e) => log!("systemd boot wait: {e}"),
    }
}

/// False only when systemd reports the unit `not-found`. A masked or broken unit is a
/// bootstrap failure, not a reason to fall back.
fn has_unit(unit: &str) -> Result<bool, String> {
    let out = check(
        &["systemctl", "show", "--property=LoadState", "--value", unit],
        SYSTEMCTL,
    )?;
    match out.stdout.trim() {
        "not-found" => Ok(false),
        "loaded" => Ok(true),
        other => Err(format!("{unit} is {other}")),
    }
}

#[derive(Clone, Debug)]
struct UnitState {
    active: String,
    sub: String,
    main_pid: String,
    restarts: String,
}

impl UnitState {
    fn get(unit: &str) -> Result<Self, String> {
        let out = check(
            &[
                "systemctl",
                "show",
                "--property=ActiveState,SubState,MainPID,NRestarts",
                unit,
            ],
            SYSTEMCTL,
        )?;
        let get = |k: &str| {
            out.stdout
                .lines()
                .find_map(|l| l.strip_prefix(k)?.strip_prefix('='))
                .unwrap_or_default()
                .trim()
                .to_owned()
        };
        Ok(Self {
            active: get("ActiveState"),
            sub: get("SubState"),
            main_pid: get("MainPID"),
            restarts: get("NRestarts"),
        })
    }

    fn running(&self) -> bool {
        self.active == "active" && self.sub == "running"
    }
}

/// Failure message with the unit's journal tail (caller redacts).
fn failed(unit: &str, what: &str) -> String {
    let journal = match cmd::run(
        &[
            "journalctl",
            "--unit",
            unit,
            "--lines=40",
            "--no-pager",
            "--output=short-iso",
        ],
        SYSTEMCTL,
    ) {
        Ok(o) => o.stdout,
        Err(e) => format!("(journal unavailable: {e})"),
    };
    format!("{what}; journal tail:\n{journal}")
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The bootstrap unit's EnvironmentFile body: `NAME="value"` lines with systemd's
/// double-quote escapes. Control characters would let a value inject other variables.
fn env_file(env: &[EnvVar]) -> Result<Zeroizing<Vec<u8>>, String> {
    if !env.iter().any(|v| v.name == "ORB_TOKEN") {
        return Err("guest env has no ORB_TOKEN".into());
    }
    let mut out = Zeroizing::new(Vec::new());
    for v in env {
        if !valid_name(&v.name) {
            return Err(format!("bad guest env name {:?}", v.name));
        }
        if v.value.chars().any(char::is_control) {
            return Err(format!("guest env {} contains control characters", v.name));
        }
        out.extend_from_slice(v.name.as_bytes());
        out.extend_from_slice(b"=\"");
        for c in v.value.chars() {
            if matches!(c, '"' | '\\' | '`' | '$') {
                out.push(b'\\');
            }
            let mut buf = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
        }
        out.extend_from_slice(b"\"\n");
    }
    Ok(out)
}

/// Replaces env values in text sent back to the host (journal, stderr).
fn redact(text: &str, env: &[EnvVar]) -> String {
    let mut text = text.to_owned();
    for v in env {
        if v.value.len() >= 4 {
            text = text.replace(&v.value, &format!("[{} redacted]", v.name));
        }
    }
    text
}
