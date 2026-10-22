//! Guest control over `docker exec`. Commands are fixed argument vectors; credentials
//! reach the guest only on exec stdin, so they never appear in container or exec
//! configuration (`docker inspect`), command lines or labels.

use super::{DockerRuntime, Interrupted, bounded, container_name, log};
use crate::model::{OrbDefinition, OrbId};
use crate::runtime::{BootstrapRequest, GuestControl, GuestError, OpContext, RuntimeObservation};
use crate::secrets::PermittedGuestEnv;
use async_trait::async_trait;
use bollard::container::LogOutput;
use bollard::errors::Error as DockerError;
use bollard::exec::{CreateExecOptions, StartExecResults};
use futures::StreamExt;
use secrecy::ExposeSecret;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use zeroize::Zeroizing;

/// Runs `amp orb bootstrap` for the life of the orb; restarts only on failure.
const BOOTSTRAP_UNIT: &str = "amp-headless-bootstrap.service";
const HEADLESS_UNIT: &str = "amp-headless.service";
/// The bootstrap unit's `EnvironmentFile=`.
const ENV_FILE: &str = "/home/user/.config/amp/headless-bootstrap.env";
/// Runs as `user` (so the file is owned by it); the file body arrives on stdin.
const WRITE_ENV_SCRIPT: &str = r#"set -eu
umask 077
mkdir -p /home/user/.config/amp
t=/home/user/.config/amp/.headless-bootstrap.env.$$
cat >"$t"
chmod 0600 "$t"
mv -f "$t" /home/user/.config/amp/headless-bootstrap.env
"#;
/// How long the bootstrap unit must stay up (same process, no restarts) to count as
/// started. `amp orb bootstrap` never exits on success, so there is no exit to wait for.
const BOOTSTRAP_SETTLE: Duration = Duration::from_secs(5);
const POLL: Duration = Duration::from_secs(1);
const MAX_OUTPUT: usize = 64 * 1024;

#[derive(Default)]
struct ExecOutput {
    code: i64,
    stdout: String,
    stderr: String,
}

impl From<Interrupted> for GuestError {
    fn from(i: Interrupted) -> Self {
        match i {
            Interrupted::Cancelled => Self::Cancelled,
            Interrupted::Deadline => Self::DeadlineExceeded,
        }
    }
}

fn unreachable(e: impl std::fmt::Display) -> GuestError {
    GuestError::Unreachable(e.to_string())
}

impl DockerRuntime {
    /// Runs `cmd` in the orb's container as `user`, optionally feeding `stdin`, and
    /// returns its exit code and (capped) output.
    async fn exec(
        &self,
        orb: OrbId,
        user: &str,
        cmd: &[&str],
        stdin: Option<&[u8]>,
    ) -> Result<ExecOutput, DockerError> {
        let created = self
            .docker
            .create_exec(
                &container_name(orb),
                CreateExecOptions {
                    attach_stdin: Some(stdin.is_some()),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    user: Some(user),
                    cmd: Some(cmd.to_vec()),
                    ..Default::default()
                },
            )
            .await?;
        let mut out = ExecOutput::default();
        if let StartExecResults::Attached {
            mut output,
            mut input,
        } = self.docker.start_exec(&created.id, None).await?
        {
            if let Some(data) = stdin {
                input.write_all(data).await?;
                input.shutdown().await?;
            }
            while let Some(chunk) = output.next().await {
                let (buf, msg) = match chunk? {
                    LogOutput::StdOut { message } => (&mut out.stdout, message),
                    LogOutput::StdErr { message } => (&mut out.stderr, message),
                    _ => continue,
                };
                if buf.len() < MAX_OUTPUT {
                    buf.push_str(&String::from_utf8_lossy(&msg));
                }
            }
        }
        loop {
            let i = self.docker.inspect_exec(&created.id).await?;
            if i.running != Some(true) {
                out.code = i.exit_code.unwrap_or(-1);
                return Ok(out);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn guest_exec(
        &self,
        orb: OrbId,
        user: &str,
        cmd: &[&str],
        stdin: Option<&[u8]>,
        ctx: &OpContext,
    ) -> Result<ExecOutput, GuestError> {
        bounded(ctx, self.exec(orb, user, cmd, stdin))
            .await?
            .map_err(unreachable)
    }

    async fn wait_for_systemd(&self, orb: OrbId, ctx: &OpContext) -> Result<(), GuestError> {
        let mut last = String::new();
        loop {
            let out = self
                .guest_exec(orb, "root", &["systemctl", "is-system-running"], None, ctx)
                .await?;
            let state = out.stdout.trim().to_owned();
            if state != last {
                log(ctx, &format!("systemd is {state}"));
            }
            match state.as_str() {
                "running" => return Ok(()),
                "degraded" => {
                    let failed = self
                        .guest_exec(
                            orb,
                            "root",
                            &["systemctl", "--failed", "--plain", "--no-legend"],
                            None,
                            ctx,
                        )
                        .await?;
                    log(ctx, &format!("failed units:\n{}", failed.stdout.trim_end()));
                    return Ok(());
                }
                _ => {}
            }
            last = state;
            bounded(ctx, tokio::time::sleep(POLL)).await?;
        }
    }

    async fn bootstrap_unit_state(
        &self,
        orb: OrbId,
        ctx: &OpContext,
    ) -> Result<UnitState, GuestError> {
        let out = self
            .guest_exec(
                orb,
                "root",
                &[
                    "systemctl",
                    "show",
                    "--property=ActiveState,SubState,MainPID,NRestarts",
                    BOOTSTRAP_UNIT,
                ],
                None,
                ctx,
            )
            .await?;
        Ok(UnitState::parse(&out.stdout))
    }

    async fn bootstrap_failed(
        &self,
        orb: OrbId,
        env: &PermittedGuestEnv,
        what: &str,
        ctx: &OpContext,
    ) -> GuestError {
        let journal = match self
            .guest_exec(
                orb,
                "root",
                &[
                    "journalctl",
                    "--unit",
                    BOOTSTRAP_UNIT,
                    "--lines=40",
                    "--no-pager",
                    "--output=short-iso",
                ],
                None,
                ctx,
            )
            .await
        {
            Ok(o) => o.stdout,
            Err(e) => format!("(journal unavailable: {e})"),
        };
        let msg = redact(&format!("{what}; journal tail:\n{journal}"), env);
        log(ctx, &msg);
        GuestError::BootstrapFailed(msg)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UnitState {
    active: String,
    sub: String,
    main_pid: String,
    restarts: String,
}

impl UnitState {
    fn parse(show: &str) -> Self {
        let get = |k: &str| {
            show.lines()
                .find_map(|l| l.strip_prefix(k)?.strip_prefix('='))
                .unwrap_or_default()
                .trim()
                .to_owned()
        };
        Self {
            active: get("ActiveState"),
            sub: get("SubState"),
            main_pid: get("MainPID"),
            restarts: get("NRestarts"),
        }
    }

    fn running(&self) -> bool {
        self.active == "active" && self.sub == "running"
    }
}

#[async_trait]
impl GuestControl for DockerRuntime {
    async fn bootstrap(
        &self,
        orb: &OrbDefinition,
        request: BootstrapRequest,
        ctx: &OpContext,
    ) -> Result<(), GuestError> {
        let env = &request.env;
        let body = env_file(env).map_err(GuestError::BootstrapFailed)?;
        match bounded(ctx, self.observe(orb.id)).await? {
            Ok(RuntimeObservation::Running) => {}
            Ok(other) => return Err(GuestError::Unreachable(format!("orb is {other:?}"))),
            Err(e) => return Err(unreachable(e)),
        }
        log(
            ctx,
            &format!(
                "bootstrapping (generation {}, {} boot)",
                request.generation,
                if request.cold_boot { "cold" } else { "warm" }
            ),
        );
        self.wait_for_systemd(orb.id, ctx).await?;

        if request.cold_boot {
            // amp-headless.service (enabled by the first bootstrap) came up on this boot
            // with the env from before the pause; bootstrap only starts it when it is not
            // running, so stop it and let bootstrap start it with fresh credentials.
            let out = self
                .guest_exec(
                    orb.id,
                    "root",
                    &["systemctl", "stop", HEADLESS_UNIT],
                    None,
                    ctx,
                )
                .await?;
            if out.code != 0 {
                log(
                    ctx,
                    &format!("stopping {HEADLESS_UNIT}: {}", out.stderr.trim()),
                );
            }
        }

        let out = self
            .guest_exec(
                orb.id,
                "user",
                &["sh", "-c", WRITE_ENV_SCRIPT],
                Some(&body),
                ctx,
            )
            .await?;
        if out.code != 0 {
            return Err(GuestError::BootstrapFailed(redact(
                &format!("writing {ENV_FILE} failed: {}", out.stderr.trim()),
                env,
            )));
        }
        log(ctx, &format!("wrote {ENV_FILE}"));

        log(ctx, &format!("restarting {BOOTSTRAP_UNIT}"));
        let out = self
            .guest_exec(
                orb.id,
                "root",
                &["systemctl", "restart", BOOTSTRAP_UNIT],
                None,
                ctx,
            )
            .await?;
        if out.code != 0 {
            let what = format!(
                "systemctl restart {BOOTSTRAP_UNIT} failed: {}",
                out.stderr.trim()
            );
            return Err(self.bootstrap_failed(orb.id, env, &what, ctx).await);
        }
        let first = self.bootstrap_unit_state(orb.id, ctx).await?;
        let settled_at = Instant::now() + BOOTSTRAP_SETTLE;
        let mut state = first.clone();
        while state.running()
            && state.main_pid == first.main_pid
            && state.restarts == first.restarts
        {
            if Instant::now() >= settled_at {
                log(ctx, &format!("{BOOTSTRAP_UNIT} is running"));
                return Ok(());
            }
            bounded(ctx, tokio::time::sleep(POLL)).await?;
            state = self.bootstrap_unit_state(orb.id, ctx).await?;
        }
        let what = format!(
            "{BOOTSTRAP_UNIT} did not stay running ({}/{}, restarts {})",
            state.active, state.sub, state.restarts
        );
        Err(self.bootstrap_failed(orb.id, env, &what, ctx).await)
    }
}

/// The bootstrap unit's EnvironmentFile body: `NAME="value"` lines with systemd's
/// double-quote escapes. Control characters (newlines) would let a value inject other
/// variables, so they are rejected.
fn env_file(env: &PermittedGuestEnv) -> Result<Zeroizing<Vec<u8>>, String> {
    env.validate()?;
    if env.get("ORB_TOKEN").is_none() {
        return Err("guest env has no ORB_TOKEN".into());
    }
    let mut out = Zeroizing::new(Vec::new());
    for (name, value) in &env.0 {
        let value = value.expose_secret();
        if value.chars().any(char::is_control) {
            return Err(format!("guest env {name} contains control characters"));
        }
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(b"=\"");
        for c in value.chars() {
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

/// Replaces guest env values in text surfaced from the guest (journal, stderr).
fn redact(text: &str, env: &PermittedGuestEnv) -> String {
    let mut text = text.to_owned();
    for (name, value) in &env.0 {
        let v = value.expose_secret();
        if v.len() >= 4 {
            text = text.replace(v, &format!("[{name} redacted]"));
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;

    fn env(vars: &[(&str, &str)]) -> PermittedGuestEnv {
        PermittedGuestEnv(
            vars.iter()
                .map(|(n, v)| (n.to_string(), SecretString::from(*v)))
                .collect(),
        )
    }

    #[test]
    fn env_file_quotes_values_and_rejects_line_injection() {
        let body = env_file(&env(&[
            ("ORB_TOKEN", r#"a"b$c\d`e'f"#),
            ("AMP_URL", "https://ampcode.com"),
        ]))
        .unwrap();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            "ORB_TOKEN=\"a\\\"b\\$c\\\\d\\`e'f\"\nAMP_URL=\"https://ampcode.com\"\n"
        );
        assert!(env_file(&env(&[("ORB_TOKEN", "tok\nAMP_URL=https://evil")])).is_err());
    }
}
