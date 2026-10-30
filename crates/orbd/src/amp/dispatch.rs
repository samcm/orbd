//! `orbd dispatch`: executed (via the `orbd-dispatch` wrapper) by `amp orb provider`
//! once per job, with no arguments. Reads the job from the environment, forwards it to
//! the coordinator's local `Dispatch` socket, streams logs to stdout/stderr and exits
//! with the result.

use crate::model::command::DISPATCH_BUDGET;
use crate::secrets::PermittedGuestEnv;
use hyper_util::rt::TokioIo;
use orbd_proto::control::v1 as pb;
use orbd_proto::control::v1::dispatch_client::DispatchClient;
use orbd_proto::control::v1::dispatch_event::Event;
use secrecy::SecretString;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::time::Instant;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;

/// Exit codes. Diagnostic only: Amp does not treat them differently.
pub mod exit {
    pub const SUCCESS: i32 = 0;
    pub const FAILED: i32 = 1;
    pub const INVALID: i32 = 2;
    pub const UNAVAILABLE_OR_UNKNOWN: i32 = 75;
    pub const DEADLINE: i32 = 124;
}

/// Environment set by the coordinator for its `amp orb provider` children.
pub const ENV_OFFERING_ID: &str = "ORBD_OFFERING_ID";
pub const ENV_DISPATCH_SOCKET: &str = "ORBD_DISPATCH_SOCKET";

/// How long to keep trying before declaring the coordinator unreachable when no
/// connection has ever been established.
const INITIAL_CONNECT_WINDOW: Duration = Duration::from_secs(10);
const MAX_RETRY_PAUSE: Duration = Duration::from_secs(2);

/// Variables every spawn forwards into the guest when set, whether or not `ORB_ENVS`
/// names them: the ones Amp CLIs listed in `ORB_ENVS` before they stopped setting it.
const GUEST_ENV: &str = "AMP_URL ORB_TOKEN AMP_THREAD_ID AMP_SANDBOX_ID";

/// Names that must never be forwarded into a guest, whatever `ORB_ENVS` says.
fn forbidden_guest_env(name: &str) -> bool {
    name == "AMP_ORB_PROVIDER_TOKEN" || name.starts_with("ORBD_")
}

struct Job {
    socket: PathBuf,
    request: pb::DispatchRequest,
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn require(name: &str) -> Result<String, String> {
    var(name).ok_or_else(|| format!("required environment variable {name} is not set"))
}

/// Guest env = values of the variables named by `ORB_ENVS` (unset names are skipped).
fn collect_guest_env(
    orb_envs: &str,
    get: impl Fn(&str) -> Option<String>,
) -> Result<PermittedGuestEnv, String> {
    let mut out: Vec<(String, SecretString)> = Vec::new();
    for name in orb_envs.split_whitespace() {
        if name.len() > crate::secrets::MAX_GUEST_ENV_NAME
            || !name
                .bytes()
                .enumerate()
                .all(|(i, b)| b == b'_' || b.is_ascii_uppercase() || (i > 0 && b.is_ascii_digit()))
        {
            return Err(format!("invalid guest env name {name:?}"));
        }
        if forbidden_guest_env(name) {
            return Err(format!("ORB_ENVS names {name}, which is never forwarded"));
        }
        if out.iter().any(|(n, _)| n == name) {
            continue;
        }
        if let Some(v) = get(name) {
            out.push((name.to_owned(), SecretString::from(v)));
        }
    }
    let env = PermittedGuestEnv(out);
    env.validate()?;
    Ok(env)
}

fn build_job() -> Result<Job, String> {
    let socket = PathBuf::from(require(ENV_DISPATCH_SOCKET)?);
    let offering_id = require(ENV_OFFERING_ID)?;
    let operation = require("ORB_OPERATION")?;
    let amp_orb_id = require("ORB_ID")?;
    let action = match operation.as_str() {
        "spawn" => {
            let image_ref = require("ORB_IMAGE")?;
            // Amp's dispatch contract passes ORB_TOKEN and AMP_URL for the guest; older
            // CLIs also listed them in ORB_ENVS, newer ones (2026-10-29) do not.
            let names = format!("{} {GUEST_ENV}", var("ORB_ENVS").unwrap_or_default());
            let guest_env = collect_guest_env(&names, var)?;
            use secrecy::ExposeSecret;
            pb::dispatch_request::Action::Spawn(pb::SpawnRequest {
                image_ref,
                project_snapshot_id: var("ORB_SNAPSHOT_ID"),
                guest_env: guest_env
                    .0
                    .iter()
                    .map(|(n, v)| pb::EnvVar {
                        name: n.clone(),
                        value: v.expose_secret().to_owned(),
                    })
                    .collect(),
            })
        }
        "pause" => pb::dispatch_request::Action::Pause(pb::PauseRequest {
            project_snapshot_id: var("ORB_SNAPSHOT_ID"),
        }),
        "delete" => pb::dispatch_request::Action::Delete(pb::DeleteRequest {}),
        other => return Err(format!("unsupported ORB_OPERATION {other:?}")),
    };
    Ok(Job {
        socket,
        request: pb::DispatchRequest {
            invocation_id: uuid::Uuid::new_v4().to_string(),
            offering_id,
            amp_orb_id,
            remaining_budget_ms: 0,
            resume_after_log_seq: 0,
            amp_url: var("AMP_URL").unwrap_or_default(),
            amp_thread_id: var("AMP_THREAD_ID").unwrap_or_default(),
            amp_sandbox_id: var("AMP_SANDBOX_ID").unwrap_or_default(),
            orb_sandbox_id: var("ORB_SANDBOX_ID").unwrap_or_default(),
            action: Some(action),
        },
    })
}

/// A gRPC channel over a local unix socket.
pub(crate) async fn connect(socket: &std::path::Path) -> Result<Channel, tonic::transport::Error> {
    let socket = socket.to_owned();
    // The URI is required by tonic but unused by the connector.
    Endpoint::from_static("http://orbd.invalid")
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move { UnixStream::connect(socket).await.map(TokioIo::new) }
        }))
        .await
}

fn say(msg: impl AsRef<str>) {
    let mut e = std::io::stderr().lock();
    let _ = writeln!(e, "orbd dispatch: {}", msg.as_ref());
    let _ = e.flush();
}

fn write_log(chunk: &pb::LogChunk) {
    fn put<W: Write>(mut w: W, data: &[u8]) {
        let _ = w.write_all(data);
        let _ = w.flush();
    }
    if chunk.dropped_before > 0 {
        say(format!(
            "{} log chunk(s) were dropped before this point",
            chunk.dropped_before
        ));
    }
    if chunk.stream == pb::LogStream::Stderr as i32 {
        put(std::io::stderr().lock(), &chunk.data);
    } else {
        put(std::io::stdout().lock(), &chunk.data);
    }
}

fn result_exit(r: &pb::DispatchResult) -> i32 {
    use pb::DispatchStatus as S;
    match S::try_from(r.status).unwrap_or(S::Unspecified) {
        S::Succeeded => exit::SUCCESS,
        S::Failed => exit::FAILED,
        S::Invalid => exit::INVALID,
        S::Unavailable | S::OutcomeUnknown | S::Unspecified => exit::UNAVAILABLE_OR_UNKNOWN,
        S::DeadlineExceeded => exit::DEADLINE,
    }
}

/// Outcome of one attempt (one connection) to follow the invocation.
enum Attempt {
    Done(i32),
    /// Stream broke or connect failed.
    Retry,
}

struct Progress {
    last_seq: u64,
    ever_connected: bool,
}

async fn attempt(job: &mut Job, deadline: Instant, prog: &mut Progress) -> Attempt {
    let channel = match connect(&job.socket).await {
        Ok(c) => c,
        Err(_) => return Attempt::Retry,
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    job.request.remaining_budget_ms = remaining.as_millis() as u64;
    job.request.resume_after_log_seq = prog.last_seq;
    let mut client = DispatchClient::new(channel);
    let mut stream = match client.execute(job.request.clone()).await {
        Ok(r) => r.into_inner(),
        Err(s) => return handle_status(s),
    };
    prog.ever_connected = true;
    loop {
        let next = tokio::time::timeout_at(deadline, stream.message()).await;
        let msg = match next {
            Err(_) => return Attempt::Retry,
            Ok(Err(s)) => return handle_status(s),
            Ok(Ok(None)) => {
                say("stream ended without a result; reconnecting");
                return Attempt::Retry;
            }
            Ok(Ok(Some(m))) => m,
        };
        match msg.event {
            Some(Event::Queued(q)) => say(format!("queued ({} ahead)", q.position)),
            Some(Event::Accepted(a)) => say(format!(
                "accepted operation {} generation {}{}",
                a.operation_id,
                a.generation,
                if a.attached { " (attached)" } else { "" }
            )),
            Some(Event::Progress(p)) => say(format!("{}: {}", p.phase, p.message)),
            Some(Event::Log(l)) => {
                if l.seq > prog.last_seq {
                    prog.last_seq = l.seq;
                    write_log(&l);
                }
            }
            Some(Event::Result(r)) => {
                let code = result_exit(&r);
                say(format!(
                    "result {:?}{}: {}",
                    pb::DispatchStatus::try_from(r.status).unwrap_or_default(),
                    if r.outcome_known || code != exit::UNAVAILABLE_OR_UNKNOWN {
                        ""
                    } else {
                        " (outcome not known)"
                    },
                    r.message
                ));
                return Attempt::Done(code);
            }
            None => {}
        }
    }
}

fn handle_status(s: tonic::Status) -> Attempt {
    match s.code() {
        tonic::Code::InvalidArgument | tonic::Code::PermissionDenied => {
            say(format!("rejected: {}", s.message()));
            Attempt::Done(exit::INVALID)
        }
        // Server-side failures and transport errors both mean the coordinator may still
        // know about this invocation: retry with the same id.
        code => {
            say(format!("coordinator call failed ({code:?}); will retry"));
            Attempt::Retry
        }
    }
}

/// `DISPATCH_BUDGET`, optionally lowered (never raised) by `ORBD_DISPATCH_BUDGET_MS`.
/// The coordinator derives the work and cleanup deadlines from the remaining budget.
fn dispatch_budget() -> std::time::Duration {
    std::env::var("ORBD_DISPATCH_BUDGET_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .map(std::time::Duration::from_millis)
        .map_or(DISPATCH_BUDGET, |d| d.min(DISPATCH_BUDGET))
}

/// Runs one dispatch job from the process environment; returns the exit code.
pub async fn run_from_env() -> i32 {
    let started = Instant::now();
    let deadline = started + dispatch_budget();
    let mut job = match build_job() {
        Ok(j) => j,
        Err(e) => {
            say(format!("invalid job: {e}"));
            return exit::INVALID;
        }
    };
    say(format!(
        "invocation {} for orb {} ({})",
        job.request.invocation_id,
        job.request.amp_orb_id,
        match &job.request.action {
            Some(pb::dispatch_request::Action::Spawn(_)) => "spawn",
            Some(pb::dispatch_request::Action::Pause(_)) => "pause",
            _ => "delete",
        }
    ));
    let mut prog = Progress {
        last_seq: 0,
        ever_connected: false,
    };
    let mut pause = Duration::from_millis(200);
    loop {
        match attempt(&mut job, deadline, &mut prog).await {
            Attempt::Done(code) => return code,
            Attempt::Retry => {}
        }
        let now = Instant::now();
        if now >= deadline {
            say("dispatch budget exhausted before a result arrived; outcome unknown");
            return exit::UNAVAILABLE_OR_UNKNOWN;
        }
        if !prog.ever_connected && now.duration_since(started) >= INITIAL_CONNECT_WINDOW {
            say(format!(
                "coordinator not reachable at {}",
                job.socket.display()
            ));
            return exit::UNAVAILABLE_OR_UNKNOWN;
        }
        tokio::time::sleep(pause.min(deadline.saturating_duration_since(now))).await;
        pause = (pause * 2).min(MAX_RETRY_PAUSE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_orb_envs_listed_vars_are_forwarded() {
        let get = |n: &str| match n {
            "ORB_TOKEN" => Some("t".to_owned()),
            "AMP_URL" => Some("u".to_owned()),
            "AMP_ORB_PROVIDER_TOKEN" => Some("secret".to_owned()),
            _ => None,
        };
        let env = collect_guest_env("ORB_TOKEN MISSING", get).unwrap();
        let names: Vec<_> = env.0.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["ORB_TOKEN"]);
        // Newer Amp CLIs set no ORB_ENVS; the token must still reach the guest.
        let env = collect_guest_env(&format!("{GUEST_ENV} "), get).unwrap();
        let names: Vec<_> = env.0.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["AMP_URL", "ORB_TOKEN"]);
        assert!(collect_guest_env("AMP_ORB_PROVIDER_TOKEN", get).is_err());
        assert!(collect_guest_env("bad-name", |_| Some("x".into())).is_err());
    }
}
