//! Process-level test cluster: a real `orbd coordinator` with its built-in CA, real
//! `orbd agent`s with the fake runtime that enroll with `orbd join-token` tokens, and jobs
//! run through `orbd dispatch` exactly as `amp orb provider` would run them.

#![allow(dead_code)]

use orbd::model::OrbId;
use orbd::runtime::fake::{FakeOrbState, load_orb_state};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const OFFERING: &str = "fake-2vcpu-2gb";
pub const TOKEN: &str = "orb-token-value-that-must-never-be-logged";

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn write(path: &Path, contents: impl AsRef<[u8]>) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

pub struct Cluster {
    pub dir: tempfile::TempDir,
    control_port: u16,
    metrics_port: u16,
    coordinator: Option<Child>,
    agents: BTreeMap<String, Option<Child>>,
    /// Each agent's join token, passed on every start like a container's ORBD_JOIN.
    tokens: BTreeMap<String, String>,
    agent_orbs: u64,
    offering: String,
}

pub struct Job {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl Cluster {
    /// Writes configs for a coordinator and the given agents, each agent with room for
    /// `agent_orbs` orbs of the test offering.
    pub fn new(agent_ids: &[&str], agent_orbs: u64) -> Self {
        Self::with_runtime(
            agent_ids,
            agent_orbs,
            "fake",
            "runtime:\n  fake:\n    memory_preserving_sleep: true\n",
        )
    }

    /// A cluster whose single offering (2 vCPUs, 2 GiB, 4 GiB disk) runs on `runtime`;
    /// every agent reads `agent_yaml` as its optional config file.
    pub fn with_runtime(
        agent_ids: &[&str],
        agent_orbs: u64,
        runtime: &str,
        agent_yaml: &str,
    ) -> Self {
        let offering = if runtime == "fake" {
            OFFERING.to_owned()
        } else {
            format!("{runtime}-2vcpu-2gb")
        };
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let control_port = free_port();
        let metrics_port = free_port();
        write(
            &root.join("coordinator.yaml"),
            format!(
                r#"public_url: https://localhost:{control_port}
listen: 127.0.0.1:{control_port}
state_dir: {root}/coordinator
metrics_listen: 127.0.0.1:{metrics_port}
heartbeat_interval_ms: 300
offerings:
  - id: {offering}
    runtime: {runtime}
    resources: {{ vcpus: 2, memory_mib: 2048, disk_gib: 4 }}
"#,
                root = root.display(),
            ),
        );
        // Only runtime settings go in a file; everything else is flags and env.
        write(&root.join("agent.yaml"), agent_yaml);
        Self {
            dir,
            control_port,
            metrics_port,
            coordinator: None,
            agents: agent_ids.iter().map(|a| (a.to_string(), None)).collect(),
            tokens: BTreeMap::new(),
            agent_orbs,
            offering,
        }
    }

    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    fn spawn_orbd(&self, args: &[&str], log: &str, failpoints: &str) -> Child {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root().join(log))
            .unwrap();
        // ORBD_TEST_COORDINATOR_CMD runs the coordinator through another command, e.g.
        // "qemu-aarch64-static target/aarch64-unknown-linux-musl/release/orbd".
        let custom = std::env::var("ORBD_TEST_COORDINATOR_CMD")
            .ok()
            .filter(|_| args[0] == "coordinator");
        let mut cmd = match &custom {
            Some(c) => {
                let mut parts = c.split_whitespace();
                let mut cmd = Command::new(parts.next().unwrap());
                cmd.args(parts);
                cmd
            }
            None => Command::new(env!("CARGO_BIN_EXE_orbd")),
        };
        cmd.args(args)
            .env("RUST_LOG", "info,orbd=debug")
            .env("FAILPOINTS", failpoints)
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap()
    }

    fn coordinator_config(&self) -> String {
        self.root().join("coordinator.yaml").display().to_string()
    }

    pub fn start_coordinator(&mut self, failpoints: &str) {
        let cfg = self.coordinator_config();
        let child = self.spawn_orbd(
            &["coordinator", "--config", &cfg],
            "coordinator.log",
            failpoints,
        );
        self.coordinator = Some(child);
        let sock = self.dispatch_socket();
        wait_until(Duration::from_secs(20), "dispatch socket", || sock.exists());
    }

    /// Runs an admin command (`join-token`, `agent list|remove`) against the coordinator.
    pub fn admin(&self, args: &[&str]) -> Job {
        let out = Command::new(env!("CARGO_BIN_EXE_orbd"))
            .args(args)
            .args(["--config", &self.coordinator_config()])
            .output()
            .unwrap();
        Job {
            code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).trim().into(),
            stderr: String::from_utf8_lossy(&out.stderr).into(),
        }
    }

    pub fn join_token(&self, id: &str) -> String {
        let j = self.admin(&["join-token", id]);
        assert_ok(&j, "join-token");
        j.stdout
    }

    /// An agent process for `id` with `token`, logging to `agent-<id>.log`.
    pub fn agent_command(&self, id: &str, token: &str, failpoints: &str) -> Command {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root().join(format!("agent-{id}.log")))
            .unwrap();
        let state = self.root().join("agents").join(id).join("state");
        let n = self.agent_orbs;
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_orbd"));
        cmd.arg("agent")
            .arg("--state-dir")
            .arg(state)
            .arg("--config")
            .arg(self.root().join("agent.yaml"))
            .env("ORBD_JOIN", token)
            .env("ORBD_CAPACITY_VCPUS", (2 * n).to_string())
            .env("ORBD_CAPACITY_MEMORY_MIB", (2048 * n).to_string())
            .env("ORBD_CAPACITY_DISK_GIB", (4 * n).to_string())
            .env("RUST_LOG", "info,orbd=debug")
            .env("FAILPOINTS", failpoints)
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        cmd
    }

    /// Starts agent `id`, minting its join token on first start.
    pub fn start_agent(&mut self, id: &str, failpoints: &str) {
        if !self.tokens.contains_key(id) {
            let token = self.join_token(id);
            self.tokens.insert(id.to_string(), token);
        }
        let child = self
            .agent_command(id, &self.tokens[id], failpoints)
            .spawn()
            .unwrap();
        self.agents.insert(id.to_string(), Some(child));
    }

    /// Starts an agent that is not part of the initial cluster, with a given token.
    pub fn start_agent_with_token(&mut self, id: &str, token: &str) {
        self.tokens.insert(id.to_string(), token.to_string());
        self.start_agent(id, "");
    }

    /// Starts the coordinator and all agents and waits until every agent is connected.
    pub fn start_all(&mut self) {
        self.start_coordinator("");
        let ids: Vec<String> = self.agents.keys().cloned().collect();
        for a in &ids {
            self.start_agent(a, "");
        }
        self.wait_connected(ids.len());
    }

    /// Hard-kills (SIGKILL) a process, like a power cut.
    pub fn kill_agent(&mut self, id: &str) {
        if let Some(mut c) = self.agents.get_mut(id).unwrap().take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Sends a signal (e.g. `TERM`) to a running agent without waiting.
    pub fn signal_agent(&self, id: &str, signal: &str) {
        let child = self.agents.get(id).unwrap().as_ref().unwrap();
        let _ = Command::new("kill")
            .args([&format!("-{signal}"), &child.id().to_string()])
            .status();
    }

    /// Stops an agent gracefully (SIGTERM, as a container stop does) and waits for it.
    pub fn terminate_agent(&mut self, id: &str, within: Duration) -> std::process::ExitStatus {
        let mut child = self.agents.get_mut(id).unwrap().take().unwrap();
        let _ = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status();
        wait_exit(&mut child, within).expect("agent did not stop in time")
    }

    pub fn kill_coordinator(&mut self) {
        if let Some(mut c) = self.coordinator.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Waits for a process started with a crash failpoint to abort.
    pub fn wait_agent_crash(&mut self, id: &str, within: Duration) {
        let child = self.agents.get_mut(id).unwrap().as_mut().unwrap();
        let status = wait_exit(child, within).expect("agent did not hit its crash point");
        assert!(
            !status.success(),
            "agent exited cleanly instead of crashing"
        );
        self.agents.insert(id.to_string(), None);
    }

    pub fn wait_coordinator_crash(&mut self, within: Duration) {
        let child = self.coordinator.as_mut().unwrap();
        let status = wait_exit(child, within).expect("coordinator did not hit its crash point");
        assert!(!status.success());
        self.coordinator = None;
        // The socket file outlives the process; the restarted coordinator replaces it.
    }

    pub fn dispatch_socket(&self) -> PathBuf {
        self.root().join("coordinator/dispatch.sock")
    }

    pub fn connected_agents(&self) -> Option<u64> {
        let mut s = TcpStream::connect(("127.0.0.1", self.metrics_port)).ok()?;
        s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
        s.write_all(b"GET /metrics HTTP/1.0\r\nHost: x\r\n\r\n")
            .ok()?;
        let mut body = String::new();
        s.read_to_string(&mut body).ok()?;
        body.lines()
            .find(|l| l.starts_with("orbd_fleet_connected_agents"))
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse::<f64>().ok())
            .map(|v| v as u64)
    }

    pub fn wait_connected(&self, n: usize) {
        wait_until(Duration::from_secs(20), "agents connected", || {
            self.connected_agents() == Some(n as u64)
        });
    }

    fn dispatch_cmd(&self, op: &str, orb: &str, extra: &[(&str, &str)]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_orbd"));
        c.arg("dispatch")
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("ORBD_DISPATCH_SOCKET", self.dispatch_socket())
            .env("ORBD_OFFERING_ID", &self.offering)
            .env("ORB_OPERATION", op)
            .env("ORB_ID", orb)
            .env("ORB_SANDBOX_ID", format!("sandbox-{orb}"))
            .env("AMP_URL", "https://ampcode.com")
            .env("AMP_THREAD_ID", format!("T-{orb}"))
            .env("AMP_SANDBOX_ID", format!("orb-provider:test:{orb}"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if op == "spawn" {
            c.env("ORB_IMAGE", "example.invalid/amp-orb:latest")
                .env("ORB_TOKEN", TOKEN)
                .env("ORB_ENVS", "AMP_URL ORB_TOKEN AMP_THREAD_ID AMP_SANDBOX_ID");
        }
        for (k, v) in extra {
            c.env(k, v);
        }
        c
    }

    /// Runs one job to completion.
    pub fn dispatch(&self, op: &str, orb: &str, extra: &[(&str, &str)]) -> Job {
        let out = self.dispatch_cmd(op, orb, extra).output().unwrap();
        Job {
            code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).into(),
            stderr: String::from_utf8_lossy(&out.stderr).into(),
        }
    }

    /// Starts a job in the background; join the handle for its result.
    pub fn dispatch_bg(
        &self,
        op: &str,
        orb: &str,
        extra: &[(&str, &str)],
    ) -> std::thread::JoinHandle<Job> {
        let child = self.dispatch_cmd(op, orb, extra).spawn().unwrap();
        std::thread::spawn(move || {
            let out = child.wait_with_output().unwrap();
            Job {
                code: out.status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&out.stdout).into(),
                stderr: String::from_utf8_lossy(&out.stderr).into(),
            }
        })
    }

    /// Every orb the fake runtime on `agent` has ever seen.
    pub fn fake_orbs(&self, agent: &str) -> Vec<FakeOrbState> {
        let state = self.root().join("agents").join(agent).join("state");
        let Ok(rd) = std::fs::read_dir(state.join("fake")) else {
            return vec![];
        };
        rd.filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            let id = OrbId::parse(name.strip_suffix(".json")?).ok()?;
            load_orb_state(&state, id).ok()?
        })
        .collect()
    }

    /// The single orb on `agent` (panics if there isn't exactly one).
    pub fn only_orb(&self, agent: &str) -> FakeOrbState {
        let mut orbs = self.fake_orbs(agent);
        assert_eq!(orbs.len(), 1, "expected one orb on {agent}");
        orbs.pop().unwrap()
    }

    pub fn logs(&self) -> String {
        let mut all = String::new();
        for e in std::fs::read_dir(self.root()).unwrap().flatten() {
            let p = e.path();
            if p.extension().is_some_and(|x| x == "log") {
                all.push_str(&std::fs::read_to_string(&p).unwrap_or_default());
            }
        }
        all
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if std::thread::panicking() {
            for e in std::fs::read_dir(self.root()).unwrap().flatten() {
                let p = e.path();
                if p.extension().is_some_and(|x| x == "log") {
                    let text = std::fs::read_to_string(&p).unwrap_or_default();
                    let tail: Vec<&str> = text.lines().rev().take(40).collect();
                    eprintln!("==== {} (last 40 lines)", p.display());
                    for l in tail.iter().rev() {
                        eprintln!("{l}");
                    }
                }
            }
        }
        self.kill_coordinator();
        let ids: Vec<String> = self.agents.keys().cloned().collect();
        for a in ids {
            self.kill_agent(&a);
        }
    }
}

pub fn wait_until(timeout: Duration, what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_exit(child: &mut Child, within: Duration) -> Option<std::process::ExitStatus> {
    let start = Instant::now();
    loop {
        if let Some(s) = child.try_wait().unwrap() {
            return Some(s);
        }
        if start.elapsed() > within {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub fn assert_ok(job: &Job, what: &str) {
    assert_eq!(job.code, 0, "{what} failed: stderr:\n{}", job.stderr);
}
