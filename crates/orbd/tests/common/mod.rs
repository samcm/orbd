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

/// A port no other cluster of this process was given, free when it was handed out. Taken
/// from below the ephemeral ranges (Linux 32768-60999, macOS 49152-65535): a port that
/// `bind(:0)` handed out and released can be any outbound connection's before the
/// coordinator binds it ("Address already in use").
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU32, Ordering};
    const FIRST: u32 = 20000;
    const PORTS: u32 = 12000;
    // A different start per test process, so concurrent test binaries rarely meet.
    static NEXT: AtomicU32 = AtomicU32::new(u32::MAX);
    let _ = NEXT.compare_exchange(
        u32::MAX,
        std::process::id().wrapping_mul(7919) % PORTS,
        Ordering::Relaxed,
        Ordering::Relaxed,
    );
    loop {
        let port = (FIRST + NEXT.fetch_add(1, Ordering::Relaxed) % PORTS) as u16;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
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
    /// The offering jobs are dispatched to.
    pub offering: String,
    /// When set, jobs come from this provider with orb types instead of `offering`.
    pub provider: Option<String>,
    /// Extra agent environment, applied last (e.g. `ORBD_CAPACITY_DISK_GIB`).
    pub agent_env: Vec<(String, String)>,
    /// Agents with their own config file instead of the shared `agent.yaml`.
    agent_configs: BTreeMap<String, PathBuf>,
    /// Agents run this `orbd` instead of the one under test (e.g. an older release).
    pub agent_bin: Option<PathBuf>,
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
            // A roomy simulated filesystem, so tests do not depend on the host's.
            "runtime:\n  fake:\n    memory_preserving_sleep: true\n    \
             disk: { free_gib: 1000, total_gib: 2000 }\n",
        )
    }

    /// A cluster whose single offering (2 vCPUs, 4 GiB disk; 2 GiB, or 1 GiB on a real VM
    /// runtime, whose checkpoints write it all) runs on `runtime`; every agent reads
    /// `agent_yaml` as its optional config file.
    pub fn with_runtime(
        agent_ids: &[&str],
        agent_orbs: u64,
        runtime: &str,
        agent_yaml: &str,
    ) -> Self {
        let (offering, memory_mib) = if runtime == "fake" {
            (OFFERING.to_owned(), 2048)
        } else {
            (format!("{runtime}-2vcpu-1gb"), 1024)
        };
        // On arm64 hosts (Macs) QEMU orbs are arm64 orbs.
        let arch = if runtime == "qemu" && cfg!(target_arch = "aarch64") {
            "\n    arch: arm64"
        } else {
            ""
        };
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let control_port = free_port();
        let metrics_port = free_port();
        write(
            &root.join("coordinator.yaml"),
            format!(
                r#"public_url: https://localhost:{control_port}
extra_urls: [https://127.0.0.1:{control_port}]
listen: 127.0.0.1:{control_port}
state_dir: {root}/coordinator
metrics_listen: 127.0.0.1:{metrics_port}
heartbeat_interval_ms: 300
running_orb_disk_headroom_gib: 1
offerings:
  - id: {offering}
    runtime: {runtime}
    resources: {{ vcpus: 2, memory_mib: {memory_mib}, disk_gib: 4 }}{arch}
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
            provider: None,
            // Real runtimes use the host's disk: keep little of it in reserve. amd64 QEMU
            // offerings give orbs /dev/kvm, which agents allow only when told to. Sleeps
            // are written at once unless a test holds them in RAM.
            agent_env: match runtime {
                "fake" => vec![],
                "qemu" if cfg!(target_arch = "x86_64") => vec![
                    ("ORBD_DISK_RESERVE_GIB".into(), "1".into()),
                    ("ORBD_NESTED_KVM".into(), "true".into()),
                    ("ORBD_SLEEP_HOLD_MINUTES".into(), "0".into()),
                ],
                _ => vec![
                    ("ORBD_DISK_RESERVE_GIB".into(), "1".into()),
                    ("ORBD_SLEEP_HOLD_MINUTES".into(), "0".into()),
                ],
            },
            agent_configs: BTreeMap::new(),
            agent_bin: None,
        }
    }

    /// Gives agent `id` its own config file (before it starts).
    pub fn set_agent_yaml(&mut self, id: &str, yaml: &str) {
        let path = self.root().join(format!("agent-{id}.yaml"));
        write(&path, yaml);
        self.agent_configs.insert(id.to_string(), path);
    }

    /// The coordinator's AgentControl port (on 127.0.0.1).
    pub fn control_port(&self) -> u16 {
        self.control_port
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

    /// `orbd orb list --json` as (Amp orb id, state, agent, orb type) rows.
    pub fn orb_rows(&self) -> Vec<(String, String, String, String)> {
        let list = self.admin(&["orb", "list", "--json"]);
        assert_ok(&list, "orb list --json");
        let v: serde_json::Value = serde_json::from_str(&list.stdout).unwrap();
        v["orbs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| {
                let f = |k: &str| o[k].as_str().unwrap().to_owned();
                (f("amp_orb_id"), f("state"), f("agent_id"), f("orb_type"))
            })
            .collect()
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
        let mut cmd = match &self.agent_bin {
            Some(bin) => Command::new(bin),
            None => Command::new(env!("CARGO_BIN_EXE_orbd")),
        };
        cmd.arg("agent")
            .arg("--state-dir")
            .arg(state)
            .arg("--config")
            .arg(
                self.agent_configs
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| self.root().join("agent.yaml")),
            )
            .env("ORBD_JOIN", token)
            .env("ORBD_CAPACITY_VCPUS", (2 * n).to_string())
            .env("ORBD_CAPACITY_MEMORY_MIB", (2048 * n).to_string())
            .env("RUST_LOG", "info,orbd=debug")
            .env("FAILPOINTS", failpoints)
            .envs(self.agent_env.iter().cloned())
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

    /// Starts agent `id` (minting its token) with extra environment on top of `agent_env`.
    pub fn start_agent_with_env(&mut self, id: &str, env: &[(&str, &str)]) {
        let token = self.join_token(id);
        self.tokens.insert(id.to_string(), token.clone());
        let child = self
            .agent_command(id, &token, "")
            .envs(env.iter().map(|(k, v)| (k.to_string(), v.to_string())))
            .spawn()
            .unwrap();
        self.agents.insert(id.to_string(), Some(child));
    }

    /// Starts agent `id` (minting its token) with `failpoints` and extra environment on
    /// top of `agent_env`.
    pub fn start_agent_with(&mut self, id: &str, failpoints: &str, env: &[(&str, &str)]) {
        let token = self.join_token(id);
        self.tokens.insert(id.to_string(), token.clone());
        let child = self
            .agent_command(id, &token, failpoints)
            .envs(env.iter().map(|(k, v)| (k.to_string(), v.to_string())))
            .spawn()
            .unwrap();
        self.agents.insert(id.to_string(), Some(child));
    }

    /// Starts an agent that is not part of the initial cluster, with a given token.
    pub fn start_agent_with_token(&mut self, id: &str, token: &str) {
        self.tokens.insert(id.to_string(), token.to_string());
        self.start_agent(id, "");
    }

    /// Starts the coordinator and all agents and waits until every agent is connected
    /// and has reported its disk (admission refuses an agent's first orb until then).
    pub fn start_all(&mut self) {
        self.start_coordinator("");
        let ids: Vec<String> = self.agents.keys().cloned().collect();
        for a in &ids {
            self.start_agent(a, "");
        }
        self.wait_connected(ids.len());
        wait_until(
            Duration::from_secs(20),
            "agents reported their disks",
            || {
                let m = self.metrics().unwrap_or_default();
                ids.iter()
                    .all(|a| m.contains(&format!("orbd_agent_disk_free_bytes{{agent=\"{a}\"}}")))
            },
        );
    }

    /// The pid of agent `id`, if it runs.
    pub fn agent_pid(&self, id: &str) -> Option<u32> {
        self.agents.get(id)?.as_ref().map(Child::id)
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
        let sent = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status();
        let status = wait_exit(&mut child, within);
        if status.is_none() {
            // Not left running: it would hold the agent's lock and state for every
            // test after this one.
            let _ = child.kill();
            let _ = child.wait();
            panic!("agent did not stop within {within:?} of SIGTERM (kill: {sent:?})");
        }
        status.unwrap()
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

    pub fn metrics(&self) -> Option<String> {
        let mut s = TcpStream::connect(("127.0.0.1", self.metrics_port)).ok()?;
        s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
        s.write_all(b"GET /metrics HTTP/1.0\r\nHost: x\r\n\r\n")
            .ok()?;
        let mut body = String::new();
        s.read_to_string(&mut body).ok()?;
        Some(body)
    }

    pub fn connected_agents(&self) -> Option<u64> {
        self.metrics()?
            .lines()
            .find(|l| l.starts_with("orbd_fleet_connected_agents"))
            .and_then(|l| l.split_whitespace().last())
            .and_then(|v| v.parse::<f64>().ok())
            .map(|v| v as u64)
    }

    /// Waits until `n` agents are connected and have each sent a heartbeat: placement
    /// refuses an agent whose disk it has not heard about yet.
    pub fn wait_connected(&self, n: usize) {
        wait_until(Duration::from_secs(20), "agents connected", || {
            let Some(m) = self.metrics() else {
                return false;
            };
            let connected = m
                .lines()
                .find(|l| l.starts_with("orbd_fleet_connected_agents"))
                .and_then(|l| l.split_whitespace().last())
                .and_then(|v| v.parse::<f64>().ok());
            let reported = m
                .lines()
                .filter(|l| l.starts_with("orbd_agent_disk_free_bytes{"))
                .count();
            connected == Some(n as f64) && reported >= n
        });
    }

    fn dispatch_cmd(&self, op: &str, orb: &str, extra: &[(&str, &str)]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_orbd"));
        c.arg("dispatch")
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("ORBD_DISPATCH_SOCKET", self.dispatch_socket())
            .env("ORB_OPERATION", op)
            .env("ORB_ID", orb)
            .env("ORB_SANDBOX_ID", format!("sandbox-{orb}"))
            .env("AMP_URL", "https://ampcode.com")
            .env("AMP_THREAD_ID", format!("T-{orb}"))
            .env("AMP_SANDBOX_ID", format!("orb-provider:test:{orb}"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match &self.provider {
            Some(p) => c.env("ORBD_PROVIDER_ID", p),
            None => c.env("ORBD_OFFERING_ID", &self.offering),
        };
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

    /// Waits until the coordinator has a command open for `amp_orb`: a job dispatched
    /// after this queues behind it, or attaches to it.
    pub fn wait_open_command(&self, amp_orb: &str) {
        wait_until(Duration::from_secs(10), "an open command", || {
            self.admin(&["orb", "show", amp_orb, "--json"])
                .stdout
                .contains("\"open_command\": {")
        });
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

    /// Ends the VM of the single orb on `agent` behind the agent's back, as a killed or
    /// crashed VMM would: the fake runtime then observes it stopped.
    pub fn kill_vm(&self, agent: &str) {
        let fake = self.root().join("agents").join(agent).join("state/fake");
        let file = std::fs::read_dir(&fake)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "json"))
            .unwrap();
        let mut st: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
        st["running"] = false.into();
        // Atomically: a running agent may be reading it.
        let tmp = file.with_extension("json.kill");
        std::fs::write(&tmp, serde_json::to_vec(&st).unwrap()).unwrap();
        std::fs::rename(&tmp, &file).unwrap();
    }

    /// The internal id of the orb Amp knows as `amp_orb`.
    pub fn orb_id(&self, amp_orb: &str) -> OrbId {
        let list = self.admin(&["orb", "list", "--json"]);
        assert_ok(&list, "orb list --json");
        let v: serde_json::Value = serde_json::from_str(&list.stdout).unwrap();
        let id = v["orbs"]
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["amp_orb_id"] == amp_orb)
            .unwrap_or_else(|| panic!("no orb {amp_orb}"))["orb_id"]
            .as_str()
            .unwrap()
            .to_owned();
        OrbId::parse(&id).unwrap()
    }

    /// The fake state of the orb Amp knows as `amp_orb`, on `agent`.
    pub fn fake_orb(&self, agent: &str, amp_orb: &str) -> FakeOrbState {
        let state = self.root().join("agents").join(agent).join("state");
        load_orb_state(&state, self.orb_id(amp_orb))
            .unwrap()
            .expect("orb on the agent")
    }

    /// Edits the fake state of `amp_orb` on `agent`. Stop the agent first.
    fn edit_fake_orb(&self, agent: &str, amp_orb: &str, edit: impl FnOnce(&mut serde_json::Value)) {
        let file = self
            .root()
            .join("agents")
            .join(agent)
            .join("state/fake")
            .join(format!("{}.json", self.orb_id(amp_orb)));
        let mut st: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
        edit(&mut st);
        std::fs::write(&file, serde_json::to_vec(&st).unwrap()).unwrap();
    }

    /// Moves the save time of `amp_orb`'s memory checkpoints on `agent` back by `by`, as
    /// if it had slept that much longer. Stop the agent first.
    pub fn age_checkpoints(&self, agent: &str, amp_orb: &str, by: Duration) {
        self.edit_fake_orb(agent, amp_orb, |st| {
            for saved in st["checkpoint_saved_unix_ms"]
                .as_object_mut()
                .unwrap()
                .values_mut()
            {
                *saved = (saved.as_u64().unwrap() - by.as_millis() as u64).into();
            }
        });
    }

    /// Makes `amp_orb`'s memory checkpoints on `agent` disappear (consumed), as a
    /// checkpoint its runtime discarded. Stop the agent first.
    pub fn lose_checkpoints(&self, agent: &str, amp_orb: &str) {
        self.edit_fake_orb(agent, amp_orb, |st| {
            st["consumed"] = st["checkpoints"].clone();
        });
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
