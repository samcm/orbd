//! Boat backend through real processes: a coordinator with `runtime: boat` against a
//! fake Boat API, jobs run through `orbd dispatch`.

mod common;
use common::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

const API_KEY: &str = "boat_test-key";

#[derive(Default)]
struct Sandbox {
    state: String,
    create_body: Value,
}

#[derive(Default)]
struct FakeBoat {
    sandboxes: BTreeMap<String, Sandbox>,
    /// Idempotency key -> sandbox id.
    keys: BTreeMap<String, String>,
    /// Every request: "METHOD /path".
    calls: Vec<String>,
    /// (sandbox, path, content) of every file write.
    files: Vec<(String, String, String)>,
    /// (sandbox, command, detached) of every command.
    commands: Vec<(String, String, bool)>,
    /// Close the connection instead of answering the next create (it still happens).
    lose_next_create_response: bool,
}

fn respond(s: &mut TcpStream, status: u16, body: Value) {
    let body = body.to_string();
    let _ = write!(
        s,
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
}

fn error(code: &str) -> Value {
    json!({ "ok": false, "code": code, "message": code })
}

fn sandbox_json(id: &str, sb: &Sandbox) -> Value {
    json!({ "id": id, "state": sb.state, "labels": sb.create_body["labels"] })
}

impl FakeBoat {
    /// Serves the fake API on a free port; returns its base URL.
    fn start() -> (Arc<Mutex<FakeBoat>>, String) {
        let fake = Arc::new(Mutex::new(FakeBoat::default()));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/api/v1", listener.local_addr().unwrap());
        let f = fake.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming().flatten() {
                let f = f.clone();
                std::thread::spawn(move || handle(&f, conn));
            }
        });
        (fake, url)
    }
}

fn handle(fake: &Mutex<FakeBoat>, mut conn: TcpStream) {
    let mut reader = BufReader::new(conn.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let mut parts = line.split_whitespace();
    let (method, target) = (
        parts.next().unwrap_or("").to_owned(),
        parts.next().unwrap_or("").to_owned(),
    );
    let mut headers = BTreeMap::new();
    loop {
        let mut h = String::new();
        reader.read_line(&mut h).unwrap();
        if h.trim().is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_owned());
        }
    }
    let len: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0; len];
    reader.read_exact(&mut body).unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    if headers.get("authorization").map(String::as_str) != Some(&format!("Bearer {API_KEY}")) {
        return respond(&mut conn, 401, error("unauthorized"));
    }
    let (path, query) = target.split_once('?').unwrap_or((&target, ""));
    let path = path.strip_prefix("/api/v1").unwrap_or(path).to_owned();
    let seg: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let mut f = fake.lock().unwrap();
    f.calls.push(format!("{method} {path}"));
    match (method.as_str(), seg.as_slice()) {
        ("POST", ["sandboxes"]) => {
            let key = headers.get("idempotency-key").cloned().unwrap_or_default();
            assert!(!key.is_empty(), "create without an idempotency key");
            let id = match f.keys.get(&key) {
                Some(id) => id.clone(),
                None => {
                    let id = format!("bx_{:08}", f.sandboxes.len() + 2);
                    f.sandboxes.insert(
                        id.clone(),
                        Sandbox {
                            state: "provisioning".into(),
                            create_body: body,
                        },
                    );
                    f.keys.insert(key, id.clone());
                    id
                }
            };
            if std::mem::take(&mut f.lose_next_create_response) {
                return; // connection closes unanswered
            }
            let sb = sandbox_json(&id, &f.sandboxes[&id]);
            respond(&mut conn, 202, json!({ "ok": true, "sandbox": sb }));
        }
        ("GET", ["sandboxes"]) => {
            let want: Vec<(String, String)> = query
                .split('&')
                .filter_map(|kv| kv.strip_prefix("label="))
                .map(|l| {
                    let l = l.replace("%3D", "=").replace("%3d", "=");
                    let (k, v) = l.split_once('=').unwrap();
                    (k.to_owned(), v.to_owned())
                })
                .collect();
            let list: Vec<Value> = f
                .sandboxes
                .iter()
                .filter(|(_, sb)| {
                    want.iter()
                        .all(|(k, v)| sb.create_body["labels"][k].as_str() == Some(v.as_str()))
                })
                .map(|(id, sb)| sandbox_json(id, sb))
                .collect();
            respond(
                &mut conn,
                200,
                json!({ "ok": true, "sandboxes": list, "pageInfo": { "hasMore": false } }),
            );
        }
        ("GET", ["sandboxes", id]) => {
            let Some(sb) = f.sandboxes.get_mut(*id) else {
                return respond(&mut conn, 404, error("not_found"));
            };
            let shown = sandbox_json(id, sb);
            // Each state in progress is seen once, then completes.
            sb.state = match sb.state.as_str() {
                "provisioning" => "ready".into(),
                "archiving" => "archived".into(),
                s => s.into(),
            };
            respond(&mut conn, 200, json!({ "ok": true, "sandbox": shown }));
        }
        ("POST", ["sandboxes", id, action]) => {
            let Some(sb) = f.sandboxes.get_mut(*id) else {
                return respond(&mut conn, 404, error("not_found"));
            };
            sb.state = match *action {
                "stop" => "archiving".into(),
                "resume" => "provisioning".into(),
                "commands" => {
                    let id = id.to_string();
                    let cmd = body["command"].as_str().unwrap().to_owned();
                    let detached = body["detached"].as_bool().unwrap_or(false);
                    f.commands.push((id, cmd, detached));
                    return if detached {
                        respond(&mut conn, 200, json!({ "ok": true, "processId": 7 }))
                    } else {
                        respond(
                            &mut conn,
                            200,
                            json!({ "ok": true, "exitCode": 0, "stdout": "0.0.1 (fake)\n",
                                    "stderr": "", "timedOut": false }),
                        )
                    };
                }
                other => panic!("unexpected action {other}"),
            };
            respond(&mut conn, 202, json!({ "ok": true }));
        }
        ("GET", ["sandboxes", _, "commands", "7"]) => respond(
            &mut conn,
            200,
            json!({ "ok": true, "running": true, "exitCode": null, "stdout": "", "stderr": "" }),
        ),
        ("PUT", ["sandboxes", id, "files"]) => {
            use base64::Engine;
            let content = base64::engine::general_purpose::STANDARD
                .decode(body["content"].as_str().unwrap())
                .unwrap();
            let entry = (
                id.to_string(),
                body["path"].as_str().unwrap().to_owned(),
                String::from_utf8(content).unwrap(),
            );
            f.files.push(entry);
            respond(&mut conn, 200, json!({ "ok": true }));
        }
        ("DELETE", ["sandboxes", id]) => {
            assert_eq!(
                headers.get("x-ascii-confirm-delete").map(String::as_str),
                Some(*id)
            );
            if f.sandboxes.remove(*id).is_none() {
                return respond(&mut conn, 404, error("not_found"));
            }
            respond(&mut conn, 202, json!({ "ok": true }));
        }
        _ => respond(&mut conn, 404, error("not_found")),
    }
}

/// A cluster with no agents whose only offering runs on the fake Boat at `url`.
fn boat_cluster(url: &str) -> Cluster {
    let mut c = Cluster::new(&[], 0);
    let key = c.root().join("boat-key");
    std::fs::write(&key, format!("{API_KEY}\n")).unwrap();
    let path = c.root().join("coordinator.yaml");
    let yaml = std::fs::read_to_string(&path).unwrap();
    let head = yaml.split("offerings:").next().unwrap();
    std::fs::write(
        &path,
        format!(
            "{head}boat:\n  api_key_file: {}\n  capacity: {{ vcpus: 4, memory_mib: 8192 }}\n  \
             auto_stop_secs: 7200\n  api_url: {url}\n\
             offerings:\n  - id: boat-2vcpu-4gb\n    runtime: boat\n    \
             resources: {{ vcpus: 2, memory_mib: 4096, disk_gib: 51 }}\n",
            key.display()
        ),
    )
    .unwrap();
    c.offering = "boat-2vcpu-4gb".into();
    c
}

/// Spawn (create, with its response lost once) → spawn again → pause → spawn
/// (resume) → delete, and the orb token reaches the sandbox only in its env file.
#[test]
fn boat_orb_lifecycle() {
    let (fake, url) = FakeBoat::start();
    let mut c = boat_cluster(&url);
    c.start_coordinator("");
    fake.lock().unwrap().lose_next_create_response = true;

    let first = c.dispatch("spawn", "amp-boat-1", &[]);
    assert_ok(&first, "first spawn");
    assert!(
        first.stdout.contains("creating a small Boat sandbox"),
        "no setup log:\n{}",
        first.stdout
    );
    {
        let f = fake.lock().unwrap();
        assert_eq!(
            f.sandboxes.len(),
            1,
            "a lost create response made a second sandbox"
        );
        let (id, sb) = f.sandboxes.iter().next().unwrap();
        assert_eq!(sb.state, "ready");
        let b = &sb.create_body;
        assert_eq!(b["type"], "small");
        assert_eq!(b["noEnv"], true);
        assert_eq!(b["ttlSeconds"], 7200);
        assert_eq!(b["labels"]["amp-orb"], "amp-boat-1");
        let env: Vec<_> = f.files.iter().filter(|(s, _, _)| s == id).collect();
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].1, "/home/user/.config/amp/orbd-bootstrap.env");
        assert!(env[0].2.contains(&format!("ORB_TOKEN=\"{TOKEN}\"")));
        let detached: Vec<_> = f.commands.iter().filter(|(_, _, d)| *d).collect();
        assert_eq!(detached.len(), 1);
        assert!(detached[0].1.ends_with("exec amp orb bootstrap"));
    }

    assert_ok(
        &c.dispatch("spawn", "amp-boat-1", &[]),
        "spawn of a running orb",
    );
    assert_ok(&c.dispatch("pause", "amp-boat-1", &[]), "pause");
    {
        let f = fake.lock().unwrap();
        assert_eq!(f.sandboxes.values().next().unwrap().state, "archived");
        assert_eq!(f.calls.iter().filter(|c| c.ends_with("/stop")).count(), 1);
    }

    assert_ok(&c.dispatch("spawn", "amp-boat-1", &[]), "spawn after pause");
    {
        let f = fake.lock().unwrap();
        assert_eq!(f.sandboxes.len(), 1);
        assert_eq!(f.sandboxes.values().next().unwrap().state, "ready");
        assert_eq!(f.calls.iter().filter(|c| c.ends_with("/resume")).count(), 1);
        assert_eq!(
            f.calls.iter().filter(|c| *c == "POST /sandboxes").count(),
            2
        );
        assert_eq!(f.commands.iter().filter(|(_, _, d)| *d).count(), 3);
    }

    assert_ok(&c.dispatch("delete", "amp-boat-1", &[]), "delete");
    let f = fake.lock().unwrap();
    assert!(f.sandboxes.is_empty());
    for (_, cmd, _) in &f.commands {
        assert!(!cmd.contains(TOKEN), "orb token in a command line");
    }
    assert!(!c.logs().contains(TOKEN), "orb token in a log");
}

/// An agent with room for one orb, and the offering spills to Boat: the second new orb
/// goes to Boat, and wakes there again after a pause even once the agent has room.
#[test]
fn new_orbs_spill_to_boat_only_when_the_agents_are_full() {
    let (fake, url) = FakeBoat::start();
    let mut c = Cluster::new(&["agent-a"], 1);
    let key = c.root().join("boat-key");
    std::fs::write(&key, format!("{API_KEY}\n")).unwrap();
    let path = c.root().join("coordinator.yaml");
    let yaml = std::fs::read_to_string(&path).unwrap();
    let offering = c.offering.clone();
    let head = yaml.split("offerings:").next().unwrap();
    std::fs::write(
        &path,
        format!(
            "{head}boat:\n  api_key_file: {}\n  capacity: {{ vcpus: 4, memory_mib: 8192 }}\n  \
             auto_stop_secs: 7200\n  api_url: {url}\n\
             offerings:\n  - id: {offering}\n    runtime: fake\n    \
             resources: {{ vcpus: 2, memory_mib: 2048, disk_gib: 4 }}\n    \
             spill_to: [boat-2vcpu-4gb]\n  - id: boat-2vcpu-4gb\n    runtime: boat\n    \
             resources: {{ vcpus: 2, memory_mib: 4096, disk_gib: 51 }}\n",
            key.display()
        ),
    )
    .unwrap();
    c.start_all();

    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "first spawn");
    assert_eq!(
        c.fake_orbs("agent-a").len(),
        1,
        "the first orb is on the agent"
    );
    assert!(fake.lock().unwrap().sandboxes.is_empty());

    let second = c.dispatch("spawn", "amp-2", &[]);
    assert_ok(&second, "second spawn");
    assert!(
        second.stdout.contains("creating a small Boat sandbox"),
        "the second orb did not spill:\n{}{}",
        second.stdout,
        second.stderr
    );
    assert_eq!(c.fake_orbs("agent-a").len(), 1);
    assert_eq!(fake.lock().unwrap().sandboxes.len(), 1);

    // Room on the agent again; the spilled orb still lives on Boat.
    assert_ok(
        &c.dispatch("delete", "amp-1", &[]),
        "delete the agent's orb",
    );
    assert_ok(&c.dispatch("pause", "amp-2", &[]), "pause the spilled orb");
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "wake the spilled orb");
    {
        let f = fake.lock().unwrap();
        assert_eq!(f.sandboxes.values().next().unwrap().state, "ready");
        assert_eq!(f.calls.iter().filter(|c| c.ends_with("/resume")).count(), 1);
    }
    assert!(
        c.fake_orbs("agent-a").iter().all(|o| !o.running),
        "the spilled orb woke on the agent"
    );
    assert_ok(
        &c.dispatch("delete", "amp-2", &[]),
        "delete the spilled orb",
    );
    assert!(fake.lock().unwrap().sandboxes.is_empty());
}
