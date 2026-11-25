//! Agent enrollment and removal through real processes.

mod common;
use common::*;
use std::process::Stdio;
use std::time::Duration;

/// Runs an agent that is expected to give up at once; returns its log.
fn refused_agent(c: &Cluster, id: &str, token: &str) -> String {
    let status = c
        .agent_command(id, token, "")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!status.status.success(), "agent {id} must not start");
    String::from_utf8_lossy(&status.stderr).into_owned()
}

/// A join token enrolls one agent, once, before it expires; and is never logged.
#[test]
fn join_tokens_are_single_use_and_expire() {
    let mut c = Cluster::new(&["agent-a"], 1);
    c.start_all();

    // Reuse: a second host presenting agent-a's token (its own, different key).
    let token = c.join_token("agent-b");
    c.start_agent_with_token("agent-b", &token);
    c.wait_connected(2);
    let err = refused_agent(&c, "agent-b-copy", &token);
    assert!(err.contains("already used"), "{err}");

    let j = c.admin(&["join-token", "agent-c", "--ttl-secs", "1"]);
    assert_ok(&j, "join-token");
    std::thread::sleep(Duration::from_millis(1500));
    let err = refused_agent(&c, "agent-c", &j.stdout);
    assert!(err.contains("expired"), "{err}");

    // An id that is already enrolled gets no new token.
    let again = c.admin(&["join-token", "agent-a"]);
    assert_ne!(again.code, 0);
    assert!(
        again.stderr.contains("already registered"),
        "{}",
        again.stderr
    );

    assert_ok(
        &c.dispatch("spawn", "amp-1", &[]),
        "spawn after enrollments",
    );
    assert!(!c.logs().contains(&token), "join token leaked into logs");
}

/// An agent can dial any of the coordinator's URLs (`public_url` or `extra_urls`): one
/// picked for its join token, or one set later with `ORBD_COORDINATOR_URL`, without
/// enrolling again. The coordinator's certificate names every host.
#[test]
fn agents_dial_any_coordinator_url() {
    let mut c = Cluster::new(&["agent-a"], 1);
    c.start_all();
    let extra = format!("https://127.0.0.1:{}", c.control_port());

    let bad = c.admin(&[
        "join-token",
        "agent-b",
        "--url",
        "https://elsewhere.example:1",
    ]);
    assert_ne!(bad.code, 0);
    assert!(bad.stderr.contains("extra_urls"), "{}", bad.stderr);

    let j = c.admin(&["join-token", "agent-b", "--url", &extra]);
    assert_ok(&j, "join-token --url");
    c.start_agent_with_token("agent-b", &j.stdout);
    c.wait_connected(2);

    c.terminate_agent("agent-a", Duration::from_secs(30));
    c.wait_connected(1);
    c.agent_env
        .push(("ORBD_COORDINATOR_URL".into(), extra.clone()));
    c.start_agent("agent-a", "");
    c.wait_connected(2);
    assert!(
        c.logs()
            .contains("dialing the coordinator at an override URL"),
        "agent-a did not use ORBD_COORDINATOR_URL"
    );
}

/// An overflow agent (`ORBD_OVERFLOW`) gets a new orb only when no other agent fits it,
/// even with more room than they have.
#[test]
fn overflow_agents_take_new_orbs_only_when_the_others_are_full() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.start_all();
    c.start_agent_with_env(
        "spare",
        &[
            ("ORBD_OVERFLOW", "true"),
            ("ORBD_CAPACITY_VCPUS", "16"),
            ("ORBD_CAPACITY_MEMORY_MIB", "16384"),
        ],
    );
    c.wait_connected(2);
    wait_until(Duration::from_secs(20), "spare reported its disk", || {
        c.metrics()
            .unwrap_or_default()
            .contains("orbd_agent_disk_free_bytes{agent=\"spare\"}")
    });
    let list = c.admin(&["agent", "list"]);
    assert!(
        list.stdout.contains("spare\tconnected (overflow)"),
        "{}",
        list.stdout
    );

    for orb in ["amp-1", "amp-2"] {
        assert_ok(&c.dispatch("spawn", orb, &[]), orb);
    }
    assert_eq!(c.fake_orbs("agent-a").len(), 2, "agent-a fills first");
    assert!(c.fake_orbs("spare").is_empty(), "spare stays empty");

    assert_ok(&c.dispatch("spawn", "amp-3", &[]), "amp-3");
    assert_eq!(
        c.fake_orbs("spare").len(),
        1,
        "agent-a is full: spare takes it"
    );
}

/// Agents report their architecture and orbs only go to agents of theirs. An amd64
/// offering spilling to an arm64 one puts a new orb on the arm64 agent once the amd64
/// one is full, and the orb stays there; an arm64 offering never uses an amd64 agent.
#[test]
fn new_orbs_spill_across_architectures_and_stay_on_theirs() {
    let mut c = Cluster::new(&["amd"], 1);
    let path = c.root().join("coordinator.yaml");
    let yaml = std::fs::read_to_string(&path).unwrap();
    let head = yaml.split("offerings:").next().unwrap();
    let fake = "runtime: fake\n    resources: { vcpus: 2, memory_mib: 2048, disk_gib: 4 }";
    std::fs::write(
        &path,
        format!(
            "{head}offerings:\n  - id: {OFFERING}\n    {fake}\n    spill_to: [{OFFERING}@arm64]\n\
             \x20 - id: {OFFERING}@arm64\n    {fake}\n    arch: arm64\n\
             \x20 - id: arm-only\n    {fake}\n    arch: arm64\n"
        ),
    )
    .unwrap();
    c.set_agent_yaml(
        "mac",
        "runtime:\n  fake:\n    memory_preserving_sleep: true\n    arch: arm64\n    \
         disk: { free_gib: 1000, total_gib: 2000 }\n",
    );
    c.start_all();
    c.start_agent_with_env(
        "mac",
        &[
            ("ORBD_CAPACITY_VCPUS", "4"),
            ("ORBD_CAPACITY_MEMORY_MIB", "4096"),
        ],
    );
    c.wait_connected(2);
    // Orbs on an agent that still have a disk.
    let orbs = |c: &Cluster, agent: &str| c.fake_orbs(agent).iter().filter(|o| o.disk).count();

    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1");
    assert_eq!(orbs(&c, "amd"), 1, "amd64 first");
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "amp-2");
    assert_eq!(orbs(&c, "mac"), 1, "amd is full: the arm64 agent takes it");

    // Room on amd again; the arm64 orb wakes where it lives.
    assert_ok(&c.dispatch("delete", "amp-1", &[]), "delete amp-1");
    assert_ok(&c.dispatch("pause", "amp-2", &[]), "pause amp-2");
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "wake amp-2");
    assert!(c.only_orb("mac").running, "amp-2 woke on mac");

    // arm64 only: the Mac, never amd, even with the Mac full and amd empty.
    c.offering = "arm-only".into();
    assert_ok(&c.dispatch("spawn", "amp-3", &[]), "amp-3");
    assert_eq!(orbs(&c, "mac"), 2);
    let full = c.dispatch("spawn", "amp-4", &[]);
    assert_ne!(full.code, 0, "amp-4 must not be placed:\n{}", full.stdout);
    assert_eq!(orbs(&c, "amd"), 0, "an arm64 orb went to amd");
}

/// `orbd agent remove` cuts the agent off for good: its session ends, it cannot
/// reconnect, and its orbs stay recorded on it (unavailable, never moved elsewhere).
#[test]
fn removed_agent_cannot_connect_and_keeps_its_orbs() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 1);
    c.start_all();
    assert_ok(&c.dispatch("spawn", "amp-x", &[]), "spawn x");
    let home = if c.fake_orbs("agent-a").is_empty() {
        "agent-b"
    } else {
        "agent-a"
    };

    assert_ok(&c.admin(&["agent", "remove", home]), "agent remove");
    c.wait_connected(1);
    let list = c.admin(&["agent", "list"]);
    assert!(
        list.stdout.contains(&format!("{home}\tremoved")),
        "{}",
        list.stdout
    );

    // The still-running agent process keeps retrying and is refused every time.
    wait_until(Duration::from_secs(20), "the removed agent refused", || {
        c.logs().contains("not an active agent")
    });
    assert_eq!(c.connected_agents(), Some(1));

    let job = c.dispatch("spawn", "amp-x", &[]);
    assert_eq!(job.code, 75, "orb on a removed agent: {}", job.stderr);
    assert!(job.stderr.contains("BackendUnavailable"), "{}", job.stderr);
    let other = if home == "agent-a" {
        "agent-b"
    } else {
        "agent-a"
    };
    assert!(c.fake_orbs(other).is_empty(), "orb must not be re-placed");

    let rejoin = c.admin(&["join-token", home]);
    assert_ne!(rejoin.code, 0, "a removed id is never reused");
}

/// `ORBD_IMAGE_ALLOW` is the agent's own: a spawn of another image fails naming the
/// setting before the runtime sees it, whatever the coordinator placed there.
#[test]
fn agents_refuse_images_their_allowlist_does_not_name() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.agent_env.push((
        "ORBD_IMAGE_ALLOW".into(),
        "example.invalid/amp-orb:stable, example.invalid/tools/*".into(),
    ));
    c.start_all();

    // The cluster's spawns name example.invalid/amp-orb:latest.
    let refused = c.dispatch("spawn", "amp-1", &[]);
    assert_ne!(refused.code, 0, "{}", refused.stdout);
    let out = format!("{}{}", refused.stdout, refused.stderr);
    assert!(
        out.contains(
            "example.invalid/amp-orb:latest is not allowed on this agent (ORBD_IMAGE_ALLOW)"
        ),
        "{out}"
    );
    assert!(
        c.fake_orbs("agent-a").iter().all(|o| !o.disk),
        "the runtime never created a disk"
    );

    for (orb, image) in [
        ("amp-2", "example.invalid/amp-orb:stable"),
        ("amp-3", "example.invalid/tools/orb:1"),
    ] {
        assert_ok(&c.dispatch("spawn", orb, &[("ORB_IMAGE", image)]), image);
    }
    assert!(
        c.logs()
            .contains("only images ORBD_IMAGE_ALLOW lists are prepared and run")
    );
}

/// An orb type with restricted egress (`egress`) goes only to agents that enforce it
/// (here a fake runtime with `egress_policy`), never to one that does not (as Docker and
/// macOS agents, and fake runtimes by default), and an orb of it does not wake on its
/// host once that host no longer enforces it. Orb types without `egress` go anywhere.
#[test]
fn restricted_egress_goes_only_to_agents_that_enforce_it() {
    let mut c = Cluster::new(&["open"], 4);
    let path = c.root().join("coordinator.yaml");
    let yaml = std::fs::read_to_string(&path).unwrap();
    let head = yaml.split("offerings:").next().unwrap();
    let fake = "runtime: fake\n    resources: { vcpus: 2, memory_mib: 2048, disk_gib: 4 }";
    std::fs::write(
        &path,
        format!(
            "{head}offerings:\n  - id: {OFFERING}\n    {fake}\n    \
             egress: {{ domains: [ampcode.com, \"*.ampcode.com\"], allow: [\"tcp:203.0.113.0/24:443\"] }}\n\
             \x20 - id: plain\n    {fake}\n"
        ),
    )
    .unwrap();
    let agent_yaml = |enforces: bool| {
        format!(
            "runtime:\n  fake:\n    memory_preserving_sleep: true\n    egress_policy: {enforces}\n    \
             disk: {{ free_gib: 1000, total_gib: 2000 }}\n"
        )
    };
    c.set_agent_yaml("open", &agent_yaml(false));
    c.start_all();

    let refused = c.dispatch("spawn", "amp-1", &[]);
    assert_ne!(
        refused.code, 0,
        "a restricted orb went to an agent that does not enforce it"
    );
    assert!(c.fake_orbs("open").is_empty());
    c.offering = "plain".into();
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "unrestricted");
    assert_eq!(c.fake_orbs("open").len(), 1);

    c.set_agent_yaml("strict", &agent_yaml(true));
    c.start_agent("strict", "");
    c.wait_connected(2);
    wait_until(Duration::from_secs(20), "strict reported its disk", || {
        c.metrics()
            .unwrap_or_default()
            .contains("orbd_agent_disk_free_bytes{agent=\"strict\"}")
    });
    c.offering = OFFERING.into();
    assert_ok(&c.dispatch("spawn", "amp-3", &[]), "restricted");
    assert_eq!(c.fake_orbs("strict").len(), 1, "the agent that enforces it");
    assert_eq!(c.fake_orbs("open").len(), 1, "only the unrestricted orb");

    // Its host stops enforcing it (e.g. downgraded): it stays asleep there.
    assert_ok(&c.dispatch("pause", "amp-3", &[]), "pause");
    c.kill_agent("strict");
    c.set_agent_yaml("strict", &agent_yaml(false));
    c.start_agent("strict", "");
    c.wait_connected(2);
    let job = c.dispatch("spawn", "amp-3", &[]);
    assert_ne!(
        job.code, 0,
        "a restricted orb woke on a host that does not enforce it"
    );
    let out = format!("{}{}", job.stdout, job.stderr);
    assert!(out.contains("restricted egress"), "{out}");
    assert!(!c.only_orb("strict").running);
    assert_ok(&c.dispatch("delete", "amp-3", &[]), "delete");
}

/// The coordinator's log line for a new orb that names a project snapshot but starts
/// from its image.
const SNAPSHOT_MISS: &str = "project snapshot miss: the new orb starts from its image";

/// The coordinator's count of new orbs naming a project snapshot, by `result`.
fn placements(c: &Cluster, result: &str) -> u64 {
    let want = format!("orbd_project_snapshot_placements_total{{result=\"{result}\"}} ");
    c.metrics()
        .unwrap_or_default()
        .lines()
        .find_map(|l| l.strip_prefix(&want)?.parse().ok())
        .unwrap_or(0)
}

/// A new orb that names a project snapshot goes to a host holding it; the coordinator
/// counts each such placement by whether it hit, missed because the hosts holding the
/// snapshot could not take the orb (one stopped, one full), or missed because none
/// holds it, and logs each miss. An agent's deletions reach the coordinator.
#[test]
fn project_snapshot_placements_are_counted() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 2);
    c.start_all();
    c.wait_connected(2);
    let snap = [("ORB_SNAPSHOT_ID", "project-1")];
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1");
    assert_ok(&c.dispatch("pause", "amp-1", &snap), "publish project-1");
    let (holder, other) = match c.fake_orbs("agent-a").is_empty() {
        true => ("agent-b", "agent-a"),
        false => ("agent-a", "agent-b"),
    };

    assert_ok(&c.dispatch("spawn", "amp-2", &snap), "amp-2");
    assert!(
        c.fake_orb(holder, "amp-2").running,
        "amp-2 goes where project-1 is"
    );
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "wake amp-1");
    assert_ok(&c.dispatch("spawn", "amp-3", &snap), "amp-3");
    assert!(
        c.fake_orb(other, "amp-3").running,
        "{holder} is full (amp-1, amp-2): amp-3 goes to {other}"
    );
    assert_ok(
        &c.dispatch("spawn", "amp-4", &[("ORB_SNAPSHOT_ID", "project-2")]),
        "amp-4",
    );
    c.terminate_agent(holder, Duration::from_secs(30));
    c.wait_connected(1);
    assert_ok(&c.dispatch("delete", "amp-3", &[]), "delete amp-3");
    assert_ok(&c.dispatch("spawn", "amp-5", &snap), "amp-5");
    assert!(
        c.fake_orb(other, "amp-5").running,
        "amp-5 goes to {other}: {holder} is gone"
    );

    let got: Vec<u64> = ["hit", "elsewhere", "absent", "unsupported"]
        .iter()
        .map(|r| placements(&c, r))
        .collect();
    assert_eq!(got, [1, 2, 1, 0], "hit, elsewhere, absent, unsupported");
    assert_eq!(
        c.logs().matches(SNAPSHOT_MISS).count(),
        3,
        "one log line per miss"
    );
}

/// The hosts that hold a project snapshot count whatever keeps them from taking the
/// orb: an overflow agent that holds it is passed over while another agent fits, and an
/// agent of another architecture (a spill target's) holds it for none of the first
/// offering's orbs.
#[test]
fn snapshot_misses_count_overflow_and_other_architectures() {
    let mut c = Cluster::new(&["amd"], 1);
    let path = c.root().join("coordinator.yaml");
    let yaml = std::fs::read_to_string(&path).unwrap();
    let head = yaml.split("offerings:").next().unwrap();
    let fake = "runtime: fake\n    resources: { vcpus: 2, memory_mib: 2048, disk_gib: 4 }";
    std::fs::write(
        &path,
        format!(
            "{head}offerings:\n  - id: {OFFERING}\n    {fake}\n    spill_to: [{OFFERING}@arm64]\n\
             \x20 - id: {OFFERING}@arm64\n    {fake}\n    arch: arm64\n"
        ),
    )
    .unwrap();
    c.set_agent_yaml(
        "mac",
        "runtime:\n  fake:\n    memory_preserving_sleep: true\n    arch: arm64\n    \
         disk: { free_gib: 1000, total_gib: 2000 }\n",
    );
    c.start_all();
    c.start_agent_with_env(
        "mac",
        &[
            ("ORBD_OVERFLOW", "true"),
            ("ORBD_CAPACITY_VCPUS", "4"),
            ("ORBD_CAPACITY_MEMORY_MIB", "4096"),
        ],
    );
    c.wait_connected(2);
    let snap = [("ORB_SNAPSHOT_ID", "project-1")];

    // amd is full: amp-2 spills to the (overflow, arm64) Mac, which publishes project-1.
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1");
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "amp-2");
    assert!(c.fake_orb("mac", "amp-2").running, "amp-2 spilled to mac");
    assert_ok(&c.dispatch("pause", "amp-2", &snap), "publish project-1");
    // Room on amd again: a new orb naming project-1 goes there, not to the Mac.
    assert_ok(&c.dispatch("delete", "amp-1", &[]), "delete amp-1");
    assert_ok(&c.dispatch("spawn", "amp-3", &snap), "amp-3");
    assert!(c.fake_orb("amd", "amp-3").running, "amp-3 goes to amd");
    assert_eq!(placements(&c, "elsewhere"), 1);
    assert_eq!(placements(&c, "absent"), 0);
}

/// `log` without its colours.
fn plain(log: &str) -> String {
    let mut out = String::with_capacity(log.len());
    let mut chars = log.chars();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            for x in chars.by_ref() {
                if x == 'm' {
                    break;
                }
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// Heartbeats the coordinator has received from its agents.
fn heartbeats(c: &Cluster) -> u64 {
    c.metrics()
        .unwrap_or_default()
        .lines()
        .find_map(|l| {
            l.strip_prefix("orbd_fleet_messages_total{type=\"heartbeat\"} ")?
                .parse()
                .ok()
        })
        .unwrap_or(0)
}

/// Whether the coordinator would let `amp_orb` start on its agent now.
fn wakes_now(c: &Cluster, amp_orb: &str) -> bool {
    let list = c.admin(&["orb", "list", "--json"]);
    let v: serde_json::Value = serde_json::from_str(&list.stdout).unwrap_or_default();
    v["orbs"].as_array().into_iter().flatten().any(|o| {
        o["amp_orb_id"] == amp_orb && o["wake_blockers"].as_array().is_some_and(|b| b.is_empty())
    })
}

/// A sleeping orb's wake is admitted net of what its agent reports the wake frees:
/// here a checkpoint a restore reads into memory and deletes (as QEMU's), whose room
/// the orb's next checkpoint takes again. The agent's disk keeps room for one more
/// orb's growth (1 GiB of headroom) but not for its growth and a second checkpoint
/// (2 GiB). Where the restored VM keeps its checkpoint until its next sleep is written
/// (as Firecracker's), its wake frees nothing and needs both.
#[test]
fn a_wake_needs_room_for_a_second_checkpoint_only_where_the_first_stays() {
    for keeps in [false, true] {
        let mut c = Cluster::with_runtime(
            &["agent-a"],
            2,
            "fake",
            &format!(
                "runtime:\n  fake:\n    memory_preserving_sleep: true\n    \
                 restore_keeps_checkpoint: {keeps}\n    disk: {{ free_gib: 6, total_gib: 100 }}\n"
            ),
        );
        c.agent_env
            .push(("ORBD_DISK_RESERVE_GIB".into(), "1".into()));
        c.start_all();
        // 6 GiB free less the 1 GiB reserve: 5 GiB; a running orb holds 1 + 2 GiB.
        assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1");
        assert_ok(&c.dispatch("pause", "amp-1", &[]), "amp-1 sleeps");
        assert_ok(&c.dispatch("spawn", "amp-2", &[]), "amp-2");
        // 2 GiB left: room for amp-1 to grow, not for a second checkpoint.
        if keeps {
            // Heartbeats sent after amp-1 slept would carry a release, were there one.
            let seen = heartbeats(&c);
            wait_until(Duration::from_secs(10), "two more heartbeats", || {
                heartbeats(&c) >= seen + 2
            });
            assert!(!wakes_now(&c, "amp-1"), "nothing frees amp-1's checkpoint");
            let wake = c.dispatch("spawn", "amp-1", &[]);
            assert_eq!(wake.code, 1, "{}", wake.stderr);
            assert!(
                wake.stderr.contains("InsufficientDisk")
                    && wake
                        .stderr
                        .contains("needs 3.0 GiB headroom, 2.0 GiB usable"),
                "{}",
                wake.stderr
            );
        } else {
            wait_until(
                Duration::from_secs(10),
                "the coordinator heard what amp-1's wake frees",
                || wakes_now(&c, "amp-1"),
            );
            assert_ok(&c.dispatch("spawn", "amp-1", &[]), "wake amp-1");
            assert!(c.fake_orb("agent-a", "amp-1").running);
        }
    }
}

/// A new orb leaves its host the room to wake the orbs asleep there (they cannot wake
/// anywhere else) when another host can take it, even passing over its project
/// snapshot, which is counted as a `wake_room` miss.
#[test]
fn new_orbs_leave_room_to_wake_the_orbs_asleep_on_their_host() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 2);
    c.start_all();
    let snap = [("ORB_SNAPSHOT_ID", "project-1")];
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1");
    assert_ok(
        &c.dispatch("pause", "amp-1", &snap),
        "amp-1 publishes project-1",
    );
    let (holder, other) = match c.fake_orbs("agent-a").is_empty() {
        true => ("agent-b", "agent-a"),
        false => ("agent-a", "agent-b"),
    };

    // Each agent runs two orbs: amp-2 and amp-1's wake fit on the holder.
    assert_ok(&c.dispatch("spawn", "amp-2", &snap), "amp-2");
    assert!(
        c.fake_orb(holder, "amp-2").running,
        "amp-2 goes where project-1 is"
    );
    // amp-3 there would leave amp-1 unable to wake.
    assert_ok(&c.dispatch("spawn", "amp-3", &snap), "amp-3");
    assert!(
        c.fake_orb(other, "amp-3").running,
        "amp-3 leaves {holder} the room to wake amp-1"
    );
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "wake amp-1");
    assert!(c.fake_orb(holder, "amp-1").running);
    assert_eq!(placements(&c, "hit"), 1);
    assert_eq!(placements(&c, "wake_room"), 1);
    assert_eq!(placements(&c, "elsewhere"), 0);
    assert_eq!(c.logs().matches(SNAPSHOT_MISS).count(), 1);
}

/// An orb whose first spawn its agent refused for reasons of its own (here its image
/// is not allowed there), before creating anything, has nothing on that agent: its next
/// spawn places a new incarnation away from it, once, and that agent is told to delete
/// what it recorded of the old one. An orb that ever started stays where its disk is.
#[test]
fn an_orb_its_agent_refused_before_creating_it_is_placed_anew() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 2);
    c.start_coordinator("");
    c.start_agent_with_env(
        "agent-a",
        &[("ORBD_IMAGE_ALLOW", "example.invalid/tools/*")],
    );
    c.wait_connected(1);
    let refused = c.dispatch("spawn", "amp-1", &[]);
    assert_ne!(refused.code, 0, "{}", refused.stdout);
    let old = c.orb_id("amp-1");
    assert!(c.fake_orbs("agent-a").iter().all(|o| !o.disk));
    let tools = [("ORB_IMAGE", "example.invalid/tools/orb:1")];
    assert_ok(&c.dispatch("spawn", "amp-2", &tools), "amp-2");
    assert_ok(
        &c.dispatch("pause", "amp-2", &[]),
        "amp-2 sleeps on agent-a",
    );

    c.start_agent("agent-b", "");
    c.wait_connected(2);
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1 placed anew");
    assert!(
        c.fake_orb("agent-b", "amp-1").running,
        "amp-1 runs on agent-b"
    );
    assert_ne!(c.orb_id("amp-1"), old, "a new incarnation");
    wait_until(
        Duration::from_secs(20),
        "agent-a deleted what it recorded of the old amp-1",
        || {
            let log =
                std::fs::read_to_string(c.root().join("agent-agent-a.log")).unwrap_or_default();
            plain(&log).lines().any(|l| {
                l.contains("operation terminal") && l.contains("action=\"delete\" ok=true")
            })
        },
    );
    // Its next spawn finds it where it runs now.
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1 again");
    assert_eq!(
        c.logs()
            .matches("spawn of an orb that never started: placing it anew")
            .count(),
        1
    );

    // amp-2 has its disk on agent-a: it waits for agent-a.
    c.terminate_agent("agent-a", Duration::from_secs(30));
    c.wait_connected(1);
    let job = c.dispatch("spawn", "amp-2", &tools);
    assert_eq!(job.code, 75, "{}", job.stderr);
    assert!(job.stderr.contains("BackendUnavailable"), "{}", job.stderr);
    assert_eq!(c.fake_orbs("agent-b").len(), 1, "amp-2 is not re-placed");
}

const PLACED_ANEW: &str = "spawn of an orb that never started: placing it anew";

/// An agent that runs only tools images (`ORBD_IMAGE_ALLOW`), with room for `orbs`.
fn picky(c: &mut Cluster, id: &str, orbs: u64) {
    c.start_agent_with_env(
        id,
        &[
            ("ORBD_IMAGE_ALLOW", "example.invalid/tools/*"),
            ("ORBD_CAPACITY_VCPUS", &(2 * orbs).to_string()),
            ("ORBD_CAPACITY_MEMORY_MIB", &(2048 * orbs).to_string()),
        ],
    );
}

/// The refusing host is passed over even where it has the most room: avoiding it alone
/// decides where the orb goes.
#[test]
fn a_new_placement_avoids_the_host_that_refused_it() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 2);
    c.start_coordinator("");
    c.start_agent("agent-b", "");
    c.wait_connected(1);
    assert_ok(&c.dispatch("spawn", "amp-0", &[]), "amp-0 on agent-b");
    picky(&mut c, "agent-a", 8);
    c.wait_connected(2);
    // agent-a has far more room: amp-1 goes there and is refused.
    assert_ne!(c.dispatch("spawn", "amp-1", &[]).code, 0);
    assert!(c.fake_orbs("agent-b").len() == 1, "amp-1 went to agent-a");
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1 placed anew");
    assert!(c.fake_orb("agent-b", "amp-1").running);
}

/// When only the host that refused an orb fits it, the orb stays where it is: no new
/// incarnation, refused again.
#[test]
fn an_orb_only_its_refusing_host_fits_stays_where_it_is() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 1);
    c.start_coordinator("");
    c.start_agent("agent-b", "");
    c.wait_connected(1);
    assert_ok(&c.dispatch("spawn", "amp-0", &[]), "amp-0 fills agent-b");
    picky(&mut c, "agent-a", 4);
    c.wait_connected(2);
    assert_ne!(c.dispatch("spawn", "amp-1", &[]).code, 0);
    let first = c.orb_id("amp-1");
    assert_ne!(c.dispatch("spawn", "amp-1", &[]).code, 0, "refused again");
    assert_eq!(c.orb_id("amp-1"), first, "the same incarnation");
    assert_eq!(c.logs().matches(PLACED_ANEW).count(), 0);
}

/// An orb refused by two agents is placed away from both, reaching the third that runs
/// it, though the two have more room.
#[test]
fn an_orb_two_hosts_refused_reaches_a_third() {
    let mut c = Cluster::new(&["agent-a", "agent-b", "agent-c"], 2);
    c.start_coordinator("");
    c.start_agent("agent-c", "");
    c.wait_connected(1);
    assert_ok(&c.dispatch("spawn", "amp-0", &[]), "amp-0 on agent-c");
    picky(&mut c, "agent-a", 8);
    picky(&mut c, "agent-b", 8);
    c.wait_connected(3);
    for _ in 0..2 {
        assert_ne!(c.dispatch("spawn", "amp-1", &[]).code, 0, "refused");
    }
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1 on agent-c");
    assert!(c.fake_orb("agent-c", "amp-1").running);
    assert_eq!(c.logs().matches(PLACED_ANEW).count(), 2);
}

/// A refusal never moves an orb onto an overflow agent: the always-on agent that
/// refused it is the only one of its kind that fits, so the orb stays there.
#[test]
fn a_refusal_does_not_move_an_orb_onto_an_overflow_agent() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.start_coordinator("");
    picky(&mut c, "agent-a", 4);
    c.start_agent_with_env("spare", &[("ORBD_OVERFLOW", "true")]);
    c.wait_connected(2);
    assert_ne!(c.dispatch("spawn", "amp-1", &[]).code, 0);
    let first = c.orb_id("amp-1");
    assert_ne!(c.dispatch("spawn", "amp-1", &[]).code, 0, "refused again");
    assert_eq!(c.orb_id("amp-1"), first);
    assert!(
        c.fake_orbs("spare").is_empty(),
        "nothing on the overflow agent"
    );
}
