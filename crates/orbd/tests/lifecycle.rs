//! Milestone-1 gate: lifecycle and placement through real processes.

mod common;
use common::*;
use std::time::{Duration, Instant};

/// Spawn → pause (memory checkpoint) → spawn (restore) → delete → spawn (new
/// incarnation). The orb token never reaches any log.
#[test]
fn lifecycle_and_reincarnation_after_delete() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.start_all();

    assert_ok(
        &c.dispatch("pause", "amp-never-seen", &[]),
        "pause of unknown orb",
    );

    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "first spawn");
    let s = c.only_orb("agent-a");
    assert!(s.running && s.disk);
    assert_eq!((s.counters.disk_creates, s.counters.boots), (1, 1));
    let b = &s.bootstraps[0];
    assert!(b.cold_boot);
    assert_eq!(
        b.env_names,
        ["AMP_URL", "ORB_TOKEN", "AMP_THREAD_ID", "AMP_SANDBOX_ID"]
    );

    assert_ok(&c.dispatch("pause", "amp-1", &[]), "pause");
    let s = c.only_orb("agent-a");
    assert!(!s.running);
    assert_eq!(s.counters.checkpoints, 1);

    // Pause is state-idempotent: a repeated pause changes nothing.
    assert_ok(&c.dispatch("pause", "amp-1", &[]), "repeated pause");
    assert_eq!(c.only_orb("agent-a").counters.checkpoints, 1);

    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "wake");
    let s = c.only_orb("agent-a");
    assert!(s.running);
    assert_eq!(
        (
            s.counters.disk_creates,
            s.counters.boots,
            s.counters.restores
        ),
        (1, 1, 1)
    );
    // Warm restore re-bootstraps with the fresh token without a cold boot.
    let last = s.bootstraps.last().unwrap();
    assert!(!last.cold_boot);
    assert!(last.generation > b.generation);

    assert_ok(&c.dispatch("delete", "amp-1", &[]), "delete");
    let s = c.only_orb("agent-a");
    assert!(!s.disk && !s.running);
    assert_ok(&c.dispatch("delete", "amp-1", &[]), "repeated delete");

    // Amp resets an orb by deleting it and spawning the same ORB_ID again: that spawn
    // gets a new orb with a fresh disk; the deleted one is never brought back.
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn after delete");
    let orbs = c.fake_orbs("agent-a");
    assert_eq!(
        orbs.len(),
        2,
        "spawn after delete must create a new incarnation"
    );
    let old = orbs
        .iter()
        .find(|o| !o.disk)
        .expect("deleted incarnation stays deleted");
    assert_eq!(old.counters.disk_creates, 1);
    let new = orbs.iter().find(|o| o.disk).unwrap();
    assert!(new.running && new.bootstraps[0].cold_boot);
    assert_eq!(new.counters.disk_creates, 1);

    assert!(
        !c.logs().contains(TOKEN),
        "orb token leaked into coordinator/agent logs"
    );
}

/// `orbd orb delete` (for orbs of archived threads, which Amp never deletes) deletes
/// only an orb asleep with no job for the given time, through the ordinary delete path:
/// a running or freshly paused orb is kept, and a later spawn starts a fresh orb.
#[test]
fn orb_delete_takes_only_orbs_asleep_long_enough() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.start_all();
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn 1");
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "spawn 2");
    assert_ok(&c.dispatch("pause", "amp-2", &[]), "pause 2");

    let row = |id: &str, state: &str| (id.into(), state.into(), "agent-a".into(), OFFERING.into());
    assert_eq!(
        c.orb_rows(),
        [row("amp-1", "running"), row("amp-2", "asleep")]
    );

    let running = c.admin(&["orb", "delete", "amp-1", "--min-asleep-mins", "0"]);
    assert_ne!(running.code, 0, "a running orb must not be deleted");
    assert!(
        running.stderr.contains("amp-1 is running"),
        "{}",
        running.stderr
    );
    let recent = c.admin(&["orb", "delete", "amp-2"]);
    assert_ne!(recent.code, 0, "asleep for less than the default hour");
    assert!(
        recent.stderr.contains("amp-2 is asleep"),
        "{}",
        recent.stderr
    );
    let unknown = c.admin(&["orb", "delete", "amp-3", "--min-asleep-mins", "0"]);
    assert!(
        unknown.stderr.contains("no orb amp-3"),
        "{}",
        unknown.stderr
    );
    assert!(c.fake_orbs("agent-a").iter().all(|o| o.disk));

    let deleted = c.admin(&["orb", "delete", "amp-2", "--min-asleep-mins", "0"]);
    assert_ok(&deleted, "delete asleep orb");
    let orbs = c.fake_orbs("agent-a");
    assert_eq!(
        orbs.iter().filter(|o| o.disk).count(),
        1,
        "amp-2's disk is gone"
    );
    assert!(orbs.iter().any(|o| o.running), "amp-1 still runs");
    let list = c.admin(&["orb", "list"]);
    assert!(!list.stdout.contains("amp-2"), "{}", list.stdout);

    assert_ok(&c.dispatch("pause", "amp-1", &[]), "pause 1");
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "spawn after delete");
    let orbs = c.fake_orbs("agent-a");
    assert_eq!(
        orbs.len(),
        3,
        "a fresh incarnation, the deleted one stays deleted"
    );
    let running: Vec<_> = orbs.iter().filter(|o| o.running).collect();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].counters.restores, 0);
    assert!(running[0].bootstraps.iter().all(|b| b.cold_boot));
}

/// `orbd orb pause` pauses an orb Amp no longer pauses (e.g. its provider token is no
/// longer served) as Amp would, and the thread's next spawn wakes it.
#[test]
fn orb_pause_pauses_as_amp_would() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.start_all();
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn");

    let paused = c.admin(&["orb", "pause", "amp-1"]);
    assert_ok(&paused, "orb pause");
    assert_eq!(paused.stdout, "amp-1 paused: pause succeeded");
    let s = c.only_orb("agent-a");
    assert!(!s.running && s.disk);
    assert_eq!(s.counters.checkpoints, 1, "paused with a memory checkpoint");
    let rows = c.orb_rows();
    assert_eq!(
        (rows[0].0.as_str(), rows[0].1.as_str()),
        ("amp-1", "asleep")
    );
    let again = c.admin(&["orb", "pause", "amp-1"]);
    assert_ok(&again, "repeated orb pause");
    assert!(again.stdout.contains("already stopped"), "{}", again.stdout);
    assert_eq!(c.only_orb("agent-a").counters.checkpoints, 1);

    let unknown = c.admin(&["orb", "pause", "amp-2"]);
    assert!(
        unknown.stderr.contains("no orb amp-2"),
        "{}",
        unknown.stderr
    );

    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "wake");
    let s = c.only_orb("agent-a");
    assert!(s.running && !s.bootstraps.last().unwrap().cold_boot);
    assert_eq!(s.counters.restores, 1);
}

/// `orbd orb delete --force` deletes an orb whatever it is doing: a running orb is
/// stopped and deleted, and a later spawn starts a fresh one.
#[test]
fn orb_delete_force_takes_a_running_orb() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.start_all();
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn");

    let both = c.admin(&[
        "orb",
        "delete",
        "amp-1",
        "--force",
        "--min-asleep-mins",
        "0",
    ]);
    assert_ne!(both.code, 0, "--force takes no --min-asleep-mins");
    let unknown = c.admin(&["orb", "delete", "amp-2", "--force"]);
    assert!(
        unknown.stderr.contains("no orb amp-2"),
        "{}",
        unknown.stderr
    );
    assert!(c.only_orb("agent-a").running);

    let forced = c.admin(&["orb", "delete", "amp-1", "--force"]);
    assert_ok(&forced, "forced delete of a running orb");
    assert_eq!(forced.stdout, "amp-1 deleted: delete succeeded");
    let s = c.only_orb("agent-a");
    assert!(!s.running && !s.disk);
    assert!(c.orb_rows().is_empty());

    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn after delete");
    let orbs = c.fake_orbs("agent-a");
    assert_eq!(orbs.len(), 2, "a fresh incarnation");
    assert!(orbs.iter().any(|o| o.running && o.bootstraps[0].cold_boot));
}

/// One provider whose orb types are offerings: a spawn of a new orb runs on the
/// offering its ORB_TYPE names (the first orb type when it has none), every later job
/// acts on the orb as created, and an orb type orbd does not serve fails without
/// blaming the Amp CLI.
#[test]
fn provider_orb_types_choose_the_offering() {
    let mut c = Cluster::new(&["agent-a"], 4);
    let path = c.root().join("coordinator.yaml");
    let yaml = std::fs::read_to_string(&path).unwrap();
    let token = c.root().join("homelab-token");
    std::fs::write(&token, "aop_test\n").unwrap();
    // The provider child itself is not under test: give it a CLI that cannot run.
    std::fs::write(
        &path,
        format!(
            "amp_cli: /nonexistent/amp\namp_cli_update_interval_secs: 0\n{yaml}\
             \x20 - id: fake-4vcpu-4gb\n    runtime: fake\n    \
             resources: {{ vcpus: 4, memory_mib: 4096, disk_gib: 4 }}\n\
             providers:\n  - id: homelab\n    token_file: {}\n    \
             orb_types: [{OFFERING}, fake-4vcpu-4gb]\n",
            token.display()
        ),
    )
    .unwrap();
    c.provider = Some("homelab".into());
    c.start_all();

    let big = [("ORB_TYPE", "fake-4vcpu-4gb")];
    assert_ok(&c.dispatch("spawn", "amp-big", &big), "spawn big");
    assert_ok(
        &c.dispatch("spawn", "amp-default", &[]),
        "spawn, no ORB_TYPE",
    );
    let row = |id: &str, ty: &str| (id.into(), "running".into(), "agent-a".into(), ty.into());
    assert_eq!(
        c.orb_rows(),
        [
            row("amp-big", "fake-4vcpu-4gb"),
            row("amp-default", OFFERING)
        ]
    );

    // The default orb type is the other offering; the orb's own must be found. An orb
    // keeps the offering it was created as, even when a spawn names another (a thread
    // whose orb type changed, or a retired provider's job with no orb type).
    assert_ok(&c.dispatch("pause", "amp-big", &[]), "pause big");
    assert_ok(
        &c.dispatch("spawn", "amp-big", &[]),
        "wake big, no ORB_TYPE",
    );
    assert_eq!(
        c.orb_rows()[0],
        row("amp-big", "fake-4vcpu-4gb"),
        "big keeps its offering"
    );
    assert_ok(&c.dispatch("pause", "amp-big", &[]), "pause big again");
    assert_ok(&c.dispatch("spawn", "amp-big", &big), "wake big");
    assert_ok(&c.dispatch("delete", "amp-big", &[]), "delete big");
    assert_ok(&c.dispatch("delete", "amp-default", &[]), "delete default");
    assert!(c.fake_orbs("agent-a").iter().all(|o| !o.disk));

    let gpu = c.dispatch("spawn", "amp-gpu", &[("ORB_TYPE", "gpu")]);
    assert_eq!(gpu.code, 1, "{}", gpu.stderr);
    assert!(
        gpu.stderr
            .contains("orb type \"gpu\" is not one of provider homelab's"),
        "{}",
        gpu.stderr
    );
    assert!(!gpu.stderr.contains("cli-check bad"), "{}", gpu.stderr);
}

/// A host that dies without a drain (reboot, freeze) takes its VMs with it. When its
/// agent reconnects, the coordinator learns the orb stopped: it frees the orb's compute
/// for other orbs, and the next wake cold-boots it.
#[test]
fn orbs_whose_vm_died_with_the_host_are_stopped_when_it_returns() {
    let mut c = Cluster::new(&["agent-a"], 1);
    c.start_all();
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn 1");

    // The host goes down hard: no drain, and the VM is gone when it comes back.
    c.kill_agent("agent-a");
    c.kill_vm("agent-a");
    c.start_agent("agent-a", "");
    c.wait_connected(1);

    wait_until(Duration::from_secs(20), "amp-1 recorded as stopped", || {
        c.orb_rows()
            .iter()
            .any(|r| r.0 == "amp-1" && r.1 == "stopped")
    });
    // Its compute is free again: the agent has room for only one running orb.
    assert_ok(
        &c.dispatch("spawn", "amp-2", &[]),
        "spawn 2 in amp-1's room",
    );
    assert_ok(&c.dispatch("pause", "amp-2", &[]), "pause 2");
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "wake 1");
    let one = c
        .fake_orbs("agent-a")
        .into_iter()
        .find(|o| o.running)
        .unwrap();
    assert_eq!(one.counters.boots, 2, "amp-1 cold-booted again");
    assert!(one.bootstraps.last().unwrap().cold_boot);
}

/// A VM that dies while its agent runs (killed, crashed) is noticed within seconds: the
/// orb is recorded as stopped and its compute freed without an agent restart, and the
/// next wake cold-boots it.
#[test]
fn orbs_whose_vm_dies_while_the_agent_runs_are_stopped() {
    let mut c = Cluster::new(&["agent-a"], 1);
    c.start_all();
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn 1");

    c.kill_vm("agent-a");
    wait_until(Duration::from_secs(20), "amp-1 recorded as stopped", || {
        c.orb_rows()
            .iter()
            .any(|r| r.0 == "amp-1" && r.1 == "stopped")
    });
    // Its compute is free again, on the agent too: it has room for one running orb.
    assert_ok(
        &c.dispatch("spawn", "amp-2", &[]),
        "spawn 2 in amp-1's room",
    );
    assert_ok(&c.dispatch("pause", "amp-2", &[]), "pause 2");
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "wake 1");
    let one = c
        .fake_orbs("agent-a")
        .into_iter()
        .find(|o| o.running)
        .unwrap();
    assert_eq!(one.counters.boots, 2, "amp-1 cold-booted again");
    assert!(one.bootstraps.last().unwrap().cold_boot);
}

/// ORBD_CHECKPOINT_MAX_AGE_HOURS. Unset (the default), an orb asleep for days still
/// wakes from its memory checkpoint. Set, the agent checks ten minutes after it starts
/// (here sooner, by failpoint) and drops the checkpoint of an orb asleep longer: the
/// coordinator records the orb stopped, and its next wake cold-boots its disk. An orb
/// asleep less long keeps its checkpoint; one whose checkpoint is gone from the host is
/// recorded stopped too.
#[cfg(feature = "failpoints")]
#[test]
fn memory_checkpoints_older_than_the_max_age_are_dropped() {
    const HOUR: Duration = Duration::from_secs(3600);
    const FIRST_CHECK_NOW: &str = "agent::checkpoint_age::first_check_secs=return(0)";
    let state = |c: &Cluster, amp: &str| {
        c.orb_rows()
            .into_iter()
            .find(|r| r.0 == amp)
            .map(|r| r.1)
            .unwrap_or_default()
    };
    let mut c = Cluster::new(&["agent-a"], 3);
    c.start_all();
    assert_ok(&c.dispatch("spawn", "amp-old", &[]), "spawn old");
    assert_ok(&c.dispatch("pause", "amp-old", &[]), "pause old");

    c.kill_agent("agent-a");
    c.age_checkpoints("agent-a", "amp-old", 48 * HOUR);
    c.start_agent("agent-a", FIRST_CHECK_NOW);
    c.wait_connected(1);
    assert_ok(&c.dispatch("spawn", "amp-old", &[]), "wake old");
    let old = c.fake_orb("agent-a", "amp-old");
    assert_eq!(
        (old.counters.boots, old.counters.restores),
        (1, 1),
        "off by default: restored after two days asleep"
    );
    assert_ok(&c.dispatch("pause", "amp-old", &[]), "pause old again");
    for amp in ["amp-new", "amp-gone"] {
        assert_ok(&c.dispatch("spawn", amp, &[]), "spawn");
        assert_ok(&c.dispatch("pause", amp, &[]), "pause");
    }

    c.kill_agent("agent-a");
    c.age_checkpoints("agent-a", "amp-old", 13 * HOUR);
    c.lose_checkpoints("agent-a", "amp-gone");
    c.agent_env
        .push(("ORBD_CHECKPOINT_MAX_AGE_HOURS".into(), "12".into()));
    c.start_agent("agent-a", "");
    c.wait_connected(1);
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        state(&c, "amp-old"),
        "asleep",
        "no check right at the start"
    );

    c.kill_agent("agent-a");
    c.start_agent("agent-a", FIRST_CHECK_NOW);
    c.wait_connected(1);
    wait_until(
        Duration::from_secs(20),
        "amp-old and amp-gone recorded as stopped",
        || state(&c, "amp-old") == "stopped" && state(&c, "amp-gone") == "stopped",
    );
    assert_eq!(state(&c, "amp-new"), "asleep");
    let logs = c.logs();
    assert!(logs.contains("dropped the memory checkpoint"));
    assert!(logs.contains("asleep orb lost its memory checkpoint on its host"));

    assert_ok(&c.dispatch("spawn", "amp-old", &[]), "wake old cold");
    let old = c.fake_orb("agent-a", "amp-old");
    assert_eq!(
        (old.counters.boots, old.counters.restores),
        (2, 1),
        "cold-booted"
    );
    assert!(old.disk && old.bootstraps.last().unwrap().cold_boot);
    assert_ok(&c.dispatch("spawn", "amp-gone", &[]), "wake gone cold");
    let gone = c.fake_orb("agent-a", "amp-gone");
    assert_eq!((gone.counters.boots, gone.counters.restores), (2, 0));
    assert_ok(&c.dispatch("spawn", "amp-new", &[]), "wake new");
    let new = c.fake_orb("agent-a", "amp-new");
    assert_eq!((new.counters.boots, new.counters.restores), (1, 1));
    assert!(!new.bootstraps.last().unwrap().cold_boot);
}

/// An orb stays on its recorded agent for life: while that agent is offline its jobs
/// fail fast instead of being re-placed, and it resumes there once the agent is back.
/// New orbs go to agents with room; with no room anywhere, spawns are refused.
#[test]
fn placement_is_fixed_and_admission_is_exact() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 1);
    c.start_all();

    assert_ok(&c.dispatch("spawn", "amp-x", &[]), "spawn x");
    assert_ok(&c.dispatch("spawn", "amp-y", &[]), "spawn y");
    // One orb fits per agent, so they were spread out.
    assert_eq!(c.fake_orbs("agent-a").len(), 1);
    assert_eq!(c.fake_orbs("agent-b").len(), 1);

    let full = c.dispatch("spawn", "amp-z", &[]);
    assert_eq!(full.code, 1, "third orb must be refused: {}", full.stderr);
    assert!(
        full.stderr.contains("InsufficientMemory"),
        "{}",
        full.stderr
    );

    // Find x's agent and take it offline.
    assert_ok(&c.dispatch("pause", "amp-x", &[]), "pause x");
    let home = if c.fake_orbs("agent-a")[0].running {
        "agent-b"
    } else {
        "agent-a"
    };
    let other = if home == "agent-a" {
        "agent-b"
    } else {
        "agent-a"
    };
    c.kill_agent(home);
    c.wait_connected(1);

    let t = Instant::now();
    let offline = c.dispatch("spawn", "amp-x", &[]);
    // UNAVAILABLE (exit 75): nothing was submitted.
    assert_eq!(
        offline.code, 75,
        "spawn on offline agent must fail: {}",
        offline.stderr
    );
    assert!(
        offline.stderr.contains("BackendUnavailable"),
        "{}",
        offline.stderr
    );
    assert!(
        t.elapsed() < Duration::from_secs(30),
        "should fail fast, not wait out the budget"
    );
    assert_eq!(
        c.fake_orbs(other).len(),
        1,
        "orb must not be re-placed on another agent"
    );

    c.start_agent(home, "");
    c.wait_connected(2);
    assert_ok(
        &c.dispatch("spawn", "amp-x", &[]),
        "spawn x after agent returns",
    );
    let x = c.fake_orbs(home).into_iter().next().unwrap();
    assert!(x.running);
    assert_eq!((x.counters.disk_creates, x.counters.restores), (1, 1));
}

/// Disk is admitted by what hosts actually use, not by summing orb disk sizes: sleeping
/// orbs hold no disk headroom, so a host whose orbs' disk sizes add up far past its
/// disk limit still takes new orbs (the failure that blocked every spawn while 1.4 TB
/// was free). Running orbs do hold headroom, so the limit still binds.
#[test]
fn sleeping_orbs_do_not_block_new_orbs_on_disk() {
    let mut c = Cluster::new(&["agent-a"], 8);
    // Each running orb holds 3 GiB (1 GiB growth + its 2 GiB memory checkpoint); each
    // orb's disk is 4 GiB.
    c.agent_env
        .push(("ORBD_CAPACITY_DISK_GIB".into(), "8".into()));
    c.start_all();

    for i in 0..5 {
        let orb = format!("amp-asleep-{i}");
        assert_ok(&c.dispatch("spawn", &orb, &[]), "spawn");
        assert_ok(&c.dispatch("pause", &orb, &[]), "pause");
    }
    // 20 GiB of disk sizes against an 8 GiB limit, all asleep.
    assert_ok(
        &c.dispatch("spawn", "amp-1", &[]),
        "spawn beside sleeping orbs",
    );
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "second running orb");
    let full = c.dispatch("spawn", "amp-3", &[]);
    assert_eq!(
        full.code, 1,
        "third running orb must be refused: {}",
        full.stderr
    );
    assert!(
        full.stderr.contains("InsufficientDisk")
            && full
                .stderr
                .contains("agent-a: disk: needs 3.0 GiB headroom")
            && full.stderr.contains("agent disk limit 8.0 GiB")
            && full.stderr.contains("6.0 GiB held by 2 running)"),
        "{}",
        full.stderr
    );

    let list = c.admin(&["agent", "list"]);
    assert_ok(&list, "agent list");
    assert!(
        list.stdout.contains("7 orbs (2 running)")
            && list.stdout.contains("6.0 GiB reserved, limit 8.0 GiB"),
        "{}",
        list.stdout
    );

    // Waking a sleeping orb needs only its growth (1 GiB) once its agent reported that
    // the wake frees its memory checkpoint (the fake's restore reads it into memory, as
    // QEMU's does), whose room the next one takes again. Admitted with 2 GiB left; the
    // next wake, with none left, is refused until an orb sleeps.
    wait_until(
        Duration::from_secs(10),
        "the agent reported what waking amp-asleep-0 frees",
        || {
            let list = c.admin(&["orb", "list", "--json"]);
            let v: serde_json::Value = serde_json::from_str(&list.stdout).unwrap_or_default();
            v["orbs"].as_array().into_iter().flatten().any(|o| {
                o["amp_orb_id"] == "amp-asleep-0"
                    && o["wake_blockers"].as_array().is_some_and(|b| b.is_empty())
            })
        },
    );
    assert_ok(&c.dispatch("spawn", "amp-asleep-0", &[]), "wake");
    let wake = c.dispatch("spawn", "amp-asleep-1", &[]);
    assert_eq!(wake.code, 1, "{}", wake.stderr);
    assert!(
        wake.stderr.contains("InsufficientDisk")
            && wake
                .stderr
                .contains("the orb's host lacks capacity to run it: disk: needs 1.0 GiB headroom"),
        "{}",
        wake.stderr
    );
    assert_ok(&c.dispatch("pause", "amp-1", &[]), "pause amp-1");
    assert_ok(&c.dispatch("spawn", "amp-asleep-1", &[]), "wake");
}

/// A host whose filesystem is nearly full refuses new orbs with InsufficientDisk and
/// the numbers, whatever its orbs' sizes: free space below the reserve (default
/// max(10 GiB, 5%)) is never handed out.
#[test]
fn nearly_full_filesystem_refuses_new_orbs() {
    let mut c = Cluster::with_runtime(
        &["agent-a"],
        8,
        "fake",
        "runtime:\n  fake:\n    memory_preserving_sleep: true\n    \
         disk: { free_gib: 52, total_gib: 1000 }\n",
    );
    c.start_all();

    let full = c.dispatch("spawn", "amp-1", &[]);
    assert_eq!(full.code, 1, "spawn must be refused: {}", full.stderr);
    assert!(
        full.stderr.contains("InsufficientDisk")
            && full.stderr.contains(
                "agent-a: disk: needs 3.0 GiB headroom, 2.0 GiB usable (filesystem 52.0 GiB \
                 free of 1000.0 GiB, 50.0 GiB reserve"
            ),
        "{}",
        full.stderr
    );
    assert!(c.fake_orbs("agent-a").is_empty(), "nothing was created");
    let list = c.admin(&["agent", "list"]);
    assert!(
        list.stdout
            .contains("disk 52.0 GiB free of 1000.0 GiB (50.0 GiB kept free)"),
        "{}",
        list.stdout
    );
}
