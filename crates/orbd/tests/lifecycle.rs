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

    // Waking a sleeping orb needs the same headroom: refused while two run, admitted
    // once one sleeps.
    let wake = c.dispatch("spawn", "amp-asleep-0", &[]);
    assert_eq!(wake.code, 1, "{}", wake.stderr);
    assert!(wake.stderr.contains("InsufficientDisk"), "{}", wake.stderr);
    assert_ok(&c.dispatch("pause", "amp-1", &[]), "pause amp-1");
    assert_ok(&c.dispatch("spawn", "amp-asleep-0", &[]), "wake");
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
