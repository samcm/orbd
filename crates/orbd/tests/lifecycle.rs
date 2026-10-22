//! Milestone-1 gate: lifecycle and placement through real processes.

mod common;
use common::*;
use std::time::{Duration, Instant};

/// Spawn → pause (memory checkpoint) → spawn (restore) → delete, then a late spawn is
/// refused by the delete tombstone. The orb token never reaches any log.
#[test]
fn lifecycle_and_delete_tombstone() {
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

    let late = c.dispatch("spawn", "amp-1", &[]);
    assert_eq!(
        late.code, 1,
        "late spawn after delete must fail: {}",
        late.stderr
    );
    assert!(late.stderr.contains("OrbDeleted"), "{}", late.stderr);
    assert_eq!(
        c.only_orb("agent-a").counters.disk_creates,
        1,
        "delete must not be undone"
    );

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
