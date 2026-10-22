//! Milestone-1 gate: at-least-once delivery, serialization, crashes around durable
//! boundaries and deadlines, through real processes. Needs `--features failpoints`.
#![cfg(feature = "failpoints")]

mod common;
use common::*;
use std::time::Duration;

/// Identical spawn retries (Amp re-claims a job when a provider dies) attach to one
/// operation; a different command for the same orb waits its turn instead of
/// overlapping.
#[test]
fn duplicate_spawns_attach_and_conflicting_commands_serialize() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.start_coordinator("");
    c.start_agent("agent-a", "fake::start::delay=return(2000)");
    c.wait_connected(1);

    let first = c.dispatch_bg("spawn", "amp-1", &[]);
    std::thread::sleep(Duration::from_millis(300));
    let retry = c.dispatch_bg("spawn", "amp-1", &[]);
    std::thread::sleep(Duration::from_millis(300));
    let pause = c.dispatch_bg("pause", "amp-1", &[]);

    for (name, j) in [("first", first), ("retry", retry), ("pause", pause)] {
        assert_ok(&j.join().unwrap(), name);
    }
    let s = c.only_orb("agent-a");
    assert_eq!((s.counters.disk_creates, s.counters.boots), (1, 1));
    assert_eq!(
        s.bootstraps.len(),
        1,
        "the retry must attach, not bootstrap again"
    );
    assert!(
        !s.running,
        "pause must run after the spawn, not before or alongside it"
    );
    assert_eq!(s.counters.checkpoints, 1);
}

/// The agent is killed at every durable boundary of a spawn; after it restarts, the same
/// job finishes successfully with exactly one disk and one boot.
#[test]
fn agent_crash_at_each_boundary_converges_without_duplicates() {
    for point in [
        "agent::accepted_committed",
        "fake::start::after_effect",
        "agent::spawn::after_start",
        "fake::bootstrap::after_effect",
        "agent::terminal_committed",
    ] {
        let mut c = Cluster::new(&["agent-a"], 2);
        c.start_coordinator("");
        c.start_agent("agent-a", &format!("{point}=return"));
        c.wait_connected(1);

        let job = c.dispatch_bg("spawn", "amp-1", &[]);
        c.wait_agent_crash("agent-a", Duration::from_secs(20));
        c.start_agent("agent-a", "");
        let job = job.join().unwrap();
        assert_eq!(
            job.code, 0,
            "[{point}] spawn should succeed after agent restart: {}",
            job.stderr
        );

        let s = c.only_orb("agent-a");
        assert!(s.running, "[{point}]");
        assert_eq!(
            (s.counters.disk_creates, s.counters.boots),
            (1, 1),
            "[{point}]"
        );
    }
}

/// The coordinator is killed after committing a command but before sending it, and after
/// committing a result but before acknowledging it. The dispatch client reconnects with
/// the same invocation, and the work happens exactly once.
#[test]
fn coordinator_crash_resumes_the_same_operation() {
    for point in [
        "coordinator::command_committed",
        "coordinator::result_committed",
    ] {
        let mut c = Cluster::new(&["agent-a"], 2);
        c.start_coordinator(&format!("{point}=return"));
        c.start_agent("agent-a", "");
        c.wait_connected(1);

        let job = c.dispatch_bg("spawn", "amp-1", &[]);
        c.wait_coordinator_crash(Duration::from_secs(20));
        c.start_coordinator("");
        let job = job.join().unwrap();
        assert_eq!(
            job.code, 0,
            "[{point}] spawn should complete after coordinator restart: {}",
            job.stderr
        );

        let s = c.only_orb("agent-a");
        assert_eq!(
            (s.counters.disk_creates, s.counters.boots),
            (1, 1),
            "[{point}]"
        );
        assert_eq!(s.bootstraps.len(), 1, "[{point}]");

        // The orb remains usable afterwards.
        assert_ok(&c.dispatch("pause", "amp-1", &[]), "pause after recovery");
    }
}

/// A command that outlives its work budget is cancelled and reported as a deadline
/// failure; a request queued behind it that expires is never executed; and the orb is
/// usable again afterwards.
#[test]
fn deadlines_cancel_work_and_expire_queued_requests() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.start_coordinator("");
    c.start_agent("agent-a", "fake::start::delay=return(60000)");
    c.wait_connected(1);

    // 50 s dispatch budget → work deadline 10 s after start.
    let budget = [("ORBD_DISPATCH_BUDGET_MS", "50000")];
    let slow = c.dispatch_bg("spawn", "amp-1", &budget);
    std::thread::sleep(Duration::from_millis(500));
    // 45 s budget → work deadline ~5.5 s, while the spawn holds the orb for 10 s.
    let queued = c.dispatch_bg("pause", "amp-1", &[("ORBD_DISPATCH_BUDGET_MS", "45000")]);

    let slow = slow.join().unwrap();
    assert_ne!(slow.code, 0, "spawn past its deadline must fail");
    assert!(slow.stderr.contains("DeadlineExceeded"), "{}", slow.stderr);
    let queued = queued.join().unwrap();
    assert_ne!(
        queued.code, 0,
        "queued pause must expire: {}",
        queued.stderr
    );

    let s = c.fake_orbs("agent-a");
    assert!(
        s.iter()
            .all(|o| o.counters.checkpoints == 0 && o.counters.cold_stops == 0),
        "the expired pause must never run"
    );

    c.kill_agent("agent-a");
    c.start_agent("agent-a", "");
    c.wait_connected(1);
    assert_ok(
        &c.dispatch("spawn", "amp-1", &[]),
        "spawn after deadline failure",
    );
    assert!(c.only_orb("agent-a").running);
}
