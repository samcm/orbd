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
    c.wait_open_command("amp-1");
    let retry = c.dispatch_bg("spawn", "amp-1", &[]);
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

        // The orb remains usable afterwards, once the agent is back (a result committed
        // before the crash is answered without it).
        c.wait_connected(1);
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

    // 46 s dispatch budget → work deadline 6 s after start.
    let budget = [("ORBD_DISPATCH_BUDGET_MS", "46000")];
    let slow = c.dispatch_bg("spawn", "amp-1", &budget);
    c.wait_open_command("amp-1");
    // 43 s budget → work deadline 3 s after it, while the spawn holds the orb until 6 s.
    let queued = c.dispatch_bg("pause", "amp-1", &[("ORBD_DISPATCH_BUDGET_MS", "43000")]);

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

/// A delete that failed on the agent leaves the orb tombstoned (Amp's spawns are
/// refused) but not deleted; `orbd orb delete --force` still reaches it.
#[test]
fn forced_delete_finishes_an_orb_whose_delete_failed() {
    let mut c = Cluster::new(&["agent-a"], 2);
    c.start_coordinator("");
    c.start_agent("agent-a", "fake::delete=return");
    c.wait_connected(1);
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn");
    let failed = c.dispatch("delete", "amp-1", &[]);
    assert_ne!(
        failed.code, 0,
        "delete fails on the agent: {}",
        failed.stdout
    );
    assert!(c.only_orb("agent-a").disk, "not deleted");
    let plain = c.admin(&["orb", "delete", "amp-1"]);
    assert!(plain.stderr.contains("no orb amp-1"), "{}", plain.stderr);

    c.kill_agent("agent-a");
    c.wait_connected(0);
    c.start_agent("agent-a", "");
    c.wait_connected(1);
    let forced = c.admin(&["orb", "delete", "amp-1", "--force"]);
    assert_ok(&forced, "forced delete of a tombstoned orb");
    let s = c.only_orb("agent-a");
    assert!(!s.running && !s.disk, "deleted");
}

/// An orb whose first spawn failed for no reason of its host's (here the start itself
/// failed, nothing created) is retried on that host while the host can take it: no new
/// incarnation. A host that is gone is waited for (it may be restarting); once it is
/// removed, nothing ties the orb there and its next spawn places it anew.
#[test]
fn an_orb_that_never_started_moves_only_when_its_host_cannot_take_it() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 2);
    c.start_coordinator("");
    c.start_agent("agent-a", "fake::start=2*return(boom)");
    c.wait_connected(1);
    for orb in ["amp-1", "amp-2"] {
        let failed = c.dispatch("spawn", orb, &[]);
        assert_ne!(failed.code, 0, "{orb}: {}", failed.stdout);
    }
    assert!(c.fake_orbs("agent-a").iter().all(|o| !o.disk));
    c.start_agent("agent-b", "");
    c.wait_connected(2);

    // agent-a can take amp-1 now: it is retried there.
    let first = c.orb_id("amp-1");
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1 on agent-a");
    assert_eq!(c.orb_id("amp-1"), first);
    assert!(c.fake_orb("agent-a", "amp-1").running);

    // agent-a is gone, maybe restarting: amp-2 waits for it.
    c.terminate_agent("agent-a", Duration::from_secs(30));
    c.wait_connected(1);
    let waiting = c.dispatch("spawn", "amp-2", &[]);
    assert_eq!(waiting.code, 75, "{}", waiting.stderr);
    assert!(c.fake_orbs("agent-b").is_empty());
    // Removed for good: amp-2, never started, is placed on agent-b.
    assert_ok(&c.admin(&["agent", "remove", "agent-a"]), "agent remove");
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "amp-2 placed anew");
    assert!(c.fake_orb("agent-b", "amp-2").running);
    assert_eq!(
        c.logs()
            .matches("spawn of an orb that never started: placing it anew")
            .count(),
        1
    );
}

/// An agent's refusal for want of room may pass (room admitted lately frees up): while
/// the coordinator finds room there, the orb is retried on that agent, not moved.
#[test]
fn a_passing_refusal_for_room_retries_the_orb_where_it_is() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 2);
    c.start_coordinator("");
    c.start_agent("agent-b", "");
    c.wait_connected(1);
    assert_ok(&c.dispatch("spawn", "amp-0", &[]), "amp-0 on agent-b");
    c.start_agent_with(
        "agent-a",
        "agent::admit=1*return",
        &[
            ("ORBD_CAPACITY_VCPUS", "16"),
            ("ORBD_CAPACITY_MEMORY_MIB", "16384"),
        ],
    );
    c.wait_connected(2);
    // agent-a has more room: amp-1 goes there, refused once for want of memory.
    let refused = c.dispatch("spawn", "amp-1", &[]);
    assert_ne!(refused.code, 0);
    assert!(
        refused.stderr.contains("InsufficientMemory"),
        "{}",
        refused.stderr
    );
    let first = c.orb_id("amp-1");
    assert_ok(
        &c.dispatch("spawn", "amp-1", &[]),
        "amp-1 retried on agent-a",
    );
    assert_eq!(c.orb_id("amp-1"), first, "the same incarnation");
    assert!(c.fake_orb("agent-a", "amp-1").running);
}

/// A spawn sent to an agent that is then removed for good ends (failed: the agent will
/// never answer), and the orb, never started, is placed anew on its next spawn.
#[test]
fn a_removed_agents_commands_end_and_its_unstarted_orbs_move() {
    let mut c = Cluster::new(&["agent-a", "agent-b"], 2);
    c.start_coordinator("");
    c.start_agent("agent-a", "fake::start::delay=return(600000)");
    c.wait_connected(1);
    let job = c.dispatch_bg("spawn", "amp-1", &[]);
    c.wait_open_command("amp-1");
    c.kill_agent("agent-a");
    c.start_agent("agent-b", "");
    c.wait_connected(1);
    assert_ok(&c.admin(&["agent", "remove", "agent-a"]), "agent remove");
    let ended = job.join().unwrap();
    assert_ne!(ended.code, 0, "{}", ended.stdout);
    assert!(ended.stderr.contains("removed"), "{}", ended.stderr);
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "amp-1 placed anew");
    assert!(c.fake_orb("agent-b", "amp-1").running);
}
