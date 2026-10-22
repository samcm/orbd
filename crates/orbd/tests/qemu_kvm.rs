//! Opt-in QEMU/KVM gate tests: real VMs through a real coordinator, agent and
//! `orbd dispatch`. Ignored by default (CI has no KVM). Run on a host with `/dev/kvm`,
//! QEMU 7.2 and nested virtualization, one at a time:
//!
//! ```sh
//! guest/kernel/build.sh /tmp/k        # or any kernel with orbd's config
//! crates/orbd/tests/kvm-guest/build-rootfs.sh <orbd-guest> /tmp/k/bzImage /tmp/rootfs.ext4
//! ORBD_KVM_KERNEL=/tmp/k/bzImage ORBD_KVM_ROOTFS=/tmp/rootfs.ext4 \
//!   cargo test -p orbd --features failpoints --test qemu_kvm -- --ignored --test-threads=1
//! ```
//!
//! The guest prints a process counter and a nested KVM guest's counter to its serial
//! console. A memory restore must continue both sequences: a counter that restarts means
//! processes were lost, one that repeats or goes back means stale RAM was replayed.
#![cfg(feature = "failpoints")]

mod common;
use common::*;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const IMAGE: &str = "example.invalid/amp-orb:latest";
const MEMORY_MIB: u64 = 2048;

fn cluster(on_checkpoint_failure: &str, cmdline_extra: &str, orbs: u64) -> Cluster {
    let kernel = std::env::var("ORBD_KVM_KERNEL").expect("set ORBD_KVM_KERNEL");
    let rootfs = std::env::var("ORBD_KVM_ROOTFS").expect("set ORBD_KVM_ROOTFS");
    Cluster::with_runtime(
        &["agent-a"],
        &RuntimeSetup {
            offering: "qemu-2vcpu-2gb".into(),
            runtime: "qemu".into(),
            vcpus: 2,
            memory_mib: MEMORY_MIB,
            disk_gib: 4,
            on_checkpoint_failure: on_checkpoint_failure.into(),
            agent_orbs: orbs,
            agent_runtime: format!(
                r#"  qemu:
    kernel: {kernel}
    kernel_cmdline: "console=ttyS0 root=/dev/vda rw {cmdline_extra}"
    dns_servers: ["1.1.1.1"]
    shutdown_grace_secs: 20
    static_images:
      - {{ image_ref: "{IMAGE}", rootfs: {rootfs} }}
"#
            ),
        },
    )
}

/// Kills every QEMU started under this cluster, even when a test fails.
struct Vms(PathBuf);

impl Drop for Vms {
    fn drop(&mut self) {
        for pid in qemu_pids(&self.0) {
            let _ = std::process::Command::new("kill")
                .args(["-9", &pid.to_string()])
                .status();
        }
    }
}

fn state(c: &Cluster) -> PathBuf {
    c.root().join("agents/agent-a/state")
}

/// QEMU processes whose arguments point into `root`.
fn qemu_pids(root: &std::path::Path) -> Vec<i32> {
    let needle = root.display().to_string();
    let mut pids = Vec::new();
    for e in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(pid) = e.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        let Ok(cmd) = std::fs::read(e.path().join("cmdline")) else {
            continue;
        };
        let cmd = String::from_utf8_lossy(&cmd);
        if cmd.starts_with("/usr/bin/qemu-system") && cmd.contains(&needle) {
            pids.push(pid);
        }
    }
    pids
}

/// The cluster's orb directories, oldest orb first (orb ids are time-ordered).
fn orb_dirs(c: &Cluster) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(state(c).join("orbs"))
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .collect();
    dirs.sort();
    dirs
}

fn orb_dir(c: &Cluster) -> PathBuf {
    let mut dirs = orb_dirs(c);
    assert_eq!(dirs.len(), 1, "expected one orb");
    dirs.pop().unwrap()
}

fn console(orb: &Path) -> String {
    std::fs::read_to_string(orb.join("logs/console.log")).unwrap_or_default()
}

/// Counter samples in console order: `(pid, n)` for the process counter and `n` for
/// the nested guest. Lines garbled by interleaving are skipped.
fn samples(console: &str) -> (Vec<(u32, u64)>, Vec<u64>) {
    let num = |s: &str| -> Option<u64> {
        let d: String = s.chars().take_while(char::is_ascii_digit).collect();
        d.parse().ok()
    };
    let (mut counter, mut l2) = (Vec::new(), Vec::new());
    for line in console.lines() {
        if let Some(rest) = line.strip_prefix("counter pid=")
            && let Some((pid, n)) = rest.split_once(" n=")
            && let (Some(pid), Some(n)) = (num(pid), num(n))
        {
            counter.push((pid as u32, n));
        } else if let Some(rest) = line.strip_prefix("l2: l2counter n=")
            && let Some(n) = num(rest)
        {
            l2.push(n);
        }
    }
    (counter, l2)
}

/// Waits until both counters advanced by at least two since `after`.
fn wait_progress(orb: &Path, after: (usize, usize)) -> ((u32, u64), u64) {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let (counter, l2) = samples(&console(orb));
        if counter.len() >= after.0 + 2 && l2.len() >= after.1 + 2 {
            return (*counter.last().unwrap(), *l2.last().unwrap());
        }
        assert!(
            Instant::now() < deadline,
            "guest counters did not advance; console tail:\n{}",
            tail(&console(orb))
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn counts(orb: &Path) -> (usize, usize) {
    let (a, b) = samples(&console(orb));
    (a.len(), b.len())
}

fn tail(s: &str) -> String {
    let lines: Vec<&str> = s.lines().rev().take(30).collect();
    lines.into_iter().rev().collect::<Vec<_>>().join("\n")
}

/// Every counter sample since the first boot strictly increases: no restart (processes
/// lost) and no repeat (stale RAM replayed). Same for the nested guest's counter.
fn assert_continuous(orb: &Path) {
    let (counter, l2) = samples(&console(orb));
    let n: Vec<u64> = counter.iter().map(|(_, n)| *n).collect();
    assert!(
        n.windows(2).all(|w| w[1] > w[0]),
        "process counter restarted, repeated or went back: {n:?}"
    );
    assert!(
        l2.windows(2).all(|w| w[1] > w[0]),
        "nested guest counter restarted, repeated or went back: {l2:?}"
    );
}

fn checkpoint_files(orb: &Path) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(orb.join("checkpoints")) {
        for e in rd.flatten() {
            let mem = e.path().join("memory");
            if let Ok(m) = std::fs::metadata(&mem) {
                out.push((e.file_name().to_string_lossy().into(), m.len()));
            }
        }
    }
    out
}

/// Sleep → wake keeps a running process and a nested KVM guest, sleeping orbs hold no
/// QEMU process, and a restarted agent adopts the running VM instead of booting another.
#[test]
#[ignore = "needs KVM; see the module docs"]
fn sleep_preserves_processes_and_nested_guest() {
    let mut c = cluster("cold_stop", "", 1);
    let _vms = Vms(c.root().to_owned());
    c.start_all();

    let t = Instant::now();
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "first spawn");
    eprintln!("cold spawn (create, boot, Wake ack): {:?}", t.elapsed());
    wait_progress(&orb_dir(&c), (0, 0));

    for round in 1..=3 {
        let before = counts(&orb_dir(&c));
        let t = Instant::now();
        assert_ok(&c.dispatch("pause", "amp-1", &[]), "pause");
        let sleep_time = t.elapsed();
        assert!(
            qemu_pids(c.root()).is_empty(),
            "a sleeping orb must not hold a QEMU process"
        );
        let files = checkpoint_files(&orb_dir(&c));
        assert_eq!(
            files.len(),
            1,
            "exactly one published checkpoint: {files:?}"
        );
        eprintln!(
            "round {round}: sleep {sleep_time:?}, checkpoint {} = {} MiB for {MEMORY_MIB} MiB RAM",
            files[0].0,
            files[0].1 >> 20
        );
        assert!(files[0].1 < (MEMORY_MIB + 256) << 20);

        let t = Instant::now();
        let job = c.dispatch("spawn", "amp-1", &[]);
        assert_ok(&job, "wake");
        let restored = job
            .stdout
            .lines()
            .chain(job.stderr.lines())
            .filter(|l| l.contains("restored checkpoint") || l.contains("guest "))
            .collect::<Vec<_>>()
            .join("; ");
        eprintln!("round {round}: wake {:?} ({restored})", t.elapsed());
        assert!(
            checkpoint_files(&orb_dir(&c)).is_empty(),
            "the restored checkpoint is discarded"
        );
        wait_progress(&orb_dir(&c), before);
        assert_continuous(&orb_dir(&c));
    }

    // Agent crash with the VM running: the restarted agent adopts it.
    let before = counts(&orb_dir(&c));
    c.kill_agent("agent-a");
    let vms = qemu_pids(c.root());
    assert_eq!(vms.len(), 1, "QEMU must outlive the agent");
    c.start_agent("agent-a", "");
    c.wait_connected(1);
    assert_ok(
        &c.dispatch("spawn", "amp-1", &[]),
        "spawn after agent restart",
    );
    assert_eq!(qemu_pids(c.root()), vms, "the running VM was adopted");
    wait_progress(&orb_dir(&c), before);
    assert_continuous(&orb_dir(&c));

    assert_ok(&c.dispatch("delete", "amp-1", &[]), "delete");
    assert!(qemu_pids(c.root()).is_empty());
    assert!(
        std::fs::read_dir(state(&c).join("orbs"))
            .unwrap()
            .next()
            .is_none()
    );
}

/// The agent is killed (abort, like a power cut) at every publication boundary of a
/// save and a restore. After it restarts, the interrupted operation converges: a
/// pause ends with no VM process, a spawn with a running VM, and the guest's counters
/// never restart or replay.
#[test]
#[ignore = "needs KVM; see the module docs"]
fn agent_crash_at_publication_boundaries_never_replays_stale_ram() {
    let mut c = cluster("cold_stop", "", 1);
    let _vms = Vms(c.root().to_owned());
    c.start_all();
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "first spawn");
    wait_progress(&orb_dir(&c), (0, 0));

    let agent_log = |c: &Cluster| {
        std::fs::read_to_string(c.root().join("agent-agent-a.log")).unwrap_or_default()
    };
    // Every boundary below runs against a VM that was itself restored.
    let before = counts(&orb_dir(&c));
    assert_ok(&c.dispatch("pause", "amp-1", &[]), "warm-up pause");
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "warm-up wake");
    wait_progress(&orb_dir(&c), before);

    let crash = |c: &mut Cluster, op: &str, point: &str| {
        let before = counts(&orb_dir(c));
        let mark = agent_log(c).len();
        c.kill_agent("agent-a");
        c.start_agent("agent-a", &format!("{point}=return"));
        c.wait_connected(1);
        let job = c.dispatch_bg(op, "amp-1", &[]);
        c.wait_agent_crash("agent-a", Duration::from_secs(120));
        c.start_agent("agent-a", "");
        let job = job.join().unwrap();
        assert_ok(&job, &format!("{op} interrupted at {point}"));
        let vms = qemu_pids(c.root()).len();
        match op {
            "pause" => {
                assert_eq!(vms, 0, "{point}: a paused orb must not run");
                assert_eq!(
                    checkpoint_files(&orb_dir(c)).len(),
                    1,
                    "{point}: one checkpoint"
                );
            }
            _ => {
                assert_eq!(vms, 1, "{point}: a spawned orb must run");
                wait_progress(&orb_dir(c), before);
            }
        }
        let logs = agent_log(c);
        let recovery: Vec<&str> = logs[mark..]
            .lines()
            .filter(|l| {
                l.contains("orbd::runtime::qemu") && (l.contains(" WARN ") || l.contains("ERROR"))
            })
            .filter_map(|l| l.split("orbd::runtime::qemu: ").nth(1))
            .collect();
        eprintln!("{op} crash at {point}: converged; runtime recovery: {recovery:?}");
    };

    for point in [
        "qemu::save::after_stop",
        "qemu::save::after_stream",
        "qemu::save::after_quit",
        "qemu::save::after_publish",
        "agent::pause::after_stop",
    ] {
        crash(&mut c, "pause", point);
        let before = counts(&orb_dir(&c));
        assert_ok(&c.dispatch("spawn", "amp-1", &[]), "wake");
        wait_progress(&orb_dir(&c), before);
        assert_continuous(&orb_dir(&c));
    }
    for point in [
        "qemu::restore::after_load",
        "qemu::restore::after_consume",
        "agent::spawn::after_start",
    ] {
        assert_ok(&c.dispatch("pause", "amp-1", &[]), "pause");
        crash(&mut c, "spawn", point);
        assert_continuous(&orb_dir(&c));
    }

    // Disk full mid-save under the cold-stop policy: the pause fails with processes
    // lost, the VM is shut down, and the next spawn cold-boots without replaying RAM.
    c.kill_agent("agent-a");
    c.start_agent("agent-a", "qemu::save::write=8*off->return");
    c.wait_connected(1);
    let job = c.dispatch("pause", "amp-1", &[]);
    assert_ne!(job.code, 0, "an injected ENOSPC must fail the pause");
    assert!(
        qemu_pids(c.root()).is_empty(),
        "cold-stop policy stops the VM"
    );
    assert!(
        checkpoint_files(&orb_dir(&c)).is_empty(),
        "the partial checkpoint is gone"
    );
    // A restarted counter shows up as its count going down.
    let boots = |c: &Cluster| {
        let n: Vec<u64> = samples(&console(&orb_dir(c)))
            .0
            .iter()
            .map(|(_, n)| *n)
            .collect();
        n.windows(2).filter(|w| w[1] < w[0]).count()
    };
    let boots_before = boots(&c);
    let before = counts(&orb_dir(&c));
    assert_ok(
        &c.dispatch("spawn", "amp-1", &[]),
        "cold boot after failed save",
    );
    wait_progress(&orb_dir(&c), before);
    assert_eq!(
        boots(&c),
        boots_before + 1,
        "the spawn cold-boots a new counter"
    );
}

fn line_count(orb: &Path, pat: &str) -> usize {
    console(orb).lines().filter(|l| l.contains(pat)).count()
}

/// A graceful agent stop (SIGTERM, as a container stop sends) checkpoints both running
/// orbs and leaves no VMM behind; the next agent start restores them without any Amp
/// command: processes and nested guests continue, Amp's bootstrap is not restarted, and
/// the guest only had its clock and network resynced. Then a drain cut short by kill -9
/// (with the VMMs dying alongside, as in a container) ends honestly: what was not saved
/// is reported stopped with processes lost and cold-boots on the next spawn.
#[test]
#[ignore = "needs KVM; see the module docs"]
fn graceful_agent_restart_keeps_running_orbs() {
    let mut c = cluster("cold_stop", "", 2);
    let _vms = Vms(c.root().to_owned());
    c.start_all();
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn 1");
    assert_ok(&c.dispatch("spawn", "amp-2", &[]), "spawn 2");
    let orbs = orb_dirs(&c);
    for o in &orbs {
        wait_progress(o, (0, 0));
    }
    let bootstraps: Vec<usize> = orbs
        .iter()
        .map(|o| line_count(o, "fake-amp-bootstrap:"))
        .collect();
    let before: Vec<(usize, usize)> = orbs.iter().map(|o| counts(o)).collect();

    let t = Instant::now();
    let status = c.terminate_agent("agent-a", Duration::from_secs(300));
    eprintln!("drain took {:?}, agent exit {status:?}", t.elapsed());
    assert!(status.success());
    assert!(qemu_pids(c.root()).is_empty(), "drained orbs hold no VMM");
    for o in &orbs {
        assert_eq!(checkpoint_files(o).len(), 1, "one checkpoint per orb");
    }

    let t = Instant::now();
    c.start_agent("agent-a", "");
    c.wait_connected(1);
    eprintln!("agent ready with restored orbs after {:?}", t.elapsed());
    let log = std::fs::read_to_string(c.root().join("agent-agent-a.log")).unwrap();
    for l in log.lines().filter(|l| {
        l.contains("drain") || l.contains("restore finished") || l.contains("after agent restart")
    }) {
        eprintln!("  {}", l.split_once("Z ").map_or(l, |(_, r)| r));
    }
    assert_eq!(qemu_pids(c.root()).len(), 2, "both orbs run again");
    for (i, o) in orbs.iter().enumerate() {
        wait_progress(o, before[i]);
        assert_continuous(o);
        assert!(
            checkpoint_files(o).is_empty(),
            "restored checkpoints are discarded"
        );
        assert_eq!(
            line_count(o, "fake-amp-bootstrap:"),
            bootstraps[i],
            "Amp's bootstrap must not be restarted without a spawn"
        );
        assert!(line_count(o, "orbd-guest: resync") >= 1, "guest resynced");
    }
    // A spawn after the maintenance restore is an ordinary warm wake.
    assert_ok(&c.dispatch("spawn", "amp-1", &[]), "spawn after restore");
    assert_continuous(&orbs[0]);

    // Drain cut short: the first save is slowed down, the agent is killed mid-save and
    // every VMM dies with it.
    c.terminate_agent("agent-a", Duration::from_secs(300));
    c.start_agent("agent-a", "qemu::save::throttle=return(500)");
    c.wait_connected(1);
    assert_eq!(qemu_pids(c.root()).len(), 2);
    c.signal_agent("agent-a", "TERM");
    std::thread::sleep(Duration::from_secs(5));
    c.kill_agent("agent-a");
    for pid in qemu_pids(c.root()) {
        let _ = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status();
    }
    let max_before: Vec<u64> = orbs
        .iter()
        .map(|o| {
            samples(&console(o))
                .0
                .iter()
                .map(|(_, n)| *n)
                .max()
                .unwrap()
        })
        .collect();
    c.start_agent("agent-a", "");
    c.wait_connected(1);
    assert!(qemu_pids(c.root()).is_empty(), "nothing unsaved is resumed");
    let log = std::fs::read_to_string(c.root().join("agent-agent-a.log")).unwrap();
    let honest: Vec<&str> = log
        .lines()
        .filter(|l| l.contains("no longer running") || l.contains("not restorable"))
        .collect();
    eprintln!("after the cut-short drain:\n{}", honest.join("\n"));
    assert!(!honest.is_empty());
    for (i, o) in orbs.iter().enumerate() {
        assert!(
            checkpoint_files(o).is_empty(),
            "no unfinished save survives"
        );
        let job = c.dispatch("spawn", &format!("amp-{}", i + 1), &[]);
        assert_ok(&job, "spawn after a cut-short drain");
        let before = counts(o);
        let ((_, n), _) = wait_progress(o, before);
        assert!(n < max_before[i], "a cold boot restarts the counter");
    }
}
