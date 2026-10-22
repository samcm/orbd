//! Running the guest's own tools (systemctl, journalctl, ...) with fixed argument
//! vectors, bounded time and capped output.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const MAX_OUTPUT: usize = 32 * 1024;

pub struct Output {
    pub ok: bool,
    pub stdout: String,
    pub stderr: String,
}

pub fn run(argv: &[&str], timeout: Duration) -> Result<Output, String> {
    let mut child = Command::new(argv[0])
        .args(&argv[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("{}: {e}", argv[0]))?;
    let capture = |r: Option<Box<dyn Read + Send>>| {
        std::thread::spawn(move || {
            let mut buf = Vec::new();
            if let Some(r) = r {
                let _ = r.take(MAX_OUTPUT as u64).read_to_end(&mut buf);
            }
            String::from_utf8_lossy(&buf).into_owned()
        })
    };
    let out = capture(child.stdout.take().map(|s| Box::new(s) as _));
    let err = capture(child.stderr.take().map(|s| Box::new(s) as _));
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(s)) => break s,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("{} timed out after {timeout:?}", argv.join(" ")));
            }
            Err(e) => return Err(format!("{}: {e}", argv[0])),
        }
    };
    Ok(Output {
        ok: status.success(),
        stdout: out.join().unwrap_or_default(),
        stderr: err.join().unwrap_or_default(),
    })
}

/// Runs `argv` and fails with its stderr unless it exits 0.
pub fn check(argv: &[&str], timeout: Duration) -> Result<Output, String> {
    let out = run(argv, timeout)?;
    if !out.ok {
        return Err(format!("{} failed: {}", argv.join(" "), out.stderr.trim()));
    }
    Ok(out)
}
