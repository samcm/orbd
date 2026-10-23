//! The `orbd.guest.v1` session loop: u32 big-endian length + protobuf frames (max
//! 64 KiB). Hello is answered on the reading thread; Wake, PrepareSleep and Shutdown run
//! one at a time on a worker so a long bootstrap never blocks Hello on a new connection.
//! The protocol (greeting, acked Wake generations, sync before sleep, poweroff) is
//! handled here; what Wake and Resync do is the [`Handler`]'s.

use crate::proto::{
    Ack, Error, GuestMessage, Hello, HelloReply, HostMessage, Resync, Wake, guest_message,
    host_message,
};
use crate::transport::{Connector, Session};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use zeroize::Zeroize;

pub const MAX_FRAME: usize = 64 * 1024;
const GENERATION_FILE: &str = "/run/orbd-guest/generation";

struct Shared {
    /// Highest acked Wake generation since boot. Kept in /run (tmpfs) so it survives a
    /// helper restart and a memory restore but not a reboot.
    last_generation: AtomicU64,
    boot_id: String,
}

/// What the guest does for `Wake` and `Resync`. Runs on the worker thread, one request
/// at a time; an `Err` is sent to the host as the request's error.
pub trait Handler: Send + Sync + 'static {
    /// Called once per Wake generation newer than the last acked one (a repeated
    /// generation is acked without calling it again).
    fn wake(&self, wake: &Wake) -> Result<(), String>;
    fn resync(&self, resync: &Resync) -> Result<(), String>;
}

struct Job {
    request_id: u64,
    session: Arc<dyn Session>,
    body: host_message::Body,
}

/// Serves the host on `connector` forever, one connection at a time.
pub fn serve(connector: &dyn Connector, handler: Arc<dyn Handler>) -> ! {
    let shared = Arc::new(Shared {
        last_generation: AtomicU64::new(
            std::fs::read_to_string(GENERATION_FILE)
                .ok()
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(0),
        ),
        boot_id: std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .unwrap_or_default()
            .trim()
            .to_owned(),
    });
    let (tx, rx) = mpsc::channel::<Job>();
    let worker_shared = shared.clone();
    std::thread::spawn(move || worker(rx, &worker_shared, &*handler));

    loop {
        let session = match connector.accept() {
            Ok(s) => s,
            Err(e) => {
                log!("accept: {e}");
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            }
        };
        log!("host connected");
        match run_session(&session, &shared, &tx) {
            Ok(()) => log!("host disconnected"),
            Err(e) => log!("session ended: {e}"),
        }
        session.close();
    }
}

fn run_session(
    session: &Arc<dyn Session>,
    shared: &Shared,
    tx: &mpsc::Sender<Job>,
) -> Result<(), String> {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut chunk = vec![0u8; 16 * 1024];
    let mut greeted = false;
    loop {
        let n = session.read(&mut chunk).map_err(|e| e.to_string())?;
        if n == 0 {
            // Partial frames from this connection are discarded with the buffer.
            buf.zeroize();
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        chunk[..n].zeroize();
        while buf.len() >= 4 {
            let len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
            if len > MAX_FRAME {
                buf.zeroize();
                reply_error(
                    session,
                    0,
                    &format!("frame of {len} bytes exceeds {MAX_FRAME}"),
                );
                return Err("oversized frame".into());
            }
            if buf.len() < 4 + len {
                break;
            }
            let decoded = HostMessage::decode(&buf[4..4 + len]);
            buf[..4 + len].zeroize();
            buf.drain(..4 + len);
            let msg = match decoded {
                Ok(m) => m,
                Err(e) => {
                    reply_error(session, 0, &format!("undecodable frame: {e}"));
                    return Err(format!("undecodable frame: {e}"));
                }
            };
            let Some(body) = msg.body else {
                reply_error(session, msg.request_id, "message has no body");
                continue;
            };
            match body {
                host_message::Body::Hello(Hello {}) => {
                    greeted = true;
                    send(
                        session,
                        msg.request_id,
                        guest_message::Body::Hello(HelloReply {
                            guest_version: env!("CARGO_PKG_VERSION").into(),
                            last_generation: shared.last_generation.load(Ordering::SeqCst),
                            boot_id: shared.boot_id.clone(),
                        }),
                    );
                }
                mut other if !greeted => {
                    scrub(&mut other);
                    send(
                        session,
                        msg.request_id,
                        guest_message::Body::Error(Error {
                            message: "expected Hello first".into(),
                            retry: true,
                        }),
                    );
                }
                body => {
                    let _ = tx.send(Job {
                        request_id: msg.request_id,
                        session: session.clone(),
                        body,
                    });
                }
            }
        }
    }
}

fn worker(rx: mpsc::Receiver<Job>, shared: &Shared, handler: &dyn Handler) {
    for mut job in rx {
        let result = match &job.body {
            host_message::Body::Wake(w) => {
                let last = shared.last_generation.load(Ordering::SeqCst);
                if w.generation < last {
                    Err(format!(
                        "wake generation {} is older than acked generation {last}",
                        w.generation
                    ))
                } else if w.generation == last && last != 0 {
                    // Already done for this generation (host retry after reconnect).
                    Ok(last)
                } else {
                    handler.wake(w).map(|()| {
                        shared.last_generation.store(w.generation, Ordering::SeqCst);
                        let _ = std::fs::create_dir_all("/run/orbd-guest");
                        let _ = std::fs::write(GENERATION_FILE, w.generation.to_string());
                        log!("acked generation {}", w.generation);
                        w.generation
                    })
                }
            }
            host_message::Body::PrepareSleep(_) => {
                // SAFETY: sync(2) has no preconditions.
                unsafe { libc::sync() };
                log!("prepared for sleep (filesystems synced)");
                Ok(shared.last_generation.load(Ordering::SeqCst))
            }
            host_message::Body::Resync(r) => handler
                .resync(r)
                .map(|()| shared.last_generation.load(Ordering::SeqCst)),
            host_message::Body::Shutdown(_) => {
                log!("shutdown requested");
                // SAFETY: as above.
                unsafe { libc::sync() };
                Ok(shared.last_generation.load(Ordering::SeqCst))
            }
            host_message::Body::Hello(_) => unreachable!("answered by the session loop"),
        };
        let shutdown = matches!(job.body, host_message::Body::Shutdown(_));
        scrub(&mut job.body);
        match result {
            Ok(generation) => send(
                &job.session,
                job.request_id,
                guest_message::Body::Ack(Ack { generation }),
            ),
            Err(e) => {
                log!("request {} failed: {}", job.request_id, first_line(&e));
                reply_error(&job.session, job.request_id, &e);
            }
        }
        if shutdown {
            let _ = crate::cmd::run(
                &["systemctl", "poweroff", "--no-block"],
                std::time::Duration::from_secs(30),
            );
        }
    }
}

/// Wipes credential values from a handled message.
fn scrub(body: &mut host_message::Body) {
    if let host_message::Body::Wake(w) = body {
        for v in &mut w.permitted_env {
            v.value.zeroize();
        }
    }
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or_default()
}

fn reply_error(session: &Arc<dyn Session>, request_id: u64, message: &str) {
    // Leave room for framing within MAX_FRAME.
    let mut message = message.to_owned();
    if message.len() > MAX_FRAME / 2 {
        let mut cut = MAX_FRAME / 2;
        while !message.is_char_boundary(cut) {
            cut -= 1;
        }
        message.truncate(cut);
        message.push_str("\n(truncated)");
    }
    send(
        session,
        request_id,
        guest_message::Body::Error(Error {
            message,
            retry: false,
        }),
    );
}

fn send(session: &Arc<dyn Session>, request_id: u64, body: guest_message::Body) {
    let msg = GuestMessage {
        request_id,
        body: Some(body),
    };
    let mut frame = Vec::with_capacity(4 + msg.encoded_len());
    frame.extend_from_slice(&(msg.encoded_len() as u32).to_be_bytes());
    msg.encode(&mut frame).expect("vec has capacity");
    // A reply for a connection that is gone is dropped; the host's next Hello reports
    // the acked generation instead.
    if let Err(e) = session.write_all(&frame) {
        log!("reply {request_id} dropped: {e}");
    }
}
