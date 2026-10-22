//! Host side of the guest channel (`orbd.guest.v1`), shared by the VM runtimes' guest
//! control adapters.
//!
//! Transport: a byte stream from a [`GuestConnector`]. For QEMU that is the
//! virtio-serial port `org.orbd.control`, whose host end is the chardev socket
//! `<run>/qemu/<orb>/guest.sock` (one client at a time); for Firecracker it is vsock
//! through Firecracker's unix socket and its `CONNECT <port>` handshake. Frames are a
//! u32 big-endian length and a protobuf message, at most 64 KiB.
//!
//! Every connection starts with Hello; replies are matched by request id, so stale bytes
//! from an earlier connection (e.g. across a memory restore) are ignored. Connections
//! are short-lived: one per Hello+request, closed afterwards.

use super::log;
use crate::model::OrbDefinition;
use crate::runtime::{BootstrapRequest, GuestError, OpContext};
use crate::secrets::PermittedGuestEnv;
use async_trait::async_trait;
use orbd_proto::guest::v1 as gpb;
use prost::Message;
use secrecy::ExposeSecret;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME: usize = 64 * 1024;
const RETRY: Duration = Duration::from_millis(200);
/// How long a spawn waits for the guest helper's first answer before failing bootstrap.
const GUEST_CONNECT_TIMEOUT: Duration = Duration::from_secs(120);
/// Hello timeouts per connection: 1 s, doubling to 10 s. A Hello can be lost when a
/// restored guest drops a connection it still held from before the save.
const HELLO_ATTEMPT_MAX: Duration = Duration::from_secs(10);

pub trait ByteStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ByteStream for T {}

/// Opens a byte stream to the guest helper.
#[async_trait]
pub trait GuestConnector: Send + Sync {
    async fn connect(&self) -> std::io::Result<Box<dyn ByteStream>>;
}

/// QEMU virtio-serial: the chardev socket QEMU listens on.
pub struct VirtioSerialConnector {
    pub socket: PathBuf,
}

#[async_trait]
impl GuestConnector for VirtioSerialConnector {
    async fn connect(&self) -> std::io::Result<Box<dyn ByteStream>> {
        Ok(Box::new(
            tokio::net::UnixStream::connect(&self.socket).await?,
        ))
    }
}

/// Firecracker vsock: its unix socket, then `CONNECT <port>\n` → `OK <host port>\n`.
pub struct FirecrackerVsockConnector {
    pub socket: PathBuf,
    pub port: u32,
}

#[async_trait]
impl GuestConnector for FirecrackerVsockConnector {
    async fn connect(&self) -> std::io::Result<Box<dyn ByteStream>> {
        let mut stream =
            tokio::io::BufReader::new(tokio::net::UnixStream::connect(&self.socket).await?);
        stream
            .get_mut()
            .write_all(format!("CONNECT {}\n", self.port).as_bytes())
            .await?;
        // The guest says nothing before Hello, so the buffer holds only this line.
        let mut line = String::new();
        stream.read_line(&mut line).await?;
        if !line.starts_with("OK ") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("vsock port {} not listening", self.port),
            ));
        }
        Ok(Box::new(stream.into_inner()))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("guest channel: {0}")]
    Io(#[from] std::io::Error),
    #[error("guest channel: {0}")]
    Protocol(String),
    #[error("guest channel timed out")]
    Timeout,
}

pub struct Channel {
    stream: Box<dyn ByteStream>,
    next_id: u64,
}

impl Channel {
    async fn send(&mut self, body: gpb::host_message::Body) -> Result<u64, ChannelError> {
        self.next_id += 1;
        let msg = gpb::HostMessage {
            request_id: self.next_id,
            body: Some(body),
        };
        // Wake carries credentials: wipe the encoded copies once sent.
        let bytes = zeroize::Zeroizing::new(msg.encode_to_vec());
        if bytes.len() > MAX_FRAME {
            return Err(ChannelError::Protocol(format!(
                "message of {} bytes exceeds the {MAX_FRAME}-byte frame limit",
                bytes.len()
            )));
        }
        let mut frame = zeroize::Zeroizing::new(Vec::with_capacity(bytes.len() + 4));
        frame.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(&bytes);
        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;
        Ok(self.next_id)
    }

    /// Reads replies until the one for `request_id`, discarding stale ones.
    async fn reply(&mut self, request_id: u64) -> Result<gpb::guest_message::Body, ChannelError> {
        loop {
            let mut len = [0u8; 4];
            self.stream.read_exact(&mut len).await?;
            let n = u32::from_be_bytes(len) as usize;
            if n > MAX_FRAME {
                return Err(ChannelError::Protocol(format!("frame of {n} bytes")));
            }
            let mut buf = vec![0u8; n];
            self.stream.read_exact(&mut buf).await?;
            let msg = gpb::GuestMessage::decode(&buf[..])
                .map_err(|e| ChannelError::Protocol(e.to_string()))?;
            if msg.request_id == request_id
                && let Some(body) = msg.body
            {
                return Ok(body);
            }
        }
    }

    pub async fn request(
        &mut self,
        body: gpb::host_message::Body,
        deadline: Instant,
    ) -> Result<gpb::guest_message::Body, ChannelError> {
        let fut = async {
            let id = self.send(body).await?;
            self.reply(id).await
        };
        tokio::time::timeout_at(deadline.into(), fut)
            .await
            .map_err(|_| ChannelError::Timeout)?
    }
}

/// Connects and completes Hello, retrying (the guest may still be booting or
/// reopening its port) until `deadline`.
pub async fn open(
    connector: &dyn GuestConnector,
    deadline: Instant,
) -> Result<(Channel, gpb::HelloReply), ChannelError> {
    let mut last = ChannelError::Timeout;
    let mut wait = Duration::from_secs(1);
    while Instant::now() < deadline {
        match connector.connect().await {
            Ok(stream) => {
                let mut ch = Channel { stream, next_id: 0 };
                // Bounded per connection: a guest helper that restarted meanwhile only
                // answers on a fresh connection.
                let attempt = (Instant::now() + wait).min(deadline);
                wait = (wait * 2).min(HELLO_ATTEMPT_MAX);
                match ch
                    .request(gpb::host_message::Body::Hello(gpb::Hello {}), attempt)
                    .await
                {
                    Ok(gpb::guest_message::Body::Hello(h)) => return Ok((ch, h)),
                    Ok(other) => {
                        last = ChannelError::Protocol(format!("unexpected Hello reply {other:?}"))
                    }
                    Err(e) => last = e,
                }
            }
            Err(e) => last = e.into(),
        }
        tokio::time::sleep(RETRY.min(deadline.saturating_duration_since(Instant::now()))).await;
    }
    Err(last)
}

/// Asks the guest to sync filesystems before a memory save. Returns the guest's boot id.
/// The channel is closed before the VM is stopped.
pub async fn prepare_sleep(
    connector: &dyn GuestConnector,
    deadline: Instant,
) -> Result<String, ChannelError> {
    let (mut ch, hello) = open(connector, deadline).await?;
    match ch
        .request(
            gpb::host_message::Body::PrepareSleep(gpb::PrepareSleep {}),
            deadline,
        )
        .await?
    {
        gpb::guest_message::Body::Ack(_) => Ok(hello.boot_id),
        gpb::guest_message::Body::Error(e) => Err(ChannelError::Protocol(e.message)),
        other => Err(ChannelError::Protocol(format!(
            "unexpected reply {other:?}"
        ))),
    }
}

/// Best effort: asks the guest to shut down cleanly.
pub async fn shutdown(connector: &dyn GuestConnector, deadline: Instant) {
    if let Ok((mut ch, _)) = open(connector, deadline).await {
        let _ = ch
            .request(
                gpb::host_message::Body::Shutdown(gpb::Shutdown {}),
                deadline,
            )
            .await;
    }
}

/// One running VM's guest, as the guest control functions need it.
pub struct Guest<'a> {
    pub connector: &'a dyn GuestConnector,
    pub network: gpb::NetworkConfig,
    /// Serial console log, quoted when the guest helper never answers.
    pub console_log: &'a Path,
}

fn now_unix_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn redact(text: &str, env: &PermittedGuestEnv) -> String {
    let mut text = text.to_owned();
    for (name, value) in &env.0 {
        let v = value.expose_secret();
        if v.len() >= 4 {
            text = text.replace(v, &format!("[{name} redacted]"));
        }
    }
    text
}

pub fn interrupted(ctx: &OpContext) -> GuestError {
    if ctx.cancel.is_cancelled() {
        GuestError::Cancelled
    } else {
        GuestError::DeadlineExceeded
    }
}

/// After a memory restore that no spawn asked for: steps the guest clock and reapplies
/// its network.
pub async fn resync(guest: Guest<'_>, ctx: &OpContext) -> Result<(), GuestError> {
    loop {
        let attempt = async {
            let (mut ch, _) = open(guest.connector, ctx.work_deadline).await?;
            let resync = gpb::Resync {
                current_time_unix_ns: now_unix_ns(),
                network: Some(guest.network.clone()),
            };
            match ch
                .request(gpb::host_message::Body::Resync(resync), ctx.work_deadline)
                .await?
            {
                gpb::guest_message::Body::Ack(_) => Ok(Ok(())),
                gpb::guest_message::Body::Error(e) if e.retry => {
                    Err(ChannelError::Protocol(e.message))
                }
                gpb::guest_message::Body::Error(e) => Ok(Err(e.message)),
                other => Err(ChannelError::Protocol(format!(
                    "unexpected Resync reply {other:?}"
                ))),
            }
        };
        let res = tokio::select! {
            _ = ctx.cancel.cancelled() => return Err(GuestError::Cancelled),
            r = attempt => r,
        };
        match res {
            Ok(Ok(())) => {
                log(ctx, "guest resynced (clock, network)");
                return Ok(());
            }
            Ok(Err(msg)) => return Err(GuestError::BootstrapFailed(msg)),
            Err(_) if Instant::now() >= ctx.work_deadline => {
                return Err(GuestError::DeadlineExceeded);
            }
            Err(e) => {
                log(ctx, &format!("guest channel: {e}; reconnecting"));
                tokio::time::sleep(RETRY).await;
            }
        }
    }
}

/// Sends Wake (env, clock, network, cold/warm) to a running VM and waits for the guest
/// to ack the generation, within the work deadline. Idempotent per generation: a guest
/// that already acked it (HelloReply.last_generation) is not woken twice.
pub async fn bootstrap(
    guest: Guest<'_>,
    orb: &OrbDefinition,
    request: BootstrapRequest,
    ctx: &OpContext,
) -> Result<(), GuestError> {
    let env = &request.env;
    env.validate().map_err(GuestError::BootstrapFailed)?;
    log(
        ctx,
        &format!(
            "waking guest (generation {}, {} boot)",
            request.generation,
            if request.cold_boot { "cold" } else { "warm" }
        ),
    );
    let connector = guest.connector;
    let deadline = ctx.work_deadline;
    let started = Instant::now();
    // A guest that never answers is a bootstrap failure, not a spent budget.
    let ready_by = (Instant::now() + GUEST_CONNECT_TIMEOUT).min(deadline);
    let ready = tokio::select! {
        _ = ctx.cancel.cancelled() => return Err(GuestError::Cancelled),
        r = open(connector, ready_by) => r,
    };
    // The first attempt reuses this connection. Reconnecting right after a Hello races
    // the guest: it can answer the new Hello on its old session, then notice the
    // disconnect and reopen, so the Wake lands on an ungreeted session.
    let mut greeted = match ready {
        Ok(ch) => Some(ch),
        Err(e) => {
            if Instant::now() >= deadline {
                return Err(GuestError::DeadlineExceeded);
            }
            let msg = format!(
                "guest helper did not answer within {}s ({e}); console: {}",
                GUEST_CONNECT_TIMEOUT.as_secs(),
                super::log_tail(guest.console_log)
            );
            log(ctx, &msg);
            return Err(GuestError::Unreachable(msg));
        }
    };
    let mut announced = false;
    loop {
        let reuse = greeted.take();
        let attempt = async {
            let (mut ch, hello) = match reuse {
                Some(c) => c,
                None => open(connector, deadline).await?,
            };
            if !announced {
                log(
                    ctx,
                    &format!(
                        "guest helper {} answered after {} ms (last generation {})",
                        hello.guest_version,
                        started.elapsed().as_millis(),
                        hello.last_generation
                    ),
                );
            }
            if hello.last_generation >= request.generation {
                return Ok(Ok(()));
            }
            let wake = gpb::Wake {
                generation: request.generation,
                current_time_unix_ns: now_unix_ns(),
                permitted_env: env
                    .0
                    .iter()
                    .map(|(n, v)| gpb::EnvVar {
                        name: n.clone(),
                        value: v.expose_secret().to_owned(),
                    })
                    .collect(),
                network: Some(guest.network.clone()),
                cold_boot: request.cold_boot,
                hostname: crate::runtime::orb_hostname(orb),
            };
            match ch
                .request(gpb::host_message::Body::Wake(wake), deadline)
                .await?
            {
                gpb::guest_message::Body::Ack(a) if a.generation == request.generation => {
                    Ok(Ok(()))
                }
                gpb::guest_message::Body::Error(e) if e.retry => {
                    Err(ChannelError::Protocol(e.message))
                }
                gpb::guest_message::Body::Error(e) => Ok(Err(e.message)),
                other => Err(ChannelError::Protocol(format!(
                    "unexpected Wake reply {other:?}"
                ))),
            }
        };
        let res = tokio::select! {
            _ = ctx.cancel.cancelled() => return Err(GuestError::Cancelled),
            r = attempt => r,
        };
        announced = true;
        match res {
            Ok(Ok(())) => {
                log(
                    ctx,
                    &format!(
                        "guest acked generation {} after {} ms",
                        request.generation,
                        started.elapsed().as_millis()
                    ),
                );
                return Ok(());
            }
            Ok(Err(msg)) => {
                let msg = redact(&msg, env);
                log(ctx, &format!("guest bootstrap failed: {msg}"));
                return Err(GuestError::BootstrapFailed(msg));
            }
            Err(_) if Instant::now() >= deadline => return Err(GuestError::DeadlineExceeded),
            // EOF mid-request (guest helper restarted, port reopened): reconnect.
            Err(e) => {
                log(ctx, &format!("guest channel: {e}; reconnecting"));
                tokio::time::sleep(RETRY).await;
            }
        }
    }
}
