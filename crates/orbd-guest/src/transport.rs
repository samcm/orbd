//! Byte-stream transports to the host agent, behind [`Connector`] so Firecracker's vsock
//! (guest listens; the host connects with `CONNECT <port>`) can slot in next to QEMU's
//! virtio-serial port.

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Produces one [`Session`] per host connection.
pub trait Connector: Send + Sync {
    /// Blocks until the host is connected and returns that connection.
    fn accept(&self) -> io::Result<Arc<dyn Session>>;
}

/// One host connection. Reads happen on one thread, writes may come from others.
pub trait Session: Send + Sync {
    /// Blocks until bytes arrive. `Ok(0)` means the host went away (session over).
    fn read(&self, buf: &mut [u8]) -> io::Result<usize>;
    /// Writes one whole frame within a bounded time or fails.
    fn write_all(&self, buf: &[u8]) -> io::Result<()>;
    /// Ends the session and drops anything buffered for it. Later writes fail.
    fn close(&self);
}

const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const BACKOFF_MIN: Duration = Duration::from_millis(10);
const BACKOFF_MAX: Duration = Duration::from_secs(1);

/// A virtio-serial port (QEMU `virtserialport`), found by name. The device node is
/// opened per session: closing it discards data left over from a previous host
/// connection, so a new connection never starts mid-frame.
///
/// The port reports `POLLHUP` while no host is connected and raises `SIGIO` when that
/// changes; [`block_sigio`] must run before any thread starts.
pub struct VirtioSerial {
    name: String,
}

impl VirtioSerial {
    pub fn new(name: &str) -> Self {
        Self { name: name.into() }
    }

    /// `/dev/virtio-ports/<name>` is a udev symlink; images without udev only have the
    /// kernel's `/dev/vportNpM`, whose name is in sysfs.
    fn device(&self) -> io::Result<PathBuf> {
        let link = PathBuf::from("/dev/virtio-ports").join(&self.name);
        if link.exists() {
            return Ok(link);
        }
        for entry in std::fs::read_dir("/sys/class/virtio-ports")? {
            let entry = entry?;
            let name = std::fs::read_to_string(entry.path().join("name")).unwrap_or_default();
            if name.trim() == self.name {
                return Ok(PathBuf::from("/dev").join(entry.file_name()));
            }
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no virtio-serial port named {}", self.name),
        ))
    }

    fn open(&self) -> io::Result<File> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(self.device()?)?;
        let fd = file.as_raw_fd();
        // SAFETY: plain fcntl calls on an fd we own.
        unsafe {
            if libc::fcntl(fd, libc::F_SETOWN, libc::getpid()) < 0 {
                return Err(io::Error::last_os_error());
            }
            let flags = libc::fcntl(fd, libc::F_GETFL);
            if flags < 0 || libc::fcntl(fd, libc::F_SETFL, flags | libc::O_ASYNC) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(file)
    }
}

impl Connector for VirtioSerial {
    fn accept(&self) -> io::Result<Arc<dyn Session>> {
        let mut backoff = BACKOFF_MIN;
        let mut reported = false;
        loop {
            match self.open() {
                Ok(file) => {
                    // Wait for the host end without spinning: SIGIO fires when it connects.
                    loop {
                        if poll(&file, libc::POLLIN, Some(Duration::ZERO))? & libc::POLLHUP == 0 {
                            return Ok(Arc::new(VirtioSession {
                                file: RwLock::new(Some(file)),
                            }));
                        }
                        wait_sigio(backoff);
                        backoff = (backoff * 2).min(BACKOFF_MAX);
                    }
                }
                Err(e) => {
                    if !reported {
                        eprintln!("orbd-guest: waiting for port {}: {e}", self.name);
                        reported = true;
                    }
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                }
            }
        }
    }
}

struct VirtioSession {
    file: RwLock<Option<File>>,
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "session closed")
}

impl Session for VirtioSession {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let guard = self.file.read().unwrap();
        let mut file = guard.as_ref().ok_or_else(closed)?;
        loop {
            match file.read(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    poll(file, libc::POLLIN, None)?;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        let guard = self.file.read().unwrap();
        let mut file = guard.as_ref().ok_or_else(closed)?;
        let deadline = Instant::now() + WRITE_TIMEOUT;
        while !buf.is_empty() {
            match file.write(buf) {
                Ok(0) => return Err(closed()),
                Ok(n) => buf = &buf[n..],
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "write timed out"));
                    }
                    if poll(file, libc::POLLOUT, Some(left))? & libc::POLLHUP != 0 {
                        return Err(closed());
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn close(&self) {
        self.file.write().unwrap().take();
    }
}

/// Returns `revents` (0 on timeout).
fn poll(
    file: &File,
    events: libc::c_short,
    timeout: Option<Duration>,
) -> io::Result<libc::c_short> {
    let mut pfd = libc::pollfd {
        fd: file.as_raw_fd(),
        events,
        revents: 0,
    };
    let ms = timeout.map_or(-1, |t| t.as_millis().min(i32::MAX as u128) as i32);
    loop {
        // SAFETY: one valid pollfd.
        let r = unsafe { libc::poll(&mut pfd, 1, ms) };
        if r >= 0 {
            return Ok(pfd.revents);
        }
        let e = io::Error::last_os_error();
        if e.kind() != io::ErrorKind::Interrupted {
            return Err(e);
        }
    }
}

fn sigio_set() -> libc::sigset_t {
    // SAFETY: sigset_t is plain data initialised by sigemptyset.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGIO);
        set
    }
}

/// Blocks SIGIO for this thread and threads spawned after it, so port state changes are
/// collected by [`wait_sigio`] instead of killing the process.
pub fn block_sigio() {
    let set = sigio_set();
    // SAFETY: valid sigset; null old-set pointer is allowed.
    unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut()) };
}

fn wait_sigio(timeout: Duration) {
    let set = sigio_set();
    let ts = libc::timespec {
        tv_sec: timeout.as_secs() as _,
        tv_nsec: timeout.subsec_nanos() as _,
    };
    // SAFETY: valid sigset and timespec; info pointer may be null.
    unsafe { libc::sigtimedwait(&set, std::ptr::null_mut(), &ts) };
}
