//! Byte-stream transports to the host agent, behind [`Connector`]: QEMU's virtio-serial
//! port and Firecracker's vsock (the guest listens; the host connects through
//! Firecracker's unix socket with `CONNECT <port>`).

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
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

/// An AF_VSOCK listener (Firecracker). The listening socket survives a snapshot
/// restore; connections open at the snapshot are reset, so the host reconnects.
pub struct Vsock {
    port: u32,
    listener: std::sync::Mutex<Option<OwnedFd>>,
}

impl Vsock {
    pub fn new(port: u32) -> Self {
        Self {
            port,
            listener: std::sync::Mutex::new(None),
        }
    }

    fn listen(&self) -> io::Result<OwnedFd> {
        // SAFETY: plain socket calls; the fd is owned right after creation.
        unsafe {
            let fd = libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = OwnedFd::from_raw_fd(fd);
            let mut addr: libc::sockaddr_vm = std::mem::zeroed();
            addr.svm_family = libc::AF_VSOCK as libc::sa_family_t;
            addr.svm_cid = libc::VMADDR_CID_ANY;
            addr.svm_port = self.port;
            if libc::bind(
                fd.as_raw_fd(),
                (&addr as *const libc::sockaddr_vm).cast(),
                std::mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
            ) < 0
                || libc::listen(fd.as_raw_fd(), 4) < 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(fd)
        }
    }
}

impl Connector for Vsock {
    fn accept(&self) -> io::Result<Arc<dyn Session>> {
        let mut backoff = BACKOFF_MIN;
        let mut reported = false;
        loop {
            let mut guard = self.listener.lock().unwrap();
            if guard.is_none() {
                match self.listen() {
                    Ok(fd) => *guard = Some(fd),
                    Err(e) => {
                        drop(guard);
                        if !reported {
                            eprintln!("orbd-guest: waiting for vsock port {}: {e}", self.port);
                            reported = true;
                        }
                        std::thread::sleep(backoff);
                        backoff = (backoff * 2).min(BACKOFF_MAX);
                        continue;
                    }
                }
            }
            let listener = guard.as_ref().unwrap().as_raw_fd();
            // SAFETY: accept on our listening socket; the new fd is owned at once.
            let fd = unsafe {
                libc::accept4(
                    listener,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    libc::SOCK_CLOEXEC,
                )
            };
            if fd < 0 {
                let e = io::Error::last_os_error();
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            // SAFETY: `fd` is a new socket nothing else owns.
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            let timeout = libc::timeval {
                tv_sec: WRITE_TIMEOUT.as_secs() as _,
                tv_usec: 0,
            };
            // SAFETY: valid fd and option struct.
            unsafe {
                libc::setsockopt(
                    fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_SNDTIMEO,
                    (&timeout as *const libc::timeval).cast(),
                    std::mem::size_of::<libc::timeval>() as libc::socklen_t,
                );
            }
            return Ok(Arc::new(VsockSession { fd }));
        }
    }
}

struct VsockSession {
    fd: OwnedFd,
}

impl Session for VsockSession {
    fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            // SAFETY: reads into a valid buffer of the given length.
            let n = unsafe { libc::read(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            if n >= 0 {
                return Ok(n as usize);
            }
            let e = io::Error::last_os_error();
            match e.kind() {
                io::ErrorKind::Interrupted => {}
                // A reset connection (snapshot taken, host gone) ends the session.
                io::ErrorKind::ConnectionReset | io::ErrorKind::NotConnected => return Ok(0),
                _ => return Err(e),
            }
        }
    }

    fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            // SAFETY: writes from a valid buffer; MSG_NOSIGNAL avoids SIGPIPE.
            let n = unsafe {
                libc::send(
                    self.fd.as_raw_fd(),
                    buf.as_ptr().cast(),
                    buf.len(),
                    libc::MSG_NOSIGNAL,
                )
            };
            if n > 0 {
                buf = &buf[n as usize..];
                continue;
            }
            if n == 0 {
                return Err(closed());
            }
            let e = io::Error::last_os_error();
            match e.kind() {
                io::ErrorKind::Interrupted => {}
                io::ErrorKind::WouldBlock => {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "write timed out"));
                }
                _ => return Err(e),
            }
        }
        Ok(())
    }

    fn close(&self) {
        // SAFETY: shutdown on our socket; the fd itself closes when the session drops.
        unsafe { libc::shutdown(self.fd.as_raw_fd(), libc::SHUT_RDWR) };
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
