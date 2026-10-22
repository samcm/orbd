//! Small filesystem helpers. Paths under /home/user are controlled by the orb's user,
//! so files are created with O_EXCL|O_NOFOLLOW and moved into place with rename(2).

use std::ffi::CString;
use std::fs::File;
use std::io::{self, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, fchown};
use std::path::Path;

/// Atomically replaces `path` with `body` (mode `mode`, owner `owner` if given).
pub fn write_atomic(
    path: &Path,
    body: &[u8],
    mode: u32,
    owner: Option<(u32, u32)>,
) -> io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("/"));
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let tmp = dir.join(format!(".{name}.orbd-{}", std::process::id()));
    let _ = std::fs::remove_file(&tmp);
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&tmp)?;
    let result = (|| {
        if let Some((uid, gid)) = owner {
            fchown(&f, Some(uid), Some(gid))?;
        }
        // The umask may have narrowed the mode; set it exactly.
        f.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(mode))?;
        f.write_all(body)?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
        File::open(dir)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// `mkdir -p` where every created directory gets `owner`; existing ones are untouched.
pub fn create_dirs_owned(path: &Path, owner: (u32, u32)) -> io::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        create_dirs_owned(parent, owner)?;
    }
    match std::fs::create_dir(path) {
        Ok(()) => {
            let c = CString::new(path.as_os_str().as_bytes())?;
            // SAFETY: valid C string; lchown does not follow symlinks.
            if unsafe { libc::lchown(c.as_ptr(), owner.0, owner.1) } < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

/// uid and primary gid of a local user.
pub fn user_ids(name: &str) -> io::Result<(u32, u32)> {
    let c = CString::new(name)?;
    // SAFETY: getpwnam returns a pointer into static storage or null; this process does
    // not call it concurrently.
    let pw = unsafe { libc::getpwnam(c.as_ptr()) };
    if pw.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no user {name}"),
        ));
    }
    // SAFETY: non-null passwd entry.
    Ok(unsafe { ((*pw).pw_uid, (*pw).pw_gid) })
}
