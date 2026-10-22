//! OCI layout → raw ext4: `umoci raw add-layer` (guest integration), `umoci raw unpack`
//! as root (ownership, modes, xattrs and device nodes preserved), `mkfs.ext4 -d` into a
//! sparse file. Guest content is only ever read by these tools: nothing is mounted or
//! executed on the host.

use super::integration::IntegrationLayer;
use super::registry::{BlobCache, Resolved};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

/// umoci release the pipeline is written and tested against.
pub const UMOCI_VERSION: &str = "0.6.0";
const BASE_TAG: &str = "base";
const ORBD_TAG: &str = "orbd";
const MIB: u64 = 1 << 20;

pub struct Tools {
    pub umoci: PathBuf,
    pub mkfs_ext4: PathBuf,
}

/// Runs a tool, as root via `sudo -n` when orbd itself is not root (development hosts;
/// production agents run as root). Fails with the tool's stderr tail.
async fn run(privileged: bool, program: &Path, args: &[&OsStr]) -> Result<String, String> {
    let mut cmd = if privileged && !nix::unistd::geteuid().is_root() {
        let mut c = Command::new("sudo");
        c.arg("-n").arg(program);
        c
    } else {
        Command::new(program)
    };
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("{}: {e}", program.display()))?;
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let (r1, r2, status) = tokio::join!(
        stdout.read_to_end(&mut out),
        stderr.read_to_end(&mut err),
        child.wait()
    );
    let status = status.map_err(|e| e.to_string())?;
    let _ = (r1, r2);
    if !status.success() {
        let err = String::from_utf8_lossy(&err);
        let tail: Vec<&str> = err.lines().rev().take(10).collect();
        let tail: Vec<&str> = tail.into_iter().rev().collect();
        return Err(format!(
            "{} {} failed ({status}): {}",
            program.display(),
            args.first()
                .map_or(String::new(), |a| a.to_string_lossy().into_owned()),
            tail.join("\n")
        ));
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

/// The pinned umoci version, or why the configured binary is not usable.
pub async fn umoci_version(umoci: &Path) -> Result<String, String> {
    let out = run(false, umoci, &[OsStr::new("--version")]).await?;
    let version = out
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("umoci version "))
        .unwrap_or("")
        .trim()
        .to_owned();
    if version != UMOCI_VERSION {
        return Err(format!(
            "{} is umoci {version:?}; orbd needs umoci {UMOCI_VERSION}",
            umoci.display()
        ));
    }
    Ok(version)
}

/// Writes a minimal OCI layout for `resolved` (blobs linked from the cache).
pub fn write_layout(layout: &Path, cache: &BlobCache, resolved: &Resolved) -> Result<(), String> {
    let blobs = layout.join("blobs/sha256");
    std::fs::create_dir_all(&blobs).map_err(|e| e.to_string())?;
    for desc in resolved.blobs() {
        cache.link_into(&desc.digest, layout)?;
    }
    let hex = resolved.manifest_digest.trim_start_matches("sha256:");
    std::fs::write(blobs.join(hex), &resolved.manifest_raw).map_err(|e| e.to_string())?;
    std::fs::write(
        layout.join("oci-layout"),
        br#"{"imageLayoutVersion":"1.0.0"}"#,
    )
    .map_err(|e| e.to_string())?;
    let index = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": resolved.manifest_digest,
            "size": resolved.manifest_raw.len(),
            "annotations": {"org.opencontainers.image.ref.name": BASE_TAG},
        }],
    });
    std::fs::write(layout.join("index.json"), index.to_string()).map_err(|e| e.to_string())
}

/// Builds `out` (raw ext4) from the layout plus the integration layer. `work` is a
/// scratch directory that the caller removes (its rootfs is root-owned).
pub async fn build(
    tools: &Tools,
    layout: &Path,
    integration: &IntegrationLayer,
    work: &Path,
    out: &Path,
    status: &(dyn Fn(&str) + Sync),
) -> Result<u64, String> {
    let layer = work.join("integration.tar");
    std::fs::write(&layer, &integration.tar).map_err(|e| e.to_string())?;
    let base = format!("{}:{BASE_TAG}", layout.display());
    status("adding guest integration layer");
    run(
        false,
        &tools.umoci,
        &[
            "raw".as_ref(),
            "add-layer".as_ref(),
            "--image".as_ref(),
            base.as_ref(),
            "--tag".as_ref(),
            ORBD_TAG.as_ref(),
            "--no-history".as_ref(),
            layer.as_os_str(),
        ],
    )
    .await?;

    let rootfs = work.join("rootfs");
    let image = format!("{}:{ORBD_TAG}", layout.display());
    status("unpacking layers");
    run(
        true,
        &tools.umoci,
        &[
            "raw".as_ref(),
            "unpack".as_ref(),
            "--image".as_ref(),
            image.as_ref(),
            rootfs.as_os_str(),
        ],
    )
    .await?;

    status("sizing root filesystem");
    let du = |flag: &'static str| {
        let rootfs = rootfs.clone();
        async move {
            let out = run(
                true,
                Path::new("du"),
                &[
                    "-s".as_ref(),
                    "-x".as_ref(),
                    flag.as_ref(),
                    rootfs.as_os_str(),
                ],
            )
            .await?;
            out.split_whitespace()
                .next()
                .and_then(|n| n.parse::<u64>().ok())
                .ok_or_else(|| format!("unexpected du output {out:?}"))
        }
    };
    let used = du("--block-size=1").await?;
    let files = du("--inodes").await?;
    // Room for metadata, the journal and first-boot writes before the guest grows the
    // filesystem to the orb's disk size. Inode density follows the image, with the
    // default 16 KiB/inode ratio as a floor so a grown disk keeps enough inodes.
    let inodes = (files * 2).max(used / 16384);
    let size = (used + used / 5 + inodes * 256 + 512 * MIB).div_ceil(64 * MIB) * 64 * MIB;

    status(&format!(
        "building ext4 ({} MiB of files, {files} inodes, {} MiB image)",
        used / MIB,
        size / MIB
    ));
    // Created by orbd so the published file is owned by the agent user, not root.
    let f = std::fs::File::create(out).map_err(|e| format!("{}: {e}", out.display()))?;
    f.set_len(size).map_err(|e| e.to_string())?;
    drop(f);
    let inodes = inodes.to_string();
    let size_kib = format!("{}k", size / 1024);
    run(
        true,
        &tools.mkfs_ext4,
        &[
            "-q".as_ref(),
            "-F".as_ref(),
            "-t".as_ref(),
            "ext4".as_ref(),
            "-L".as_ref(),
            "orbd-root".as_ref(),
            "-N".as_ref(),
            inodes.as_ref(),
            "-E".as_ref(),
            "nodiscard".as_ref(),
            "-d".as_ref(),
            rootfs.as_os_str(),
            out.as_os_str(),
            size_kib.as_ref(),
        ],
    )
    .await?;
    Ok(size)
}

/// Removes a staging directory whose contents may be root-owned.
pub async fn remove_tree(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    run(
        true,
        Path::new("rm"),
        &[
            "-rf".as_ref(),
            "--one-file-system".as_ref(),
            path.as_os_str(),
        ],
    )
    .await
    .map(|_| ())
}
