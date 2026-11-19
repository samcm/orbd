//! OCI layout → raw ext4: `umoci raw add-layer` (guest integration), `umoci raw unpack`
//! as root (ownership, modes, xattrs and device nodes preserved), `mkfs.ext4 -d` into a
//! sparse file. Guest content is only ever read by these tools: nothing is mounted or
//! executed on the host.
//!
//! With a builder image (macOS, which has neither umoci nor a filesystem that keeps
//! Linux ownership and device nodes) the same steps run in one `docker run` of that
//! image: the rootfs is unpacked inside the container and only the work directory
//! (layout, integration layer, output file) is shared with the host.

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
    /// Docker image with umoci, e2fsprogs and GNU coreutils; set, every step runs in it.
    pub builder: Option<String>,
}

/// The pipeline inside a builder container: `$1` work dir, `$2` OCI layout, `$3` output.
/// Sizing is [`sizing`]'s (a test checks they agree). The last stdout line is
/// `size <bytes>`; the rootfs's os-release is left in the work dir as [`OS_RELEASE`].
const BUILDER_SCRIPT: &str = r#"set -euo pipefail
work=$1 layout=$2 out=$3
umoci raw add-layer --image "$layout:base" --tag orbd --no-history "$work/integration.tar"
umoci raw unpack --image "$layout:orbd" /rootfs
for f in usr/lib/os-release etc/os-release; do
  if [ -f "/rootfs/$f" ] && [ ! -L "/rootfs/$f" ]; then cp "/rootfs/$f" "$work/os-release"; break; fi
done
used=$(du -s -x --block-size=1 /rootfs | cut -f1)
files=$(du -s -x --inodes /rootfs | cut -f1)
inodes=$(( files * 2 > used / 16384 ? files * 2 : used / 16384 ))
mib=$((1 << 20))
size=$(( (used + used / 5 + inodes * 256 + 512 * mib + 64 * mib - 1) / (64 * mib) * 64 * mib ))
truncate -s "$size" "$out"
mkfs.ext4 -q -F -t ext4 -L orbd-root -N "$inodes" -E nodiscard -d /rootfs "$out" "$((size / 1024))k"
echo "size $size"
"#;

/// `docker run` of the local builder image (never pulled), without network.
pub(crate) const DOCKER_RUN: [&str; 5] = ["run", "--rm", "--pull=never", "--network", "none"];

/// Inodes and size in bytes of the ext4 image for `used` bytes in `files` files: room for
/// metadata, the journal and first-boot writes before the guest grows the filesystem to
/// the orb's disk size. Inode density follows the image, with the default 16 KiB/inode
/// ratio as a floor so a grown disk keeps enough inodes.
fn sizing(used: u64, files: u64) -> (u64, u64) {
    let inodes = (files * 2).max(used / 16384);
    let size = (used + used / 5 + inodes * 256 + 512 * MIB).div_ceil(64 * MIB) * 64 * MIB;
    (inodes, size)
}

/// Removes the named builder container when dropped before [`Self::done`]: killing
/// `docker run` (a cancelled or timed-out build) leaves its container running.
struct Container(Option<String>);

impl Container {
    fn done(mut self) {
        self.0 = None;
    }
}

impl Drop for Container {
    fn drop(&mut self) {
        if let Some(name) = &self.0 {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", name])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
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

/// The pinned umoci version, or why the configured binary (or the builder's) is not
/// usable.
pub async fn umoci_version(tools: &Tools) -> Result<String, String> {
    let (out, umoci) = match &tools.builder {
        Some(builder) => {
            let args: Vec<&OsStr> = DOCKER_RUN
                .into_iter()
                .chain([builder.as_str(), "umoci", "--version"])
                .map(OsStr::new)
                .collect();
            (
                run(false, Path::new("docker"), &args).await?,
                builder.clone(),
            )
        }
        None => (
            run(false, &tools.umoci, &[OsStr::new("--version")]).await?,
            tools.umoci.display().to_string(),
        ),
    };
    let version = out
        .lines()
        .next()
        .and_then(|l| l.strip_prefix("umoci version "))
        .unwrap_or("")
        .trim()
        .to_owned();
    if version != UMOCI_VERSION {
        return Err(format!(
            "{umoci} is umoci {version:?}; orbd needs umoci {UMOCI_VERSION}"
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

/// Where the builder container leaves the rootfs's os-release in the work dir.
const OS_RELEASE: &str = "os-release";

/// The distribution of an os-release file: `ID` and `VERSION_ID` (`VERSION_CODENAME`
/// when there is no version, as on Debian testing), e.g. `debian 13`.
fn os_identity(os_release: &str) -> Option<String> {
    let field = |key: &str| {
        os_release.lines().find_map(|l| {
            let v = l.trim().strip_prefix(key)?.strip_prefix('=')?;
            Some(v.trim().trim_matches(['"', '\''])).filter(|v| !v.is_empty())
        })
    };
    let id = field("ID")?;
    let version = field("VERSION_ID").or_else(|| field("VERSION_CODENAME"))?;
    Some(format!("{id} {version}"))
}

/// [`os_identity`] of the first of `candidates` that is a regular file (a symlink in an
/// image is not followed on the host).
fn read_os_identity(candidates: &[PathBuf]) -> Option<String> {
    candidates.iter().find_map(|p| {
        if !std::fs::symlink_metadata(p).ok()?.is_file() {
            return None;
        }
        os_identity(&std::fs::read_to_string(p).ok()?)
    })
}

/// Builds `out` (raw ext4) from the layout plus the integration layer. `work` is a
/// scratch directory that the caller removes (its rootfs is root-owned). Returns the
/// image's size and its distribution ([`os_identity`]) when it has an os-release.
pub async fn build(
    tools: &Tools,
    layout: &Path,
    integration: &IntegrationLayer,
    work: &Path,
    out: &Path,
    status: &(dyn Fn(&str) + Sync),
) -> Result<(u64, Option<String>), String> {
    let layer = work.join("integration.tar");
    std::fs::write(&layer, &integration.tar).map_err(|e| e.to_string())?;
    if let Some(builder) = &tools.builder {
        status(&format!("unpacking layers and building ext4 in {builder}"));
        // The work dir is unique per build; it is shared at the same path.
        let name = format!(
            "orbd-build-{}",
            work.file_name().unwrap_or_default().display()
        );
        let mut volume = work.as_os_str().to_owned();
        volume.push(":");
        volume.push(work.as_os_str());
        let mut args: Vec<std::ffi::OsString> = DOCKER_RUN.iter().map(Into::into).collect();
        args.extend(["--name".into(), name.clone().into(), "-v".into(), volume]);
        args.extend([builder, "bash", "-c", BUILDER_SCRIPT, "build"].map(Into::into));
        args.extend([work, layout, out].map(|p| p.as_os_str().to_owned()));
        let args: Vec<&OsStr> = args.iter().map(|a| a.as_os_str()).collect();
        let container = Container(Some(name));
        let stdout = run(false, Path::new("docker"), &args).await?;
        container.done();
        let size = stdout
            .lines()
            .rev()
            .find_map(|l| l.strip_prefix("size ")?.trim().parse().ok())
            .ok_or_else(|| format!("builder printed no size: {stdout:?}"))?;
        return Ok((size, read_os_identity(&[work.join(OS_RELEASE)])));
    }
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
    let os = read_os_identity(&["usr/lib/os-release", "etc/os-release"].map(|p| rootfs.join(p)));
    let used = du("--block-size=1").await?;
    let files = du("--inodes").await?;
    let (inodes, size) = sizing(used, files);

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
    Ok((size, os))
}

/// Removes a staging directory whose contents may be root-owned.
pub async fn remove_tree(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    // Nothing root-owned lands on macOS hosts (rootfs unpacking runs in the builder).
    if cfg!(not(target_os = "linux")) {
        return std::fs::remove_dir_all(path).map_err(|e| format!("{}: {e}", path.display()));
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A manifest that lists a layer twice (Amp's image repeats an empty layer) links it
    /// once and leaves the cached blob intact.
    #[test]
    fn a_repeated_layer_keeps_its_cached_blob() {
        use oci_client::manifest::{OciDescriptor, OciImageManifest};
        use sha2::Digest;
        let tmp = tempfile::tempdir().unwrap();
        let cache = BlobCache::new(tmp.path().join("cache"));
        std::fs::create_dir_all(tmp.path().join("cache")).unwrap();
        let blob = |data: &[u8]| {
            let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(data)));
            std::fs::write(cache.path(&digest).unwrap(), data).unwrap();
            OciDescriptor {
                digest,
                size: data.len() as i64,
                ..Default::default()
            }
        };
        let (config, empty, other) = (blob(b"{}"), blob(b"empty layer"), blob(b"layer"));
        let resolved = Resolved {
            reference: "example.invalid/amp-orb:latest".parse().unwrap(),
            manifest_digest: format!("sha256:{}", "0".repeat(64)),
            manifest_raw: b"{}".to_vec(),
            manifest: OciImageManifest {
                config,
                layers: vec![empty.clone(), other, empty.clone()],
                ..Default::default()
            },
        };
        assert_eq!(resolved.blobs().count(), 3);
        write_layout(&tmp.path().join("layout"), &cache, &resolved).unwrap();
        assert_eq!(
            std::fs::read(cache.path(&empty.digest).unwrap()).unwrap(),
            b"empty layer"
        );
        // Linking a blob the layout already has is a no-op, never a copy onto itself.
        cache
            .link_into(&empty.digest, &tmp.path().join("layout"))
            .unwrap();
        assert_eq!(
            std::fs::read(cache.path(&empty.digest).unwrap()).unwrap(),
            b"empty layer"
        );
    }

    /// The builder container sizes images with `BUILDER_SCRIPT`'s shell arithmetic, which
    /// must stay [`sizing`].
    #[test]
    fn os_identity_is_the_release_not_the_point_release() {
        let trixie = "PRETTY_NAME=\"Debian GNU/Linux 13 (trixie)\"\nNAME=\"Debian GNU/Linux\"\nVERSION_ID=\"13\"\nVERSION=\"13 (trixie)\"\nVERSION_CODENAME=trixie\nDEBIAN_VERSION_FULL=13.2\nID=debian\n";
        assert_eq!(os_identity(trixie).as_deref(), Some("debian 13"));
        let sid = "PRETTY_NAME=\"Debian GNU/Linux forky/sid\"\nVERSION_CODENAME=forky\nID=debian\n";
        assert_eq!(os_identity(sid).as_deref(), Some("debian forky"));
        let ubuntu = "ID=ubuntu\nID_LIKE=debian\nVERSION_ID='24.04'\nVERSION=\"24.04.1 LTS (Noble Numbat)\"\n";
        assert_eq!(os_identity(ubuntu).as_deref(), Some("ubuntu 24.04"));
        assert_eq!(os_identity("ID_LIKE=debian\nVERSION_ID=1\n"), None);
        assert_eq!(os_identity(""), None);

        let tmp = tempfile::tempdir().unwrap();
        let (link, file) = (tmp.path().join("link"), tmp.path().join("file"));
        std::fs::write(&file, trixie).unwrap();
        std::os::unix::fs::symlink(&file, &link).unwrap();
        let missing = tmp.path().join("missing");
        assert_eq!(
            read_os_identity(std::slice::from_ref(&link)),
            None,
            "symlinks are not followed"
        );
        assert_eq!(
            read_os_identity(&[missing, link, file]).as_deref(),
            Some("debian 13")
        );
    }

    #[test]
    fn builder_script_sizes_like_sizing() {
        let arith: Vec<&str> = BUILDER_SCRIPT
            .lines()
            .filter(|l| {
                ["inodes=", "mib=", "size="]
                    .iter()
                    .any(|p| l.starts_with(p))
            })
            .collect();
        for (used, files) in [(4_700_000_000, 160_000), (300 << 20, 9_000)] {
            let script = format!(
                "used={used} files={files}\n{}\necho $inodes $size",
                arith.join("\n")
            );
            let out = std::process::Command::new("bash")
                .args(["-c", &script])
                .output()
                .unwrap();
            let (inodes, size) = sizing(used, files);
            assert_eq!(
                String::from_utf8_lossy(&out.stdout).trim(),
                format!("{inodes} {size}")
            );
        }
    }
}
