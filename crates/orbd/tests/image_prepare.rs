//! Image preparation against a real registry image (opt-in: needs network, root or
//! `sudo -n`, umoci, e2fsprogs incl. debugfs, and ~15 GB of disk).
//!
//! ```sh
//! ORBD_TEST_GUEST_HELPER=target/x86_64-unknown-linux-musl/release/orbd-guest \
//!   cargo test -p orbd --test image_prepare -- --ignored --nocapture
//! ```
//! `ORBD_TEST_IMAGE` overrides the image (default: Amp's orb image).

use orbd::config::agent::ImagesConfig;
use orbd::image::{ImageError, ImageStore};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

const AMP_ORB_IMAGE: &str = "us-central1-docker.pkg.dev/amp-prod-0/amp-orb/amp-orb:latest";

fn debugfs(image: &Path, request: &str) -> Vec<u8> {
    let out = Command::new("debugfs")
        .env("PATH", "/usr/sbin:/sbin:/usr/bin:/bin")
        .args(["-R", request])
        .arg(image)
        .output()
        .expect("debugfs");
    assert!(out.status.success(), "debugfs -R {request:?} failed");
    out.stdout
}

fn stat(image: &Path, path: &str) -> String {
    String::from_utf8_lossy(&debugfs(image, &format!("stat {path}"))).into_owned()
}

/// A cache miss returns NotReady at once and prepares in the background; the published
/// raw ext4 carries the guest integration (helper, enabled unit, credential gate); a
/// reopened store (agent restart) serves the same image without preparing again.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs network, root/sudo, umoci and ~15 GB disk"]
async fn prepares_amp_image_with_guest_integration() {
    let helper =
        PathBuf::from(std::env::var("ORBD_TEST_GUEST_HELPER").expect("set ORBD_TEST_GUEST_HELPER"));
    let image = std::env::var("ORBD_TEST_IMAGE").unwrap_or_else(|_| AMP_ORB_IMAGE.into());
    let state =
        tempfile::tempdir_in(std::env::var("ORBD_TEST_DIR").unwrap_or("/var/tmp".into())).unwrap();
    let config = ImagesConfig {
        guest_helper: helper.clone(),
        umoci: "umoci".into(),
        mkfs_ext4: "mkfs.ext4".into(),
        prewarm: vec![],
        refresh_interval_secs: 3600,
    };
    let store = ImageStore::open(&config, state.path()).unwrap();

    let t0 = Instant::now();
    match store.get(&image) {
        Err(ImageError::NotReady(_)) => {}
        other => panic!("first get must start preparation, got {other:?}"),
    }
    let prepared = loop {
        match store.get(&image) {
            Ok(p) => break p,
            Err(ImageError::NotReady(s)) => {
                assert!(t0.elapsed() < Duration::from_secs(1800), "still {s}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => panic!("{e}"),
        }
    };
    println!("prepared in {:?}: {prepared:?}", t0.elapsed());
    assert!(prepared.manifest_digest.starts_with("sha256:"));
    assert_eq!(
        std::fs::metadata(&prepared.rootfs).unwrap().len(),
        prepared.rootfs_bytes
    );
    assert!(
        prepared
            .rootfs
            .starts_with(state.path().join("images").join(&prepared.key)),
        "published under images/<key>/"
    );

    let fs = &prepared.rootfs;
    assert_eq!(
        debugfs(fs, "cat /usr/local/sbin/orbd-guest"),
        std::fs::read(&helper).unwrap(),
        "guest helper installed verbatim"
    );
    let wants = stat(
        fs,
        "/etc/systemd/system/sysinit.target.wants/orbd-guest.service",
    );
    assert!(
        wants.contains("\"/etc/systemd/system/orbd-guest.service\""),
        "{wants}"
    );
    for unit in ["amp-headless.service", "amp-headless-bootstrap.service"] {
        let gate = debugfs(
            fs,
            &format!("cat /etc/systemd/system/{unit}.d/orbd-credentials.conf"),
        );
        assert!(
            String::from_utf8_lossy(&gate)
                .contains("ConditionPathExists=/run/orbd-guest/credentials-ready"),
            "{unit} is gated on credentials"
        );
    }
    assert!(
        stat(fs, "/etc/machine-id").contains("Size: 0"),
        "machine id reset"
    );
    // Ownership from the image survives unpacking (root-owned setuid sudo, user's home).
    let sudo = stat(fs, "/usr/bin/sudo");
    assert!(
        sudo.contains("Mode:  04755") && sudo.contains("User:     0"),
        "{sudo}"
    );
    assert!(stat(fs, "/home/user").contains("User:  1000"));
    assert!(
        std::fs::read_dir(state.path().join("staging"))
            .unwrap()
            .next()
            .is_none(),
        "staging cleaned up"
    );

    drop(store);
    let reopened = ImageStore::open(&config, state.path()).unwrap();
    assert_eq!(reopened.get(&image).unwrap(), prepared);
}
