#!/usr/bin/env bash
# Builds the raw ext4 test root disk for the opt-in KVM tests (tests/qemu_kvm.rs):
# Debian 12 with systemd, the real `orbd-guest` and its units, a stand-in for Amp's
# `amp-headless-bootstrap` unit, and workloads that print to the serial console:
#   orbd-test-counter  `counter pid=<pid> n=<n>` every second (must survive sleep)
#   orbd-test-nested   a nested KVM guest (QEMU as `user`) printing `l2: l2counter n=<n>`
#   orbd-test-blob     with `orbd.blob=<MiB>` on the kernel command line: dirty RAM
#   orbd-test-marker   `marker created|present <id>`: a disk's identity, kept by snapshots
#   orbd-test-dns      `stale-dns: ...`: leaves resolv.conf as a killed tailscaled would
#
#   build-rootfs.sh ORBD_GUEST_BINARY GUEST_KERNEL OUT.ext4 [GUEST_ROOTFS_DIR]
#   ARCH=arm64 build-rootfs.sh ...   (for macOS agents: no nested guest; run on arm64)
#
# GUEST_ROOTFS_DIR defaults to the repository's guest/rootfs. Needs mmdebstrap (unshare
# mode; root mode when run as root, e.g. in a container), busybox-static and e2fsprogs.
set -euo pipefail
arch="${ARCH:-x86_64}"
case "$arch" in
x86_64) nested=1 console=ttyS0 packages=systemd-sysv,qemu-system-x86,passwd,login,sed ;;
# Hypervisor.framework gives orbs no /dev/kvm: no nested guest.
arm64) nested=0 console=ttyAMA0 packages=systemd-sysv,passwd,login,sed ;;
*) echo "unsupported ARCH=$arch (x86_64 or arm64)" >&2; exit 1 ;;
esac
if [ "$(id -u)" = 0 ]; then
    mode=root
    as_root() { sh -euc "$1"; }
else
    mode=unshare
    as_root() { unshare --map-auto --map-root-user sh -euc "$1"; }
fi
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
guest_bin=$(realpath "$1") kernel=$(realpath "$2") out=$(realpath -m "$3")
guest_rootfs=$(realpath "${4:-$here/../../../../guest/rootfs}")
work=$(mktemp -d)
chmod 0755 "$work"   # read by the unprivileged namespace root
trap 'rm -rf "$work"' EXIT

units="orbd-guest.service amp-headless-bootstrap.service orbd-test-counter.service orbd-test-blob.service orbd-test-marker.service orbd-test-dns.service"
nested_hooks=()
if [ "$nested" = 1 ]; then
    # Nested (L2) guest: the same kernel (built without initrd support) booting a tiny
    # read-only ext4 root whose init counts.
    mkdir -p "$work/l2/bin" "$work/l2/dev" "$work/l2/proc"
    cp /bin/busybox "$work/l2/bin/busybox"
    cat >"$work/l2/init" <<'EOF'
#!/bin/busybox sh
n=0
while :; do n=$((n+1)); echo "l2counter n=$n"; /bin/busybox sleep 1; done
EOF
    chmod +x "$work/l2/init"
    /sbin/mkfs.ext4 -q -F -L l2 -d "$work/l2" "$work/l2.ext4" 16M
    units="$units orbd-test-nested.service"
    nested_hooks=(
        --customize-hook="install -D -m 0644 '$kernel' \"\$1/opt/l2/bzImage\""
        --customize-hook="install -D -m 0644 '$work/l2.ext4' \"\$1/opt/l2/root.ext4\""
    )
fi

mmdebstrap --mode="$mode" --variant=minbase \
    --include="$packages" \
    --aptopt='APT::Install-Recommends "false"' \
    --customize-hook='chroot "$1" useradd -m -s /bin/bash user' \
    --customize-hook="install -D -m 0755 '$guest_bin' \"\$1/usr/local/sbin/orbd-guest\"" \
    --customize-hook="cp -r '$guest_rootfs/.' '$here/rootfs/.' \"\$1/\"" \
    "${nested_hooks[@]}" \
    --customize-hook="chroot \"\$1\" systemctl enable $units" \
    --customize-hook="chroot \"\$1\" systemctl mask serial-getty@$console.service" \
    --customize-hook=': >"$1/etc/machine-id"' \
    bookworm "$work/root.tar"
rm -f "$out"
# File ownership inside the image comes from the tarball, read as namespace root.
as_root "
    mkdir '$work/root'
    tar -C '$work/root' -xf '$work/root.tar' --exclude='./dev/*'
    /sbin/mkfs.ext4 -q -F -L root -d '$work/root' '$out' 1536M
    rm -rf '$work/root'
"
chmod 0444 "$out"
echo "built $out"
