#!/usr/bin/env bash
# Builds the raw ext4 test root disk for the opt-in KVM tests (tests/qemu_kvm.rs):
# Debian 12 with systemd, the real `orbd-guest` and its units, a stand-in for Amp's
# `amp-headless-bootstrap` unit, and workloads that print to the serial console:
#   orbd-test-counter  `counter pid=<pid> n=<n>` every second (must survive sleep)
#   orbd-test-nested   a nested KVM guest (QEMU as `user`) printing `l2: l2counter n=<n>`
#   orbd-test-blob     with `orbd.blob=<MiB>` on the kernel command line: dirty RAM
#
#   build-rootfs.sh ORBD_GUEST_BINARY GUEST_KERNEL OUT.ext4 [GUEST_ROOTFS_DIR]
#
# GUEST_ROOTFS_DIR defaults to the repository's guest/rootfs. Needs mmdebstrap (unshare
# mode, no root), busybox-static and e2fsprogs.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
guest_bin=$(realpath "$1") kernel=$(realpath "$2") out=$(realpath -m "$3")
guest_rootfs=$(realpath "${4:-$here/../../../../guest/rootfs}")
work=$(mktemp -d)
chmod 0755 "$work"   # read by the unprivileged namespace root
trap 'rm -rf "$work"' EXIT

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

mmdebstrap --mode=unshare --variant=minbase \
    --include=systemd-sysv,qemu-system-x86,passwd,login,sed \
    --aptopt='APT::Install-Recommends "false"' \
    --customize-hook='chroot "$1" useradd -m -s /bin/bash user' \
    --customize-hook="install -D -m 0755 '$guest_bin' \"\$1/usr/local/sbin/orbd-guest\"" \
    --customize-hook="cp -r '$guest_rootfs/.' '$here/rootfs/.' \"\$1/\"" \
    --customize-hook="install -D -m 0644 '$kernel' \"\$1/opt/l2/bzImage\"" \
    --customize-hook="install -D -m 0644 '$work/l2.ext4' \"\$1/opt/l2/root.ext4\"" \
    --customize-hook='chroot "$1" systemctl enable orbd-guest.service amp-headless-bootstrap.service orbd-test-counter.service orbd-test-nested.service orbd-test-blob.service' \
    --customize-hook='chroot "$1" systemctl mask serial-getty@ttyS0.service' \
    --customize-hook=': >"$1/etc/machine-id"' \
    bookworm "$work/root.tar"
rm -f "$out"
# File ownership inside the image comes from the tarball, read as namespace root.
unshare --map-auto --map-root-user sh -euc "
    mkdir '$work/root'
    tar -C '$work/root' -xf '$work/root.tar' --exclude='./dev/*'
    /sbin/mkfs.ext4 -q -F -L root -d '$work/root' '$out' 1536M
    rm -rf '$work/root'
"
chmod 0444 "$out"
echo "built $out"
