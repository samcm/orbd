#!/usr/bin/env bash
# Builds orbd's pinned QEMU guest kernel: Linux 6.18 LTS, everything built in (no modules,
# no initrd), booted with `root=/dev/vda`.
#
#   guest/kernel/build.sh [OUT_DIR]        (default: target/guest-kernel)
#
# Outputs in OUT_DIR:
#   bzImage          the kernel (a runtime asset; point the agent's kernel path at it)
#   config           the final .config
#   bzImage.sha256   sha256 of bzImage (the kernel digest pinned in checkpoint manifests)
#   manifest.txt     kernel version, source and fragment digests, final config digest
#
# Needs: curl, sha256sum, tar, xz, gcc, make, flex, bison, bc, libelf-dev, libssl-dev.
# Set ORBD_KERNEL_CACHE to reuse the downloaded tarball (default: ~/.cache/orbd-kernel).
set -euo pipefail

KERNEL_VERSION=6.18.58
KERNEL_SHA256=069b19f9032c4dac8149ae4b6520c35debca0f42a9f6d52ee55ba6ca9b818013
KERNEL_URL="https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-${KERNEL_VERSION}.tar.xz"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
out="$(realpath -m "${1:-$repo/target/guest-kernel}")"
cache="${ORBD_KERNEL_CACHE:-$HOME/.cache/orbd-kernel}"
fragment="$here/orbd.config"
tarball="$cache/linux-${KERNEL_VERSION}.tar.xz"
src="$out/build/linux-${KERNEL_VERSION}"

mkdir -p "$cache" "$out/build"

if ! echo "$KERNEL_SHA256  $tarball" | sha256sum -c --status 2>/dev/null; then
    echo "downloading $KERNEL_URL"
    curl -fL --retry 3 -o "$tarball.part" "$KERNEL_URL"
    echo "$KERNEL_SHA256  $tarball.part" | sha256sum -c --quiet
    mv "$tarball.part" "$tarball"
fi

rm -rf "$src"
tar -C "$out/build" -xf "$tarball"

# Reproducible build metadata (no host name, user or wall-clock time in the image).
export KBUILD_BUILD_USER=orbd KBUILD_BUILD_HOST=orbd
export KBUILD_BUILD_TIMESTAMP="1970-01-01 00:00:00 UTC" KBUILD_BUILD_VERSION=1

make -C "$src" -s x86_64_defconfig
make -C "$src" -s kvm_guest.config
"$src/scripts/kconfig/merge_config.sh" -m -O "$src" "$src/.config" "$fragment" >/dev/null
make -C "$src" -s olddefconfig

# Every option in the fragment must have survived dependency resolution.
missing=0
while IFS= read -r line; do
    case "$line" in
        CONFIG_*=*)
            if ! grep -qxF "$line" "$src/.config"; then
                echo "config: wanted $line, got: $(grep -E "^(# )?${line%%=*}[= ]" "$src/.config" || echo unset)" >&2
                missing=1
            fi
            ;;
        "# CONFIG_"*" is not set")
            sym="${line#\# }"
            sym="${sym%% *}"
            if grep -qE "^${sym}=" "$src/.config"; then
                echo "config: wanted $sym unset, got: $(grep -E "^${sym}=" "$src/.config")" >&2
                missing=1
            fi
            ;;
    esac
done <"$fragment"
[ "$missing" = 0 ] || { echo "config fragment not satisfied" >&2; exit 1; }

make -C "$src" -s -j"$(nproc)" bzImage

cp "$src/arch/x86/boot/bzImage" "$out/bzImage.tmp"
cp "$src/.config" "$out/config"
mv -f "$out/bzImage.tmp" "$out/bzImage"
kernel_sha="$(sha256sum "$out/bzImage" | cut -d' ' -f1)"
echo "$kernel_sha  bzImage" >"$out/bzImage.sha256"
cat >"$out/manifest.txt" <<EOF
kernel_version=${KERNEL_VERSION}
kernel_release=$(make -C "$src" -s kernelrelease)
source_url=${KERNEL_URL}
source_sha256=${KERNEL_SHA256}
fragment_sha256=$(sha256sum "$fragment" | cut -d' ' -f1)
config_sha256=$(sha256sum "$out/config" | cut -d' ' -f1)
bzImage_sha256=${kernel_sha}
EOF
cat "$out/manifest.txt"
