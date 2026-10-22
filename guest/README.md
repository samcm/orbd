# Guest assets for the VM runtimes (QEMU, Firecracker)

- `kernel/`: `build.sh` builds the pinned 6.18 LTS guest kernel (`bzImage`, final
  `config`, `bzImage.sha256`, `manifest.txt`) from `orbd.config`. Everything is built in:
  no modules, no initrd.
- `rootfs/`: files the image pipeline (`crates/orbd/src/image/`) adds to Amp's image as
  one extra layer, next to the static `orbd-guest` binary (`/usr/local/sbin/orbd-guest`),
  an empty `/etc/machine-id` and a mask for `serial-getty@ttyS0.service` (without udev it
  would hold boot for 90 s).

## Host contract

Kernel command line: `console=ttyS0 root=/dev/vda rw` (the image has no udev and no fstab
entry for `/`, so the kernel mounts the root read-write). Optionally
`systemd.machine_id=<32 hex>` derived from the orb id, which gives project-snapshot
clones their own machine id from the first boot.

Firecracker boots the same kernel with `console=ttyS0 reboot=k panic=1 root=/dev/vda rw
systemd.machine_id=<id> orbd.vsock=1024` and a CPU template that hides VMX/SVM (no
`/dev/kvm` in its orbs).

Guest channel: a virtio-serial port named `org.orbd.control` (QEMU) or, with
`orbd.vsock=<port>` on the command line, an AF_VSOCK listener on that port (Firecracker;
see `proto/orbd/guest/v1/guest.proto`). The guest finds the port through sysfs, so it
does not depend on udev's `/dev/virtio-ports` links. The host connects, sends `Hello`, then any of
`Wake`, `PrepareSleep`, `Shutdown`; replies carry the request id. A reply to a connection
that has gone is dropped: reconnect, send `Hello` and read `last_generation`.

At boot `orbd-guest init` grows `/` to the disk size, makes `/dev/kvm` root:kvm
0666 like Amp-hosted orbs (Amp's agent runs without the kvm group; `user` is still a
member), adds `/etc/hosts` entries, and regenerates SSH host keys when
the disk boots as a new orb (first boot, or a new MAC). The image ships an empty `/etc/machine-id` (systemd
generates one on first boot) with `/var/lib/dbus/machine-id` linked to it.
`Wake` also names the guest after its orb (`hostname`: /etc/hostname, the live name and
`/etc/hosts`), which renames project-snapshot clones on their first boot.
`amp-headless.service` and `amp-headless-bootstrap.service` have a drop-in that keeps
them down until `Wake` has delivered credentials (`/run/orbd-guest/credentials-ready`).
