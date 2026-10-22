# Guest assets for the QEMU runtime

- `kernel/`: `build.sh` builds the pinned 6.1 LTS guest kernel (`bzImage`, final
  `config`, `bzImage.sha256`, `manifest.txt`) from `orbd.config`. Everything is built in:
  no modules, no initrd.
- `rootfs/`: files the image pipeline (`crates/orbd/src/image/`) adds to Amp's image,
  next to the static `orbd-guest` binary (`/usr/local/sbin/orbd-guest`).

## Host contract

Kernel command line: `console=ttyS0 root=/dev/vda rw` (the image has no udev and no fstab
entry for `/`, so the kernel mounts the root read-write). Optionally
`systemd.machine_id=<32 hex>` derived from the orb id, which gives project-snapshot
clones their own machine id from the first boot.

Guest channel: a virtio-serial port named `org.orbd.control` (see
`proto/orbd/guest/v1/guest.proto`). The guest finds it through sysfs, so it does not
depend on udev's `/dev/virtio-ports` links. The host connects, sends `Hello`, then any of
`Wake`, `PrepareSleep`, `Shutdown`; replies carry the request id. A reply to a connection
that has gone is dropped: reconnect, send `Hello` and read `last_generation`.

At boot `orbd-guest init` grows `/` to the disk size, gives `/dev/kvm` to group `kvm`
(0660, `user` is a member), adds `/etc/hosts` entries, and resets SSH host keys and the
D-Bus machine id when the disk boots as a new orb (first boot, or a new MAC).
`amp-headless.service` and `amp-headless-bootstrap.service` have a drop-in that keeps
them down until `Wake` has delivered credentials (`/run/orbd-guest/credentials-ready`).
