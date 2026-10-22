# Docker hosts in an orb

Boots Debian 12 VMs with QEMU/KVM that each run dockerd with the Amp orb image already
pulled, so `orbd agent` with the Docker runtime can be tested from an unprivileged orb
(writable `/dev/kvm`, no Docker daemon, no `CAP_SYS_ADMIN`).

```
orb (127.0.0.1)                         orbd-host-N (QEMU/KVM, slirp)
  coordinator :7443  ◀── 10.0.2.2:7443 ──  orbd agent ── dockerd ── orb containers
  ssh 127.0.0.1:222N ──────────────────▶  :22
```

## Requirements

```sh
sudo apt-get install -y qemu-system-x86 qemu-utils genisoimage
ls -l /dev/kvm   # must be readable and writable by you
```

## Usage

```sh
dev/docker-hosts/hostctl base        # once: ~3 min, ~6 GB under ~/.cache/orbd-docker-hosts
dev/docker-hosts/hostctl start 1 2   # boot two hosts (~10 s) and wait for docker
dev/docker-hosts/hostctl ssh 1 docker images
dev/docker-hosts/hostctl status
dev/docker-hosts/hostctl stop 1 2    # or destroy 1 2 to drop their overlays
```

`start` daemonizes QEMU from your shell, so the VMs die when the Amp CLI restarts. To
keep them across CLI updates and orb pause/resume, run each in the foreground as an orb
service instead:

```sh
amp orb service start orbd-host-1 --command "$PWD/dev/docker-hosts/hostctl run 1"
dev/docker-hosts/hostctl wait 1
```

### Running orbd in a host

```sh
cargo build --release -p orbd
dev/docker-hosts/hostctl install-orbd 1 target/release/orbd \
    "$(target/release/orbd join-token orbd-host-1 --config orbd.yaml)"
dev/docker-hosts/hostctl ssh 1 journalctl -fu orbd-agent
```

This installs `/usr/local/bin/orbd` and the token (`/etc/orbd/agent.env`, 0600) and
(re)starts `orbd-agent.service` ([orbd-agent.service](orbd-agent.service)). The agent
detects Docker and its capacity. Give the coordinator `public_url:
https://10.0.2.2:<port>`, the address guests reach this orb on.

### Talking to a host's dockerd from the orb

```sh
export DOCKER_HOST=$(dev/docker-hosts/hostctl docker-sock 1)   # ssh-forwarded unix socket
```

## What gets built

- **Base disk** (`base.qcow2`): an overlay of the pinned Debian cloud image
  (`DEBIAN_URL`, checked against `DEBIAN_SHA512`), grown to `ORBD_HOST_DISK` (80G). A
  one-shot cloud-init boot installs pinned `docker-ce`/`containerd.io` from Docker's apt
  repository (signing key checked by fingerprint), switches dockerd to the classic
  overlay2 store (the containerd store also keeps the compressed layers), pulls
  `ORB_IMAGE`, clears the machine id and powers off. `base.ok` records the pulled image id
  and digest. `ORB_IMAGE` is Amp's `:latest` tag, so rebuild the base to pick up a newer
  image (`rm ~/.cache/orbd-docker-hosts/base.{ok,qcow2}`).
- **Hosts** (`orbd-host-N.qcow2`): overlays of the base, so the image is shared and each
  host only stores its own writes. Each gets its own cloud-init instance (hostname, SSH
  host keys, machine id), user `debian` (in the `docker` group, passwordless sudo) with
  the generated key `id_ed25519`, and `ssh_config` entries `orbd-host-N`.
- **Network**: QEMU user-mode (slirp). The guest reaches this orb's loopback services at
  `10.0.2.2` (so do containers in it, through the guest's NAT); only SSH is forwarded in,
  on `127.0.0.1:$((2220+N))`.

Sizes: `ORBD_HOST_VCPUS` (4) and `ORBD_HOST_MEMORY` (8G) per host. Memory is allocated
as the guest touches it and freed pages are handed back (virtio-balloon free page
reporting), but the guest's page cache keeps its share, so budget the full size per VM
(the base build is a VM too): two hosts need up to ~17 GiB of the orb's memory limit
(`cat /sys/fs/cgroup/memory.max`), or the orb's OOM killer stops QEMU. State lives in `ORBD_HOSTS_STATE` (`~/.cache/orbd-docker-hosts`).
