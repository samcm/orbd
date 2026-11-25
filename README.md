<p align="center">
  <img src="assets/logo.svg" width="96" height="96" alt="orbd logo">
</p>

<h1 align="center">orbd</h1>

A self-hosted backend for [Amp orbs](https://ampcode.com/docs/orbs/self-hosted).

## Why

Amp lets you run orbs on your own hardware, but you have to supply the program that
creates and stops them. The obvious version, a container per orb, falls short:

- Pausing stops the container, so the agent's processes restart on every wake.
- There's no `/dev/kvm` inside, so you can't build or test VM software (Firecracker,
  QEMU) from an orb.
- Spreading orbs across several machines means writing placement, locking and retries
  yourself, because Amp can deliver the same job twice.

orbd runs each orb in a VM that sleeps to disk: a sleeping orb uses no host RAM, wakes in
about 0.3 s with its processes intact, and has `/dev/kvm`. One coordinator serves many
hosts, and hosted [Boat](#boat) sandboxes.

## How it works

A coordinator runs `amp orb provider` for an Amp orb provider whose orb types say what
an orb gets (e.g. `4vcpu-8gb`, `kvm-4vcpu-8gb`) and places each orb on an agent or
Boat. Agents run on your own x86_64 hosts (or [Apple Silicon Macs](#macos-agents)),
dial the coordinator over mutual-TLS gRPC, and run orbs in QEMU, Firecracker or Docker.
Coordinator and agent are the same static `orbd` binary; the coordinator also runs on
aarch64.

> **Early.** Configuration and on-disk formats may still change.

```
                 Amp
                  ▲  pulls lifecycle jobs
┌─────────────────┴──────────────────┐
│ orbd coordinator                   │
│  amp orb provider (per provider)   │
│  placement, command journal        │
└─────────────────▲──────────────────┘
                  │  mTLS gRPC (agents dial in)
       ┌──────────┴──────────┐
┌──────┴───────┐      ┌──────┴───────┐
│ orbd agent   │      │ orbd agent   │   x86_64 hosts
│ orb  orb     │      │ orb          │
└──────────────┘      └──────────────┘
```

## Runtimes

Measured on a Ryzen 9 7940HS with a 2 vCPU / 4 GiB orb on Amp's orb image.

|                         | QEMU           | Firecracker | Docker      |
| ----------------------- | -------------- | ----------- | ----------- |
| Wake                    | ~0.3 s         | ~0.05 s     | ~0.5 s      |
| Processes kept on sleep | ✅             | ✅          | ❌          |
| No host RAM while asleep | ✅            | ✅          | ✅          |
| `/dev/kvm` in the orb   | opt-in (amd64) | ❌          | ❌          |
| Status                  | ✅ works       | ✅ works    | ✅ works    |

## Build

```sh
cargo build --release -p orbd
```

Static binaries for x86_64 and aarch64 are attached to each release.

## Setup

1. **Provider token.** In Amp's workspace settings, create an orb provider, e.g.
   `homelab`, and save its token as `tokens/provider`. Under the provider's
   [orb types](https://ampcode.com/docs/orbs/self-hosted#orb-types), add one per
   entry of `orb_types` below, named exactly as its `id`, and pick the default.
2. **Coordinator** (x86_64 or arm64), with `orbd.yaml`:

   ```yaml
   config_version: v2
   public_url: https://coordinator.example:7443   # agents dial this
   provider: { token_file: /etc/orbd/tokens/provider }
   orb_types:                                     # the first is the default
     - { id: 2vcpu-4gb, vcpus: 2, memory_mib: 4096, disk_gib: 40 }
     - { id: kvm-4vcpu-8gb, vcpus: 4, memory_mib: 8192, disk_gib: 40, kvm: true }
     - { id: 16vcpu-64gb, vcpus: 16, memory_mib: 65536, disk_gib: 200, arch: arm64 }
   ```

   An orb type says what an orb gets; orbd decides where it runs. On the agents an orb
   type with `kvm: true` runs in QEMU and has `/dev/kvm`, only on agents started with
   `ORBD_NESTED_KVM=true` (below); any other runs in Firecracker (vCPUs: 1 or an even
   number). A spawn of a new orb uses the orb type the thread
   picked, or the first one if it has none; an orb type not listed fails the spawn. An
   orb keeps its orb type and place for life, whatever later jobs name.
   `arch` says which CPU architectures a new orb may get: `amd64` (the default),
   `arm64`, or a list in order of preference, e.g. `[amd64, arm64]` to use arm64 agents
   when no amd64 one has room. An orb keeps the architecture it was placed with. arm64
   orbs run on [macOS agents](#macos-agents), without `kvm`.
   `retired_token_files` under `provider` lists tokens of providers you are moving
   away from: they are still served, so their threads keep their orbs, and new orbs from
   them get the default orb type.

   Files without `config_version` are v1: `offerings`, each with a `runtime` (`qemu`,
   `firecracker`, `docker`, `boat`) and `resources`, served by `providers` (`id`,
   `token_file`, `orb_types` naming offerings), and spilling by `spill_to`. A v2 file is
   turned into the same offerings when it is loaded.

   ```sh
   docker run -d --name orbd -p 7443:7443 -v orbd:/var/lib/orbd \
     -v ./orbd.yaml:/etc/orbd/orbd.yaml -v ./tokens:/etc/orbd/tokens:ro \
     ghcr.io/samcm/orbd coordinator
   docker exec orbd orbd join-token host-1   # single use, valid for an hour
   ```

3. **Agents** (x86_64), one token each:

   ```sh
   docker run -d --stop-timeout 300 \
     --cap-add SYS_ADMIN --cap-add NET_ADMIN --device /dev/kvm --device /dev/net/tun \
     --security-opt apparmor=unconfined --security-opt systempaths=unconfined \
     -v orbd-agent:/var/lib/orbd -e ORBD_JOIN=<token> ghcr.io/samcm/orbd agent
   ```

   Agents run only VM orbs and give no orb `/dev/kvm` unless told to: add
   `-e ORBD_NESTED_KVM=true` on the agents meant for `kvm` orb types. Set
   `ORBD_IMAGE_ALLOW` to the images your Amp projects use (e.g.
   `us-central1-docker.pkg.dev/amp-prod-0/amp-orb/*`), so a project cannot make your
   agents pull and unpack anything else (below).

   The agent needs no `--privileged`. On top of Docker's default capabilities it needs
   `CAP_SYS_ADMIN` (namespaces and mounts for VMM jails, image sandboxes and orb
   networks) and `CAP_NET_ADMIN` (taps, nftables); `/dev/kvm` and `/dev/net/tun`;
   AppArmor not confining it (its default profile denies mounts; ignored where AppArmor
   is off); and a writable `/proc/sys` (`systempaths=unconfined`) for forwarding in its
   own and each orb's network namespace. Docker's other defaults stay: its seccomp
   profile (orbd falls back from `pivot_root`, which it refuses), no other devices, and
   the capability set, of which orbd also uses `CAP_SETUID`, `CAP_SETGID`, `CAP_CHOWN`,
   `CAP_FOWNER`, `CAP_DAC_OVERRIDE` and `CAP_KILL`. Outside Docker, e.g. under systemd,
   run it as root with that capability set (`CapabilityBoundingSet=`), those devices
   (`DeviceAllow=`), `ReadWritePaths=/var/lib/orbd /run/netns` and `KillMode=process`
   (VMMs outlive a restart and are adopted), with `firecracker` on its `PATH` (below), and
   allow its orbs' traffic through the host's firewall ([Host
   firewalls](#host-firewalls)). Docker's user-namespace remapping is unsupported: jails
   and image sandboxes use host uids from 1300000000 up. The container image is the
   simplest way to run an agent: it brings the Firecracker, QEMU and guest kernel each
   release is tested with, and its own network namespace.

   The agent also seccomp-filters itself when it starts, and every process it starts
   (VMMs, image and disk tools, `nft`, `ip`) inherits the filter. These calls fail with
   `EPERM`: `bpf`, `perf_event_open`, `init_module`, `finit_module`, `delete_module`,
   `kexec_load`, `kexec_file_load`, `open_by_handle_at`, `fanotify_init`, `syslog`,
   `reboot`, `swapon`, `swapoff`, `acct`, `settimeofday`, `clock_settime`, and on x86
   `iopl` and `ioperm`; so does every call of another ABI (32-bit, x32). orbd uses none
   of them. An agent that cannot install the filter does not start. Without
   `CAP_SYS_ADMIN` the kernel takes the filter only with `no_new_privs`, which a root
   agent or one with any capability then sets; an agent with no capabilities at all (an
   unprivileged development agent) runs without it, and what it runs through `sudo` is
   not filtered. The filter narrows what the agent's children can do to the host kernel;
   the agent itself stays root-equivalent on the host (`CAP_SYS_ADMIN` with a writable
   `/proc/sys`).

Threads you create on that provider in Amp then run on your hosts.

Agents that cannot dial `public_url` (another network, or one reached over a VPN such as
Tailscale) can use another address of the same listener: list it in `extra_urls`
(`extra_urls: [https://coordinator.tailnet.ts.net:7443]`), whose hosts are added to the
coordinator's certificate, and pass it to `orbd join-token <id> --url <url>`. An enrolled
agent dials one instead while `ORBD_COORDINATOR_URL=<url>` is set (keep it set: the
enrolled URL is not changed), without enrolling again.

The coordinator creates its CA and keys in its volume on first start; keep that volume.
Agents detect their runtime and capacity (override with `ORBD_CAPACITY_VCPUS`,
`ORBD_CAPACITY_MEMORY_MIB`, `ORBD_CAPACITY_DISK_GIB`); orbs use DNS `1.1.1.1` and `9.9.9.9`
(`ORBD_DNS`). Each VM orb has its own private network, `10.0.2.0/24` (the orb is `.15`,
its gateway `.2`); if workloads in orbs use that subnet, e.g. Docker Compose networks,
set another with `ORBD_GUEST_NETWORK` (e.g. `10.200.0.0/24`). It applies to orbs created
afterwards; existing orbs keep theirs.

Each agent with VM runtimes needs a network namespace of its own: orbd's nftables tables
are the namespace's, and an agent rebuilds them when it starts. Docker gives every
container one; with `--network host`, or two agents run as services on one host, a second
agent refuses to start (it finds the first one's lock, an abstract unix socket, and names
its pid and uid).

VM orbs on Linux reach the internet, not the host's LAN. Their traffic to private and
other non-global addresses (`10/8`, `172.16/12`, `192.168/16`, a tailnet's `100.64/10`,
loopback, documentation, multicast and reserved ranges) is dropped, and so are IPv6, cloud
metadata endpoints (link-local, Azure's `168.63.129.16`, Alibaba's `100.100.100.200`) and
the host's Tailscale resolver `100.100.100.100`, whatever is allowed. Allow what orbs need
with `ORBD_EGRESS_ALLOW`, comma-separated `<tcp|udp|any>:<address or CIDR>[:<port or
range>]` entries, e.g. `udp:192.168.2.201:41641` (direct Tailscale to a LAN host) or
`tcp:10.0.0.0/8:8000-8100`; the orbs' DNS servers are allowed on port 53. The agent's
own traffic is not affected, other firewalls on the host still apply ([Host
firewalls](#host-firewalls)), and the host's public addresses are internet addresses to
orbs (keep services there firewalled). An orb cannot pass as another: its traffic must
come from its own guest subnet and veth. On macOS, see below.

On Linux each VMM (QEMU, Firecracker) runs jailed: as its orb's own uid and gid
(`ORBD_VMM_JAIL_UID_BASE`, default 1300000000, plus a per-orb index, up to 2^24 of them;
nothing else on the host may use these uids, so two agents on one host, each in its own
network namespace (see above), need bases 2^24 apart, clear of `/etc/subuid` ranges),
with no capabilities and `no_new_privs`, unable to create namespaces (a seccomp filter
the jail installs under the VMM's own refuses `unshare`, `setns` and `clone` with
namespace flags, `clone3` and the new mount API, so no user namespace brings
capabilities back), in a
mount namespace holding only its orb's disk, logs, checkpoints and sockets, the VMM and
its libraries, the kernel, QEMU's backing image and `/dev/{kvm,net/tun,null,urandom}`:
no `/proc`, `/sys`, agent state or other orbs. A VMM escape is an unprivileged process
that sees one orb: the agent talks only to sockets its VMM's uid serves, kills whatever
else still runs as that uid before each start and at the delete, and never hands the uid
to another orb. Making a project snapshot reads the orb's disk in its jail too
(`qemu-img` through the orb's own base only, `e2fsck`, `resize2fs`). A Linux agent with
VM runtimes must run as root (it refuses to start otherwise) and launches no VMM its
jail cannot hold. `ORBD_VMM_JAIL=off` runs VMMs unjailed as the agent's user: unsafe,
for development only.

`ORBD_FLOW_LOG=true` logs every new connection an orb in its own network namespace
(every Linux VM orb) starts as one event
(target `orbd::flow`, a JSON line with `ORBD_LOG_JSON`): `orb`, `amp_orb_id` and
`thread_id`, `proto`, `dst` (address and port), `src_port`, and `verdict`, `accept` or
`drop` by the rules above (connections to the agent itself are dropped). The kernel
sends each connection's first packets to NFLOG group 4731 (the agent needs
`CAP_NET_ADMIN`, as for its nftables table, and the host kernel's `nfnetlink_log`), and
the agent reads them on a thread of its own, so orb traffic never waits for the log.
Retransmissions of a connection are one event. Past 50 events a second for an orb
(bursts of 200) or 500 for all orbs, connections are counted instead and reported every
10 s; past 200 new connections a second for an orb (bursts of 400) or 2000 for all orbs
the kernel only counts them, so one busy orb cannot crowd out the others' log, as it
does messages that did not fit the agent's buffer (metrics `orbd_flow_log_events_total`,
`orbd_flow_log_suppressed_total`, `orbd_flow_log_lost_total`). Without the setting the
agent's nftables table logs nothing. Each event's `egress` is `restricted` for an orb
whose type has `egress` (its verdict is its policy's, below), `default` otherwise.

An orb type can also restrict its orbs to an allowlist of names and addresses: see
[Restricted egress](#restricted-egress).

Agents pull and prepare each orb's image (Amp's `ORB_IMAGE`) themselves, whatever image
an Amp project names. `ORBD_IMAGE_ALLOW` limits them to comma-separated patterns:
`<repository>` (any tag or digest), `<repository>:<tag>`, `<repository>@<digest>` or
`<registry>/<prefix>*`, e.g. `us-central1-docker.pkg.dev/amp-prod-0/amp-orb/*`, compared
in full (`debian` is `docker.io/library/debian:latest`). The agent checks it before
resolving or pulling anything, whatever the coordinator sends: a spawn of another image
fails with `image <ref> is not allowed on this agent (ORBD_IMAGE_ALLOW)`, and prewarmed
images it does not list are skipped. Disks that exist already are not checked: orbs
keep running, and a project snapshot on the agent built from an image the list no longer
allows is still used while its image lineage (Debian release) matches the spawn's.
Unset (the default) allows any image; the agent logs which applies when it starts. Set
it: an Amp project's image is otherwise whatever its owner names.

On Linux, umoci, `du` and `mkfs.ext4` handle the image's files in a sandbox
(`ORBD_IMAGE_SANDBOX=on`, the default): as root of a user namespace whose ids 0-65535
are the host's 1400000000-1400065535 (`ORBD_IMAGE_SANDBOX_UID_BASE`; nothing on the host
may use them), without network, unable to create further namespaces (the jails'
filter), and seeing only the tools and the build's own files.
Owners, modes, xattrs and file capabilities come out as before; an image with device
nodes, `trusted.*` xattrs or owners above 65535 fails to build instead (common images
have none: in a container `/dev` is the runtime's). The agent tries the sandbox when it
starts and refuses to start without one: it needs to run as root with `CAP_SYS_ADMIN`,
`CAP_SETUID`, `CAP_SETGID` and `CAP_CHOWN`, as the agent flags above give it.
`ORBD_IMAGE_SANDBOX=off` runs the tools as real root (through `sudo -n` for an agent not
running as root), as orbd did before: unsafe, since whoever can set an Amp project's
image then feeds root-run tar and ext4 tooling on every agent. An agent with
`images.builder` (macOS) builds in that container instead.

`orbd agent list` and `orbd agent remove <id>`
manage agents; a removed agent's orbs stay on it, unavailable. `orbd agent list` shows
each agent's orbs, its reservations against its capacity, and its disk: free, allocated
by orbd, reserved and limit (also exported as `orbd_agent_disk_*_bytes` and
`orbd_reserved_disk_bytes`).

A new orb goes to the agent with the most room that fits it: the most of whichever of its
memory (capacity less running orbs) and its disk (free space less the reserve and running
orbs' headroom) is scarcer, each as a share of the agent's own. Memory alone would send
every new orb to the agent with the most free memory however full its disk, and orbs
never move between agents. An agent started with
`ORBD_OVERFLOW=true`, e.g. on a laptop or a workstation that is not always on, gets one only
when no other agent fits it, since an orb stays on the agent it was created on and cannot
wake while that host is off. `orbd agent list` marks such agents `(overflow)`.

Orbs asleep or stopped on an agent can wake only there, so a new orb, which can go
anywhere, prefers an agent that still has room to wake the largest of the orbs that went
to sleep or stopped there in the last 24 hours (their vCPUs, memory and disk headroom,
each the most any of them needs; not archived, faulted or being deleted ones) once it
takes the new orb, ranked by the room it has beyond that; only when no agent does is it
placed without that room. This comes before the project snapshot (a new orb that misses
its snapshot boots slower; one that fills an agent leaves that agent's sleeping orbs
unable to wake).

An orb whose first spawn was refused before anything was created (its agent said
nothing of it exists there) is not tied to that agent. Its next spawn places it anew like
a new orb (`orbd_unstarted_orbs_replaced_total`), and the old agent is told to delete what
it recorded of it, when:

- its agent refused it for a reason the coordinator cannot see (its image not in that
  agent's `ORBD_IMAGE_ALLOW`), or for want of room three times in a row (once may pass:
  memory admitted moments ago frees up); or
- the coordinator finds that agent cannot take it now: no room, unable to run it, gone
  for more than 30 minutes (longer than a restart or upgrade), or removed with
  `orbd agent remove` (which also ends the commands it was sent and never answered).

It goes to an agent of the same kind (always-on, or overflow) that has not refused it in
the last 24 hours; when only agents that refused it fit, it stays where it is, as it does
after four new placements in a day. Not when its agent asked it to wait (its image being
prepared there).

Running orbs reserve their vCPUs and memory; sleeping orbs reserve nothing. An orb type's
`disk_gib` is each orb's disk size, a sparse file that grows as the orb writes, so disk is
admitted by what the host actually has: a new or waking orb needs free space for its
headroom (up to `running_orb_disk_headroom_gib`, default 10, of growth plus its memory
checkpoint) after the headroom of the orbs already running and the agent's reserve
(`ORBD_DISK_RESERVE_GIB`, default max(10 GiB, 5% of the filesystem)) are set aside. A wake
is credited the checkpoint room its agent reports the wake frees, which the orb's next
checkpoint takes again: a sleep held in RAM frees what its agent committed for its
checkpoint (unless that checkpoint is being written), and a QEMU checkpoint is read into
memory and deleted at the wake. A
Firecracker checkpoint on disk frees nothing at the wake (the restored VM maps it until
its next checkpoint is written), so that wake needs room for both.
`ORBD_CAPACITY_DISK_GIB` optionally limits what orbd allocates on the host (default: the
whole filesystem). A refusal names each host's limiting resource with its numbers.

To see what needs attention, run on the coordinator (e.g. `docker exec orbd orbd status`):

- `orbd status`: every agent's booked memory and usable disk, then problems worst first,
  each with a command to look further: disconnected agents, agents whose disk or memory
  keeps orbs active lately from waking, orbs that fail to wake and why, faulted orbs,
  deletes that did not finish, commands stuck in flight, orbs running with no spawn or
  pause from Amp for a day (Amp never pauses the orb of a deleted thread) and orbs asleep
  for days. `orbd status --help` lists the kinds; `--check` exits 6 when one is an error.
- `orbd orb list`: each orb's state, agent, type and what it holds on disk (its memory
  checkpoint too), with a note when it fails or would not fit on its agent now; filter
  with `--agent`, `--state`, `--asleep-days` and `--problems` (the orbs `status` warns
  about).
- `orbd orb show <orb>`: one orb (by Amp id, a unique prefix of one, or its thread id
  `T-<uuid>`): whether it would wake now, its failures since its last success, its latest
  commands and Amp requests, and next steps.

These, `orbd agent list` and `orbd orb pause|delete` (which take an Amp id or thread id,
never a prefix) take `--json` for one JSON object on stdout, for scripts and coding
agents: fields are only added, and errors go to stderr as one JSON object. They exit 1
when a pause or delete ran and failed or its outcome is unknown, 2 for bad arguments, 3
for an unknown orb, 4 when refused (nothing was done) and 5 when the coordinator or the
orb's host is unreachable. `orbd orb list` without `--json` is a table now; scripts
should use `--json`.

Amp pauses an orb when its thread goes idle or is archived and deletes it when it is no
longer needed, e.g. when the thread is deleted; archiving sends no delete, so orbs of
archived threads would stay asleep on disk forever. The
coordinator deletes orbs that have been asleep for `idle_orb_ttl_days` (default 30; 0
never deletes) through the ordinary delete path. **A deleted orb is gone:** if its thread
is used again, its next spawn starts a fresh orb from the image (or project snapshot),
without its files, uncommitted work or processes, and Amp runs setup again. Set the TTL
longer than any thread you might come back to.

Until then, a sleeping QEMU or Firecracker orb keeps its memory checkpoint, a file the
size of its memory, on its agent's disk. An agent started with
`ORBD_CHECKPOINT_MAX_AGE_HOURS` (`--checkpoint-max-age-hours`; default 0: checkpoints are
kept for as long as orbs sleep), e.g. `-e ORBD_CHECKPOINT_MAX_AGE_HOURS=12`, deletes the
checkpoint of an orb asleep longer than that. It checks 10 minutes after it starts (so a
clock that is still wrong at boot deletes nothing), then hourly. The orb's next wake
cold-boots its disk: its files and uncommitted work are kept, its processes (shells, dev
servers, builds in progress) are lost as after a reboot, and Amp's executor restarts as
it does on every wake. `orbd orb list` then shows the orb `stopped` instead of `asleep`,
with no checkpoint. A checkpoint is never deleted while its orb is being woken, paused or
deleted, and is kept however old if the guest did not sync its disk before it was saved
(it did not answer in time when it went to sleep), or if its save time is more than an
hour in the future.

An agent deletes a prepared image a tag has moved on from (the tag now resolves to a
newer digest) once nothing has used it for 6 hours: a Firecracker orb's disk is a copy,
and new orbs of the tag get the newer image. Project snapshots are published as copied,
at their source orb's disk size: no host tool checks or shrinks the guest's filesystem
then (the guest replays its journal at its next mount). An orb whose type has a smaller
disk than the snapshot gets its copy shrunk in its own jail when its disk is made, or
keeps the larger size if its files do not fit.

Most sleeps end within minutes, and a checkpoint is the orb's whole memory, so on a busy
agent writing them is most of what it writes to its disk. A Firecracker agent therefore
holds a sleep in RAM first: the guest syncs its disk and the VM is paused in
place, still holding its memory, for `ORBD_SLEEP_HOLD_MINUTES` (`--sleep-hold-minutes`;
default 20, 0 writes every checkpoint at once). A wake within that resumes the same VM in
an instant (its network and egress policy reapplied first) and writes nothing. The
checkpoint is written, exactly as an unheld sleep writes it and dated at the pause, when
the hold runs out, when an orb being admitted needs the memory or capacity (oldest holds
first), when the host's free memory would no longer fit the largest orb it runs, and
first thing when the agent stops gracefully. A pause holds only if the guest synced its
disk, the host would still fit another orb of its size, all held memory stays within
16 GiB (what a graceful stop can write) and the held checkpoints fit on disk above the
reserve. Held memory counts against the agent's capacity, and held checkpoints count as
committed disk, not free, for admission and placement; held memory counts as available
in what the agent reports only while those checkpoints fit. A checkpoint that cannot be
written (disk full, or failing as the agent stops) drops the hold instead. A held VM lost
without a graceful stop (a container killed, a crash, the OOM killer, which prefers held
VMs) loses only its RAM: the orb is recorded stopped and cold-boots from its disk, which
the guest synced and the agent made durable before the pause. A pause that publishes a
project snapshot holds too: its disk is copied from the paused VM.

The disk of a Firecracker orb stopped without a checkpoint (dropped for its age, a
failed save, or a sleep held in RAM that was lost or dropped) and unused for `ORBD_ARCHIVE_AFTER_HOURS` (`--archive-after-hours`; default
24, 0 never) is archived: compressed with `zstd` in place, about 3x for a typical orb, at
idle CPU and I/O priority, and checked before the disk is removed. An archive stops,
leaving the disk, if the filesystem would go below the agent's disk reserve or the
archive would not shrink the disk to 80% or less (that orb is tried again a day later).
On a filesystem with reflinks an orb is archived only once its base image or snapshot is
gone, since until then its disk shares their blocks. `orbd orb list` notes `disk
archived`. Its next spawn restores it before the cold boot, which takes seconds for a
typical disk; one too big to restore within the spawn's time fails with
`image_not_ready` while the restore goes on, and a later spawn finds it done. A restore
starts only if what it writes fits above the reserve after the restores already
running, and the agent keeps room for the largest archived orb's restore out of what it
reports free. A command for the orb cancels an archive in progress. Orbs stopped that
long rarely come back, and the archive and a restore together write about 1.3 times the
disk, so the setting is worth raising where they often do.

To delete the orbs of archived threads sooner, run a job that checks each orb's thread
in Amp: `orbd orb list --json` lists every orb's Amp id (`amp-<thread id>`), state,
agent and what it holds on disk (`--asleep-days 7` narrows it to orbs asleep a week or
more), and `orbd orb delete <amp orb id>` deletes one the same way. It refuses an orb unless
it was last seen asleep or stopped at least `--min-asleep-mins` ago (default 60), with
no job from Amp open or started since (finished jobs are kept for about an hour), so an
orb in use is never deleted.

An orb Amp no longer drives, e.g. one left running after its provider's token was
replaced without listing the old one in `retired_token_files`, is never paused by Amp.
`orbd orb pause <amp orb id>` pauses it as Amp would (its thread's next spawn wakes it),
and `orbd orb delete --force <amp orb id>` deletes it whatever it is doing, stopping it
first if it runs. Both queue behind any job Amp has for the orb; a forced delete loses
the orb's files and processes even if its thread is in use.

Amp never deletes project snapshots either: it reuses one for at most 72 hours, then
names a new one. Amp takes each new snapshot from an orb that may itself have started
from the previous one, so a VM agent records the image lineage a snapshot descends from:
the image's distribution release (from its os-release) and the generation of orbd's
guest integration. A new orb whose spawn image is of another lineage starts from that
image instead, and snapshots taken from it carry the new lineage. Amp's routine image
rebuilds keep the lineage; a new Debian release or a guest integration change that must
reach existing projects changes it, which costs each project one start without its
snapshot. A snapshot that records no lineage is passed over too, once the image's
lineage is known. An agent deletes a project snapshot or prepared image once no orb on
it is built on it and nothing has used it for 72 hours (the current image of a
prewarmed reference is kept), and image layers no build has used for as long. A
project snapshot goes sooner in two cases:
- **Dead**, after an hour unused: its lineage is none of those the agent's images
  resolve to, as recorded when each reference last resolved to a prepared image. Only
  when every reference resolved or used in the last 72 hours has a known lineage (an
  image without one keeps every snapshot) that has not changed in the last 12 hours: an
  image or orbd upgrade rolled back within that keeps the snapshots.
- **Superseded**, after 12 hours unused: Amp moved on to a newer snapshot published
  from an orb created from it. The newer one was published later, from an orb of the
  same size (Amp keeps one per size), was recorded, and has been named since, and
  nothing used the old one after that. On repositories where Amp keeps a snapshot per
  member, nothing here can tell whose is whose: a member's next orb may then start from
  its image.

Only QEMU orbs are built on their base, since their disks read through to it; a
Firecracker orb's disk is a standalone copy and keeps nothing. It checks at startup,
hourly and after each orb delete, counting deletions in
`orbd_project_snapshots_deleted_total{reason}` (`idle`, `superseded`, `dead_lineage`),
and tells the coordinator in its next heartbeat.

The coordinator places a new orb that names a project snapshot on an agent holding
it when one fits. `orbd_project_snapshot_placements_total{result}` counts these
placements: `hit`, `elsewhere` (the agents holding it could not take the orb: full,
down, overflow or of another architecture), `wake_room` (an agent holding it could, but
only with the room it keeps to wake its own orbs, see above), `absent` (no agent of the
backends tried holds it) and `unsupported` (placed where project snapshots are not kept,
e.g. on Boat); each `elsewhere`, `wake_room` and `absent` is logged. An agent counts orbs it creates from the
image although placed for a snapshot, once each, in
`orbd_project_snapshot_fallbacks_total{reason}`: `missing` (deleted before the
coordinator heard; a dead snapshot deleted early shows up here rather than under
`lineage`) or `lineage`.

`prefer` sets where new orbs go, in order: `[local, boat]` (the default) runs them on
the agents and sends them to Boat when no agent can take them (all full, down or
unreported); `[boat, local]` uses Boat until its `capacity` is held, then the agents.
Boat only takes orb types without `kvm` in one of its sizes. An orb lives where it was
placed, so an orb placed on Boat wakes there for the rest of its life. Resumes never
move. (In v1, an offering's `spill_to` lists like-for-like offerings to the same
effect.)

An agent's host can be off while no orb runs on it, e.g. an autoscaling group scaled
to zero, with the coordinator on a small always-on machine. `wake:` gives the commands
that start and stop it (argv, run one at a time):

```yaml
wake:
  command: [aws, autoscaling, set-desired-capacity, --auto-scaling-group-name, orbs, --desired-capacity, "1"]
  timeout_secs: 180              # default; at most 200
  sleep_command: [aws, autoscaling, set-desired-capacity, --auto-scaling-group-name, orbs, --desired-capacity, "0"]
  sleep_after_idle_secs: 7200
  hook_timeout_secs: 120         # default; either command is killed after this
```

A spawn on the agents that finds no agent available runs `command` and waits up to
`timeout_secs` for one to connect, then fails as before. Once no orb has run or had a
job on the agents for `sleep_after_idle_secs`, `sleep_command` runs; sleeping orbs do
not count, since they keep their disk and checkpoint. A spawn that arrives while
`sleep_command` runs, or after it while an agent is still connected, waits for every
agent to disconnect and then runs `command`, so `sleep_command` can return as soon as
it has asked the host to stop. If an agent stays connected for `timeout_secs` after
the sleep, orbd assumes the host stayed up and places the spawn on it. A held spawn
waits at most `timeout_secs` in total.

`wake:` is one target for all agents. It runs only when no agent can take a spawn, so
with several agents a sleeping host is not woken because another is full, and the
sleep runs only once every agent is idle. It suits a single host that scales to zero.

The coordinator keeps the Amp CLI current with `amp update` (every
`amp_cli_update_interval_secs`, 6 h by default) and puts each new version on probation.
The first spawn it runs that succeeds makes it the known-good CLI, kept as a copy in
`<state>/bin/amp-good`. If jobs on two different orbs fail in a way that blames the CLI
first (orbd could not read the job, the coordinator found it invalid, or the guest got
no usable bootstrap environment), the providers go back to the known-good copy and that
version is never run again; the next release gets its own probation. A broken release
therefore costs a couple of failed spawns, which Amp retries, instead of an outage.

Agents use QEMU and Firecracker, and start only when they can run them
(`ORBD_VM_ONLY=true`, the default): on Linux `/dev/kvm` must work; on macOS QEMU must be
installed and on the agent's `PATH`. Docker orbs are containers sharing the host's
kernel: an agent runs them only with `ORBD_VM_ONLY=false`, and then uses Docker (mount
`/var/run/docker.sock`) when `/dev/kvm` does not work or its config has
`runtime.docker`. An agent keeps the runtime it started with, so one set up for Docker
orbs refuses to start without `ORBD_VM_ONLY=false`. Nested virtualization exposes more
of the host's KVM to orbs, so agents allow none by default: only one started with
`ORBD_NESTED_KVM=true` advertises it, so the coordinator places orb types with `kvm`
only there (a spawn finding none fails, saying so). An agent without it refuses to
start such an orb itself (one created there earlier, or one a coordinator sends anyway);
it still pauses and deletes them. On x86, a QEMU orb whose type has no `kvm`, or any on
such an agent, sees no VMX or SVM from its next cold boot (a sleeping orb wakes with the
CPU it slept with). On `docker stop` an agent saves its running VM orbs and
restores them when it starts again,
so give it time (`--stop-timeout 300`; under systemd, `KillMode=mixed` and
`TimeoutStopSec=300`). When the host shuts down, systemd stops `docker.service` itself,
which by default it kills after 90 s, cutting saves short whatever `--stop-timeout` says:
raise it too, e.g. a drop-in `/etc/systemd/system/docker.service.d/orbd.conf` with
`[Service]` and `TimeoutStopSec=360`. Orb disk sizes must be at least Amp's image
(~5 GiB).

The image ships QEMU 11.0 (Debian 13 backports), Firecracker 1.17 and orbd's Linux 7.3
guest kernel; an agent run outside it needs QEMU 9.0 or newer, `ip` and `nft`, `zstd`
(for archived disks), and Firecracker as `firecracker` on its `PATH` (`orbd doctor`
checks). Orb types without
`kvm` run only in Firecracker, so an agent without it takes none of them, and with nested
KVM off (the default) no orbs at all; it warns when it starts. VM orbs run in their own
network namespace behind NAT and cannot reach the agent itself, each other or private
networks (see above). A sleeping orb wakes only on the VMM build and CPU it slept on, so
after an upgrade it cold-boots once; the agent deletes such memory checkpoints when it
starts, as it deletes every checkpoint that was restored or superseded.

### Host storage

Orb disks, project snapshots, prepared images and memory checkpoints all live in the
agent's state directory (`/var/lib/orbd`, the `orbd-agent` volume above). Every
orb disk starts as a copy of an image or a project snapshot, and every snapshot as a copy
of an orb's disk. On a filesystem with reflinks those copies are clones: instant, and
sharing the base's blocks until the orb changes them. On ext4 each is a full copy. The
agent logs at startup which it got.

- **A disk of its own for the state directory, formatted XFS** (`mkfs.xfs`, reflinks on
  by default): clones, mature with large VM images, no tuning. Mount it at the volume's
  path (or bind it in with `-v /srv/orbd:/var/lib/orbd`).
- **On a btrfs root**, give the state directory a subvolume of its own and set No_COW on
  it while it is empty (`chattr +C`), so everything created under it inherits it. btrfs
  clones only between files that agree on No_COW (the agent warns at startup if its
  directories disagree), otherwise silently copying in full. Copy-on-write VM disks
  fragment, and compression needs it, so leave compression off there (no
  `compress-force` on the filesystem); never defragment it (that unshares every clone)
  and never snapshot it.
- **Trim:** orbs return the space their guests free (they trim before every sleep, and
  their disks pass discards through, with Firecracker 1.17 or later), so let discards
  reach the drive: `fstrim.timer` on the host, and `allow-discards` (`discard` in
  crypttab) on LUKS.
- **Drives:** clones and trims save writes, but an agent still writes a lot (every sleep
  writes the orb's memory). Prefer drives rated for it (TLC with DRAM) over DRAM-less QLC; on
  those keep 15-20% free (`ORBD_DISK_RESERVE_GIB`), as they slow down sharply when full.

### Host firewalls

An agent forwards its VM orbs' traffic: each orb's tap sits in a network namespace of its
own, behind the agent's namespace, which NATs it onwards. In a container of its own
(above), that forwarding stays inside the container and the host sees ordinary container
traffic. An agent in the host's network namespace (under systemd, or a container with
`--network host`) forwards through the host's firewall instead, and a packet that any
firewall's forward chain drops is dropped, whatever orbd's table accepts. ufw (`deny
(routed)`, its default), Docker (it sets iptables' FORWARD policy to DROP when it turns
forwarding on) and firewalld (it rejects forwarding between interfaces it has not been
told about) all do that by default, cutting every orb off. Allow traffic from `orbd*` out,
and its replies back:

- ufw: `ufw route allow in on orbd+` (ufw already passes replies), or
  `-A ufw-before-forward -i orbd+ -j ACCEPT` after the `RELATED,ESTABLISHED` line of
  `ufw-before-forward` in `/etc/ufw/before.rules`, then `ufw reload`.
- Docker hosts: both `iptables -I DOCKER-USER -i orbd+ -j ACCEPT` and `iptables -I
  DOCKER-USER -o orbd+ -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT` (Docker
  passes replies only to its own bridges). They survive a Docker restart, not a reboot:
  save them the way your host saves iptables rules.
- firewalld: `firewall-cmd --permanent --zone=trusted --add-interface=orbd+`, then
  `firewall-cmd --reload`.
- nftables: `iifname "orbd*" accept` and `oifname "orbd*" ct state established,related
  accept` in your forward chain, before what drops.

This does not widen what orbs reach: orbd's own table still drops their traffic to
private ranges, the host and metadata endpoints (above). Firecracker orbs have always
taken this path; since 0.3.0, Linux QEMU orbs (`kvm` types) do too, where 0.2's used
QEMU's user-mode network, whose traffic was QEMU's own. QEMU orbs created by 0.2.x are
refused and must be deleted, and upgrades from 0.2 go through 0.3.0.

An agent with VM runtimes checks this path shortly after it starts, without delaying
it: it asks one of the orbs' DNS servers (`ORBD_DNS`; not loopback, link-local or the
host's own) a question itself, and the same question from a namespace behind a veth
named like an orb's (`orbdprobe`, on `169.254.169.0/30`, which no orb gets). If only its
own question is answered, it warns that a firewall drops forwarded orb traffic, with
these fixes; if neither is, it says the check was inconclusive. `orbd doctor` runs the
same check where the agent runs (as root; `docker exec <agent> orbd doctor` for one in a
container).

### Upgrading to 0.4

Agents are VM-only and allow no nested KVM unless told otherwise:

- An agent running Docker orbs refuses to start until it has `ORBD_VM_ONLY=false`.
- Agents meant for orb types with `kvm` need `ORBD_NESTED_KVM=true`. Without it, new
  orbs of those types find no agent. Running ones are checkpointed when the agent stops,
  as always, but the new agent refuses to restore or wake them: they stay asleep with
  their checkpoint until it runs with `ORBD_NESTED_KVM=true` (unless the checkpoint is
  dropped meanwhile, by `ORBD_CHECKPOINT_MAX_AGE_HOURS` or a new QEMU or kernel). An
  agent holding such orbs says how many when it starts.

### Inside an orb

Amp's agent and every tool it starts run as `user`, who has passwordless sudo, next to
the orb's credentials (`~/.config/amp/headless-bootstrap.env` and the environment of
Amp's processes). For processes that cannot use sudo, e.g. tools in a sandbox, orbd's
guest kernel and the sysctls it adds to the image
([`60-orbd-hardening.conf`](guest/rootfs/etc/sysctl.d/60-orbd-hardening.conf)) set the
limits of a hardened distribution, on QEMU and Firecracker, x86_64 and arm64 alike:

- Landlock: any process can confine itself and its children to chosen paths and TCP
  ports without privileges (ABI 8 or newer).
- Yama (`kernel.yama.ptrace_scope=1`): ptrace, `/proc/<pid>/mem`, `process_vm_readv`
  and `pidfd_getfd` work only on descendants, so a tool cannot attach to the agent that
  started it. `gdb ./prog` and `strace cmd` work; `gdb -p` takes sudo, and inside a
  Docker container in the orb `--cap-add SYS_PTRACE` (sudo does not reach in there).
- `dmesg` and kernel addresses (`kptr_restrict`) take root, unprivileged eBPF is off
  until reboot, the BPF JIT hardens every program, setuid programs leave no core dumps,
  `TIOCSTI` cannot type into a shared terminal, and the lowest 64 KiB cannot be mapped.
- Unchanged: seccomp filters, unprivileged user namespaces (bwrap, `unshare -r`,
  rootless containers), Docker in the orb, nested KVM and everything run with sudo.

What they do not do:

- Stand in the way of sudo: a tool that can run it is root and can undo all of this.
  They bind processes that cannot, e.g. under `no_new_privs` (which bwrap, Landlock and
  seccomp sandboxes set).
- Keep processes of `user` apart. Yama limits attaching only: reading another process's
  `/proc/<pid>/environ`, or its open files through `/proc/<pid>/fd`, `cwd` and `root`,
  is read access, which any process of `user` keeps on the agent and every other
  dumpable process of `user`. `hidepid` (orbd does not set it) never hides a process
  from its own uid either. A Landlock sandbox denies these for processes outside it,
  and a PID namespace hides them; otherwise keep secrets from `user` (another uid, or a
  non-dumpable process).
- Protect files: any tool can read what `user` can, the credentials file included,
  unless a sandbox (Landlock, a mount namespace) hides it.

A new guest kernel: QEMU checkpoints record the kernel's path and digest and restore
only while that file is unchanged. Replacing it (a new orbd image does, as does a new
`Image` on a Mac) makes the agent discard the checkpoints of sleeping orbs, and those it
took of running ones when it stopped for the upgrade: these orbs cold-boot once, on
their next spawn, with their disks and without their processes. A Firecracker restore
does not read the kernel, so its orbs wake on the kernel they slept with and get the new
one at their next cold boot. The guest integration (`orbd-guest`, the sysctl file)
reaches only new disks made from the image: an existing orb keeps its disk as it was,
and a disk made from a project snapshot of the same lineage keeps the snapshot's, though
its kernel still boots with Yama, the `dmesg` restriction, unprivileged eBPF off and no
`TIOCSTI`.

### macOS agents

An Apple Silicon Mac runs arm64 orbs (orb types with `arch: arm64`) in QEMU with
Hypervisor.framework, from Amp's `amp-orb-arm64` image. They sleep and wake with their
memory as on Linux, even while the Mac is locked. On the Mac:

- `brew install qemu e2fsprogs`, with `$(brew --prefix e2fsprogs)/sbin` on the agent's
  `PATH`, and Docker (e.g. OrbStack).
- `docker build -t orbd-image-builder:umoci-0.6.0 guest/builder`: orb images are
  unpacked in this container.
- From a [release](https://github.com/samcm/orbd/releases): `orbd-*-aarch64-apple-darwin`
  (`orbd`), and `orbd-kernel-*-arm64.Image` and `orbd-guest-*-aarch64-unknown-linux-musl`
  at `/usr/local/share/orbd/{Image,orbd-guest}` (or set `runtime.qemu.kernel` and
  `images.guest_helper`). To build them: `cargo build --release -p orbd`, and
  `ARCH=arm64 guest/kernel/build.sh` and `orbd-guest` in a linux/arm64 container.
- Run `orbd agent` as your user, e.g. from a LaunchAgent; `orbd doctor` checks the setup.
- Fence orbs off your LAN with pf (below): without that QEMU orbs do not start.

QEMU runs in a Seatbelt sandbox (`sandbox-exec`, where Linux uses seccomp): it reads
only the system, its installation and its orb's kernel and disks, writes only its orb's
files, runs no other program, and sends no ICMP and nothing to `localhost`.

macOS has no network namespaces: orbs use QEMU's user-mode network and otherwise reach
whatever the Mac can, its LAN included. To fence them in, set
`runtime.qemu.network_group` (a group of its own, e.g. `_orbdvm` created with `dscl`),
install the release's `orbd-netgroup` setgid to that group on the agent's `PATH` (`sudo
install -o root -g <group> -m 2755 orbd-netgroup /usr/local/bin/`; the agent checks it on
every launch), and block the group with pf `group` rules, e.g. in an anchor that
`/etc/pf.conf` evaluates (`com.apple/orbd` is, through its `anchor "com.apple/*"`):

```
table <orbd_blocked> const { 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, \
    169.254.0.0/16, 172.16.0.0/12, 192.168.0.0/16, 198.18.0.0/15, 224.0.0.0/4, 240.0.0.0/4 }
block return out quick inet proto { tcp, udp } from any to <orbd_blocked> group _orbdvm
block drop in quick inet proto { tcp, udp } from <orbd_blocked> to any group _orbdvm
block return out quick inet6 proto { tcp, udp } from any to any group _orbdvm
```

QEMU maps the orb's gateway to the Mac's `127.0.0.1`, so pf must not skip `lo0`; on
tunnel interfaces (e.g. Tailscale's) pf sees only inbound replies, hence the `in` rule.
Reboots, network changes and other software can disable pf or flush rules: keep them
loaded with a root LaunchDaemon (e.g. every 30 s: `pfctl -a com.apple/orbd -f <file>` when
the anchor is empty, `pfctl -E` when pf is disabled).

QEMU orbs start only while this fence verifiably holds. The agent cannot read pf's state
(that needs root), so it tests it: it connects to a listener of its own on `127.0.0.1`,
then has `orbd-netgroup` run `orbd fence-probe`, the same connection with the group. A
QEMU orb starts only when pf blocks that one and not the agent's; a passing test counts
for 30 s, and the agent repeats it every 30 s, logging when the fence comes or goes
(metric `orbd_egress_fence_verified`). Otherwise, or without `network_group`, spawns and
wakes fail with the reason (orbs already running keep running). `orbd doctor -c <agent
config>` runs the same test. It proves that pf blocks the group from the Mac itself;
that your rules also cover the LAN and tailnet is yours to check, as is UDP (the test
connects over TCP; block both, as above):
`orbd-netgroup nc -z -G 3 <LAN host> <port>` connects as an orb would, and must fail.
`ORBD_ALLOW_UNFENCED_EGRESS=true` starts QEMU orbs without a fence.

## Restricted egress

For orbs that act on untrusted input (an agent steered by other people's issues, pull
requests or documents), give an orb type `egress`: its orbs reach only what it lists.

```yaml
orb_types:
  - id: untrusted-4vcpu-8gb
    vcpus: 4
    memory_mib: 8192
    disk_gib: 40
    egress:
      domains: [ampcode.com, "*.ampcode.com"]  # names, as the agent's DNS proxy resolves them
      ports: ["tcp:443"]                       # where those are reached (the default)
      allow: ["tcp:140.82.112.0/20:22"]        # addresses (GitHub's SSH), as in ORBD_EGRESS_ALLOW
```

- `domains`: `example.com` is that name only, `*.example.com` every name below it but
  not `example.com` itself (list both for both). Hostnames in ASCII (internationalized
  ones in punycode) of at least two labels; `*` only as the whole first label, and not
  over a suffix under which anyone can have names: a public suffix of the [Public Suffix
  List](https://publicsuffix.org) (private section included: `co.uk`, `github.io`,
  `workers.dev`, `s3.us-east-1.amazonaws.com`, `ts.net`, ...), a name whose children
  are (`compute.amazonaws.com`), or one of a few shared domains the list lacks
  (`amazonaws.com`, `storage.googleapis.com`, `app.github.dev`,
  `r2.cloudflarestorage.com`, ...). List the names under those instead. The coordinator
  checks this against the list compiled into its build when it loads `orbd.yaml`, not
  for orbs that exist already. At most 256.
- `ports`: `tcp:<port or range>` or `udp:<port or range>`, for every listed name alike;
  default `[tcp:443]` (add `tcp:80` for plain HTTP, e.g. Debian's mirrors). At most 16.
- `allow`: destinations by address, `<tcp|udp|any>:<address or CIDR>[:<port or
  range>]`. At most 64. Link-local (cloud metadata) never. Public addresses as listed;
  private ones (the LAN, a tailnet, documentation ranges, ...) only as far as the
  agent's own `ORBD_EGRESS_ALLOW` allows them too: the agent's setting wins, so a
  coordinator's config cannot open an agent's LAN.
- `egress: {}` reaches nothing.

Everything else is dropped for these orbs: the rest of the internet, the LAN and every
other private range, a tailnet, IPv6, the agent itself, and DNS servers other than the
agent's proxy (`ORBD_EGRESS_ALLOW` itself opens nothing for them). Tailscale inside such
an orb works only if its control plane and DERP relays are listed (e.g.
`controlplane.tailscale.com`, `login.tailscale.com` and the `derp<n>.tailscale.com`
names, with `udp:3478` for STUN; not verified), and then reaches peers through DERP
only.

How it works. The agent runs a DNS proxy, and the orb's DNS server (set through the
guest's network config) is its gateway, which the orb's network namespace sends on to the
proxy (`169.254.0.1:53` on the agent's loopback; only restricted orbs reach it, each
known by its veth address, at most 300 packets a second each; the agent does not answer
ARP for that address where the kernel has nftables' ARP family, and warns where not). A name the orb type does not list gets NXDOMAIN and is never sent
upstream. A listed one is resolved through the agent's `ORBD_DNS` servers (UDP, then TCP
for long answers, then the next server on SERVFAIL or REFUSED; a fresh port and id per
query, and the answer must match it); its CNAME chain is followed as returned (the
targets need not be listed: CDNs name their own hosts), and the addresses at its end
are opened, for this orb only, before the orb gets the answer. Every record of the
answer carries its lowest TTL, at most 2 minutes, so orbs look names up again soon; an
address stays open at least 10 minutes and 30 s, and connections opened meanwhile
continue after it. Other query types for listed names get empty answers (orbs have no
IPv6); UDP answers fit 512 bytes, or the orb's EDNS size up to 1232, with as many
records as fit and TC set when not all do. Addresses that are private, link-local, cloud
metadata or otherwise not global are never opened, whatever name resolves to them (DNS
rebinding): the proxy leaves them out of the answer and logs it, and the orb's nftables
chain drops them anyway; the agent's own addresses are never reachable. The proxy bounds
its work (256 queries in flight, 32 per orb; 64 TCP connections, 4 per orb, each at most
30 s and 10 s idle; 5 s per query; 1024 open addresses per orb) and runs on a thread of
its own. What orbs send it is read only by codecs, `orbd dns-codec`: child processes,
one per restricted orb's launch (started at its first query, stopped when the launch is
over or after 10 minutes without queries), jailed like VMMs (their own uid above every
orb's, no capabilities, an empty read-only root, a network namespace with nothing in
it), undumpable, at the lowest CPU priority, with 32 MiB of memory and a CPU minute
each, under a seccomp filter that lets them only
read and write their socket to the agent, signal themselves and exit. The agent receives
and sends every packet; it hands the orb's codec each query, gets back the question,
decides, and has the codec build the answer from its records, which it sends only to
that orb. It parses its upstream servers' answers itself. So a codec taken over by an
orb can at most answer that orb's own queries wrongly, or look up names on that orb's
own list: nothing the orb could not do to its own DNS anyway. It opens nothing and sees
no other orb's queries (a TCP connection serves only the orb launch it was accepted
from). A codec that sends anything malformed, takes over a second or
exits is killed and started again (after 0.1 s, doubling up to 10 s while it keeps
failing; `orbd_egress_dns_codec_restarts_total`), and its orb gets no DNS answers
meanwhile. Refused names and addresses are logged (target `orbd::egress`, a few a second
per orb), with metrics `orbd_egress_dns_queries_total{result}` and
`orbd_egress_dns_blocked_addresses_total`. The flow log shows these orbs' connections,
to the agent too, with `egress: restricted`.

Placement fails closed. Such an orb type runs only on agents that enforce it, Linux
agents with a VM runtime (Firecracker, or QEMU for `kvm`), never on Docker or macOS
agents or Boat (`prefer` skips Boat for it). An agent refuses to start
one it cannot enforce, whatever the coordinator sends, and the coordinator does not
wake one on a host that no longer enforces it. The orb's recipe on its agent records
that it is restricted, so it is for life: it never launches unrestricted, whatever a
coordinator later sends, and without a recorded policy it reaches nothing. The policy
is installed before the VMM starts, at every launch (create, cold boot and wake), with a
fresh conntrack mark: only connections of that launch count as established, so none of
an earlier orb on the same network index, or of this orb under a wider policy, slips
through. The mark takes the whole conntrack mark of those orbs' connections: conntrack
is per network namespace, so in Docker nothing else sees it, but an agent sharing the
host's namespace overwrites what other tools mark on those connections. The policy and mark are recorded in `<state>/orbs/<orb>/egress.json`. An
agent that restarts installs the rules of every restricted orb on its host before
anything else, keeping the addresses they had open, so a VMM that outlived it is never
unrestricted and keeps working; an unreadable record restricts its orb to nothing. A
wake under an unchanged policy keeps the orb's open addresses too.

Edits of an orb type's lists reach its existing orbs at their next launch (a wake from
sleep or a cold boot), not while they run: `orbd orb pause` one to apply them now. A
connection a woken orb still has open is decided again by the new policy. An orb is
restricted for life if its type had `egress` when it was created; adding `egress` to an
orb type affects only orbs created afterwards. If a type loses its `egress`, or is
renamed or removed, its existing orbs keep the policy they last launched with: delete
them to change that. The coordinator is trusted for the policy's contents: whoever
controls it decides what restricted orbs may reach (within what the agents' own
`ORBD_EGRESS_ALLOW` allows of private destinations); agents only refuse what they
cannot enforce.

Rolling an agent back to a version without restricted egress while restricted orbs run
on it leaves their VMMs unrestricted: such an agent replaces orbd's nftables table
without their rules. Drain the agent first (`docker stop` checkpoints them), or pause
or delete those orbs; that agent cannot read their recipes, so it never starts them.

Caveats. Enforcement is by address after DNS, not by name: an opened address is open on
the policy's ports whatever host the orb then asks it for (TLS SNI and HTTP Host are
not checked). Shared CDN and cloud front-end addresses (Cloudflare, Fastly, Google's
front ends; `storage.googleapis.com` serves every Cloud Storage bucket) open everything
else they serve while open, so prefer specific names and keep the list short. DNS over
HTTPS or TLS to an opened address (an allowlisted resolver, or one sharing a CDN
address) resolves other names but still reaches only opened addresses. A wildcard
sends lookups of any name below it upstream, so an orb can encode data in such names
for that domain's servers; names off the list are never sent. Nothing stops an orb from
sending data to what is allowed (a listed code host takes uploads too). The proxy's
sockets, limits and upstream lookups (whose answers it parses) are the agent's; only
what orbs send is left to the jailed codec.

An example for Amp orbs (**needs confirming** against your Amp version: derived from
what the amp-orb image's bootstrap units fetch and the hosts the Amp CLI names; a name
it lacks shows as refused in the agent's log):

```yaml
egress:
  domains:
    - ampcode.com                     # AMP_URL: the thread's connection and Amp's API
    - "*.ampcode.com"                 # static.ampcode.com: cli-version.txt, `amp update`, install.sh
    - storage.googleapis.com          # amp-public-assets-prod-0: tools the CLI fetches (ripgrep)
    - github.com                      # the repository, if Amp clones it from GitHub
    - api.github.com
    - codeload.github.com
    - objects.githubusercontent.com   # release downloads
    - raw.githubusercontent.com
  ports: ["tcp:443"]
```

Add what the projects' own setup fetches (e.g. `registry.npmjs.org`, `pypi.org` and
`files.pythonhosted.org`, `deb.debian.org` with `tcp:80`).

## Boat

The coordinator can also run orbs as [Boat](https://boat.dev) sandboxes, with no agent:
it drives Boat's API itself. Add a `boat:` section; orb types without `kvm` whose vCPUs
and memory are a Boat size (`small` 2/4096, `default` 4/8192, `large` 8/16384, `xlarge`
16/32768; the free trial has only the first two) then run on Boat as `prefer` says:

```yaml
idle_orb_ttl_days: 7                       # below Boat's 14 days, see below
boat:
  api_key_file: /etc/orbd/tokens/boat      # a boat_... API key
  capacity: { vcpus: 4, memory_mib: 8192 } # what running Boat orbs may hold together
  auto_stop_secs: 86400                    # optional; Boat stops a forgotten orb (0: never)
prefer: [boat, local]                      # Boat first, the agents once it is full
```

Boat sizes the disk itself; `disk_gib` does not apply there.

Orbs run on Boat's Ubuntu machine, not Amp's orb image; the first spawn installs the
Amp CLI. Sandboxes get none of the Boat account's secrets (`noEnv`). A pause stops the
sandbox: Boat keeps its disk, not its processes, and bills nothing for it, but deletes
the disk after 14 days stopped, so keep `idle_orb_ttl_days` below 14 (an orb whose disk
expired anyway starts again from a fresh sandbox). Project snapshots are not supported.
The coordinator keeps its Boat bookkeeping in `boat.sqlite` in its state directory.

## License

[MIT](LICENSE)
