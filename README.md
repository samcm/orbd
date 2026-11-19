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
| `/dev/kvm` in the orb   | ✅             | ❌          | ❌          |
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
   type with `kvm: true` runs in QEMU and has `/dev/kvm`, any other in Firecracker
   (vCPUs: 1 or an even number). A spawn of a new orb uses the orb type the thread
   picked, or the first one if it has none; an orb type not listed fails the spawn. An
   orb keeps its orb type and place for life, whatever later jobs name.
   `arch` says which CPU architectures a new orb may get: `amd64` (the default),
   `arm64`, or a list in order of preference, e.g. `[amd64, arm64]` to use arm64 agents
   when no amd64 one has room. An orb keeps the architecture it was placed with. arm64
   orbs run on [macOS agents](#macos-agents), without `kvm`.
   `retired_token_files` under `provider` lists tokens of providers you are moving
   away from, e.g. per-offering ones: they are still served, so their threads keep their
   orbs, and new orbs from them get the default orb type.

   Files without `config_version` are v1: `offerings`, each with a `runtime` (`qemu`,
   `firecracker`, `docker`, `boat`) and `resources`, served by `providers` (`id`,
   `token_file`, `orb_types` naming offerings) or by a per-offering
   `provider_token_file`, and spilling by `spill_to`. A v2 file is turned into the same
   offerings when it is loaded.

   ```sh
   docker run -d --name orbd -p 7443:7443 -v orbd:/var/lib/orbd \
     -v ./orbd.yaml:/etc/orbd/orbd.yaml -v ./tokens:/etc/orbd/tokens:ro \
     ghcr.io/samcm/orbd coordinator
   docker exec orbd orbd join-token host-1   # single use, valid for an hour
   ```

3. **Agents** (x86_64), one token each:

   ```sh
   docker run -d --privileged --stop-timeout 300 -v orbd-agent:/var/lib/orbd \
     -e ORBD_JOIN=<token> ghcr.io/samcm/orbd agent
   ```

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

Firecracker orbs reach the internet, not the host's LAN. Their traffic to private and
other non-global addresses (`10/8`, `172.16/12`, `192.168/16`, a tailnet's `100.64/10`,
loopback, documentation, multicast and reserved ranges) is dropped, and so are IPv6, cloud
metadata endpoints (link-local, Azure's `168.63.129.16`, Alibaba's `100.100.100.200`) and
the host's Tailscale resolver `100.100.100.100`, whatever is allowed. Allow what orbs need
with `ORBD_EGRESS_ALLOW`, comma-separated `<tcp|udp|any>:<address or CIDR>[:<port or
range>]` entries, e.g. `udp:192.168.2.201:41641` (direct Tailscale to a LAN host) or
`tcp:10.0.0.0/8:8000-8100`; the orbs' DNS servers are allowed on port 53.
`ORBD_ALLOW_PRIVATE_EGRESS=true` allows every private destination, as orbd did before
0.2: **upgrading from 0.1 cuts running orbs off the LAN** (their open connections to it
too), so list what they need first. The agent's own traffic is not affected, other
firewalls on the host still apply, and the host's public addresses are internet
addresses to orbs (keep services there firewalled). QEMU orbs (orb types with `kvm`) use
QEMU's user-mode network, which orbd does not filter: on Linux they reach what the agent
reaches, so fence them with host firewall rules (on macOS, see below).

`orbd agent list` and `orbd agent remove <id>`
manage agents; a removed agent's orbs stay on it, unavailable. `orbd agent list` shows
each agent's orbs, its reservations against its capacity, and its disk: free, allocated
by orbd, reserved and limit (also exported as `orbd_agent_disk_*_bytes` and
`orbd_reserved_disk_bytes`).

A new orb goes to the agent with the most free memory that fits it. An agent started with
`ORBD_OVERFLOW=true`, e.g. on a laptop or a workstation that is not always on, gets one only
when no other agent fits it, since an orb stays on the agent it was created on and cannot
wake while that host is off. `orbd agent list` marks such agents `(overflow)`.

Running orbs reserve their vCPUs and memory; sleeping orbs reserve nothing. An orb type's
`disk_gib` is each orb's disk size, a sparse file that grows as the orb writes, so disk is
admitted by what the host actually has: a new or waking orb needs free space for its
headroom (up to `running_orb_disk_headroom_gib`, default 10, of growth plus its memory
checkpoint) after the headroom of the orbs already running and the agent's reserve
(`ORBD_DISK_RESERVE_GIB`, default max(10 GiB, 5% of the filesystem)) are set aside.
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
snapshot. Snapshots from orbd before 0.2.8 record no lineage, so every project starts
once from its image after an upgrade. An agent deletes a project snapshot or prepared
image once no orb on it is built on it and nothing has used it for 72 hours (the current
image of a prewarmed reference is kept), and image layers no build has used for as long.
Only QEMU orbs are built on their base, since their disks read through to it; a
Firecracker orb's disk is a standalone copy and keeps nothing. It checks at startup,
hourly and after each orb delete.

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

Agents use QEMU and Firecracker when `/dev/kvm` works, otherwise Docker (mount
`/var/run/docker.sock`). On `docker stop` an agent saves its running VM orbs and restores
them when it starts again,
so give it time (`--stop-timeout 300`; under systemd, `KillMode=mixed` and
`TimeoutStopSec=300`). When the host shuts down, systemd stops `docker.service` itself,
which by default it kills after 90 s, cutting saves short whatever `--stop-timeout` says:
raise it too, e.g. a drop-in `/etc/systemd/system/docker.service.d/orbd.conf` with
`[Service]` and `TimeoutStopSec=360`. Orb disk sizes must be at least Amp's image
(~5 GiB).

The image ships QEMU 11.0 (Debian 13 backports), Firecracker 1.17 and orbd's Linux 7.3
guest kernel; an agent run outside it needs QEMU 9.0 or newer, and for Firecracker
`ip` and `nft` (`orbd doctor` checks). Firecracker orbs run in their own network
namespace behind NAT and cannot reach the agent itself, each other or private networks
(see above). A sleeping orb wakes only on the
VMM build and CPU it slept on, so after an upgrade it cold-boots once; the agent deletes
such memory checkpoints when it starts, as it deletes every checkpoint that was restored
or superseded.

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

QEMU runs in a Seatbelt sandbox (`sandbox-exec`, where Linux uses seccomp): it reads
only the system, its installation and its orb's kernel and disks, writes only its orb's
files, runs no other program, and sends no ICMP and nothing to `localhost`.

macOS has no network namespaces: orbs use QEMU's user-mode network and otherwise reach
whatever the Mac can, its LAN included. To fence them in, set
`runtime.qemu.network_group`, install the release's `orbd-netgroup` setgid to that group
on the agent's `PATH` (`sudo install -o root -g <group> -m 2755 orbd-netgroup
/usr/local/bin/`; the agent checks it on every launch), and block the group with pf
`group` rules. pf must not skip `lo0`, and on tunnel interfaces (e.g. Tailscale's) sees
only inbound replies, so drop those too.

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
