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

A coordinator runs one `amp orb provider` per offering (e.g. `qemu-4vcpu-8gb`) and places
each orb on an agent. Agents run on your own x86_64 hosts, dial the coordinator over
mutual-TLS gRPC, and run orbs in QEMU, Firecracker or Docker. Coordinator and agent are
the same static `orbd` binary; the coordinator also runs on aarch64.

> **Early.** Configuration and on-disk formats may still change.

```
                 Amp
                  ▲  pulls lifecycle jobs
┌─────────────────┴──────────────────┐
│ orbd coordinator                   │
│  amp orb provider (per offering)   │
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

1. **Provider tokens.** In Amp's workspace settings, create an orb provider per offering
   and save each token as `tokens/<offering>`.
2. **Coordinator** (x86_64 or arm64), with `orbd.yaml`:

   ```yaml
   public_url: https://coordinator.example:7443   # agents dial this
   offerings:
     - id: qemu-4vcpu-8gb
       runtime: qemu
       resources: { vcpus: 4, memory_mib: 8192, disk_gib: 40 }
       provider_token_file: /etc/orbd/tokens/qemu-4vcpu-8gb
     - id: firecracker-2vcpu-4gb
       runtime: firecracker            # vCPUs: 1 or an even number
       resources: { vcpus: 2, memory_mib: 4096, disk_gib: 40 }
       provider_token_file: /etc/orbd/tokens/firecracker-2vcpu-4gb
   ```

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

The coordinator creates its CA and keys in its volume on first start; keep that volume.
Agents detect their runtime and capacity (override with `ORBD_CAPACITY_VCPUS`,
`ORBD_CAPACITY_MEMORY_MIB`, `ORBD_CAPACITY_DISK_GIB`); orbs use DNS `1.1.1.1` and `9.9.9.9`
(`ORBD_DNS`). `orbd agent list` and `orbd agent remove <id>` manage agents; a removed
agent's orbs stay on it, unavailable. `orbd agent list` shows each agent's orbs, its
reservations against its capacity, and its disk: free, allocated by orbd, reserved and
limit (also exported as `orbd_agent_disk_*_bytes` and `orbd_reserved_disk_bytes`).

Running orbs reserve their vCPUs and memory; sleeping orbs reserve nothing. An offering's
`disk_gib` is each orb's disk size, a sparse file that grows as the orb writes, so disk is
admitted by what the host actually has: a new or waking orb needs free space for its
headroom (up to `running_orb_disk_headroom_gib`, default 10, of growth plus its memory
checkpoint) after the headroom of the orbs already running and the agent's reserve
(`ORBD_DISK_RESERVE_GIB`, default max(10 GiB, 5% of the filesystem)) are set aside.
`ORBD_CAPACITY_DISK_GIB` optionally limits what orbd allocates on the host (default: the
whole filesystem). A refusal names each host's limiting resource with its numbers.

Amp pauses an orb when its thread goes idle or is archived and deletes it when it is no
longer needed, e.g. when the thread is deleted; archiving sends no delete, so orbs of
archived threads would stay asleep on disk forever. The
coordinator deletes orbs that have been asleep for `idle_orb_ttl_days` (default 30; 0
never deletes) through the ordinary delete path. **A deleted orb is gone:** if its thread
is used again, its next spawn starts a fresh orb from the image (or project snapshot),
without its files, uncommitted work or processes, and Amp runs setup again. Set the TTL
longer than any thread you might come back to.

To delete the orbs of archived threads sooner, run a job that checks each orb's thread
in Amp: `orbd orb list` prints every orb's Amp id (`amp-<thread id>`), state and agent,
and `orbd orb delete <amp orb id>` deletes one the same way. It refuses an orb unless
it was last seen asleep or stopped at least `--min-asleep-mins` ago (default 60), with
no job from Amp open or started since (finished jobs are kept for about an hour), so an
orb in use is never deleted.

Amp never deletes project snapshots either: it reuses one for at most 72 hours, then
names a new one. An agent deletes a project snapshot or prepared image once no orb on it
is built on it and nothing has used it for 72 hours (the current image of a prewarmed
reference is kept), and image layers no build has used for as long. It checks at
startup, hourly and after each orb delete.

An offering can list `spill_to` offerings: when none of its hosts can take a new orb
(all full, down or unreported), the first listed offering that can takes it instead,
e.g. a Firecracker offering spilling to a Boat size when the homelab is full. Targets
must be like-for-like: `/dev/kvm` in the orb on both or neither, and at least as many
vCPUs and as much memory. An orb lives where it was placed, so a spilled orb wakes on
its spill target for the rest of its life. Resumes never spill.

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
`TimeoutStopSec=300`). Orb disk sizes must be at least Amp's image
(~5 GiB).

The image ships QEMU 11.0 (Debian 13 backports), Firecracker 1.17 and orbd's Linux 6.18
LTS guest kernel; an agent run outside it needs QEMU 9.0 or newer, and for Firecracker
`ip` and `nft` (`orbd doctor` checks). Firecracker orbs run in their own network
namespace behind NAT and cannot reach the agent itself or each other. A sleeping orb wakes only on the
VMM build and CPU it slept on, so after an upgrade it cold-boots once; the agent deletes
such memory checkpoints when it starts, as it deletes every checkpoint that was restored
or superseded.

## Boat

The coordinator can also run orbs as [Boat](https://boat.dev) sandboxes, with no agent:
it drives Boat's API itself. Add a `boat:` section and offerings with `runtime: boat`
whose vCPUs and memory are a Boat size (`small` 2/4096, `default` 4/8192, `large`
8/16384, `xlarge` 16/32768; the free trial has only the first two):

```yaml
idle_orb_ttl_days: 7                       # below Boat's 14 days, see below
boat:
  api_key_file: /etc/orbd/tokens/boat      # a boat_... API key
  capacity: { vcpus: 4, memory_mib: 8192 } # what running Boat orbs may hold together
  auto_stop_secs: 86400                    # optional; Boat stops a forgotten orb (0: never)
offerings:
  - id: boat-2vcpu-4gb
    runtime: boat
    resources: { vcpus: 2, memory_mib: 4096, disk_gib: 51 }
    provider_token_file: /etc/orbd/tokens/boat-2vcpu-4gb
```

Orbs run on Boat's Ubuntu machine, not Amp's orb image; the first spawn installs the
Amp CLI. Sandboxes get none of the Boat account's secrets (`noEnv`). A pause stops the
sandbox: Boat keeps its disk, not its processes, and bills nothing for it, but deletes
the disk after 14 days stopped, so keep `idle_orb_ttl_days` below 14 (an orb whose disk
expired anyway starts again from a fresh sandbox). Project snapshots are not supported.
The coordinator keeps its Boat bookkeeping in `boat.sqlite` in its state directory.

## License

[MIT](LICENSE)
