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
hosts, and later hosted sandboxes such as E2B.

## How it works

A coordinator runs one `amp orb provider` per offering (e.g. `qemu-4vcpu-8gb`) and places
each orb on an agent. Agents run on your own x86_64 hosts, dial the coordinator over
mutual-TLS gRPC, and run orbs in QEMU, Firecracker or Docker. Coordinator and agent are
the same static `orbd` binary; the coordinator also runs on aarch64.

> **Work in progress.** Docker works end to end with Amp; QEMU is being built.
> Configuration and on-disk formats will change.

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
| Status                  | 🚧 in progress | 🗓️ planned  | ✅ works    |

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
     - id: docker-2vcpu-4gb
       runtime: docker
       resources: { vcpus: 2, memory_mib: 4096, disk_gib: 20 }
       provider_token_file: /etc/orbd/tokens/docker-2vcpu-4gb
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
     -v /var/run/docker.sock:/var/run/docker.sock \
     -e ORBD_JOIN=<token> ghcr.io/samcm/orbd agent
   ```

Threads you create on that provider in Amp then run on your hosts.

The coordinator creates its CA and keys in its volume on first start; keep that volume.
Agents detect their runtime and capacity (override with `ORBD_CAPACITY_VCPUS`,
`ORBD_CAPACITY_MEMORY_MIB`, `ORBD_CAPACITY_DISK_GIB`); orbs use DNS `1.1.1.1` and `9.9.9.9`
(`ORBD_DNS`). `orbd agent list` and `orbd agent remove <id>` manage agents; a removed
agent's orbs stay on it, unavailable. Stopping an agent's container stops its QEMU orbs.

## License

[MIT](LICENSE)
