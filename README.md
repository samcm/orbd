<p align="center">
  <img src="assets/logo.svg" width="96" height="96" alt="orbd logo">
</p>

<h1 align="center">orbd</h1>

A self-hosted backend for [Amp orbs](https://ampcode.com/docs/orbs/self-hosted).

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

|                        | QEMU                    | Firecracker      | Docker     |
| ---------------------- | ----------------------- | ---------------- | ---------- |
| Wake                   | ~0.3 s                  | ~0.05 s          | ~0.5 s     |
| Processes across sleep | kept                    | kept             | restarted  |
| Host RAM while asleep  | none                    | none             | none       |
| `/dev/kvm` in the orb  | yes                     | no               | no         |
| Status                 | in progress             | planned          | works      |

## Build

```sh
cargo build --release -p orbd
```

Static binaries for x86_64 and aarch64 are attached to each release.

## Setup

You need a coordinator host with the [Amp CLI](https://ampcode.com/docs) (glibc, x86_64
or aarch64) and one or more agent hosts with the runtime you want (Docker for now).

1. **Provider token.** In Amp's workspace settings, create an orb provider for each
   offering and save its token to a file on the coordinator host.
2. **Certificates.** Create a CA, a server certificate for the coordinator, and a client
   certificate per agent whose CN (or DNS SAN) is the agent's id.
3. **Secret keys.** `orbd gen-key > key` on each host, mode 0600.
4. **Coordinator** — `orbd coordinator --config coordinator.yaml`:

   ```yaml
   coordinator:
     state_dir: /var/lib/orbd
     dispatch_socket: /run/orbd/dispatch.sock
     secret_key_file: /etc/orbd/key
     metrics_listen: 127.0.0.1:9464

   amp_endpoints:
     - id: amp
       url: https://ampcode.com

   backends:
     - kind: agent_fleet
       id: fleet
       listen: 0.0.0.0:7443
       tls:
         certificate_file: /etc/orbd/server.pem
         private_key_file: /etc/orbd/server.key
         ca_file: /etc/orbd/ca.pem
       agents:
         - id: host-1

   offerings:
     - id: docker-2vcpu-4gb
       amp_endpoint: amp
       provider_token_file: /etc/orbd/docker-2vcpu-4gb.token
       backend: fleet
       runtime: docker
       resources: { vcpus: 2, memory_mib: 4096, disk_gib: 20 }
       pause: { mode: disk_only }
   ```

5. **Agent** — `orbd agent --config agent.yaml` on each host:

   ```yaml
   agent:
     id: host-1
     coordinator_url: https://coordinator.example:7443
     state_dir: /var/lib/orbd
     run_dir: /run/orbd
     secret_key_file: /etc/orbd/key
     capacity: { vcpus: 16, memory_mib: 32768, disk_gib: 500 }
     tls:
       certificate_file: /etc/orbd/host-1.pem
       private_key_file: /etc/orbd/host-1.key
       ca_file: /etc/orbd/ca.pem

   runtime:
     docker:
       dns_servers: [1.1.1.1, 8.8.8.8]
       # Amp's orb image is ~6 GB; spawns fail until it is pulled.
       prewarm_images: [us-central1-docker.pkg.dev/amp-prod-0/amp-orb/amp-orb:latest]
   ```

Threads you create on that provider in Amp then run on your hosts. Prometheus metrics
are on `metrics_listen`; `orbd doctor` checks a host's prerequisites.

## Run with Docker

`ghcr.io/samcm/orbd` (amd64, arm64) has the Amp CLI and, on amd64, everything the QEMU
runtime needs. Configs go in `/etc/orbd/`, state in `/var/lib/orbd`. The agent needs a
long stop timeout so it can checkpoint its orbs on `docker stop`.

```sh
docker run -d --name orbd-coordinator -p 7443:7443 \
  -v /etc/orbd:/etc/orbd:ro -v orbd-coordinator:/var/lib/orbd \
  ghcr.io/samcm/orbd coordinator --config /etc/orbd/coordinator.yaml

docker run -d --name orbd-agent --privileged --stop-timeout 300 \
  -v /etc/orbd:/etc/orbd:ro -v orbd-agent:/var/lib/orbd \
  ghcr.io/samcm/orbd agent --config /etc/orbd/agent.yaml
```

## License

[MIT](LICENSE)
