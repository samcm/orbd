<p align="center">
  <img src="assets/logo.svg" width="96" height="96" alt="orbd logo">
</p>

<h1 align="center">orbd</h1>

orbd is a self-hosted backend for [Amp orbs](https://ampcode.com/docs/orbs/self-hosted). A
central coordinator runs one `amp orb provider` per offering (for example
`qemu-4vcpu-8gb`), turns Amp's lifecycle jobs into durable commands, and places each orb
on a compute backend. The first backend is a fleet of agents on your own x86_64 hosts that
dial the coordinator over mutual-TLS gRPC and run orbs in QEMU (nested `/dev/kvm`,
memory-preserving sleep), Firecracker, or Docker. Later backends will call hosted sandbox
APIs directly from the coordinator. The coordinator and the agent are the same static
`orbd` binary.

> **Status: under construction (milestone 1).** Interfaces, configuration and on-disk
> formats will change without notice. Nothing here is ready for production use.

## Architecture

```
                 Amp (ampcode.com)
                        ▲
                        │ HTTPS: pull lifecycle jobs
┌───────────────────────┼─────────────────────────────────────┐
│ orbd coordinator      │                                     │
│  ┌────────────────────┴───┐   ┌────────────────────────┐    │
│  │ amp orb provider       │ … │ amp orb provider       │    │
│  │ (offering A)           │   │ (offering B)           │    │
│  └───────────┬────────────┘   └───────────┬────────────┘    │
│              │ orbd dispatch (per job)    │                 │
│              ▼                            ▼                 │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ core: admission, placement, command journal (SQLite)  │  │
│  └───────────┬───────────────────────────────┬───────────┘  │
│              │ ComputeBackend                │              │
│  ┌───────────┴────────────┐      ┌───────────┴───────────┐  │
│  │ fleet backend          │      │ API backends (later)  │  │
│  └───────────┬────────────┘      └───────────┬───────────┘  │
└──────────────┼───────────────────────────────┼──────────────┘
               │ gRPC over mTLS                │ HTTPS
               │ (agents dial in)              ▼
       ┌───────┴─────────┐             hosted sandbox APIs
       ▼                 ▼
┌────────────────┐ ┌────────────────┐
│ orbd agent     │ │ orbd agent     │ …  x86_64 hosts
│ runtime: QEMU, │ │ runtime: QEMU, │
│ FC or Docker   │ │ FC or Docker   │
│ ┌─────┐┌─────┐ │ │ ┌─────┐        │
│ │ orb ││ orb │ │ │ │ orb │        │
│ └─────┘└─────┘ │ │ └─────┘        │
└────────────────┘ └────────────────┘
```

The coordinator can run on x86_64 or aarch64 Linux. Agents need x86_64 hosts with KVM for
the VM runtimes.

## Runtimes

Measured on a Ryzen 9 7940HS with a 2 vCPU / 4 GiB orb running the Amp orb image.

|                        | QEMU (microvm/q35)                       | Firecracker 1.17                    | Docker (systemd container) |
| ---------------------- | ---------------------------------------- | ----------------------------------- | -------------------------- |
| Boot                   | 1.7 s                                    | 1.2–1.3 s                           | 0.5–0.6 s                  |
| Idle RSS               | ~295 MiB                                 | ~245 MiB                            | ~10 MiB                    |
| Sleep                  | save, 1.05 s (161 MiB file, zero pages skipped) | full snapshot, 2.2–2.4 s (4 GiB file) | stop, 0.3–0.5 s     |
| Wake                   | 0.28–0.32 s                              | 0.04–0.06 s (lazy)                  | 0.5 s                      |
| Processes across sleep | preserved                                | preserved                           | restarted                  |
| `/dev/kvm` in the orb  | yes; nested guests survive save/restore  | no                                  | no                         |
| Role in orbd           | first runtime                            | second runtime                      | routing tests only         |

## Building from source

orbd needs Rust 1.90 or newer. `protoc` is vendored, so no system protobuf compiler is
required.

```sh
cargo build --release -p orbd
./target/release/orbd --version
```

Static musl binaries, as published on the releases page, are cross-compiled with
[`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild) and
[Zig](https://ziglang.org/) as the C toolchain for the bundled SQLite and `ring`:

```sh
rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
cargo zigbuild --release -p orbd \
  --target x86_64-unknown-linux-musl --target aarch64-unknown-linux-musl
```

Run the checks CI runs:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --locked --features orbd/failpoints -- -D warnings
cargo test --workspace --locked
```

## Setup

> These sections are placeholders until the corresponding parts work. All names below
> are examples, not recommendations.

### Coordinator

_TODO._ The coordinator host also needs the [Amp CLI](https://ampcode.com/docs), which is
published for linux-x64 and linux-arm64 (glibc).

### Agents

_TODO._

### Certificates

_TODO._ Agents and the coordinator authenticate each other with mutual TLS.

### Offerings

_TODO._ Each offering, such as `example-4vcpu-8gb`, maps to one `amp orb provider` and
one orb shape.

### Amp provider tokens

_TODO._ Each offering needs its own orb provider token from your Amp workspace settings.
Keep tokens out of configuration files that you commit.

## Dependency updates

Dependabot opens one grouped pull request per week for Cargo and one for GitHub Actions,
plus individual pull requests for major updates (including 0.x minor bumps, which Cargo
treats as breaking). Nothing merges automatically. Before merging, a maintainer:

- reads the upstream changelog or release notes, and the code diff between versions for
  anything touching build scripts, proc macros, `unsafe`, networking or crypto;
- checks advisories (`cargo deny` runs in CI) and supply-chain signals: changed
  maintainers or publishers, yanked versions, new transitive dependencies, a sudden jump
  in what a crate pulls in;
- for GitHub Actions, checks the new version is still pinned by commit SHA and comes from
  the expected owner;
- requires CI (`ci-ok`) to be green and runs the relevant tests locally, including the
  real-process crash-recovery suite (`cargo test --features failpoints`) for changes to
  crypto, storage or RPC crates.

The PR then gets a short comment saying what was checked, and is merged, fixed, or
closed with a reason.

## License

[MIT](LICENSE)
