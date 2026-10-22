<p align="center">
  <img src="assets/logo.svg" width="96" height="96" alt="orbd logo">
</p>

<h1 align="center">orbd</h1>

A self-hosted backend for [Amp orbs](https://ampcode.com/docs/orbs/self-hosted). A
coordinator runs `amp orb provider` for each offering and places orbs on agents, which
run them on your own hosts in QEMU (nested `/dev/kvm`, sleep keeps processes),
Firecracker or Docker.

> **Work in progress.** Not ready for use; config and formats will change.

```
Amp ◀── amp orb provider ── orbd coordinator ◀── mTLS gRPC ── orbd agent ── orbs
                             (x86_64/aarch64)                 (x86_64 hosts)
```

| Runtime     | Wake     | Processes across sleep | `/dev/kvm` in orb |
| ----------- | -------- | ---------------------- | ----------------- |
| QEMU        | ~0.3 s   | kept                   | yes               |
| Firecracker | ~0.05 s  | kept                   | no                |
| Docker      | ~0.5 s   | restarted              | no                |

## Build

```sh
cargo build --release -p orbd
```

## License

[MIT](LICENSE)
