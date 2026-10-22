# syntax=docker/dockerfile:1
#
# ghcr.io/samcm/orbd: `orbd coordinator` (amd64, arm64) and `orbd agent` with the QEMU and
# Firecracker runtimes (amd64). Built from prebuilt artifacts (the build.yml outputs) in
# the context:
#
#   amd64/orbd  amd64/orbd-guest  amd64/bzImage  arm64/orbd
#
#   docker buildx build --platform linux/amd64,linux/arm64 -f Dockerfile <context>

FROM --platform=$BUILDPLATFORM debian:13-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad AS fetch
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl
# Runs on the agent host only (x86_64); see crates/orbd/src/image/rootfs.rs.
RUN curl -fsSL -o /umoci https://github.com/opencontainers/umoci/releases/download/v0.6.0/umoci.linux.amd64 \
    && echo "b51c267ec394499e42c6fde47f240b7b7dba57ea49df0b5acd304378b82a3b71  /umoci" | sha256sum -c \
    && chmod 0755 /umoci
# Firecracker (release binary only; no jailer). A new build means sleeping Firecracker
# orbs cold-boot once: checkpoints restore only on the exact binary.
RUN curl -fsSL -o /tmp/fc.tgz https://github.com/firecracker-microvm/firecracker/releases/download/v1.17.0/firecracker-v1.17.0-x86_64.tgz \
    && echo "06094a1108ae9e82aa4c23a775aa92758f53f1175d422270d9d6162cb9ade558  /tmp/fc.tgz" | sha256sum -c \
    && tar -xzf /tmp/fc.tgz -C /tmp \
    && install -m 0755 /tmp/release-v1.17.0-x86_64/firecracker-v1.17.0-x86_64 /firecracker
# Amp CLI as baked in; the coordinator runs `amp update` at startup and every 6 h.
# Empty AMP_VERSION means the current release.
ARG TARGETARCH
ARG AMP_VERSION=
RUN case "$TARGETARCH" in amd64) p=linux-x64 ;; arm64) p=linux-arm64 ;; *) exit 1 ;; esac \
    && url=https://static.ampcode.com/cli \
    && v=${AMP_VERSION:-$(curl -fsSL "$url/cli-version.txt")} \
    && curl -fsSL "$url/$v/amp-$p.gz" | gunzip >/amp \
    && echo "$(curl -fsSL "$url/$v/$p-amp.sha256")  /amp" | sha256sum -c \
    && chmod 0755 /amp

FROM debian:13-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad AS base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /etc/orbd /var/lib/orbd /run/orbd
# `amp update` installs to ~/.amp/bin/amp, so the CLI lives there and is first on PATH.
COPY --from=fetch /amp /root/.amp/bin/amp
ENV PATH=/root/.amp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

FROM base AS base-arm64

FROM base AS base-amd64
# QEMU from trixie-backports (orbd needs >= 9.0; trixie itself has 10.0). A new QEMU
# build means sleeping orbs cold-boot once: checkpoints restore only on the exact binary.
RUN printf 'Types: deb\nURIs: http://deb.debian.org/debian\nSuites: trixie-backports\nComponents: main\nSigned-By: /usr/share/keyrings/debian-archive-keyring.gpg\n' \
        >/etc/apt/sources.list.d/backports.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends e2fsprogs iproute2 nftables \
    && apt-get install -y --no-install-recommends -t trixie-backports qemu-system-x86 qemu-utils \
    && rm -rf /var/lib/apt/lists/* \
    && qemu-system-x86_64 --version
COPY --from=fetch /umoci /usr/local/bin/umoci
COPY --from=fetch /firecracker /usr/local/bin/firecracker
COPY amd64/bzImage /usr/share/orbd/bzImage
COPY --chmod=0755 amd64/orbd-guest /usr/share/orbd/orbd-guest

FROM base-$TARGETARCH
ARG TARGETARCH
COPY --chmod=0755 $TARGETARCH/orbd /usr/local/bin/orbd
LABEL org.opencontainers.image.source=https://github.com/samcm/orbd
VOLUME /var/lib/orbd
# tini forwards SIGTERM to orbd (orbs are checkpointed on stop) and reaps orphans.
ENTRYPOINT ["/usr/bin/tini", "--", "orbd"]
CMD ["--help"]
