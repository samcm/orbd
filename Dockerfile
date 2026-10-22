# syntax=docker/dockerfile:1
#
# ghcr.io/samcm/orbd: `orbd coordinator` (amd64, arm64) and `orbd agent` with the QEMU
# runtime (amd64). Built from prebuilt artifacts (the build.yml outputs) in the context:
#
#   amd64/orbd  amd64/orbd-guest  amd64/bzImage  arm64/orbd
#
#   docker buildx build --platform linux/amd64,linux/arm64 -f Dockerfile <context>

FROM --platform=$BUILDPLATFORM debian:12-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8 AS fetch
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl
# Runs on the agent host only (x86_64); see crates/orbd/src/image/rootfs.rs.
RUN curl -fsSL -o /umoci https://github.com/opencontainers/umoci/releases/download/v0.6.0/umoci.linux.amd64 \
    && echo "b51c267ec394499e42c6fde47f240b7b7dba57ea49df0b5acd304378b82a3b71  /umoci" | sha256sum -c \
    && chmod 0755 /umoci
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

FROM debian:12-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8 AS base
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /etc/orbd /var/lib/orbd /run/orbd
# `amp update` installs to ~/.amp/bin/amp, so the CLI lives there and is first on PATH.
COPY --from=fetch /amp /root/.amp/bin/amp
ENV PATH=/root/.amp/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

FROM base AS base-arm64

FROM base AS base-amd64
RUN apt-get update \
    && apt-get install -y --no-install-recommends qemu-system-x86 qemu-utils e2fsprogs \
    && rm -rf /var/lib/apt/lists/*
COPY --from=fetch /umoci /usr/local/bin/umoci
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
