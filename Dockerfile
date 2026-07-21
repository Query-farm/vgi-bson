# Copyright 2026 Query Farm LLC - https://query.farm
#
# Single image that serves every network transport of the `bson` VGI worker:
#   docker run ... IMG            -> HTTP server on $PORT      (default; Fly.io / local)
#   docker run ... IMG tcp        -> raw Arrow-IPC over TCP on $PORT_TCP
#   docker run -i ... IMG stdio   -> stdio worker DuckDB spawns on-host
# See docker-entrypoint.sh.
#
# Like vgi-units (and unlike the Python model-registry workers) this worker is
# STATELESS: it is pure in-engine scalar/table compute over a BLOB column — no
# network, no state, zero egress — so there is no /data volume, no state env, and
# no `farm.query.vgi.volumes` mount-discovery label. The image is just the binary
# + a tiny entrypoint. The file-reading table functions (bson_seq /
# mongodump_read) read whatever the caller passes; they hold no per-attach state.
# syntax=docker/dockerfile:1

# ---- build stage -----------------------------------------------------------
# Pinned glibc (bookworm) so the binary links against the same libc the slim
# runtime ships. `vgi`/`vgi-rpc`/`bson`/`arrow` are all published crates.io deps
# (no git patch), so a stock rust image builds the workspace offline-of-git.
FROM rust:1-bookworm AS build
WORKDIR /src

# Copy the whole workspace (manifests + sources + lockfile). The cargo registry
# and target dir are BuildKit cache mounts, so the binary is copied OUT to a
# non-cache path before the layer ends (cache mounts don't persist in the image).
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin bson-worker \
    && cp target/release/bson-worker /bson-worker

# ---- runtime stage ---------------------------------------------------------
# debian-slim (not distroless) so the HEALTHCHECK below has a real `curl`. The
# HTTP transport is plain inbound HTTP (no TLS in the dependency tree), so no
# libssl / ca-certificates are needed.
FROM debian:bookworm-slim

# Build metadata, wired from docker/metadata-action outputs in CI.
ARG VERSION=0.0.0
ARG GIT_COMMIT=unknown
ARG SOURCE_URL=https://github.com/Query-farm/vgi-bson

# Standard OCI labels + the VGI transport-advertisement label. `transports`
# lists the NETWORK transports this image serves (http + raw tcp); stdio is a
# spawn mode, not a network transport, so it is not listed.
LABEL org.opencontainers.image.title="vgi-bson" \
      org.opencontainers.image.description="BSON decode/encode, MongoDB Extended JSON v2, typed field extraction, and mongodump/oplog/GridFS stream splitting as a VGI worker for DuckDB/SQL (stdio + HTTP + TCP)" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${GIT_COMMIT}" \
      org.opencontainers.image.licenses="MIT" \
      farm.query.vgi.transports='["http","tcp"]'

ENV PORT=8000 \
    PORT_TCP=8001 \
    # Build provenance only; the version the worker advertises over VGI comes
    # from the compiled CARGO_PKG_VERSION, not this.
    VGI_BSON_GIT_COMMIT=${GIT_COMMIT}

WORKDIR /app

# curl backs the HEALTHCHECK below; nothing else is needed at runtime.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# `--chmod` sets the mode in the COPY layer itself. A separate `RUN chmod` would
# rewrite the whole binary into a second layer (overlayfs copies the file on a
# metadata change), needlessly doubling its on-disk footprint in the image.
COPY --from=build --chmod=0755 /bson-worker /usr/local/bin/bson-worker
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# Run unprivileged. No state, no volume — there is nothing to own or persist.
RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8000 8001

# Readiness probe for HTTP mode. Inert for a short-lived stdio container, which
# has no HTTP server (the probe just fails harmlessly there).
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS "http://localhost:${PORT:-8000}/health" || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["http"]
