FROM rust:slim-bookworm AS builder

RUN apt-get update && apt-get install -y \
    make \
    perl \
    gcc \
    g++ \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/maelstrom-proxy

# Copy Cargo.toml
COPY Cargo.toml ./
# Cargo.lock is intentionally NOT copied here.
# This allows `cargo build` to generate a new Cargo.lock compatible with the builder's Cargo version,
# resolving issues with lock file version mismatches.
# This might result in slightly different dependency versions than your local Cargo.lock.

COPY src ./src
COPY proto ./proto
COPY build.rs ./

RUN cargo generate-lockfile && cargo update -p sfv --precise 0.10.4

RUN cargo build --release

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

# Install necessary runtime dependencies (ca-certificates for TLS, libssl for Pingora cryptography)
RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/maelstrom-proxy/target/release/vortex-router /usr/local/bin/maelstrom-proxy

# Expose the Pingora proxy service port
EXPOSE 8000

ENTRYPOINT ["maelstrom-proxy"]
