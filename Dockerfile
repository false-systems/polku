# Build stage - using nightly for edition2024
FROM rustlang/rust:nightly-bookworm-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY core/ core/
COPY gateway/ gateway/
COPY ci/ ci/

# Build release binary with AHTI emitter support
RUN cargo build --release --package polku-gateway --features ahti

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/polku-gateway /usr/local/bin/polku-gateway

USER nobody:nogroup
EXPOSE 50051

ENTRYPOINT ["polku-gateway"]
