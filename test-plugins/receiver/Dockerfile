# Build stage
FROM rustlang/rust:nightly-bookworm-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Copy workspace files needed for build
COPY Cargo.toml Cargo.lock ./
COPY core/ core/
COPY test-plugins/receiver/ test-plugins/receiver/

# Build release binary
RUN cargo build --release --package polku-test-receiver

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/polku-test-receiver /usr/local/bin/polku-test-receiver

USER nobody:nogroup
EXPOSE 9001

ENTRYPOINT ["polku-test-receiver"]
