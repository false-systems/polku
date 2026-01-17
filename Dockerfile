# Build stage - using nightly for edition2024
FROM rustlang/rust:nightly-bookworm-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Copy workspace files (only production crates, not test infrastructure)
COPY Cargo.lock ./
COPY core/ core/
COPY gateway/ gateway/
COPY ci/ ci/

# Create minimal workspace Cargo.toml for build (excludes test crates with external deps)
RUN printf '%s\n' \
    '[workspace]' \
    'resolver = "2"' \
    'members = ["core", "gateway", "ci"]' \
    '' \
    '[workspace.package]' \
    'version = "0.1.0"' \
    'edition = "2024"' \
    'authors = ["POLKU Team"]' \
    'license = "Apache-2.0"' \
    'repository = "https://github.com/falsesystems/polku"' \
    '' \
    '[workspace.lints.rust]' \
    'unsafe_code = "deny"' \
    '' \
    '[workspace.lints.clippy]' \
    'unwrap_used = "warn"' \
    'expect_used = "warn"' \
    'panic = "warn"' \
    'todo = "warn"' \
    '' \
    '[profile.release]' \
    'lto = "fat"' \
    'codegen-units = 1' \
    'opt-level = 3' \
    'strip = true' > Cargo.toml

# Build release binary with AHTI emitter support
RUN cargo build --release --package polku-gateway --features ahti

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/polku-gateway /usr/local/bin/polku-gateway

USER nobody:nogroup
EXPOSE 50051

ENTRYPOINT ["polku-gateway"]
