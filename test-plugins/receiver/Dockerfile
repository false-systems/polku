# Build stage
FROM rustlang/rust:nightly-bookworm-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# Copy workspace files needed for build
COPY Cargo.lock ./
COPY core/ core/
COPY test-plugins/receiver/ test-plugins/receiver/

# Create minimal workspace Cargo.toml for build
RUN printf '%s\n' \
    '[workspace]' \
    'resolver = "2"' \
    'members = ["core", "test-plugins/receiver"]' \
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

# Build release binary
RUN cargo build --release --package polku-test-receiver

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/polku-test-receiver /usr/local/bin/polku-test-receiver

USER nobody:nogroup
EXPOSE 9001

ENTRYPOINT ["polku-test-receiver"]
