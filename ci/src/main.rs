//! Sykli CI pipeline for POLKU
//!
//! Run locally: sykli
//! Debug: sykli --verbose

use sykli::Pipeline;

fn main() {
    let mut p = Pipeline::new();

    // === RESOURCES ===
    let src = p.dir(".");
    let cargo_registry = p.cache("cargo-registry");
    let cargo_git = p.cache("cargo-git");
    let target_cache = p.cache("target");

    // === TASKS ===

    // Format check
    let _ = p
        .task("fmt")
        .container("rust:1.85")
        .mount(&src, "/src")
        .mount_cache(&cargo_registry, "/usr/local/cargo/registry")
        .mount_cache(&cargo_git, "/usr/local/cargo/git")
        .workdir("/src")
        .run("rustup component add rustfmt && cargo fmt --all --check")
        .inputs(&["**/*.rs", "Cargo.toml"]);

    // Clippy lint
    let _ = p
        .task("clippy")
        .container("rust:1.85")
        .mount(&src, "/src")
        .mount_cache(&cargo_registry, "/usr/local/cargo/registry")
        .mount_cache(&cargo_git, "/usr/local/cargo/git")
        .mount_cache(&target_cache, "/src/target")
        .workdir("/src")
        .run("apt-get update && apt-get install -y protobuf-compiler && rustup component add clippy && cargo clippy --all-targets -- -D warnings")
        .inputs(&["**/*.rs", "Cargo.toml", "Cargo.lock"])
        .after(&["fmt"]);

    // Tests
    let _ = p
        .task("test")
        .container("rust:1.85")
        .mount(&src, "/src")
        .mount_cache(&cargo_registry, "/usr/local/cargo/registry")
        .mount_cache(&cargo_git, "/usr/local/cargo/git")
        .mount_cache(&target_cache, "/src/target")
        .workdir("/src")
        .run("apt-get update && apt-get install -y protobuf-compiler && cargo test --all")
        .inputs(&["**/*.rs", "**/*.toml", "Cargo.lock"])
        .after(&["clippy"]);

    // Release build
    let _ = p
        .task("build")
        .container("rust:1.85")
        .mount(&src, "/src")
        .mount_cache(&cargo_registry, "/usr/local/cargo/registry")
        .mount_cache(&cargo_git, "/usr/local/cargo/git")
        .mount_cache(&target_cache, "/src/target")
        .workdir("/src")
        .run("apt-get update && apt-get install -y protobuf-compiler && cargo build --release")
        .inputs(&["**/*.rs", "**/*.toml", "Cargo.lock"])
        .output("binary", "target/release/polku-gateway")
        .after(&["test"]);

    p.emit();
}
