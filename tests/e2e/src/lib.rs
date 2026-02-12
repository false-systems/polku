//! POLKU End-to-End Tests
//!
//! Black-box tests using Seppo to deploy POLKU to Kubernetes.
//!
//! Test scenarios:
//! 1. Send 1 event → verify it arrives
//! 2. Send 100 events → verify all 100 arrive
//! 3. Send 430 events with 12% broken → verify all 430 arrive (no schema validation at gateway level)
//! 4. Plugin transforms event → verify transformation
//! 5. Plugin down → verify POLKU handles gracefully
//! 6. Stress test → verify stability under load
//! 7. Performance test → measure throughput/latency

pub mod proto {
    #![allow(clippy::large_enum_variant)]
    include!("proto/polku.v1.rs");
}

pub mod client;
pub mod setup;

pub use client::PolkuClient;
pub use setup::PolkuTestEnv;
