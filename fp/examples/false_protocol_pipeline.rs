//! End-to-end FALSE Protocol pipeline
//!
//! Demonstrates all three polku-fp capabilities:
//! - **Transport**: OccurrenceIngestor + OccurrenceRouter decode and route Occurrences
//! - **Self-Aware**: Probe observes the pipeline and emits Occurrences about its health
//! - **Correlation**: OccurrenceCorrelator groups related Occurrences by correlation keys
//!
//! Run with:
//! ```sh
//! cargo run --example false_protocol_pipeline -p polku-fp
//! ```

use polku_fp::{OccurrenceCorrelator, OccurrenceIngestor, OccurrenceRouter, Probe};
use polku_runtime::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    polku_runtime::run(|hub| async move {
        // ── Self-Aware: Probe observes pipeline health ──────────────
        // Runs as an independent task with its own StdoutEmitter.
        // Not part of the message pipeline — avoids feedback loops.
        let probe = Probe::new("polku-demo")
            .pressure_threshold(0.8)
            .tick_secs(5)
            .emitter(StdoutEmitter::pretty());
        tokio::spawn(probe.run());

        // ── Transport + Correlation pipeline ────────────────────────
        Ok(hub
            .ingestor(OccurrenceIngestor::new())
            .middleware(OccurrenceCorrelator::new().ttl_secs(300))
            .middleware(
                OccurrenceRouter::new()
                    .route("ci.", "stdout")
                    .route("kernel.", "stdout")
                    .route("gateway.", "stdout"),
            )
            .emitter(StdoutEmitter::pretty()))
    })
    .await
}
