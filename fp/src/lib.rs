//! polku-fp — FALSE Protocol plugins for POLKU
//!
//! Three capabilities, all optional:
//!
//! - **Transport**: `OccurrenceIngestor` + `OccurrenceRouter` — decode and route Occurrences
//! - **Self-Aware**: `Probe` — observe the pipeline and emit Occurrences about it
//! - **Clean**: Don't import this crate — default POLKU behavior
//!
//! # Transport Mode
//!
//! ```ignore
//! use polku_fp::{OccurrenceIngestor, OccurrenceRouter};
//!
//! polku_runtime::run(|hub| async move {
//!     Ok(hub
//!         .ingestor(OccurrenceIngestor::new())
//!         .middleware(OccurrenceRouter::new()
//!             .route("ci.", "sykli")
//!             .route("kernel.", "tapio"))
//!         .emitter(grpc_sykli)
//!         .emitter(grpc_tapio))
//! }).await
//! ```
//!
//! # Self-Aware Mode
//!
//! ```ignore
//! use polku_fp::Probe;
//!
//! let probe = Probe::new("polku-prod")
//!     .pressure_threshold(0.8)
//!     .tick_secs(5)
//!     .emitter(ahti_emitter);
//! tokio::spawn(probe.run());
//! ```

pub mod correlator;
pub mod ingest;
pub mod middleware;
pub mod occurrence;
pub mod probe;

pub use correlator::OccurrenceCorrelator;
pub use ingest::OccurrenceIngestor;
pub use middleware::OccurrenceRouter;
pub use occurrence::{Context, Entity, Error, Occurrence, Outcome, Reasoning, Severity};
pub use probe::Probe;
