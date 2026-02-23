//! Minimal POLKU pipeline — demonstrates the runtime API.
//!
//! ```bash
//! cargo run -p polku-runtime --example simple_pipeline
//! ```

use polku_runtime::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    polku_runtime::run(|hub| async move {
        Ok(hub
            .ingestor(JsonIngestor::new())
            .middleware(Filter::new(|msg: &Message| {
                msg.message_type.starts_with("important.")
            }))
            .emitter(StdoutEmitter::pretty()))
    })
    .await
}
