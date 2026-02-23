//! Power-user example — custom ports, no gRPC.
//!
//! ```bash
//! cargo run -p polku-runtime --example custom_runtime
//! ```

use polku_runtime::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    RuntimeBuilder::new()
        .metrics_port(9091)
        .disable_grpc()
        .configure(|hub| async move { Ok(hub.emitter(StdoutEmitter::pretty())) })
        .await
}
