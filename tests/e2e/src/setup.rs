//! Test environment setup using Seppo

use seppo::{Context, PortForward, deployment, eventually, service};
use std::time::Duration;

/// POLKU test environment in Kubernetes
pub struct PolkuTestEnv {
    pub polku_addr: String,
    /// Keep the port forward alive for the duration of the test
    #[allow(dead_code)]
    _port_forward: PortForward,
}

impl PolkuTestEnv {
    /// Deploy POLKU and test-receiver to a new namespace
    pub async fn setup(ctx: &Context) -> anyhow::Result<Self> {
        // Deploy test-receiver first (POLKU needs it as downstream)
        let receiver_deploy = deployment("test-receiver")
            .image("polku-test-receiver:latest")
            .image_pull_policy("Never") // Required for Kind with pre-loaded images
            .port(9001)
            .env("RECEIVER_ADDR", "0.0.0.0:9001")
            .build();

        let receiver_svc = service("test-receiver")
            .selector("app", "test-receiver")
            .port(9001, 9001)
            .build();

        ctx.apply(&receiver_deploy).await?;
        ctx.apply(&receiver_svc).await?;
        ctx.wait_ready("deployment/test-receiver").await?;

        // Deploy POLKU configured to emit to test-receiver
        let polku_deploy = deployment("polku")
            .image("polku-gateway:latest")
            .image_pull_policy("Never") // Required for Kind with pre-loaded images
            .port(50051)
            .env("POLKU_GRPC_ADDR", "0.0.0.0:50051")
            .env("POLKU_EMIT_GRPC_ENDPOINTS", "http://test-receiver:9001")
            .env("POLKU_EMIT_GRPC_LAZY", "true")
            .env("POLKU_LOG_LEVEL", "debug")
            .build();

        let polku_svc = service("polku")
            .selector("app", "polku")
            .port(50051, 50051)
            .build();

        ctx.apply(&polku_deploy).await?;
        ctx.apply(&polku_svc).await?;
        ctx.wait_ready("deployment/polku").await?;

        // Port forward to POLKU for tests
        let pf = ctx.port_forward("svc/polku", 50051).await?;
        let polku_addr = format!("http://{}", pf.local_addr());

        // Wait for POLKU to be healthy
        let addr_clone = polku_addr.clone();
        eventually(|| {
            let addr = addr_clone.clone();
            async move {
                let client_result = crate::PolkuClient::connect(&addr).await;
                if let Ok(mut client) = client_result {
                    let event = crate::PolkuClient::make_event("test.health");
                    client.send_event(event).await.is_ok()
                } else {
                    false
                }
            }
        })
        .timeout(Duration::from_secs(30))
        .await_condition()
        .await?;

        Ok(Self {
            polku_addr,
            _port_forward: pf,
        })
    }
}

/// Helper to create POLKU deployment without receiver (for testing graceful degradation)
pub fn polku_deployment_no_receiver() -> k8s_openapi::api::apps::v1::Deployment {
    deployment("polku")
        .image("polku-gateway:latest")
        .image_pull_policy("Never")
        .port(50051)
        .env("POLKU_GRPC_ADDR", "0.0.0.0:50051")
        .env("POLKU_EMIT_GRPC_ENDPOINTS", "http://nonexistent:9001")
        .env("POLKU_EMIT_GRPC_LAZY", "true")
        .build()
}

/// Helper to create POLKU service
pub fn polku_service() -> k8s_openapi::api::core::v1::Service {
    service("polku")
        .selector("app", "polku")
        .port(50051, 50051)
        .build()
}
