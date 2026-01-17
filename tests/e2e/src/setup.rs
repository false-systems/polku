//! Test environment setup using Seppo

use seppo::{Context, DeploymentFixture, ServiceFixture, eventually};
use std::time::Duration;

/// POLKU test environment in Kubernetes
pub struct PolkuTestEnv {
    pub polku_addr: String,
}

impl PolkuTestEnv {
    /// Deploy POLKU and test-receiver to a new namespace
    pub async fn setup(ctx: &Context) -> anyhow::Result<Self> {
        // Deploy test-receiver first (POLKU needs it as downstream)
        let receiver = DeploymentFixture::new("test-receiver")
            .image("polku-test-receiver:latest")
            .replicas(1)
            .port(9001)
            .build();

        let receiver_svc = ServiceFixture::new("test-receiver")
            .selector("app", "test-receiver")
            .port(9001, 9001)
            .build();

        ctx.apply(&receiver).await?;
        ctx.apply(&receiver_svc).await?;
        ctx.wait_ready("deployment/test-receiver").await?;

        // Deploy POLKU configured to emit to test-receiver
        let polku = DeploymentFixture::new("polku")
            .image("polku-gateway:latest")
            .replicas(1)
            .port(50051)
            .env("POLKU_GRPC_ADDR", "0.0.0.0:50051")
            .env("POLKU_EMIT_GRPC_ENDPOINTS", "http://test-receiver:9001")
            .env("POLKU_EMIT_GRPC_LAZY", "true")
            .env("POLKU_LOG_LEVEL", "debug")
            .build();

        let polku_svc = ServiceFixture::new("polku")
            .selector("app", "polku")
            .port(50051, 50051)
            .build();

        ctx.apply(&polku).await?;
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

        Ok(Self { polku_addr })
    }
}

/// K8s manifests for POLKU deployment
pub mod manifests {
    /// POLKU gateway deployment YAML (alternative to fixtures)
    pub const POLKU_DEPLOYMENT: &str = r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: polku
spec:
  replicas: 1
  selector:
    matchLabels:
      app: polku
  template:
    metadata:
      labels:
        app: polku
    spec:
      containers:
      - name: polku
        image: polku-gateway:latest
        imagePullPolicy: Never
        ports:
        - containerPort: 50051
        env:
        - name: POLKU_GRPC_ADDR
          value: "0.0.0.0:50051"
        - name: POLKU_EMIT_GRPC_ENDPOINTS
          value: "http://test-receiver:9001"
        - name: POLKU_EMIT_GRPC_LAZY
          value: "true"
"#;

    /// Test receiver deployment YAML
    pub const RECEIVER_DEPLOYMENT: &str = r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-receiver
spec:
  replicas: 1
  selector:
    matchLabels:
      app: test-receiver
  template:
    metadata:
      labels:
        app: test-receiver
    spec:
      containers:
      - name: test-receiver
        image: polku-test-receiver:latest
        imagePullPolicy: Never
        ports:
        - containerPort: 9001
        env:
        - name: RECEIVER_ADDR
          value: "0.0.0.0:9001"
"#;
}
