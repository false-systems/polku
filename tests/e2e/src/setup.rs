//! Test environment setup using Seppo

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Service;
use seppo::{Context, PortForward, eventually};
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
        // Parse YAML to get imagePullPolicy: Never for Kind
        let receiver_deploy: Deployment = serde_yaml::from_str(manifests::RECEIVER_DEPLOYMENT)?;
        let receiver_svc: Service = serde_yaml::from_str(manifests::RECEIVER_SERVICE)?;

        ctx.apply(&receiver_deploy).await?;
        ctx.apply(&receiver_svc).await?;
        ctx.wait_ready("deployment/test-receiver").await?;

        // Deploy POLKU configured to emit to test-receiver
        let polku_deploy: Deployment = serde_yaml::from_str(manifests::POLKU_DEPLOYMENT)?;
        let polku_svc: Service = serde_yaml::from_str(manifests::POLKU_SERVICE)?;

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

/// K8s manifests for POLKU deployment
pub mod manifests {
    /// POLKU gateway deployment YAML with imagePullPolicy: Never for Kind
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
        - name: POLKU_LOG_LEVEL
          value: "debug"
"#;

    /// POLKU service YAML
    pub const POLKU_SERVICE: &str = r#"
apiVersion: v1
kind: Service
metadata:
  name: polku
spec:
  selector:
    app: polku
  ports:
  - port: 50051
    targetPort: 50051
"#;

    /// Test receiver deployment YAML with imagePullPolicy: Never for Kind
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

    /// Test receiver service YAML
    pub const RECEIVER_SERVICE: &str = r#"
apiVersion: v1
kind: Service
metadata:
  name: test-receiver
spec:
  selector:
    app: test-receiver
  ports:
  - port: 9001
    targetPort: 9001
"#;
}
