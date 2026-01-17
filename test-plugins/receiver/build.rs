fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Central proto repo is at ../../../proto/ relative to test-plugins/receiver/
    let proto_root = "../../../proto";
    let gateway_proto = format!("{proto_root}/polku/v1/gateway.proto");

    println!("cargo:rerun-if-changed={gateway_proto}");

    // Skip if proto source not found (CI uses pre-generated)
    if !std::path::Path::new(&gateway_proto).exists() {
        println!("cargo:warning=Proto source not found, using pre-generated file");
        return Ok(());
    }

    std::fs::create_dir_all("src/proto").ok();

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .out_dir("src/proto")
        .extern_path(".polku.event.v1.Event", "::polku_core::Event")
        .extern_path(".polku.event.v1", "::polku_core::proto")
        .compile_protos(&[&gateway_proto], &[proto_root])?;

    Ok(())
}
