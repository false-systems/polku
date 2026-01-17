fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Central proto repo
    let proto_root = "../../../proto";
    let gateway_proto = format!("{proto_root}/polku/v1/gateway.proto");

    println!("cargo:rerun-if-changed={gateway_proto}");

    if !std::path::Path::new(&gateway_proto).exists() {
        println!("cargo:warning=Proto source not found, using pre-generated file");
        return Ok(());
    }

    std::fs::create_dir_all("src/proto").ok();

    // Build gateway client (to send events to POLKU)
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src/proto")
        .extern_path(".polku.event.v1.Event", "::polku_core::Event")
        .extern_path(".polku.event.v1", "::polku_core::proto")
        .compile_protos(&[&gateway_proto], &[proto_root])?;

    Ok(())
}
