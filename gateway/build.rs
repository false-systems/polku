fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Central proto repo is at ../../proto/ relative to gateway/
    let proto_root = "../../proto";
    let gateway_proto = format!("{proto_root}/polku/v1/gateway.proto");
    let event_proto = format!("{proto_root}/polku/v1/event.proto");

    // Tell Cargo to rerun if the proto files change
    println!("cargo:rerun-if-changed={gateway_proto}");
    println!("cargo:rerun-if-changed={event_proto}");

    // Skip proto compilation if source doesn't exist (CI uses pre-generated file)
    if !std::path::Path::new(&gateway_proto).exists() {
        println!("cargo:warning=Proto source not found, using pre-generated file");
        return Ok(());
    }

    // Configure extern path to use polku_core::proto::Event instead of generating
    // a duplicate Event type. This ensures we have ONE Event type across the codebase.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/proto")
        .extern_path(".polku.event.v1.Event", "::polku_core::Event")
        .compile_protos(&[&gateway_proto], &[proto_root])?;

    Ok(())
}
