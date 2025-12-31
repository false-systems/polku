fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Central proto repo is at ../../proto/ relative to gateway/
    let proto_root = "../../proto";
    let proto_file = format!("{proto_root}/polku/v1/gateway.proto");

    // Tell Cargo to rerun if the proto files change
    println!("cargo:rerun-if-changed={proto_file}");

    // Skip proto compilation if source doesn't exist (CI uses pre-generated file)
    if !std::path::Path::new(&proto_file).exists() {
        println!("cargo:warning=Proto source not found, using pre-generated file");
        return Ok(());
    }

    // Compile polku proto - it's now self-contained
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/proto")
        .compile_protos(&[&proto_file], &[proto_root])?;

    Ok(())
}
