fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use tonic-prost-build which is the correct way in v0.14
    tonic_prost_build::configure()
        .build_server(false)  // We only need the client
        .build_client(true)
        .out_dir("src/generated")  // Output to a specific directory
        .compile_protos(
            &[
                "proto/types/types.proto",
                "proto/remote/bor.proto",
                "proto/remote/ethbackend.proto",
            ],
            &["proto"],  // Include directory for imports
        )?;

    println!("cargo:rerun-if-changed=proto/");

    Ok(())
}