fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build clients for remote services
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src/generated")
        .compile_protos(
            &[
                "proto/types/types.proto",
                "proto/remote/bor.proto",
                "proto/remote/ethbackend.proto",
            ],
            &["proto"],
        )?;

    // Build server and client for admin services
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/generated")
        .compile_protos(&["proto/admin/sync.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/");

    Ok(())
}
