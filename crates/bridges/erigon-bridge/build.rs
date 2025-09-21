fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
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

    println!("cargo:rerun-if-changed=proto/");

    Ok(())
}