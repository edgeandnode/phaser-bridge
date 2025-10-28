fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir("src/generated")
        .type_attribute(".", "#[allow(dead_code)]")
        .type_attribute(".", "#[allow(clippy::enum_variant_names)]")
        .compile_protos(
            &[
                "proto/types/types.proto",
                "proto/remote/bor.proto",
                "proto/remote/ethbackend.proto",
                "proto/remote/kv.proto",
                "proto/customized-erigon/trie.proto",
                "proto/customized-erigon/blockdata.proto",
            ],
            &["proto"],
        )?;

    println!("cargo:rerun-if-changed=proto/");

    Ok(())
}
