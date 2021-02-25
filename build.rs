fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.bytes(&["."]);
    config.compile_well_known_types();

    tonic_build::configure().compile_with_config(
        config,
        &["proto/himalaya.proto", "proto/himalaya_internal.proto"],
        &["proto/"],
    )?;
    Ok(())
}
