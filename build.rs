fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(
        &[
            "proto/himalaya.proto",
            "proto/himalaya_internal.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}
