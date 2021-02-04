fn main() -> Result<(),Box<dyn std::error::Error>> {
   tonic_build::configure()
   .compile(
       &["proto/himalaya_core.proto", "proto/himalaya_master.proto", "proto/himalaya_worker.proto"],
       &["proto/"],
   )?;
   Ok(())
}
