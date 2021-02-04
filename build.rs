fn main() -> Result<(),Box<dyn std::error::Error>> {
   tonic_build::compile_protos("proto/himalaya_core.proto")?;
   tonic_build::compile_protos("proto/himalaya_master.proto")?;
   tonic_build::compile_protos("proto/himalaya_worker.proto")?;
   Ok(())
}
