fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/repc.proto")?;
    tonic_build::compile_protos("proto/admin.proto")?;
    Ok(())
}
