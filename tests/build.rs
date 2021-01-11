fn main() -> Result<(), Box<dyn std::error::Error>> {
    repc_build::compile_protos("proto/adder.proto")?;
    Ok(())
}
