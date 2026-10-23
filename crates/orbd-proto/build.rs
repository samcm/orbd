fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a vendored protoc so builds need no system protobuf compiler.
    if std::env::var_os("PROTOC").is_none() {
        // SAFETY: build scripts are single-threaded.
        unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?) };
    }
    let root = "../../proto";
    let guest = "../../proto/orbd/guest/v1/guest.proto";
    prost_build::compile_protos(&[guest], &[root])?;
    println!("cargo:rerun-if-changed={guest}");
    #[cfg(feature = "control")]
    {
        let control = "../../proto/orbd/control/v1/control.proto";
        tonic_prost_build::configure().compile_protos(&[control], &[root])?;
        println!("cargo:rerun-if-changed={control}");
    }
    Ok(())
}
