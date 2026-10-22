fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a vendored protoc so builds need no system protobuf compiler.
    if std::env::var_os("PROTOC").is_none() {
        // SAFETY: build scripts are single-threaded.
        unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?) };
    }
    let root = "../../proto";
    let files = [
        "../../proto/orbd/control/v1/control.proto",
        "../../proto/orbd/guest/v1/guest.proto",
    ];
    tonic_prost_build::configure().compile_protos(&files, &[root])?;
    for f in files {
        println!("cargo:rerun-if-changed={f}");
    }
    Ok(())
}
