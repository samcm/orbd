// Compiles only the guest protocol (plain prost, no gRPC) so the guest binary stays small
// and statically linkable without the control plane's TLS and HTTP stacks.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        // SAFETY: build scripts are single-threaded.
        unsafe { std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?) };
    }
    let file = "../../proto/orbd/guest/v1/guest.proto";
    prost_build::compile_protos(&[file], &["../../proto"])?;
    println!("cargo:rerun-if-changed={file}");
    Ok(())
}
