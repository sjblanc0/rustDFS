use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("name_node_descriptor.bin"))
        .protoc_arg("-I=../proto")
        .compile_protos(&["name_node.proto", "data_node.proto"], &["proto"])?;

    Ok(())
}
