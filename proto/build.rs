/**
 * Build script for the rustdfs-proto crate.
 * Compiles data_node.proto and name_node.proto via tonic-prost-build
 * and writes a file descriptor set for reflection.
 */
use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("node_descriptor.bin"))
        //.protoc_arg("-I=../proto")
        .compile_protos(
            &[
                "data_node.proto",
                "name_node.proto",
                "name_node_persist.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
