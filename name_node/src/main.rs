/**
 * RustDFS Name Node binary.
 *
 * Loads config from the global TOML file, parses CLI args,
 * then starts the Name Node gRPC server.
 */
mod args;
mod conn;
mod files;
mod service;

use args::RustDFSArgs;
use rustdfs_shared::config::RustDFSConfig;
use service::NameNodeService;

#[tokio::main]
async fn main() {
    let config = RustDFSConfig::new().unwrap();
    let args = RustDFSArgs::new();

    NameNodeService::new(args, config)
        .unwrap()
        .serve()
        .await
        .unwrap();
}
