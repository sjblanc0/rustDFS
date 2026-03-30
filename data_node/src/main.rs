/**
 * RustDFS Data Node binary.
 *
 * Loads config from the global TOML file, parses CLI args,
 * then starts the Data Node gRPC server and registers with
 * the Name Node.
 */
mod args;
mod blocks;
mod conn;
mod service;

use args::RustDFSArgs;
use rustdfs_shared::config::RustDFSConfig;
use service::DataNodeService;

#[tokio::main]
async fn main() {
    let config = RustDFSConfig::new().unwrap();
    let args = RustDFSArgs::new();

    DataNodeService::new(args, config)
        .unwrap()
        .serve()
        .await
        .unwrap();
}
