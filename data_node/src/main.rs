mod args;
mod blocks;
mod service;

use rustdfs_shared::config::RustDFSConfig;
use service::DataNodeService;
use args::RustDFSArgs;

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
