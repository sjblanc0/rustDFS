mod args;
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
