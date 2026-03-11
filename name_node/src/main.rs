mod args;
mod files;
mod service;

use rustdfs_shared::config::RustDFSConfig;
use args::RustDFSArgs;
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
