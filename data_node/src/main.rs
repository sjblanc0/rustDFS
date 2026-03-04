mod service;
mod data_mgr;

use rustdfs_shared::base::args::RustDFSArgs;
use rustdfs_shared::base::result::Result;
use rustdfs_shared::base::config::RustDFSConfig;
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
