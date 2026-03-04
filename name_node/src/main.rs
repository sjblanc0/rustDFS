mod proto;
mod service;
mod name_mgr;

use rustdfs_shared::base::args::RustDFSArgs;
use rustdfs_shared::base::config::RustDFSConfig;

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
