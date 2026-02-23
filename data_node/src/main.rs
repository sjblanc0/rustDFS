mod service;
mod data_mgr;

use rustdfs_shared::base::args::RustDFSArgs;
use rustdfs_shared::base::result::Result;
use rustdfs_shared::base::config::RustDFSConfig;
use service::DataNodeService;

#[tokio::main]
async fn main() -> Result<()> {
    let config = RustDFSConfig::new()?;
    let args = RustDFSArgs::new();

    for (k, v) in &config.data_nodes {
        println!("Data Node ID: {}", k);
        println!("Host: {}, Port: {}", v.host, v.port);
    }

    DataNodeService::new(args, config)?
        .serve()
        .await?;

    Ok(())
}
