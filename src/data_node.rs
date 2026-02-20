mod rustdfs;

use crate::rustdfs::shared::args::RustDFSArgs;
use crate::rustdfs::shared::result::Result;

#[tokio::main]
async fn main() -> Result<()> {
    use crate::rustdfs::data_node::service::DataNodeService;
    use crate::rustdfs::shared::config::RustDFSConfig;

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