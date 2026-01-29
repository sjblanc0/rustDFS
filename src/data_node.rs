mod rustdfs;

use crate::rustdfs::shared::error::RustDFSError;

#[tokio::main]
async fn main() -> Result<(), RustDFSError> {
    use crate::rustdfs::data_node::service::DataNodeService;
    use crate::rustdfs::shared::config::RustDFSConfig;

    let config = RustDFSConfig::new()?;

    for (k, v) in &config.data_nodes {
        println!("Data Node ID: {}", k);
        println!("Host: {}, Port: {}", v.host, v.port);
    }

    DataNodeService::new(config)?
        .serve()
        .await?;

    Ok(())
}