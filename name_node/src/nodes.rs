use rustdfs_shared::error::RustDFSError;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::Status;

use rustdfs_proto::data::data_node_client::DataNodeClient;
use rustdfs_shared::conn::DataNodeConn;
use rustdfs_shared::host::HostAddr;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_shared::result::{Result, ServiceResult};

/**
 * Manages gRPC client connections to data nodes on the name node.
 * Tracks connection liveness via heartbeat timestamps and supports
 * stale-node detection for the reaper task.
 *
 *  @field connections - HashMap of hostname to [DataNodeConn].
 *  @field last_heartbeat - HashMap of hostname to last heartbeat epoch timestamp.
 *  @field log_mgr - Owned [LogManager] for logging connection events.
 */
#[derive(Debug)]
pub struct DataNodeManager {
    connections: RwLock<HashMap<String, DataNodeConn>>,
    last_heartbeat: RwLock<HashMap<String, u64>>,
    log_mgr: LogManager,
}

impl DataNodeManager {
    /**
     * Creates a new [DataNodeManager] instance.
     *
     *  @param log_mgr - [LogManager] for logging operations.
     *  @return DataNodeManager - Initialized data node manager.
     */
    pub fn new(log_mgr: LogManager) -> Self {
        DataNodeManager {
            connections: RwLock::new(HashMap::new()),
            last_heartbeat: RwLock::new(HashMap::new()),
            log_mgr,
        }
    }

    /**
     * Adds a new data node connection.
     *
     *  @param host - Hostname or IP address of the data node.
     *  @param port - Port number of the data node.
     *  @return ServiceResult<()> - Result indicating success or failure.
     */
    pub async fn add_conn(&self, host: &str, port: u16) -> ServiceResult<()> {
        let hostaddr = HostAddr {
            hostname: host.to_string(),
            port,
        };
        let endpoint = hostaddr.to_endpoint_serving(&self.log_mgr)?;

        self.log_mgr.write(LogLevel::Info, || {
            format!("Connecting to data node at {}:{}", host, port)
        });

        self.connections.write().await.insert(
            host.to_string(),
            DataNodeConn::new(
                hostaddr,
                DataNodeClient::connect(endpoint).await.map_err(|_| {
                    let err = status_err_connecting(host, port);
                    self.log_mgr.write_status(&err);
                    err
                })?,
            ),
        );

        Ok(())
    }

    /**
     * Records a heartbeat for a data node, updating its last-seen timestamp.
     *
     *  @param host - Hostname of the data node.
     */
    pub async fn record_heartbeat(&self, host: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.last_heartbeat
            .write()
            .await
            .insert(host.to_string(), now);
    }

    /**
     * Removes a data node connection and its heartbeat record.
     *
     *  @param host - Hostname of the data node to remove.
     */
    pub async fn remove_conn(&self, host: &str) {
        self.connections.write().await.remove(host);
        self.last_heartbeat.write().await.remove(host);

        self.log_mgr.write(LogLevel::Info, || {
            format!("Removed stale data node connection: {}", host)
        });
    }

    /**
     * Returns hostnames of data nodes whose last heartbeat is older than the timeout.
     *
     *  @param timeout - Max seconds since last heartbeat before a node is stale.
     *  @return Vec<String> - Hostnames of stale nodes.
     */
    pub async fn get_stale_nodes(&self, timeout: u64) -> Result<Vec<String>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                let err = RustDFSError::SystemTimeError(e);
                self.log_mgr.write_err(&err);
                err
            })?
            .as_secs();

        Ok(self
            .last_heartbeat
            .read()
            .await
            .iter()
            .filter(|(_, ts)| now.saturating_sub(**ts) > timeout)
            .map(|(host, _)| host.clone())
            .collect::<Vec<String>>())
    }

    /**
     * Retrieves all data node hosts + ports.
     *
     *  @return Vec<HostAddr> - Vector of all known data node hosts
     */
    pub async fn get_hosts(&self) -> Vec<HostAddr> {
        self.connections
            .read()
            .await
            .values()
            .map(|c| c.host.clone())
            .collect()
    }
}

fn status_err_connecting(host: &str, port: u16) -> Status {
    let msg = format!("Error connecting to data node at {}:{}", host, port);
    Status::internal(msg)
}
