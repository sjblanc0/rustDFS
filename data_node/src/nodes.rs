use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::Status;

use rustdfs_proto::data::data_node_client::DataNodeClient;
use rustdfs_shared::conn::DataNodeConn;
use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::host::HostAddr;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_shared::result::{Result, ServiceResult};

/**
 * Manages gRPC client connections to peer data nodes for replication.
 * Tracks last-used timestamps and supports TTL-based eviction of idle connections.
 *
 *  @field connections - HashMap of hostname to [DataNodeConn].
 *  @field last_used - HashMap of hostname to last-used epoch timestamp.
 *  @field log_mgr - Owned [LogManager] for logging connection events.
 */
#[derive(Debug)]
pub struct DataNodeManager {
    connections: RwLock<HashMap<String, DataNodeConn>>,
    last_used: RwLock<HashMap<String, u64>>,
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
            last_used: RwLock::new(HashMap::new()),
            log_mgr,
        }
    }

    /**
     * Adds a new peer data node connection for replication.
     *
     *  @param host - Hostname or IP address of the peer data node.
     *  @param port - Port number of the peer data node.
     *  @return ServiceResult<()> - Result indicating success or failure.
     */
    pub async fn add_conn(&self, host: &str, port: u16) -> ServiceResult<()> {
        let hostaddr = HostAddr {
            hostname: host.to_string(),
            port,
        };
        let endpoint = hostaddr.to_endpoint_serving(&self.log_mgr)?;

        self.log_mgr.write(LogLevel::Info, || {
            format!("Connecting to peer data node at {}:{}", host, port)
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

        self.touch(host).await;

        Ok(())
    }

    /**
     * Retrieves a cloned data node connection by its hostname.
     * Updates the last-used timestamp so the connection stays alive.
     *
     *  @param host - Hostname of the data node.
     *  @return ServiceResult<DataNodeConn> - Cloned connection or an error if unknown.
     */
    pub async fn get_conn(&self, host: &str) -> ServiceResult<DataNodeConn> {
        let conn = self
            .connections
            .read()
            .await
            .get(host)
            .ok_or_else(|| {
                let err = status_err_unknown_node(host);
                self.log_mgr.write_status(&err);
                err
            })
            .cloned()?;

        self.touch(host).await;
        Ok(conn)
    }

    /**
     * Checks if a peer data node connection exists by its host.
     *
     *  @param host - The host of the data node.
     *  @return bool - True if the connection exists, false otherwise.
     */
    pub async fn has_conn(&self, host: &str) -> bool {
        self.connections.read().await.contains_key(host)
    }

    /**
     * Removes connections that have been idle longer than the given TTL.
     *
     *  @param ttl - Max seconds since last use before a connection is evicted.
     */
    pub async fn remove_stale(&self, ttl: u64) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                let err = RustDFSError::SystemTimeError(e);
                self.log_mgr.write_err(&err);
                err
            })?
            .as_secs();

        let stale: Vec<String> = {
            let last_used = self.last_used.read().await;
            last_used
                .iter()
                .filter(|(_, ts)| now.saturating_sub(**ts) > ttl)
                .map(|(host, _)| host.clone())
                .collect()
        };

        if stale.is_empty() {
            return Ok(());
        }

        let mut conns = self.connections.write().await;
        let mut timestamps = self.last_used.write().await;

        for host in &stale {
            conns.remove(host);
            timestamps.remove(host);

            self.log_mgr.write(LogLevel::Info, || {
                format!("Evicted idle replication connection: {}", host)
            });
        }

        Ok(())
    }

    // update last-used timestamp for a host.
    async fn touch(&self, host: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.last_used.write().await.insert(host.to_string(), now);
    }
}

fn status_err_connecting(host: &str, port: u16) -> Status {
    let msg = format!("Error connecting to data node at {}:{}", host, port);
    Status::internal(msg)
}

fn status_err_unknown_node(id: &str) -> Status {
    let log = format!("Unknown data node: {}", id);
    Status::invalid_argument(log)
}
