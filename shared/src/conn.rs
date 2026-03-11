use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::Status;
use tonic::transport::Channel;

use crate::host::HostAddr;
use crate::logging::{LogLevel, LogManager};
use crate::proto::data_node_client::DataNodeClient;
use crate::proto::{DataReadRequest, DataReadResponse, DataWriteRequest};
use crate::result::ServiceResult;

/**
 * Manages data node connections for RustDFS.
 * Provides access to [DataNodeConn] instances by node ID.
 *  => guarded by RwLock for concurrent reads / writes
 *
 *  @field connections - HashMap of host to DataNodeConn.
 *  @field log_mgr - LogManager for logging operations.
 */
#[derive(Debug)]
pub struct DataNodeManager {
    connections: RwLock<HashMap<String, DataNodeConn>>,
    log_mgr: LogManager,
}

/**
 * Represents a connection to a data node.
 *
 * Manages gRPC client connection and provides methods
 * for reading and writing data blocks.
 *
 *  @field host - [HostAddr] location of data node.
 *  @field client_ref - Mutex-wrapped gRPC client. Lazily initialized.
 */
#[derive(Debug, Clone)]
pub struct DataNodeConn {
    pub host: HostAddr,
    client: DataNodeClient<Channel>,
}

impl DataNodeManager {
    /**
     * Creates a new [DataNodeManager] instance.
     *
     *  @param log_mgr - [LogManager] for logging operations.
     *  @return DataNodeManager - Initialized data node manager.
     */
    pub fn new(log_mgr: &LogManager) -> Self {
        DataNodeManager {
            connections: RwLock::new(HashMap::new()),
            log_mgr: log_mgr.clone(),
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
            DataNodeConn {
                host: hostaddr,
                client: DataNodeClient::connect(endpoint).await.map_err(|_| {
                    let err = status_err_connecting(host, port);
                    self.log_mgr.write_status(&err);
                    err
                })?,
            },
        );

        Ok(())
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

    /**
     * Retrieves a data node connection by its host.
     *
     *  @param host - The host of the data node.
     *  @return ServiceResult<&DataNodeConn> - Result containing a reference to [DataNodeConn] or an error.
     */
    pub async fn get_conn(&self, host: &str) -> ServiceResult<DataNodeConn> {
        self.connections
            .read()
            .await
            .get(host)
            .ok_or_else(|| {
                let err = status_err_unknown_node(host);
                self.log_mgr.write_status(&err);
                err
            })
            .cloned()
    }

    /**
     * Checks if a data node connection exists by its host.
     *
     *  @param host - The host of the data node.
     *  @return bool - True if the connection exists, false otherwise.
     */
    pub async fn has_conn(&self, host: &str) -> bool {
        self.connections.read().await.contains_key(host)
    }
}

impl DataNodeConn {
    /**
     * Writes a block of data to the connected data node.
     *
     *  @param request - DataWriteRequest containing block ID, data, and replica node IDs.
     *  @return ServiceResult<()> - Result indicating success or failure.
     */
    pub async fn write(self, request: DataWriteRequest) -> ServiceResult<()> {
        self.client.clone().write(request).await?;

        Ok(())
    }

    /**
     * Reads a block of data from the connected data node.
     *
     *  @param request - DataReadRequest containing block ID.
     *  @return ServiceResult<DataReadResponse> - Result containing the read data or an error.
     */
    pub async fn read(self, request: DataReadRequest) -> ServiceResult<DataReadResponse> {
        let response = self.client.clone().read(request).await?;

        Ok(response.into_inner())
    }
}

// Error status helpers

fn status_err_connecting(host: &str, port: u16) -> Status {
    let msg = format!("Error connecting to data node at {}:{}", host, port);
    Status::internal(msg)
}

fn status_err_unknown_node(id: &str) -> Status {
    let log = format!("Unknown data node: {}", id);
    Status::invalid_argument(log)
}
