use futures::future::FutureExt;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::Streaming;
use tonic::transport::Channel;
use tonic::{IntoStreamingRequest, Status};

use crate::host::HostAddr;
use crate::logging::{LogLevel, LogManager};
use crate::result::ServiceResult;
use rustdfs_proto::data::data_node_client::DataNodeClient;
use rustdfs_proto::data::{ReadRequest, ReadResponse, WriteRequest};

/**
 * Manages gRPC client connections to data nodes.
 * Provides thread-safe access to [DataNodeConn] instances by hostname.
 *  => connections map guarded by [RwLock] for concurrent reads / writes.
 *
 *  @field connections - HashMap of hostname to [DataNodeConn].
 *  @field log_mgr - Owned [LogManager] for logging connection events.
 */
#[derive(Debug)]
pub struct DataNodeManager {
    connections: RwLock<HashMap<String, DataNodeConn>>,
    log_mgr: LogManager,
}

/**
 * Represents a gRPC client connection to a single data node.
 * Cloneable — the underlying [DataNodeClient] uses a shared channel.
 *
 *  @field host - [HostAddr] of the connected data node.
 *  @field client - gRPC [DataNodeClient] for issuing read/write RPCs.
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
    pub fn new(log_mgr: LogManager) -> Self {
        DataNodeManager {
            connections: RwLock::new(HashMap::new()),
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
     * Retrieves a cloned data node connection by its hostname.
     *
     *  @param host - Hostname of the data node.
     *  @return ServiceResult<DataNodeConn> - Cloned connection or an error if unknown.
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
     * Sends a streaming write request to the connected data node.
     * Returns a response stream of per-chunk acknowledgements.
     *
     *  @param request - Streaming [WriteRequest] messages (block ID, data, replica info).
     *  @return ServiceResult<Streaming<()>> - Stream of acknowledgements or error.
     */
    pub async fn write(
        self,
        request: impl IntoStreamingRequest<Message = WriteRequest>,
    ) -> ServiceResult<Streaming<()>> {
        let stream = self
            .client
            .clone()
            .write(request)
            .map(|res| match res {
                Ok(response) => Ok(response.into_inner()),
                Err(e) => Err(e),
            })
            .await?;

        Ok(stream)
    }

    /**
     * Sends a read request and returns a response stream of data chunks.
     *
     *  @param request - [ReadRequest] containing block ID and byte offset.
     *  @return ServiceResult<Streaming<ReadResponse>> - Stream of data chunks or error.
     */
    pub async fn read(self, request: ReadRequest) -> ServiceResult<Streaming<ReadResponse>> {
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
