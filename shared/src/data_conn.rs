use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::Mutex;
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

use super::logging::LogManager;
use super::node::GenericNode;
use super::proto::data_node_client::DataNodeClient;
use super::proto::{
    DataReadRequest, DataReadResponse, DataWriteRequest, DataWriteResponse,
};
use super::result::{Result, ServiceResult};

type Client = DataNodeClient<Channel>;

/**
 * Represents a connection to a data node.
 *
 * Manages gRPC client connection and provides methods
 * for reading and writing data blocks.
 *
 *  @field id - Unique identifier for the data node.
 *  @field host - Hostname or IP address of the data node.
 *  @field port - Port number of the data node.
 *  @field client_ref - Mutex-wrapped gRPC client. Lazily initialized.
 */
#[derive(Debug)]
pub struct DataNodeConn {
    id: String,
    host: String,
    port: u16,
    client_ref: Mutex<Option<Client>>,
}

impl DataNodeConn {
    /**
     * Creates a new [DataNodeConn] instance.
     *
     *  @param id - Unique identifier for the data node.
     *  @param host - Hostname or IP address of the data node.
     *  @param port - Port number of the data node.
     *  @return DataNodeConn - Initialized data node connection.
     */
    pub fn new(id: String, host: String, port: u16) -> Self {
        DataNodeConn {
            id,
            host,
            port,
            client_ref: Mutex::new(None),
        }
    }

    /**
     * Writes a block of data to the connected data node. Inits connection to data node if
     * not already connected. Locks access to client during write.
     *
     *  @param request - DataWriteRequest containing block ID, data, and replica node IDs.
     *  @return ServiceResult<DataWriteResponse> - Response indicating success or failure.
     */
    pub async fn write(
        &self,
        request: DataWriteRequest,
    ) -> ServiceResult<DataWriteResponse> {
        self.init_conn().await?;

        Ok(self
            .client_ref
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| status_err_writing(&self.id))?
            .write(request)
            .await
            .map_err(|_| status_err_writing(&self.id))?
            .into_inner())
    }

    /**
     * Reads a block of data from the connected data node. Inits connection to data node if
     * not already connected. Locks access to client during read.
     *
     *  @param request - DataReadRequest containing block ID.
     *  @return ServiceResult<DataReadResponse> - Response containing the requested data or error.
     */
    pub async fn read(
        &self,
        request: DataReadRequest,
    ) -> ServiceResult<DataReadResponse> {
        self.init_conn().await?;

        Ok(self
            .client_ref
            .lock()
            .await
            .as_mut()
            .ok_or_else(|| status_err_reading(&self.id))?
            .read(request)
            .await
            .map_err(|_| status_err_reading(&self.id))?
            .into_inner())
    }

    /**
     * Initializes the gRPC client connection to the data node if not already connected.
     * Locks access to client during initialization.
     *
     *  @return ServiceResult<()> - Result indicating success or failure of connection init.
     */
    async fn init_conn(&self) -> ServiceResult<()> {
        let mut write_ref = self.client_ref.lock().await;

        if write_ref.is_some() {
            return Ok(());
        }

        *write_ref = Some(
            DataNodeClient::connect(Into::<ServiceResult<Endpoint>>::into(
                self,
            )?)
            .await
            .map_err(|_| status_err_connecting(&self.id))?,
        );

        Ok(())
    }
}

// Conversions for DataNodeConn

impl From<&DataNodeConn> for ServiceResult<Endpoint> {
    fn from(node: &DataNodeConn) -> Self {
        let socket_addr = Into::<Result<SocketAddr>>::into(node)
            .map_err(|_| status_invalid_addr(&node.id))?;
        let endpoint = format!("http://{}", socket_addr);

        Endpoint::from_shared(endpoint)
            .map_err(|_| status_invalid_addr(&node.id))
    }
}

impl From<&DataNodeConn> for Result<SocketAddr> {
    fn from(node: &DataNodeConn) -> Self {
        Into::<Result<SocketAddr>>::into(&GenericNode {
            host: node.host.clone(),
            port: node.port,
        })
    }
}

/**
 * Manages data node connections for RustDFS.
 * Provides access to [DataNodeConn] instances by node ID.
 *
 *  @field connections - HashMap of node ID to DataNodeConn.
 *  @field log_mgr - LogManager for logging operations.
 */
#[derive(Debug)]
pub struct DataNodeManager {
    connections: HashMap<String, DataNodeConn>,
    log_mgr: LogManager,
}

impl DataNodeManager {
    /**
     * Creates a new [DataNodeManager] instance.
     *
     *  @param connections - HashMap of node ID to DataNodeConn.
     *  @param log_mgr - LogManager for logging operations.
     *  @return DataNodeManager - Initialized data node manager.
     */
    pub fn new(
        connections: HashMap<String, DataNodeConn>,
        log_mgr: LogManager,
    ) -> Self {
        DataNodeManager {
            connections,
            log_mgr,
        }
    }

    /**
     * Retrieves a list of all data node IDs managed by this instance.
     *
     *  @return Vec<&String> - Vector of references to node ID strings.
     */
    pub fn get_node_ids(&self) -> Vec<&String> {
        self.connections.keys().collect()
    }

    /**
     * Retrieves a connection to a data node by its ID.
     *
     *  @param id - The ID of the data node.
     *  @return ServiceResult<&DataNodeConn> - Result containing a reference to the data node connection or an error.
     */
    pub fn get_conn(&self, id: &str) -> ServiceResult<&DataNodeConn> {
        self.connections.get(id).ok_or_else(|| {
            let err = status_err_unknown_node(id);
            self.log_mgr.write_status(&err);
            err
        })
    }
}

// Error status helpers

fn status_err_writing(id: &str) -> Status {
    let msg = format!("Error writing to data node: {}", id);
    Status::internal(msg)
}

fn status_err_reading(id: &str) -> Status {
    let msg = format!("Error reading from data node: {}", id);
    Status::internal(msg)
}

fn status_err_connecting(id: &str) -> Status {
    let msg = format!("Error connecting to data node: {}", id);
    Status::internal(msg)
}

fn status_invalid_addr(id: &str) -> Status {
    let msg = format!("Invalid data node address: {}", id);
    Status::internal(msg)
}

fn status_err_unknown_node(id: &str) -> Status {
    let log = format!("Unknown data node: {}", id);
    Status::invalid_argument(log)
}
