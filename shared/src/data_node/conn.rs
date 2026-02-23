use std::net::{SocketAddr, ToSocketAddrs};
use tokio::sync::RwLock;
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

use crate::base::error::{RustDFSError, Kind};
use crate::base::node::{GenericNode, Node};
use crate::base::result::{Result, ServiceResult};

use super::proto::data_node_client::DataNodeClient;
use super::proto::{DataWriteRequest, DataWriteResponse, DataReadRequest, DataReadResponse, DataPing};

type Client = DataNodeClient<Channel>;

#[derive(Debug)]
pub struct DataNodeConn {
    id: String,
    host: String,
    port: u16,
    client_ref: RwLock<Option<Client>>,
}

impl DataNodeConn {

    pub fn new(
        id: String,
        host: String,
        port: u16,
    ) -> Self {
        DataNodeConn {
            id: id,
            host: host,
            port: port,
            client_ref: RwLock::new(None),
        }
    }

    pub async fn write(
        &self, 
        request: DataWriteRequest,
    ) -> ServiceResult<DataWriteResponse> {
        self.init_conn().await?;

        Ok(
            self.client_ref
                .write()
                .await
                .as_mut()
                .ok_or_else(|| status_err_writing(self))?
                .write(request)
                .await
                .map_err(|_| status_err_writing(self))?
                .into_inner()
        )
    }

    pub async fn read(
        &self, 
        request: DataReadRequest,
    ) -> ServiceResult<DataReadResponse> {
        self.init_conn().await?;

        Ok(
            self.client_ref
                .write()
                .await
                .as_mut()
                .ok_or_else(|| status_err_reading(self))?
                .read(request)
                .await
                .map_err(|_| status_err_reading(self))?
                .into_inner()
        )
    }

    pub async fn ping(
        &self, 
        request: DataPing,
    ) -> ServiceResult<DataPing> {
        self.init_conn().await?;

        Ok(
            self.client_ref
                .write()
                .await
                .as_mut()
                .ok_or_else(|| status_err_ping(self))?
                .ping(request)
                .await
                .map_err(|_| status_err_ping(self))?
                .into_inner()
        )
    }

    async fn init_conn(
        &self,
    ) -> ServiceResult<()> {
        let client_opt = self.client_ref
            .read()
            .await;

        if client_opt.is_some() {
            return Ok(());
        }

        let mut write_ref = self.client_ref
            .write()
            .await;

        *write_ref = Some(
            DataNodeClient::connect(self.to_endpoint()?)
                .await
                .map_err(|_| status_err_connecting(self))?
        );

        Ok(())
    }

    pub fn to_endpoint(
        &self
    ) -> ServiceResult<Endpoint> {
        let socket_addr = self.to_socket_addr()
                .map_err(|_| status_invalid_addr(self))?;
        let endpoint = format!("http://{}", socket_addr);

        Ok(
            Endpoint::from_shared(endpoint)
                .map_err(|_| status_invalid_addr(self))?
        )
    }
}

impl Node for DataNodeConn {

    fn to_socket_addr(
        &self
    ) -> Result<SocketAddr> {
        GenericNode {
            host: self.host.clone(),
            port: self.port,
        }.to_socket_addr()
    }
}

fn status_err_writing(
    node: &DataNodeConn
) -> Status {
    let msg = format!("Error writing to data node (id = {}) {}:{}", node.id, node.host, node.port);
    Status::internal(msg)
}

fn status_err_reading(
    node: &DataNodeConn
) -> Status {
    let msg = format!("Error reading from data node (id = {}) {}:{}", node.id, node.host, node.port);
    Status::internal(msg)
}

fn status_err_ping(
    node: &DataNodeConn
) -> Status {
    let msg = format!("Error pinging data node (id = {}) {}:{}", node.id, node.host, node.port);
    Status::internal(msg)
}

fn status_err_connecting(
    node: &DataNodeConn
) -> Status {
    let msg = format!("Error connecting to data node (id = {}) {}:{}", node.id, node.host, node.port);
    Status::internal(msg)
}

fn status_invalid_addr(
    node: &DataNodeConn
) -> Status {
    let msg = format!("Invalid data node address (id = {}) {}:{}", node.id, node.host, node.port);
    Status::internal(msg)
}
