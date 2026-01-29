use std::collections::HashMap;
use std::error::Error;

use tonic::{Request, Response, Status};
use tonic::transport::{Server, Error as TonicError};
use tonic_reflection::server::Builder;

use super::proto::data_node_server::{DataNode, DataNodeServer};
use super::proto::{WriteRequest, WriteResponse, ReadRequest, ReadResponse, PingMessage};
use super::proto::FILE_DESCRIPTOR_SET;

use crate::rustdfs::shared::node::Node;
use crate::rustdfs::shared::error::{RustDFSError, Kind};
use crate::rustdfs::shared::config::RustDFSConfig;

#[derive(Debug)]
pub struct DataNodeService {
    pub id: String,
    pub data_dir: String,
    pub name_nodes: HashMap<String, Node>,
    pub data_nodes: HashMap<String, Node>,
}

#[tonic::async_trait]
impl DataNode for DataNodeService {

    async fn write_data(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        println!("Got a write data request: {:?}", request);

        let reply = WriteResponse { success: true };

        Ok(Response::new(reply))
    }

    async fn read_data(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<ReadResponse>, Status> {
        println!("Got a read data request: {:?}", request);

        let reply = ReadResponse {
            data: vec![],
        };

        Ok(Response::new(reply))
    }

    async fn ping(
        &self,
        request: Request<PingMessage>,
    ) -> Result<Response<PingMessage>, Status> {
        println!("Got a ping: {:?}", request);

        let reply = PingMessage {
            node_id: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        };

        Ok(Response::new(reply))
    }
}

impl DataNodeService {

    pub fn new(config: RustDFSConfig) -> Result<Self, RustDFSError> {
        let mut id: Option<String> = None;
        let mut data_dir: Option<String> = None;
        let mut name_nodes: HashMap<String, Node> = HashMap::new();
        let mut data_nodes: HashMap<String, Node> = HashMap::new();

        let self_node = Node {
            host: config.self_config.host,
            port: config.self_config.port,
        };

        for (k, nn_config) in config.name_nodes {
            name_nodes.insert(k, Node {
                host: nn_config.host,
                port: nn_config.port,
            });
        }

        for (k, dn_config) in config.data_nodes {
            let node = Node {
                host: dn_config.host,
                port: dn_config.port,
            };

            data_nodes.insert(k.clone(), node.clone());

            if node.to_socket_addr()? == self_node.to_socket_addr()? {
                id = Some(k);
                data_dir = Some(dn_config.data_dir.clone());
            }
        }

        if id.is_none() || data_dir.is_none() {
            return Err(RustDFSError::err_misconfigured_svc());
        }

        Ok(DataNodeService {
            id: id.unwrap(),
            data_dir: data_dir.unwrap(),
            name_nodes: name_nodes,
            data_nodes: data_nodes,
        })
    }

    pub async fn serve(self) -> Result<(), RustDFSError> {
        let self_node = self.data_nodes.get(&self.id).unwrap();
        let addr: std::net::SocketAddr = self_node.to_socket_addr()?;

        // should remove this or make it optional via config
        // only added this for testing
        let svc_reflection = Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();

        println!("DataNodeServer listening at {} on port {}", addr.ip().to_string(), addr.port());

        Server::builder()
            .add_service(svc_reflection)
            .add_service(DataNodeServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| { RustDFSError::err_serving(e) })?;

        Ok(())
    }
}
