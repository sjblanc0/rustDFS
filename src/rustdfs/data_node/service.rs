use std::collections::HashMap;
use std::convert::identity;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::sync::RwLock;

use tonic::{Request, Response, Status, client};
use tonic::transport::{Server, Channel};
use tonic_reflection::server::Builder;

use super::data_dir::DataDir;
use super::proto::data_node_server::{DataNode, DataNodeServer};
use super::proto::data_node_client::DataNodeClient;
use super::proto::{WriteRequest, WriteResponse, ReadRequest, ReadResponse, PingMessage};
use super::proto::FILE_DESCRIPTOR_SET;

use crate::rustdfs::data_node::data_dir;
use crate::rustdfs::shared::node::{self, Node};
use crate::rustdfs::shared::error::{RustDFSError, Kind};
use crate::rustdfs::shared::config::{self, RustDFSConfig};

type DataNodeConn = Node<DataNodeClient<Channel>>;

#[derive(Debug)]
pub struct DataNodeService {
    pub id: String,
    pub data_dir: DataDir,
    pub data_nodes: HashMap<String, DataNodeConn>,
}

#[tonic::async_trait]
impl DataNode for DataNodeService {

    async fn write_data(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request_ref = request.get_ref();
        let path = format!("{}/{}", self.data_dir.safe_path_str, request_ref.block_id);
        let repls: Vec< = Vec::new()


        let write_res = File::create(path)?
            .write_all(&request_ref.data);
        
        if write_res.is_err() {
            let file_err = format!("Bad block ID: {}", request_ref.block_id).to_string();
            return Err(Status::invalid_argument(file_err));
        }

        for id in request_ref.replica_node_ids.iter() {
            if id == &self.id {
                continue;
            }

            let node = self.data_nodes.get(id)
                .ok_or_else(|| RustDFSError::err_misconfigured_svc())?;

            self.fwd_write(node, request.clone())
                .await?;
        }

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
            node_id: self.id.clone(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        };

        Ok(Response::new(reply))
    }
}

impl DataNodeService {

    pub fn new(
        config: RustDFSConfig
    ) -> Result<Self, RustDFSError> {
        let mut id: Option<String> = None;
        let mut data_dir: Option<DataDir> = None;
        let mut data_nodes: HashMap<String, DataNodeConn> = HashMap::new();

        let self_node = DataNodeConn {
            host: config.self_config.host,
            port: config.self_config.port,
            client_ref: RwLock::new(None),
        };

        for (k, dn_config) in config.data_nodes {
            data_nodes.insert(k.clone(), DataNodeConn {
                host: dn_config.host,
                port: dn_config.port,
                client_ref: RwLock::new(None),
            });

            if data_nodes.get(&k).unwrap().to_socket_addr()? == self_node.to_socket_addr()? {
                id = Some(k);
                data_dir = Some(DataDir::new(&dn_config.data_dir)?);
            }
        }

        if id.is_none() || data_dir.is_none() {
            return Err(RustDFSError::err_misconfigured_svc());
        }

        Ok(DataNodeService {
            id: id.unwrap(),
            data_dir: data_dir.unwrap(),
            data_nodes: data_nodes,
        })
    }

    pub async fn serve(
        self,
    ) -> Result<(), RustDFSError> {
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

    async fn fwd_write(
        &self, 
        node: &DataNodeConn, 
        request: Request<WriteRequest>,
    ) -> Result<(), RustDFSError> {
        self.init_client(node).await?;

        node.client_ref
            .write()
            .map_err(|e| { RustDFSError::err_client_lock(e) })?
            .as_mut()
            .ok_or_else(|| RustDFSError::err_misconfigured_svc())?
            .write_data(request)
            .await
            .map_err(|e| RustDFSError::err_forwarding(e))?;

        Ok(())
    }

    async fn init_client(
        &self,
        node: &DataNodeConn,
    ) -> Result<(), RustDFSError> {
        let client_opt = node.client_ref
            .read()
            .map_err(|e| RustDFSError::err_client_lock(e))?;

        if client_opt.is_some() {
            return Ok(());
        }

        let c = DataNodeClient::connect(format!("http://{}", node.to_socket_addr()?))
            .await
            .map_err(|e| RustDFSError::err_serving(e))?;
        let mut write_ref = node.client_ref
            .write()
            .map_err(|e| RustDFSError::err_client_lock(e))?;

        *write_ref = Some(c);
        Ok(())
    }
}
