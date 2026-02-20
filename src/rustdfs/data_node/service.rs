use std::fs;
use std::collections::HashMap;
use tokio::sync::RwLock;
use futures::future::join_all;

use tonic::{Request, Response, Status};
use tonic::transport::{Server, Channel};
use tonic_reflection::server::Builder;

use super::data_mgr::DataDirManager;
use super::proto::data_node_server::{DataNode, DataNodeServer};
use super::proto::data_node_client::DataNodeClient;
use super::proto::{WriteRequest, WriteResponse, ReadRequest, ReadResponse, PingMessage};
use super::proto::FILE_DESCRIPTOR_SET;

use crate::rustdfs::shared::node::{self, Node};
use crate::rustdfs::shared::error::RustDFSError;
use crate::rustdfs::shared::result::{Result, ServiceResult};
use crate::rustdfs::shared::config::RustDFSConfig;
use crate::rustdfs::shared::logging::{LogManager, LogLevel};
use crate::rustdfs::shared::args::RustDFSArgs;

type DataNodeConn = Node<DataNodeClient<Channel>>;

#[derive(Debug)]
pub struct DataNodeService {
    pub id: String,
    pub data_nodes: HashMap<String, DataNodeConn>,
    pub data_mgr: DataDirManager,
    pub log_mgr: LogManager,
}

#[tonic::async_trait]
impl DataNode for DataNodeService {

    async fn write_data(
        &self,
        request: Request<WriteRequest>,
    ) -> ServiceResult<Response<WriteResponse>> {
        let request_ref = request.get_ref();
        let mut repls = Vec::new();

        self.data_mgr.write_block(&request_ref.block_id, &request_ref.data)
            .map_err(|e| {
                self.log_mgr.write(LogLevel::Error, || e.message.clone());
                Status::invalid_argument(e.message.clone())
            })?;

        for id in request_ref.replica_node_ids.iter() {
            if id == &self.id {
                continue;
            }

            let repl_node = self.data_nodes.get(id)
                .ok_or_else(|| { 
                    let log = format!("Bad replica node ID: {}", id).to_string();
                    self.log_mgr.write(LogLevel::Error, || log.clone());
                    Status::invalid_argument(log)
                })?;

            repls.push(
                self.fwd_write(
                    repl_node,
                    WriteRequest {
                        block_id: request_ref.block_id.clone(),
                        data: request_ref.data.clone(),
                        replica_node_ids: vec![],
                    }
                )
            );
        }

        for res in join_all(repls).await {
            if res.is_err() {
                let log = format!("Error forwarding write to replica: {}", res.err().unwrap().message);
                self.log_mgr.write(LogLevel::Error, || log.clone());
                return Err(Status::internal(log));
            }
        }

        self.log_mgr.write(
            LogLevel::Info, 
            || format!("Wrote block {} with {} bytes", request_ref.block_id, request_ref.data.len())
        );

        Ok(Response::new(WriteResponse { success: true }))
    }

    async fn read_data(
        &self,
        request: Request<ReadRequest>,
    ) -> ServiceResult<Response<ReadResponse>> {
        let request_ref = request.get_ref();
        let data: Vec<u8> = self.data_mgr.read_block(&request_ref.block_id)
            .map_err(|_| {
                let log = format!("Block not found: {}", request_ref.block_id);
                self.log_mgr.write(LogLevel::Error, || log.clone());
                Status::not_found(log)
            })?;
        
        self.log_mgr.write(
            LogLevel::Info, 
            || format!("Read block {} with {} bytes", request_ref.block_id, data.len())
        );

        Ok(Response::new(ReadResponse { data: data }))
    }

    async fn ping(
        &self,
        request: Request<PingMessage>,
    ) -> ServiceResult<Response<PingMessage>> {
        self.log_mgr.write(
            LogLevel::Info, 
            || format!("Received ping from node {}", request.get_ref().node_id)
        );

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
        args: RustDFSArgs,
        config: RustDFSConfig,
    ) -> Result<Self> {
        let mut id: Option<String> = None;
        let mut data_dir: Option<String> = None;
        let mut data_nodes: HashMap<String, DataNodeConn> = HashMap::new();
        let mut log_file: Option<String> = None;

        let self_node = DataNodeConn {
            host: args.host,
            port: args.port,
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
                data_dir = Some(dn_config.data_dir);
                log_file = Some(dn_config.log_file);
            }
        }

        if id.is_none() || data_dir.is_none() || log_file.is_none() {
            return Err(RustDFSError::err_misconfigured_svc());
        }

        Ok(DataNodeService {
            id: id.unwrap(),
            data_nodes: data_nodes,
            data_mgr: DataDirManager::new(
                &data_dir.unwrap()
            )?,
            log_mgr: LogManager::new(
                log_file.unwrap(),
                args.log_level,
                args.silent,
            )?,
        })
    }

    pub async fn serve(
        self,
    ) -> Result<()> {
        let self_node = self.data_nodes.get(&self.id).unwrap();
        let addr: std::net::SocketAddr = self_node.to_socket_addr()?;

        // should remove this or make it optional via config
        // only added this for testing
        let svc_reflection = Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();

        self.log_mgr.write(
            LogLevel::Info, 
            || format!(
                "Starting DataNodeServer with ID {} at {} on port {}", 
                self.id, 
                addr.ip().to_string(), 
                addr.port()
            )
        );

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
        request: WriteRequest,
    ) -> Result<()> {
        self.init_data_conn(node).await?;

        node.client_ref
            .write()
            .await
            .as_mut()
            .ok_or_else(|| RustDFSError::err_misconfigured_svc())?
            .write_data(request)
            .await
            .map_err(|e| RustDFSError::err_forwarding(e))?;

        Ok(())
    }

    async fn init_data_conn(
        &self,
        node: &DataNodeConn,
    ) -> Result<()> {
        let client_opt = node.client_ref
            .read()
            .await;

        if client_opt.is_some() {
            return Ok(());
        }

        let mut write_ref = node.client_ref
            .write()
            .await;

        *write_ref = Some(
            DataNodeClient::connect(node.to_endpoint()?)
                .await
                .map_err(|e| RustDFSError::err_serving(e))?
        );

        Ok(())
    }
}
