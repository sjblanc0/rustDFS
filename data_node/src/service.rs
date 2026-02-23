use std::collections::HashMap;
use futures::future::{join, join_all};

use tonic::{Request, Response, Status};
use tonic::transport::{Server, Channel};
use tonic_reflection::server::Builder;

use super::data_mgr::DataDirManager;

use rustdfs_shared::base::error::RustDFSError;
use rustdfs_shared::base::result::{Result, ServiceResult};
use rustdfs_shared::base::config::RustDFSConfig;
use rustdfs_shared::base::logging::{LogManager, LogLevel};
use rustdfs_shared::base::args::RustDFSArgs;
use rustdfs_shared::base::node::{GenericNode, Node};

use rustdfs_shared::data_node::conn::DataNodeConn;
use rustdfs_shared::data_node::proto::data_node_server::{DataNode, DataNodeServer};
use rustdfs_shared::data_node::proto::{DataWriteRequest, DataWriteResponse, DataReadRequest, DataReadResponse, DataPing};
use rustdfs_shared::data_node::proto::DATA_NODE_FILE_DESCRIPTOR_SET;

#[derive(Debug)]
pub struct DataNodeService {
    id: String,
    data_nodes: HashMap<String, DataNodeConn>,
    data_mgr: DataDirManager,
    log_mgr: LogManager,
}

#[tonic::async_trait]
impl DataNode for DataNodeService {

    async fn write(
        &self,
        request: Request<DataWriteRequest>,
    ) -> ServiceResult<Response<DataWriteResponse>> {
        let request_ref = request.get_ref();
        let mut repls = Vec::new();
        let mut ids = Vec::new();

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
            
            ids.push(
                id.clone()
            );

            repls.push(
                repl_node.write(
                    DataWriteRequest {
                        block_id: request_ref.block_id.clone(),
                        data: request_ref.data.clone(),
                        replica_node_ids: vec![],
                    }
                )
            );
        }

        let failed = join_all(repls)
            .await
            .into_iter()
            .enumerate()
            .filter(|(i, r)| r.is_err() || r.as_ref().unwrap().success == false)
            .map(|(i, _)| ids[i].clone())
            .collect::<Vec<_>>();

        if failed.len() > 0 {
            let ids_str = failed.join(",");
            let log = format!("Error forwarding write to replica (block = {}) (nodes = {})", request_ref.block_id, ids_str);
            self.log_mgr.write(LogLevel::Error, || log.clone());
            return Err(Status::internal(log));
        }

        self.log_mgr.write(
            LogLevel::Info, 
            || format!("Wrote block {} with {} bytes", request_ref.block_id, request_ref.data.len())
        );

        Ok(Response::new(DataWriteResponse { success: true }))
    }

    async fn read(
        &self,
        request: Request<DataReadRequest>,
    ) -> ServiceResult<Response<DataReadResponse>> {
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

        Ok(Response::new(DataReadResponse { data: data }))
    }

    async fn ping(
        &self,
        request: Request<DataPing>,
    ) -> ServiceResult<Response<DataPing>> {
        self.log_mgr.write(
            LogLevel::Info, 
            || format!("Received ping from node {}", request.get_ref().node_id)
        );

        let reply = DataPing {
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

        let self_node = GenericNode {
            host: args.host,
            port: args.port,
        };

        for (k, dn_config) in config.data_nodes {
            data_nodes.insert(k.clone(), DataNodeConn::new(
                k.clone(),
                dn_config.host,
                dn_config.port,
            ));

            if data_nodes.get(&k).unwrap().to_socket_addr()? == self_node.to_socket_addr()? {
                id = Some(k);
                data_dir = Some(dn_config.data_dir);
                log_file = Some(dn_config.log_file);
            }
        }

        if id.is_none() || data_dir.is_none() || log_file.is_none() {
            return Err(RustDFSError::err_misconfigured_svc_data());
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
            .register_encoded_file_descriptor_set(DATA_NODE_FILE_DESCRIPTOR_SET)
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
            .map_err(|e| { RustDFSError::err_serving_data(e) })?;

        Ok(())
    }
}
