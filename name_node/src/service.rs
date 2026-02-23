use std::collections::HashMap;
use std::pin::Pin;
use uuid::Uuid;
use rand::seq::SliceRandom;

use tokio_stream::{Stream, StreamExt};
use tokio::sync::{mpsc, RwLock};
use tonic::{Request, Response, Streaming};

use super::proto::name_node_server::NameNode;
use super::proto::{NameWriteRequest, NameWriteResponse, NameReadRequest, NameReadResponse};

use rustdfs_shared::base::error::RustDFSError;
use rustdfs_shared::base::logging::LogManager;
use rustdfs_shared::base::result::{Result, ServiceResult};
use rustdfs_shared::base::config::RustDFSConfig;
use rustdfs_shared::base::args::RustDFSArgs;
use rustdfs_shared::base::node::{GenericNode, Node};

use rustdfs_shared::data_node::conn::DataNodeConn;
use rustdfs_shared::data_node::proto::data_node_client::DataNodeClient;
use rustdfs_shared::data_node::proto::DataWriteRequest;

type ReadStream = Pin<Box<dyn Stream<Item = ServiceResult<NameReadResponse>> + Send>>;

#[derive(Debug)]
pub struct NameNodeService {
    id: String,
    replica_count: u32,
    files: HashMap<String, Vec<String>>,
    data_nodes: HashMap<String, DataNodeConn>,
    log_mgr: LogManager,
}

#[tonic::async_trait]
impl NameNode for NameNodeService {

    async fn write(
        &self,
        request: Request<Streaming<NameWriteRequest>>,
    ) -> ServiceResult<Response<NameWriteResponse>> {
        let mut name: Option<String> = None;
        let mut stream = request.into_inner();
        /* 
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            while let Some(req) = stream.next().await {
                match req {
                    Ok(req) => {
                        if name.is_none() {
                            name = Some(req.file_name.clone());
                            self.files.insert(name.clone().unwrap(), Vec::new());
                        }

                        let id = Uuid::new_v4().to_string();

                        let nodes = self.select_nodes()
                            .map_err(|e| {
                                //todo
                            });
                        


                        todo!()
                    }
                    Err(e) => {
                        todo!()
                    }
                }
            }
        });
        */

        todo!()
    }

    type ReadStream = ReadStream;

    async fn read(
        &self,
        request: Request<NameReadRequest>,
    ) -> ServiceResult<Response<Self::ReadStream>> {
        todo!()
    }
}

impl NameNodeService {

    pub fn new(
        args: RustDFSArgs,
        config: RustDFSConfig,
    ) -> Result<Self> {
        let mut id: Option<String> = None;
        let mut log_file: Option<String> = None;
        let mut data_nodes: HashMap<String, DataNodeConn> = HashMap::new();
        
        let self_node = GenericNode {
            host: args.host.clone(),
            port: args.port,
        };

        for (k, nn_config) in config.name_nodes {
            let candidate = GenericNode {
                host: args.host.clone(),
                port: args.port,
            };

            if candidate.to_socket_addr()? == self_node.to_socket_addr()? {
                id = Some(k);
                log_file = Some(nn_config.log_file);
            }
        }

        if id.is_none() || log_file.is_none() {
            return Err(RustDFSError::err_misconfigured_svc_data());
        }

        for (k, dn_config) in config.data_nodes {
            data_nodes.insert(k, DataNodeConn::new(
                id.clone().unwrap(),
                dn_config.host,
                dn_config.port,
            ));
        }

        Ok(NameNodeService {
            id: id.unwrap(),
            replica_count: config.replica_count,
            files: HashMap::new(), // TODO: handle init
            data_nodes,
            log_mgr: LogManager::new(
                log_file.unwrap(),
                args.log_level,
                args.silent,
            )?,
        })
    }

    // randomly selects a primary data node and replica nodes
    fn select_nodes(
        &self
    ) -> Result<(String, Vec<String>)> {
        let mut keys: Vec<&String> = self.data_nodes.keys().collect();

        if keys.is_empty() {
            return Err(RustDFSError::err_misconfigured_svc_name());
        }

        keys.shuffle(&mut rand::rng());

        let replica_count = (self.replica_count as usize)
            .min(keys.len() - 1);

        let replicas = keys[1..=replica_count]
            .iter()
            .map(|k| k.to_string()).collect();

        Ok((keys[0].clone(), replicas))
    }
}
