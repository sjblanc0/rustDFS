use std::collections::HashMap;
use std::pin::Pin;
use rand::seq::SliceRandom;
use futures::future::join_all;
use uuid::Uuid;

use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use super::name_mgr::NameManager;
use super::proto::name_node_server::NameNode;
use super::proto::{NameWriteRequest, NameWriteResponse, NameReadRequest, NameReadResponse};

use rustdfs_shared::base::error::RustDFSError;
use rustdfs_shared::base::logging::{LogManager, LogLevel};
use rustdfs_shared::base::result::{Result, ServiceResult};
use rustdfs_shared::base::config::RustDFSConfig;
use rustdfs_shared::base::args::RustDFSArgs;
use rustdfs_shared::base::node::{GenericNode, Node};

use rustdfs_shared::data_node::proto::{DataWriteRequest, DataWriteResponse};
use rustdfs_shared::data_node::conn::DataNodeConn;

type ReadStream = Pin<Box<dyn Stream<Item = ServiceResult<NameReadResponse>> + Send>>;

#[derive(Debug)]
pub struct NameNodeService {
    id: String,
    replica_ct: u32,
    name_mgr: NameManager,
    data_nodes: HashMap<String, DataNodeConn>,
    log_mgr: LogManager,
}

#[tonic::async_trait]
impl NameNode for NameNodeService {

    type ReadStream = ReadStream;

    async fn write(
        &self,
        request: Request<Streaming<NameWriteRequest>>,
    ) -> ServiceResult<Response<NameWriteResponse>> {
        let node_ids = self.select_nodes()?;
        let prim = node_ids[0].clone();
        let repls = node_ids[1..].to_vec();

        let primary = self.data_nodes
            .get(&prim)
            .ok_or_else(|| {
                let log = format!("Bad primary node ID: {}", prim).to_string();
                self.log_mgr.write(LogLevel::Error, || log.clone());
                Status::invalid_argument(log)
            })?;

        let mut name = None;
        let mut blocks = Vec::new();
        let mut stream = request.into_inner();
        let mut writes = Vec::new();
        let mut err_req = None;

        while let Some(req) = stream.next().await {
            match req {
                Ok(req) => {
                    if name.is_none() {
                        name = Some(req.file_name.clone());
                    }

                    let id = Uuid::new_v4().to_string();

                    blocks.push(id.clone());
                    writes.push( 
                        primary.write(
                            DataWriteRequest {
                                block_id: id,
                                data: req.data,
                                replica_node_ids: repls.clone(),
                            }
                        )
                    );
                }
                Err(e) => {
                    err_req = Some(e);
                    break;
                }
            }
        }
        
        if err_req.is_some() {
            let unwrapped = err_req.as_ref().unwrap();
            let log = format!("Error in request stream: {}", unwrapped.message());
            self.log_mgr.write(LogLevel::Error, || log.clone());
            return Err(unwrapped.clone());
        }

        let failed = join_all(writes)
            .await
            .into_iter()
            .enumerate()
            .filter(|(_, r)| r.is_err() || r.as_ref().unwrap().success == false)
            .map(|(i, _)| blocks[i].clone())
            .collect::<Vec<_>>();

        if failed.len() > 0 {
            let blocks_str = failed.join(",");
            let log = format!("Error writing to data node (blocks = {}) (node = {})", blocks_str, prim);
            self.log_mgr.write(LogLevel::Error, || log.clone());
            return Err(Status::internal(log));
        }

        self.name_mgr
            .add_file(&name.unwrap(), blocks, repls)
            .await;

        Ok(Response::new(NameWriteResponse { success: true }))
    }

    async fn read(
        &self,
        request: Request<NameReadRequest>,
    ) -> ServiceResult<Response<ReadStream>> {
        //let (tx, rx) = mpsc::channel(128);
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
            replica_ct: config.replica_count,
            name_mgr: NameManager::new(), // TODO: handle init
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
    ) -> ServiceResult<Vec<String>> {
        let mut keys: Vec<&String> = self.data_nodes.keys().collect();

        if keys.is_empty() {
            return Err(status_misconfigured_svc());
        }

        keys.shuffle(&mut rand::rng());

        let replica_ct = (self.replica_ct as usize)
            .min(keys.len() - 1);

        Ok(
            keys[0..=replica_ct]
                .iter()
                .map(|k| k.to_string())
                .collect()
        )
    }
}

fn status_misconfigured_svc() -> Status {
    Status::internal("Misconfigured Name Node service")
}
