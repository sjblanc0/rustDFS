use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use rand::seq::SliceRandom;
use futures::future::join_all;
use uuid::Uuid;

use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tonic::transport::Server;
use tonic_reflection::server::Builder;

use rustdfs_shared::base::error::RustDFSError;
use rustdfs_shared::base::logging::{LogManager, LogLevel};
use rustdfs_shared::base::result::{Result, ServiceResult};
use rustdfs_shared::base::config::RustDFSConfig;
use rustdfs_shared::base::args::RustDFSArgs;
use rustdfs_shared::base::node::{GenericNode, Node};
use rustdfs_shared::data_node::proto::{DataReadRequest, DataWriteRequest};
use rustdfs_shared::data_node::conn::DataNodeConn;
use rustdfs_shared::data_node::mgr::DataNodeManager;

use crate::name_mgr::BlockDescriptor;
use crate::name_mgr::NameManager;
use crate::proto::name_node_server::NameNode;
use crate::proto::{NameWriteRequest, NameWriteResponse, NameReadRequest, NameReadResponse};
use crate::proto::name_node_server::NameNodeServer;
use crate::proto::NAME_FILE_DESCRIPTOR_SET;

type ReadStream = Pin<Box<dyn Stream<Item = ServiceResult<NameReadResponse>> + Send>>;

#[derive(Debug)]
pub struct NameNodeService {
    id: String,
    self_node: GenericNode,
    replica_ct: u32,
    name_mgr: NameManager,
    data_nodes: Arc<DataNodeManager>,
    log_mgr: Arc<LogManager>,
}

#[tonic::async_trait]
impl NameNode for NameNodeService {

    type ReadStream = ReadStream;

    async fn write(
        &self,
        request: Request<Streaming<NameWriteRequest>>,
    ) -> ServiceResult<Response<NameWriteResponse>> {
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

                    let block_id = Uuid::new_v4().to_string();
                    let node_ids = self.select_nodes()?;
                    let prim = node_ids[0].clone();
                    let repls = node_ids[1..].to_vec();

                    blocks.push(
                        BlockDescriptor {
                            id: block_id.clone(),
                            node_ids: node_ids.clone(),
                        }
                    );

                    writes.push( 
                        self.data_nodes
                            .get_conn(&prim)?
                            .write(
                                DataWriteRequest {
                                    block_id: block_id,
                                    data: req.data,
                                    replica_node_ids: repls,
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
            .filter(|(_, r)| {
                r.is_err() || r.as_ref().unwrap().success == false
            })
            .map(|(i, _)| {
                (blocks[i].id.clone(), blocks[i].node_ids[0].clone())
            })
            .collect::<Vec<_>>();

        if failed.len() > 0 {
            for (block, node) in failed.iter() {
                let log = format!("Failed to write block (block = {}) (node = {})", block, node);
                self.log_mgr.write(LogLevel::Error, || log.clone());
            }

            return Err(status_err_writing(failed));
        }

        self.name_mgr
            .add_file(name.unwrap(), blocks)
            .await;

        Ok(
            Response::new(
                NameWriteResponse { 
                    success: true
                }
            )
        )
    }

    async fn read(
        &self,
        request: Request<NameReadRequest>,
    ) -> ServiceResult<Response<ReadStream>> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let out = ReceiverStream::new(rx);
        let mut tasks = Vec::new();

        let blocks = self.name_mgr
            .get_blocks(&req.file_name)
            .await?;

        for block in blocks.into_iter() {
            let nodes_ref = Arc::clone(&self.data_nodes);
            let logger_ref = Arc::clone(&self.log_mgr);

            tasks.push(async move {
                for id in block.node_ids.clone().into_iter() {
                    let res = nodes_ref
                        .get_conn(&id)?
                        .read(
                            DataReadRequest { 
                                block_id: block.id.clone() 
                            }
                        )
                        .await;

                    match res {
                        Ok(data) => {
                            logger_ref.write(
                                LogLevel::Info, 
                                || format!(
                                    "Read block from data node (block = {}, node = {})", 
                                    block.id, 
                                    id
                                )
                            );

                            return Ok(data);
                        }
                        Err(e) => {
                            logger_ref.write(
                                LogLevel::Error, 
                                || format!(
                                    "Error reading from data node {}: {}", 
                                    id, 
                                    e.message()
                                )
                            );
                        }
                    }
                }

                Err(
                    status_err_reading(
                        &block.id, 
                        block.node_ids
                    )
                )
            });
        }

        tokio::spawn(async move {
            for task in tasks.drain(..) {
                match task.await {
                    Ok(res) => {
                        let _ = tx.send(
                            Ok(
                                NameReadResponse {
                                    file_name: req.file_name.clone(),
                                    data: res.data,
                                }
                            )
                        ).await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
        });

        Ok(
            Response::new(
                Box::pin(out) as Self::ReadStream
            )
        )
    }
}

impl NameNodeService {

    pub fn new(
        args: RustDFSArgs,
        config: RustDFSConfig,
    ) -> Result<Self> {
        let mut log_file = None;
        let mut data_nodes = HashMap::new();
        let mut node = None;

        for (k, nn_config) in config.name_nodes {
            if k == args.id {
                log_file = Some(nn_config.log_file);
                node = Some(GenericNode {
                    host: nn_config.host,
                    port: nn_config.port,
                });
            }
        }

        if node.is_none() || log_file.is_none() {
            return Err(RustDFSError::err_misconfigured_svc_data());
        }

        for (k, dn_config) in config.data_nodes {
            data_nodes.insert(k.clone(), DataNodeConn::new(
                k,
                dn_config.host,
                dn_config.port,
            ));
        }

        Ok(
            NameNodeService {
                id: args.id,
                self_node: node.unwrap(),
                replica_ct: config.replica_count,
                name_mgr: NameManager::new(), // TODO: handle init
                data_nodes: Arc::new(
                    DataNodeManager::new(
                        data_nodes,
                    )
                ),
                log_mgr: Arc::new(
                    LogManager::new(
                        log_file.unwrap(),
                        args.log_level,
                        args.silent,
                    )?
                ),
            }
        )
    }

    pub async fn serve(
        self,
    ) -> Result<()> {
        let addr: std::net::SocketAddr = self.self_node.to_socket_addr()?;

        // should remove this or make it optional via config
        // only added this for testing
        let svc_reflection = Builder::configure()
            .register_encoded_file_descriptor_set(NAME_FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();

        self.log_mgr.write(
            LogLevel::Info, 
            || format!(
                "Starting NameNodeServer with ID {} at {} on port {}", 
                self.id, 
                addr.ip().to_string(), 
                addr.port()
            )
        );

        Server::builder()
            .add_service(svc_reflection)
            .add_service(NameNodeServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| { RustDFSError::err_serving_data(e) })?;

        Ok(())
    }

    // randomly selects a primary data node and replica nodes
    fn select_nodes(
        &self
    ) -> ServiceResult<Vec<String>> {
        let mut keys: Vec<&String> = self.data_nodes.get_node_ids();

        if keys.is_empty() {
            return Err(status_misconfigured_svc());
        }

        let replica_ct = (self.replica_ct as usize)
            .min(keys.len() - 1);

        let (selected, _) = keys.partial_shuffle(&mut rand::rng(), replica_ct + 1);

        Ok(
            selected
                .iter()
                .map(|k| k.to_string())
                .collect()
        )
    }
}

fn status_misconfigured_svc() -> Status {
    Status::internal("Misconfigured Name Node service")
}

fn status_err_writing(
    desc: Vec<(String, String)>,
) -> Status {
    let desc_str = desc
        .into_iter()
        .map(|(b, n)| format!("(block = {}, node = {})", b, n))
        .collect::<Vec<_>>()
        .join(", ");
    let msg = format!("Error writing to data nodes: {}", desc_str);
    Status::internal(msg)
}

fn status_err_reading(
    block_id: &str,
    node_ids: Vec<String>,
) -> Status {
    let nodes_str = node_ids.join(",");
    let msg = format!("Failed to read block (block = {}) (nodes = {})", block_id, nodes_str);
    Status::internal(msg)
}
