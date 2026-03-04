use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use rand::seq::SliceRandom;
use futures::Future;
use futures::pin_mut;
use uuid::Uuid;
use mpsc::Sender;

use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tonic::transport::Server;
use tonic_reflection::server::Builder;
use tonic_health::server as health_server;

use rustdfs_shared::base::error::RustDFSError;
use rustdfs_shared::base::logging::{LogManager, LogLevel};
use rustdfs_shared::base::result::{Result, ServiceResult};
use rustdfs_shared::base::config::RustDFSConfig;
use rustdfs_shared::base::args::RustDFSArgs;
use rustdfs_shared::base::node::{GenericNode, Node};
use rustdfs_shared::data_node::proto::{DataReadRequest, DataReadResponse, DataWriteRequest, DataWriteResponse};
use rustdfs_shared::data_node::conn::DataNodeConn;
use rustdfs_shared::data_node::mgr::DataNodeManager;

use crate::name_mgr::BlockDescriptor;
use crate::name_mgr::NameManager;
use crate::proto::name_node_server::NameNode;
use crate::proto::{NameWriteRequest, NameWriteResponse, NameReadRequest, NameReadResponse};
use crate::proto::name_node_server::NameNodeServer;
use crate::proto::NAME_FILE_DESCRIPTOR_SET;

type ReadStream = Pin<Box<dyn Stream<Item = ServiceResult<NameReadResponse>> + Send>>;

const WRITE_BUF_SIZE: usize = 8;
const READ_BUF_SIZE: usize = 8;

#[derive(Debug)]
pub struct NameNodeService {
    id: String,
    self_node: GenericNode,
    replica_ct: u32,
    name_mgr: NameManager,
    data_nodes: Arc<DataNodeManager>,
    log_mgr: LogManager,
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

                    writes.push((
                        prim.clone(),
                        block_id.clone(),
                        self.data_nodes
                            .get_conn(&prim)?
                            .write(
                                DataWriteRequest {
                                    block_id: block_id,
                                    data: req.data,
                                    replica_node_ids: repls,
                                }
                            )
                    ));

                    if writes.len() >= WRITE_BUF_SIZE {
                        drain_writes(&self.log_mgr, &mut writes).await?;
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }

        if !writes.is_empty() {
            drain_writes(&self.log_mgr, &mut writes).await?;
        }

        self.name_mgr
            .add_file(name.clone().unwrap(), blocks)
            .await;

        self.log_mgr.write(
            LogLevel::Info, 
            || format!(
                "Finished writing file {}", 
                name.unwrap()
            )
        );

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
        let (mut tx, rx) = mpsc::channel(128);
        let out = ReceiverStream::new(rx);
        let node_mgr = Arc::clone(&self.data_nodes);
        let logger = self.log_mgr.clone();

        let blocks = self.name_mgr
            .get_blocks(&req.file_name)
            .await?
            .clone();

        tokio::spawn(async move {
            let mut tasks = Vec::new();

            for block in blocks.into_iter() {
                let node_mgr = Arc::clone(&node_mgr);
                let logger_clone = logger.clone();

                tasks.push((
                    req.file_name.clone(),
                    block.id.clone(),
                    async move {
                        let mut nodes = block.node_ids.clone();
                        nodes.shuffle(&mut rand::rng());

                        for id in block.node_ids.iter() {
                            let res = node_mgr
                                .get_conn(&id)?
                                .read(
                                    DataReadRequest { 
                                        block_id: block.id.clone() 
                                    }
                                )
                                .await;

                            match res {
                                Ok(data) => {
                                    logger_clone.write(
                                        LogLevel::Debug, 
                                        || format!(
                                            "Read block {} from data node {}", 
                                            block.id, 
                                            id
                                        )
                                    );

                                    return Ok(data);
                                }
                                Err(e) => {
                                    logger_clone.write_status(&e);
                                }
                            }
                        }

                        Err(status_err_reading(block.id))
                    }
                ));

                if tasks.len() >= READ_BUF_SIZE {
                    match drain_reads(&logger, &mut tasks, &mut tx).await {
                        Ok(_) => {}
                        Err(_) => return,
                    }
                }
            }

            if !tasks.is_empty() {
                match drain_reads(&logger, &mut tasks, &mut tx).await {
                    Ok(_) => {}
                    Err(_) => return,
                }
            }

            logger.write(
                LogLevel::Info, 
                || format!(
                    "Finished reading file {}", 
                    req.file_name
                )
            );
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
            return Err(err_misconfigured_svc());
        }

        for (k, dn_config) in config.data_nodes {
            data_nodes.insert(k.clone(), DataNodeConn::new(
                k,
                dn_config.host,
                dn_config.port,
            ));
        }

        let logger = LogManager::new(
            log_file.clone().unwrap(),
            args.log_level,
            args.silent,
        )?;

        Ok(
            NameNodeService {
                id: args.id,
                self_node: node.unwrap(),
                replica_ct: config.replica_count,
                name_mgr: NameManager::new(), // TODO: handle init
                data_nodes: Arc::new(
                    DataNodeManager::new(
                        data_nodes,
                        logger.clone(),
                    )
                ),
                log_mgr: logger,
            }
        )
    }

    pub async fn serve(
        self,
    ) -> Result<()> {
        let (health_rep, health_svc) = health_server::health_reporter();
        let addr = self.self_node.to_socket_addr()?;
        let logger = self.log_mgr.clone();

        // should remove this or make it optional via config
        // only added this for testing
        let svc_reflection = Builder::configure()
            .register_encoded_file_descriptor_set(NAME_FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();

        logger.write(
            LogLevel::Info, 
            || format!(
                "Starting NameNodeServer with ID {} at {} on port {}", 
                self.id, 
                addr.ip().to_string(), 
                addr.port()
            )
        );

        health_rep
            .set_serving::<NameNodeServer<NameNodeService>>()
            .await;

        let res = Server::builder()
            .add_service(health_svc)
            .add_service(svc_reflection)
            .add_service(NameNodeServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| { 
                let err = RustDFSError::TonicError(e);
                logger.write_err(&err);
                err
            });
        
        health_rep
            .set_not_serving::<NameNodeServer<NameNodeService>>()
            .await;

        res
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

async fn drain_writes<T>(
    logger: &LogManager,
    buf: &mut Vec<(String, String, T)>,
) -> ServiceResult<()>
    where T: Future<Output = ServiceResult<DataWriteResponse>>
{
    let mut err = Vec::new();

    for (block, node, fut) in buf.drain(..) {
        pin_mut!(fut);

        match fut.await {
            Ok(res) if res.success => {}
            _ => {
                err.push((block, node));
            }
        }
    }

    if !err.is_empty() {
        let status = status_err_writing(err);

        logger.write(
            LogLevel::Error, 
            || status.message().to_string()
        );

        return Err(status);
    }

    Ok(())
}

async fn drain_reads<T>(
    logger: &LogManager,
    buf: &mut Vec<(String, String, T)>,
    tx: &mut Sender<ServiceResult<NameReadResponse>>,
) -> ServiceResult<()>
    where T: Future<Output = ServiceResult<DataReadResponse>>
{
    for (file, block, fut) in buf.drain(..) {
        pin_mut!(fut);

        match fut.await {
            Ok(read) => {
                let res = tx.send(Ok(
                    NameReadResponse {
                        file_name: file.clone(),
                        data: read.data,
                    }
                )).await;

                if res.is_err() {
                    let status = status_client_disconnect();

                    logger.write(
                        LogLevel::Error, 
                        || status.message().to_string()
                    );

                    return Err(status);
                }
            }
            Err(e) => {
                logger.write_status(&e);

                let _ = tx.send(
                    Err(e.clone())
                ).await;

                return Err(e);
            }
        }
    }

    Ok(())
}

fn err_misconfigured_svc() -> RustDFSError {
    RustDFSError::CustomError("Misconfigured Name Node service".to_string())
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
    block: String,
) -> Status {
    let msg = format!("Failed to read block: {}", block);
    Status::internal(msg)
}

fn status_client_disconnect() -> Status {
    Status::internal("Client disconnected")
}
