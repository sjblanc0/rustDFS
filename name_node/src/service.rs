use futures::Future;
use futures::pin_mut;
use mpsc::Sender;
use rand::seq::SliceRandom;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tonic_health::server as health_server;
use tonic_reflection::server::Builder;
use uuid::Uuid;

use rustdfs_shared::config::RustDFSConfig;
use rustdfs_shared::conn::DataNodeManager;
use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::host::HostAddr;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_shared::proto::FILE_DESCRIPTOR_SET;
use rustdfs_shared::proto::data_write_request::ReplicaNode;
use rustdfs_shared::proto::name_node_server::NameNode;
use rustdfs_shared::proto::name_node_server::NameNodeServer;
use rustdfs_shared::proto::{DataReadRequest, DataReadResponse, DataWriteRequest};
use rustdfs_shared::proto::{
    NameReadRequest, NameReadResponse, NameRegisterRequest, NameWriteRequest, NameWriteResponse,
};
use rustdfs_shared::result::{Result, ServiceResult};

use crate::args::RustDFSArgs;
use crate::files::BlockDescriptor;
use crate::files::FileManager;

type ReadStream = Pin<Box<dyn Stream<Item = ServiceResult<NameReadResponse>> + Send>>;

const WRITE_BUF_SIZE: usize = 8;
const READ_BUF_SIZE: usize = 8;

/**
 * Name Node service implementation for RustDFS.
 *
 * Handles file metadata management, including mapping files to
 * data blocks and their locations.
 *
 *  @field host - HostAddr of the name node.
 *  @field replica_ct - Number of replicas for each data block.
 *  @field name_mgr - FileManager for managing file metadata.
 *  @field data_nodes - DataNodeManager for managing data node connections.
 *  @field log_mgr - LogManager for logging operations.
 */
#[derive(Debug)]
pub struct NameNodeService {
    host: HostAddr,
    replica_ct: usize,
    name_mgr: FileManager,
    data_nodes: DataNodeManager,
    log_mgr: LogManager,
}

#[tonic::async_trait]
impl NameNode for NameNodeService {
    type ReadStream = ReadStream;

    /**
     * Writes a file to the RustDFS cluster.
     * Breaks file into blocks, assigns to data nodes, and manages replication.
     * Concurrently writes 8 blocks at a time before polling for more requests.
     *
     *  @param request - Streaming<NameWriteRequest> containing file name and data blocks.
     *  @return Result<Response<NameWriteResponse>> - Response indicating success or failure.
     */
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
                    let nodes = self.select_nodes().await?;

                    blocks.push(BlockDescriptor {
                        id: block_id.clone(),
                        nodes: nodes.clone(),
                    });

                    writes.push((
                        format!("{}:{}", nodes[0].hostname, nodes[0].port),
                        block_id.clone(),
                        self.data_nodes.get_conn(&nodes[0].hostname).await?.write(
                            DataWriteRequest {
                                block_id,
                                data: req.data,
                                replicas: nodes[1..]
                                    .iter()
                                    .map(|h| ReplicaNode {
                                        host: h.hostname.clone(),
                                        port: h.port as u32,
                                    })
                                    .collect(),
                            },
                        ),
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

        self.name_mgr.add_file(name.clone().unwrap(), blocks).await;

        self.log_mgr.write(LogLevel::Info, || {
            format!("Finished writing file {}", name.unwrap())
        });

        Ok(Response::new(NameWriteResponse { success: true }))
    }

    /**
     * Reads a file from the RustDFS cluster.
     * Retrieves file metadata and streams data blocks from data nodes. Concurrently reads
     * 8 blocks and flushes to client. This is to manage memory usage on the name node.
     *
     *  @param request - NameReadRequest containing file name.
     *  @return Result<Response<ReadStream>> - Response streaming file data or error.
     */
    async fn read(&self, request: Request<NameReadRequest>) -> ServiceResult<Response<ReadStream>> {
        let req = request.into_inner();
        let (mut tx, rx) = mpsc::channel(128);
        let out = ReceiverStream::new(rx);
        let logger = self.log_mgr.clone();
        let mut conns = Vec::new();

        let blocks = self.name_mgr.get_blocks(&req.file_name).await?;

        for block in &blocks {
            let mut vec = Vec::new();

            for host in &block.nodes {
                let conn = self.data_nodes.get_conn(&host.hostname).await?;
                vec.push(conn.clone());
            }

            conns.push(vec);
        }

        tokio::spawn(async move {
            let mut tasks = Vec::new();
            let iter = blocks.into_iter().zip(conns);

            for (block, conns) in iter {
                let logger_clone = logger.clone();

                tasks.push(async move {
                    let mut hosts = block.nodes.clone();
                    hosts.shuffle(&mut rand::rng());

                    for (i, host) in hosts.iter().enumerate() {
                        let res = conns[i]
                            .clone()
                            .read(DataReadRequest {
                                block_id: block.id.clone(),
                            })
                            .await;

                        match res {
                            Ok(data) => {
                                logger_clone.write(LogLevel::Debug, || {
                                    format!(
                                        "Read block {} from data node at {}:{}",
                                        block.id, host.hostname, host.port
                                    )
                                });
                                return Ok(data);
                            }
                            Err(e) => {
                                logger_clone.write_status(&e);
                            }
                        }
                    }

                    Err(status_err_reading(block.id))
                });

                if tasks.len() >= READ_BUF_SIZE {
                    match drain_reads(&logger, &req.file_name, &mut tasks, &mut tx).await {
                        Ok(_) => {}
                        Err(_) => return,
                    }
                }
            }

            if !tasks.is_empty() {
                match drain_reads(&logger, &req.file_name, &mut tasks, &mut tx).await {
                    Ok(_) => {}
                    Err(_) => return,
                }
            }

            logger.write(LogLevel::Info, || {
                format!("Finished reading file {}", req.file_name)
            });
        });

        Ok(Response::new(Box::pin(out) as Self::ReadStream))
    }

    /**
     * Registers a data node with the name node.
     * Adds the data node connection to the DataNodeManager.
     *
     *  @param request - NameRegisterRequest containing data node host and port.
     *  @return Result<Response<()>> - Response indicating success or failure.
     */
    async fn register(&self, request: Request<NameRegisterRequest>) -> ServiceResult<Response<()>> {
        let req = request.into_inner();

        self.data_nodes.add_conn(&req.host, req.port as u16).await?;

        self.log_mgr.write(LogLevel::Info, || {
            format!("Registered data node at {}:{}", req.host, req.port)
        });

        Ok(Response::new(()))
    }
}

impl NameNodeService {
    /**
     * Creates a new instance of NameNodeService.
     *
     *  @param args - Command line arguments for the data node.
     *  @param config - Configuration for the RustDFS cluster.
     *  @return Result<NameNodeService> - Initialized NameNodeService instance or error.
     */
    pub fn new(args: RustDFSArgs, config: RustDFSConfig) -> Result<Self> {
        let logger = LogManager::new(config.name_node.log_file, args.log_level, args.silent)?;

        Ok(NameNodeService {
            host: HostAddr {
                hostname: config.name_node.host.clone(),
                port: config.name_node.port,
            },
            replica_ct: config.replica_count as usize,
            name_mgr: FileManager::new(), // TODO: handle init
            data_nodes: DataNodeManager::new(&logger),
            log_mgr: logger,
        })
    }

    /**
     * Starts the NameNodeService server to handle incoming requests.
     * Sets up health reporting and service reflection for gRPC.
     *
     *  @return Result<()> - Result indicating success or failure of the server.
     */
    pub async fn serve(self) -> Result<()> {
        let (health_rep, health_svc) = health_server::health_reporter();
        let addr = self.host.to_socket_addr(&self.log_mgr)?;
        let logger = self.log_mgr.clone();

        // should remove this or make it optional via config
        // only added this for testing
        let svc_reflection = Builder::configure()
            .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();

        logger.write(LogLevel::Info, || {
            format!(
                "Starting NameNodeServer at {}:{}",
                self.host.hostname, self.host.port
            )
        });

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
    // for write operation
    async fn select_nodes(&self) -> ServiceResult<Vec<HostAddr>> {
        let mut keys = self.data_nodes.get_hosts().await;

        if keys.len() <= self.replica_ct {
            let err = status_not_enough_nodes(keys.len(), self.replica_ct);
            self.log_mgr.write_status(&err);
            return Err(err);
        }

        let (selected, _) = keys.partial_shuffle(&mut rand::rng(), self.replica_ct + 1);

        Ok(selected.to_vec())
    }
}

/**
 * Drains a buffer of write futures, awaiting their completion.
 * Logs any errors encountered during writes.
 *
 *  @param logger - LogManager for logging errors.
 *  @param buf - Mutable reference to a vector of (block ID, node ID, write future) tuples.
 *  @return ServiceResult<()> - Result indicating success or failure of writes.
 */
async fn drain_writes<T>(
    logger: &LogManager,
    buf: &mut Vec<(String, String, T)>,
) -> ServiceResult<()>
where
    T: Future<Output = ServiceResult<()>>,
{
    let mut err = Vec::new();

    for (block, node, fut) in buf.drain(..) {
        pin_mut!(fut);

        match fut.await {
            Ok(_) => {}
            _ => {
                err.push((block, node));
            }
        }
    }

    if !err.is_empty() {
        let status = status_err_writing(err);
        logger.write(LogLevel::Error, || status.message().to_string());
        return Err(status);
    }

    Ok(())
}

/**
 * Drains a buffer of read futures, awaiting their completion.
 * Sends results or errors back through the provided channel.
 *
 *  @param logger - LogManager for logging errors.
 *  @param buf - Mutable reference to a vector of (file name, block ID, read future) tuples.
 *  @param tx - Mutable reference to a Sender for sending read results.
 *  @return ServiceResult<()> - Result indicating success or failure of reads.
 */
async fn drain_reads<T>(
    logger: &LogManager,
    file: &str,
    buf: &mut Vec<T>,
    tx: &mut Sender<ServiceResult<NameReadResponse>>,
) -> ServiceResult<()>
where
    T: Future<Output = ServiceResult<DataReadResponse>>,
{
    for fut in buf.drain(..) {
        pin_mut!(fut);

        match fut.await {
            Ok(read) => {
                let res = tx
                    .send(Ok(NameReadResponse {
                        file_name: file.to_string(),
                        data: read.data,
                    }))
                    .await;

                if res.is_err() {
                    let status = status_client_disconnect();
                    logger.write(LogLevel::Error, || status.message().to_string());
                    return Err(status);
                }
            }
            Err(e) => {
                logger.write_status(&e);
                let _ = tx.send(Err(e.clone())).await;
                return Err(e);
            }
        }
    }

    Ok(())
}

// Error helpers

fn status_not_enough_nodes(node_ct: usize, replica_ct: usize) -> Status {
    let msg = format!(
        "Not enough data nodes for write: available {}, required {}",
        node_ct,
        replica_ct + 1
    );
    Status::internal(msg)
}

fn status_err_writing(desc: Vec<(String, String)>) -> Status {
    let desc_str = desc
        .into_iter()
        .map(|(b, n)| format!("(block {} at node = {})", b, n))
        .collect::<Vec<_>>()
        .join(", ");
    let msg = format!("Error writing to data nodes: {}", desc_str);
    Status::internal(msg)
}

fn status_err_reading(block: String) -> Status {
    let msg = format!("Failed to read block: {}", block);
    Status::internal(msg)
}

fn status_client_disconnect() -> Status {
    Status::internal("Client disconnected")
}
