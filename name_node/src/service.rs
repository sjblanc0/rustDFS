use rand::seq::SliceRandom;
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tonic::{Request, Response};
use tonic_health::server as health_server;

use rustdfs_proto::name::name_node_server::NameNode;
use rustdfs_proto::name::name_node_server::NameNodeServer;
use rustdfs_proto::name::{
    Block, ReadRequest, ReadResponse, RegisterRequest, RenewLeaseRequest, RenewLeaseResponse,
    WriteEndRequest, WriteStartRequest, WriteStartResponse, block::Node,
};
use rustdfs_shared::config::RustDFSConfig;
use rustdfs_shared::conn::DataNodeManager;
use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::host::HostAddr;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_shared::result::{Result, ServiceResult};

use crate::args::RustDFSArgs;
use crate::files::FileManager;

//type ReadStream = Pin<Box<dyn Stream<Item = ServiceResult<NameReadResponse>> + Send>>;

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
    file_mgr: FileManager,
    data_nodes: DataNodeManager,
    log_mgr: LogManager,
    message_size: usize,
}

#[tonic::async_trait]
impl NameNode for NameNodeService {
    //type ReadStream = ReadStream;

    /**
     * Writes a file to the RustDFS cluster.
     * Breaks file into blocks, assigns to data nodes, and manages replication.
     * Concurrently writes 8 blocks at a time before polling for more requests.
     *
     *  @param request - Streaming<NameWriteRequest> containing file name and data blocks.
     *  @return Result<Response<NameWriteResponse>> - Response indicating success or failure.
     */
    async fn write_start(
        &self,
        request: Request<WriteStartRequest>,
    ) -> ServiceResult<Response<WriteStartResponse>> {
        let req = request.into_inner();

        let (desc, expire) = self
            .file_mgr
            .init_write(
                &req.operation_id,
                &req.file_name,
                req.file_size,
                &self.data_nodes,
            )
            .await?;

        self.log_mgr.write(LogLevel::Info, || {
            format!("Starting write for file {}", req.file_name)
        });

        let res = WriteStartResponse {
            file_name: req.file_name.clone(),
            expire,
            message_size: self.message_size as u64,
            blocks: desc
                .blocks
                .iter()
                .map(|b| Block {
                    block_id: b.id.clone(),
                    block_size: b.size,
                    nodes: b
                        .nodes
                        .iter()
                        .map(|h| Node {
                            host: h.hostname.clone(),
                            port: h.port as u32,
                        })
                        .collect(),
                })
                .collect(),
        };

        Ok(Response::new(res))
    }

    async fn write_end(&self, request: Request<WriteEndRequest>) -> ServiceResult<Response<()>> {
        let req = request.into_inner();

        self.file_mgr
            .complete_write(&req.file_name, &req.operation_id)
            .await?;

        Ok(Response::new(()))
    }

    async fn renew_lease(
        &self,
        request: Request<RenewLeaseRequest>,
    ) -> ServiceResult<Response<RenewLeaseResponse>> {
        let req = request.into_inner();
        let expire = self
            .file_mgr
            .renew_lease(&req.file_name, &req.operation_id)
            .await?;

        self.log_mgr.write(LogLevel::Info, || {
            format!("Renewed lease for file {}", req.file_name)
        });

        Ok(Response::new(RenewLeaseResponse {
            file_name: req.file_name,
            expire,
        }))
    }

    /**
     * Reads a file from the RustDFS cluster.
     * Retrieves file metadata and streams data blocks from data nodes. Concurrently reads
     * 8 blocks and flushes to client. This is to manage memory usage on the name node.
     *
     *  @param request - NameReadRequest containing file name.
     *  @return Result<Response<ReadStream>> - Response streaming file data or error.
     */
    async fn read(&self, request: Request<ReadRequest>) -> ServiceResult<Response<ReadResponse>> {
        let req = request.into_inner();
        let desc = self.file_mgr.read(&req.file_name).await?;

        self.log_mgr.write(LogLevel::Info, || {
            format!("Providing file descriptor for read: {}", &req.file_name)
        });

        let res = ReadResponse {
            file_name: req.file_name,
            message_size: self.message_size as u64,
            blocks: desc
                .blocks
                .iter()
                .map(|b| Block {
                    block_id: b.id.clone(),
                    block_size: b.size,
                    nodes: {
                        let mut nodes = b
                            .nodes
                            .iter()
                            .map(|h| Node {
                                host: h.hostname.clone(),
                                port: h.port as u32,
                            })
                            .collect::<Vec<_>>();

                        // just shuffle the nodes
                        // in actuality HDFS would try to read from the
                        // nearest node first
                        nodes.shuffle(&mut rand::rng());
                        nodes
                    },
                })
                .collect(),
        };

        Ok(Response::new(res))
    }

    /**
     * Registers a data node with the name node.
     * Adds the data node connection to the DataNodeManager.
     *
     *  @param request - NameRegisterRequest containing data node host and port.
     *  @return Result<Response<()>> - Response indicating success or failure.
     */
    async fn register(&self, request: Request<RegisterRequest>) -> ServiceResult<Response<()>> {
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
        let log_mgr = LogManager::new(config.name_node.log_file, args.log_level, args.silent)?;
        let data_nodes = DataNodeManager::new(log_mgr.clone());

        Ok(NameNodeService {
            host: HostAddr {
                hostname: config.name_node.host.clone(),
                port: config.name_node.port,
            },
            file_mgr: FileManager::new(
                log_mgr.clone(),
                config.lease_duration,
                config.replica_count as usize,
                config.block_size.as_usize(),
            ),
            data_nodes,
            log_mgr,
            message_size: config.message_size.as_usize(),
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
}
