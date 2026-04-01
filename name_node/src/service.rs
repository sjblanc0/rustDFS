use rand::seq::SliceRandom;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::transport::Server;
use tonic::{Request, Response};
use tonic_health::server as health_server;

use crate::nodes::DataNodeManager;
use rustdfs_proto::name::name_node_server::NameNode;
use rustdfs_proto::name::name_node_server::NameNodeServer;
use rustdfs_proto::name::{
    Block, BlockReportRequest, HeartbeatRequest, HeartbeatResponse, ReadRequest, ReadResponse,
    RegisterRequest, RenewLeaseRequest, RenewLeaseResponse, WriteEndRequest, WriteStartRequest,
    WriteStartResponse, block::Node,
};
use rustdfs_shared::config::RustDFSConfig;
use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::host::HostAddr;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_shared::result::{Result, ServiceResult};

use crate::args::RustDFSArgs;
use crate::files::FileManager;

//type ReadStream = Pin<Box<dyn Stream<Item = ServiceResult<NameReadResponse>> + Send>>;

/**
 * Name Node gRPC service implementation.
 *
 * Manages file metadata, block allocation, write leases, and
 * data node registration. Acts as the central coordinator for
 * the RustDFS cluster.
 *
 *  @field host - [HostAddr] the name node listens on.
 *  @field file_mgr - [FileManager] for namespace and lease tracking.
 *  @field data_nodes - [DataNodeManager] for registered data node connections.
 *  @field log_mgr - [LogManager] for logging operations.
 *  @field message_size - Max gRPC streaming message size in bytes.
 *  @field heartbeat_timeout - Seconds before a node with no heartbeat is declared dead.
 *  @field heartbeat_recheck_interval - Seconds between reaper sweeps.
 */
#[derive(Debug)]
pub struct NameNodeService {
    host: HostAddr,
    file_mgr: FileManager,
    data_nodes: Arc<DataNodeManager>,
    log_mgr: LogManager,
    message_size: usize,
    heartbeat_timeout: u64,
    heartbeat_recheck_interval: u64,
}

#[tonic::async_trait]
impl NameNode for NameNodeService {
    //type ReadStream = ReadStream;

    /**
     * Initiates a file write.
     * Allocates blocks across data nodes and acquires a write lease.
     * Returns block assignments and the lease expiry timestamp.
     *
     *  @param request - [WriteStartRequest] with file name, size, and operation ID.
     *  @return ServiceResult<Response<WriteStartResponse>> - Block assignments and lease info.
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

    /**
     * Completes a file write, transitioning the file from InProgress to Complete.
     *
     *  @param request - [WriteEndRequest] with file name and operation ID.
     *  @return ServiceResult<Response<()>> - Success or error if lease is invalid.
     */
    async fn write_end(&self, request: Request<WriteEndRequest>) -> ServiceResult<Response<()>> {
        let req = request.into_inner();

        self.file_mgr
            .complete_write(&req.file_name, &req.operation_id)
            .await?;

        Ok(Response::new(()))
    }

    /**
     * Renews the write lease for a file, extending the expiry.
     *
     *  @param request - [RenewLeaseRequest] with file name and operation ID.
     *  @return ServiceResult<Response<RenewLeaseResponse>> - New expiry timestamp.
     */
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
     * Returns the block descriptors for a file.
     * Provides block IDs, sizes, and a shuffled list of replica nodes
     * to the client so it can read directly from data nodes.
     *
     *  @param request - [ReadRequest] containing the file name.
     *  @return ServiceResult<Response<ReadResponse>> - Block metadata for the file.
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
     * Establishes a gRPC connection and adds it to the [DataNodeManager].
     *
     *  @param request - [RegisterRequest] with host and port of the data node.
     *  @return ServiceResult<Response<()>> - Success or connection error.
     */
    async fn register(&self, request: Request<RegisterRequest>) -> ServiceResult<Response<()>> {
        let req = request.into_inner();

        self.data_nodes.add_conn(&req.host, req.port as u16).await?;
        self.data_nodes.record_heartbeat(&req.host).await;

        self.log_mgr.write(LogLevel::Info, || {
            format!("Registered data node at {}:{}", req.host, req.port)
        });

        Ok(Response::new(()))
    }

    /**
     * Handles a heartbeat from a data node, recording its liveness.
     *
     *  @param request - [HeartbeatRequest] with host and port of the data node.
     *  @return ServiceResult<Response<HeartbeatResponse>> - Empty acknowledgement.
     */
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> ServiceResult<Response<HeartbeatResponse>> {
        let req = request.into_inner();

        self.data_nodes.record_heartbeat(&req.host).await;
        Ok(Response::new(HeartbeatResponse {}))
    }

    /**
     * Handles a block report from a data node.
     * Rebuilds file descriptors from the reported block metadata
     * so that files are recoverable after a full cluster restart.
     *
     *  @param request - [BlockReportRequest] with host, port, and block metadata.
     *  @return ServiceResult<Response<()>> - Success.
     */
    async fn block_report(
        &self,
        request: Request<BlockReportRequest>,
    ) -> ServiceResult<Response<()>> {
        let req = request.into_inner();

        let host = HostAddr {
            hostname: req.host.clone(),
            port: req.port as u16,
        };

        self.file_mgr.rebuild_from_report(&host, &req.blocks).await;

        self.log_mgr.write(LogLevel::Info, || {
            format!(
                "Processed block report from {}:{} ({} blocks)",
                req.host,
                req.port,
                req.blocks.len()
            )
        });

        Ok(Response::new(()))
    }
}

impl NameNodeService {
    /**
     * Creates a new [NameNodeService] instance.
     *
     *  @param args - CLI arguments (log level, silent mode).
     *  @param config - [RustDFSConfig] with cluster-wide settings.
     *  @return Result<NameNodeService> - Initialized service or error.
     */
    pub fn new(args: RustDFSArgs, config: RustDFSConfig) -> Result<Self> {
        let log_mgr = LogManager::new(config.name_node.log_file, args.log_level, args.silent)?;
        let data_nodes = Arc::new(DataNodeManager::new(log_mgr.clone()));

        Ok(NameNodeService {
            host: HostAddr {
                hostname: config.name_node.host.clone(),
                port: config.name_node.port,
            },
            file_mgr: FileManager::load(
                log_mgr.clone(),
                config.lease_duration,
                config.replica_count as usize,
                config.block_size.as_usize(),
                config.name_node.name_dir,
                config.name_node.checkpoint_txns,
                config.name_node.checkpoint_period,
            )?,
            data_nodes,
            log_mgr,
            message_size: config.message_size.as_usize(),
            heartbeat_timeout: config.name_node.heartbeat_timeout,
            heartbeat_recheck_interval: config.name_node.heartbeat_recheck_interval,
        })
    }

    /**
     * Starts the Name Node gRPC server.
     * Registers health reporting and service reflection, then listens
     * on the configured host and port.
     *
     *  @return Result<()> - Resolves when the server shuts down.
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

        let reaper_nodes = Arc::clone(&self.data_nodes);
        let reaper_logger = self.log_mgr.clone();
        let reaper_timeout = self.heartbeat_timeout;
        let reaper_interval = self.heartbeat_recheck_interval;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        // name node reaper service
        //  => periodically removes data nodes that have
        //     not sent a heartbeat within the timeout threshold
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(reaper_interval));

            loop {
                interval.tick().await;

                match reaper_nodes.get_stale_nodes(reaper_timeout).await {
                    Ok(nodes) => {
                        for node in &nodes {
                            reaper_nodes.remove_conn(node).await;
                        }
                    }
                    Err(_) => {
                        reaper_logger.write(LogLevel::Error, || {
                            "Error checking for stale nodes. Shutting down service.".to_string()
                        });
                        let _ = shutdown_tx.send(());
                        return;
                    }
                };
            }
        });

        health_rep
            .set_serving::<NameNodeServer<NameNodeService>>()
            .await;

        let res = Server::builder()
            .add_service(health_svc)
            .add_service(NameNodeServer::new(self))
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
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
