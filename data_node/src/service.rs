use futures::future::{TryFutureExt, join_all};
use futures::join;
use std::io::Error as IoError;
use tokio_retry2::strategy::{ExponentialBackoff, MaxInterval, jitter};
use tokio_retry2::{Retry, RetryError};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tonic_health::server::{self as health_server, HealthReporter};
use tonic_reflection::server::Builder;

use rustdfs_shared::config::RustDFSConfig;
use rustdfs_shared::conn::DataNodeManager;
use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::host::HostAddr;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_shared::proto::data_node_server::{DataNode, DataNodeServer};
use rustdfs_shared::proto::name_node_client::NameNodeClient;
use rustdfs_shared::proto::{DataReadRequest, DataReadResponse, DataWriteRequest};
use rustdfs_shared::proto::{FILE_DESCRIPTOR_SET, NameRegisterRequest};
use rustdfs_shared::result::{Result, ServiceResult};

use crate::args::RustDFSArgs;
use crate::blocks::BlockManager;

type RetryResult<T> = std::result::Result<T, RetryError<RustDFSError>>;

/**
 * Data Node service implementation for RustDFS.
 *
 * Handles read and write requests for data blocks,
 * and manages replication to other data nodes.
 *
 *  @field host - HostAddr of the data node.
 *  @field name_host - HostAddr of the name node.
 *  @field data_nodes - DataNodeManager for managing data node connections.
 *  @field data_mgr - BlockManager for managing data blocks on local filesystem.
 *  @field log_mgr - LogManager for logging operations.
 */
#[derive(Debug)]
pub struct DataNodeService {
    host: HostAddr,
    name_host: HostAddr,
    data_nodes: DataNodeManager,
    data_mgr: BlockManager,
    log_mgr: LogManager,
}

#[tonic::async_trait]
impl DataNode for DataNodeService {
    /**
     * Writes a block of data to the data node.
     * Also replicates the block to other data nodes as specified.
     * Stores connections to new replica nodes in [DataNodeManager].
     *
     *  @param request - DataWriteRequest containing block ID, data, and replica node IDs.
     *  @return Result<Response<()>> - Response indicating success or failure.
     */
    async fn write(&self, request: Request<DataWriteRequest>) -> ServiceResult<Response<()>> {
        let request_ref = request.get_ref();
        let mut repls = Vec::new();
        let mut idents = Vec::new();

        self.data_mgr
            .write_block(&request_ref.block_id, &request_ref.data)?;

        for desc in request_ref.replicas.iter() {
            if !self.data_nodes.has_conn(&desc.host).await {
                self.data_nodes
                    .add_conn(&desc.host, desc.port as u16)
                    .await?;
            }

            let ident = format!("{}:{}", desc.host, desc.port);
            let write = self
                .data_nodes
                .get_conn(&desc.host)
                .await?
                .write(DataWriteRequest {
                    block_id: request_ref.block_id.clone(),
                    data: request_ref.data.clone(),
                    replicas: vec![],
                });

            idents.push(ident);
            repls.push(write);
        }

        let failed = join_all(repls)
            .await
            .into_iter()
            .enumerate()
            .filter(|(_, r)| r.is_err())
            .map(|(i, _)| idents[i].clone())
            .collect::<Vec<_>>();

        if !failed.is_empty() {
            let err = status_err_forwarding(&failed);
            self.log_mgr.write_status(&err);
            return Err(err);
        }

        self.log_mgr.write(LogLevel::Info, || {
            format!(
                "Wrote block {} with {}",
                request_ref.block_id,
                format_bytes(request_ref.data.len())
            )
        });

        Ok(Response::new(()))
    }

    /**
     * Reads a block of data from the data node.
     *
     *  @param request - DataReadRequest containing block ID.
     *  @return Result<Response<DataReadResponse>> - Response containing data block or error.
     */
    async fn read(
        &self,
        request: Request<DataReadRequest>,
    ) -> ServiceResult<Response<DataReadResponse>> {
        let request_ref = request.get_ref();
        let data: Vec<u8> = self.data_mgr.read_block(&request_ref.block_id)?;

        self.log_mgr.write(LogLevel::Info, || {
            format!(
                "Read block {} with {}",
                request_ref.block_id,
                format_bytes(data.len())
            )
        });

        Ok(Response::new(DataReadResponse { data }))
    }
}

impl DataNodeService {
    /**
     * Creates a new DataNodeService instance.
     * Maps ID from args to config and initializes components, including connections
     * to other data nodes.
     *
     *  @param args - Command line arguments for the data node.
     *  @param config - Configuration for the RustDFS cluster.
     *  @return Result<DataNodeService> - Initialized DataNodeService instance or error.
     */
    pub fn new(args: RustDFSArgs, config: RustDFSConfig) -> Result<Self> {
        let logger = LogManager::new(config.data_node.log_file, args.log_level, args.silent)?;

        Ok(DataNodeService {
            host: HostAddr {
                hostname: hostname(&logger)?,
                port: args.port,
            },
            name_host: HostAddr {
                hostname: config.name_node.host.clone(),
                port: config.name_node.port,
            },
            data_nodes: DataNodeManager::new(&logger),
            data_mgr: BlockManager::new(&config.data_node.data_dir, &logger)?,
            log_mgr: logger,
        })
    }

    /**
     * Starts the DataNodeService server to handle incoming requests.
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
                "Starting DataNodeServer at {}:{}",
                self.host.hostname, self.host.port,
            )
        });

        let host = self.host.clone();
        let name_host = self.name_host.clone();
        let retry_strat = ExponentialBackoff::from_millis(100)
            .factor(1)
            .max_delay_millis(1000)
            .max_interval(10000)
            .map(jitter);

        health_rep
            .set_serving::<DataNodeServer<DataNodeService>>()
            .await;

        let res = join!(
            Server::builder()
                .add_service(health_svc)
                .add_service(svc_reflection)
                .add_service(DataNodeServer::new(self))
                .serve(addr)
                .map_err(|e| {
                    let err = RustDFSError::TonicError(e);
                    logger.write_err(&err);
                    err
                }),
            Retry::spawn(retry_strat, || init_name_conn(
                logger.clone(),
                health_rep.clone(),
                host.clone(),
                name_host.clone(),
            )),
        );

        health_rep
            .set_not_serving::<DataNodeServer<DataNodeService>>()
            .await;

        res.0?;
        res.1?;
        Ok(())
    }
}

/**
 * Initializes connection to the Name Node and registers this Data Node.
 *
 *  @param logger - LogManager for logging.
 *  @param health_rep - HealthReporter for service health status.
 *  @param host - HostAddr of this Data Node.
 *  @param name_host - HostAddr of the Name Node.
 *  @return RetryResult<()> - Result indicating success or transient error for retrying.
 */
async fn init_name_conn(
    logger: LogManager,
    health_rep: HealthReporter,
    host: HostAddr,
    name_host: HostAddr,
) -> RetryResult<()> {
    let endpoint = name_host.to_endpoint(&logger)?;
    let client = NameNodeClient::connect(endpoint).await;

    if client.is_err() {
        let orig = client.err().unwrap();
        let err = RustDFSError::TonicError(orig);

        logger.write_err(&err);
        logger.write(LogLevel::Error, || {
            format!(
                "Failed to connect to NameNode at {}:{}. Retrying...",
                name_host.hostname, name_host.port
            )
        });

        return RetryError::to_transient(err);
    }

    let res = client
        .unwrap()
        .register(NameRegisterRequest {
            host: host.hostname.clone(),
            port: host.port as u32,
        })
        .await;

    if res.is_err() {
        let orig = res.err().unwrap();
        let err = RustDFSError::TonicStatusError(orig);

        logger.write_err(&err);
        logger.write(LogLevel::Error, || {
            format!(
                "Failed to register with NameNode at {}:{}. Retrying...",
                name_host.hostname, name_host.port
            )
        });

        return RetryError::to_transient(err);
    }

    health_rep
        .set_serving::<DataNodeServer<DataNodeService>>()
        .await;

    Ok(())
}

// Retrieves the hostname of the current machine.
fn hostname(logger: &LogManager) -> Result<String> {
    hostname::get()
        .map_err(|e| {
            let err = err_resolving_host(Some(e));
            logger.write_err(&err);
            err
        })?
        .into_string()
        .map_err(|_| {
            let err = err_resolving_host(None);
            logger.write_err(&err);
            err
        })
}

// Format bytes into human-readable string
// e.g., 1024 -> "1.00 KB", 1048576 -> "1.00 MB"
fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;

    if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

// Error status helpers

fn err_resolving_host(err: Option<IoError>) -> RustDFSError {
    match err {
        Some(e) => {
            let str = format!("Error resolving host: {}", e);
            RustDFSError::CustomError(str)
        }
        None => {
            let str = "Error resolving host".to_string();
            RustDFSError::CustomError(str)
        }
    }
}

fn status_err_forwarding(nodes: &[String]) -> Status {
    let ids_str = nodes.join(",");
    let log = format!("Error forwarding to data nodes at {}", ids_str);
    Status::internal(log)
}
