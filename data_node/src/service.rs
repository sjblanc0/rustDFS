use futures::future::{TryFutureExt, join_all};
use rustdfs_proto::data::write_request::ReplicaNode;
use tokio::sync::mpsc::error::SendError;
use tokio::task::{self, JoinError, JoinHandle, JoinSet};
use futures::{FutureExt, stream};
use tokio_stream::wrappers::ReceiverStream;
use std::alloc::handle_alloc_error;
use std::pin::Pin;
use std::io::{Error as IoError, Read, WriterPanicked};
use std::sync::Arc;
use std::vec;
use tokio::sync::Notify;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_retry2::strategy::{ExponentialBackoff, MaxInterval, jitter};
use tokio_retry2::{Retry, RetryError};
use tonic::transport::Server;
use tonic::{Request, Response, Status, client};
use tonic_health::server::{self as health_server, HealthReporter};
use tonic_reflection::server::Builder;
use tonic::Streaming;
use tokio::pin;
use tokio::join;
use async_stream::stream;
use std::io::Write;
use futures::stream::Stream;

use rustdfs_shared::config::RustDFSConfig;
use rustdfs_shared::conn::DataNodeManager;
use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::host::HostAddr;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_proto::data::data_node_server::{DataNode, DataNodeServer};
use rustdfs_proto::data::{self, ReadRequest, ReadResponse, WriteRequest};
use rustdfs_proto::name::name_node_client::NameNodeClient;
use rustdfs_proto::name::{RegisterRequest, block};
use rustdfs_shared::result::{Result, ServiceResult};

use crate::args::RustDFSArgs;
use crate::blocks::BlockManager;

type WriteStream = Pin<Box<dyn Stream<Item = ServiceResult<()>> + Send>>;
type ReadStream = Pin<Box<dyn Stream<Item = ServiceResult<ReadResponse>> + Send>>;
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
    data_nodes: Arc<DataNodeManager>,
    data_mgr: BlockManager,
    log_mgr: LogManager,
    message_size: usize,
}

#[tonic::async_trait]
impl DataNode for DataNodeService {

    type WriteStream = WriteStream;
    type ReadStream = ReadStream;

    /**
     * Writes a block of data to the data node.
     * Also replicates the block to other data nodes as specified.
     * Stores connections to new replica nodes in [DataNodeManager].
     *
     *  @param request - DataWriteRequest containing block ID, data, and replica node IDs.
     *  @return Result<Response<()>> - Response indicating success or failure.
     */
    async fn write(
        &self, 
        request: Request<Streaming<WriteRequest>>,
    ) -> ServiceResult<Response<WriteStream>> {
        let msg_size = self.message_size;
        let block_mgr = self.data_mgr.clone();
        let data_nodes = self.data_nodes.clone();
        let logger = self.log_mgr.clone();

        let (tx_repl, rx_repl) = mpsc::channel(8);
        let (tx_client_a, rx_client) = mpsc::channel(8);
        let (tx_init_a, mut rx_init) = mpsc::channel(1);
        let tx_client_b = tx_client_a.clone();
        let tx_client_c = tx_client_a.clone();
        let tx_init_b = tx_init_a.clone();

        let mut req_stream = request.into_inner();
        let res_stream = ReceiverStream::new(rx_client);
        let mut tasks = JoinSet::new();

        tasks.spawn(async move {
            while let Some(res) = req_stream.next().await {
                match res {
                    Ok(req) => {
                        let mut writer = block_mgr
                            .write_buf(&req.block_id, msg_size)
                            .await?;

                        writer.write(&req.data)
                            .await
                            .map_err(|err| status_err_writing(err))?;

                        writer.flush()
                            .await
                            .map_err(|err| status_err_writing(err))?;
                        
                        match req.replicas.len() {
                            0 => {
                                if !tx_init_a.is_closed() {
                                    tx_init_a.send(None)
                                        .await
                                        .map_err(|e| {
                                            status_err_internal_channel(e)
                                        })?;
                                }

                                tx_client_a.send(Ok(()))
                                    .await
                                    .map_err(|e| {
                                        status_err_sending_client(e)
                                    })?;
                            }
                            _ => {
                                let repl = WriteRequest {
                                    block_id: req.block_id.clone(),
                                    data: req.data,
                                    replicas: req.replicas[1..].to_vec(),
                                };

                                if !tx_init_a.is_closed() {
                                    tx_init_a.send(Some(req.replicas[0].clone()))
                                        .await
                                        .map_err(|e| {
                                            status_err_internal_channel(e)
                                        })?;
                                }

                                tx_repl.send(repl)
                                    .await
                                    .map_err(|err| {
                                        status_err_forwarding(err)
                                    })?;
                            }
                        }
                    }
                    Err(err) => { 
                        return Err(err);
                    }
                }
            }

            Ok::<(), Status>(())
        });

        tasks.spawn(async move {
            drop(tx_init_b);

            let repl = match rx_init.recv().await {
                Some(opt) => {
                    match opt {
                        Some(repl) => {
                            drop(rx_init);
                            repl
                        },
                        None => {
                            drop(rx_init);
                            return Ok(());
                        }
                    }
                },
                None => {
                    drop(rx_init);
                    return Ok(());
                }
            };

            if !data_nodes.has_conn(&repl.host).await {
                data_nodes
                    .add_conn(&repl.host, repl.port as u16)
                    .await?;
            }

            let conn = data_nodes
                .get_conn(&repl.host)
                .await?;
            let mut repl_stream = conn
                .write(ReceiverStream::new(rx_repl))
                .await?;

            while let Some(next) = repl_stream.next().await {
                match next {
                    Ok(_) => {
                    tx_client_b
                        .send(Ok(()))
                        .await
                        .map_err(|e| {
                                status_err_sending_client(e)
                            })?;
                    }
                    Err(err) => {
                        let _ = tx_client_b
                            .send(Err(err.clone()))
                            .await;
                        return Err(err);
                    }
                }
            }

            Ok::<(), Status>(())
        });

        tokio::spawn(async move {
            let mut tx_client = Some(tx_client_c);

            while let Some(res) = tasks.join_next().await {
                match res {
                    Ok(res) => match res {
                        Ok(_) => {},
                        Err(err) => {
                            tasks.shutdown().await;

                            if let Some(tx_client) = tx_client.take() {
                                let _ = tx_client.send(Err(err)).await;
                            }

                            break;
                        }
                    }
                    Err(err) => {
                        tasks.shutdown().await;

                        let err = status_err_joining_write_ops(err);
                        logger.write_status(&err);

                        if let Some(tx_client) = tx_client.take() {
                            let _ = tx_client.send(Err(err)).await;
                        }

                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(res_stream) as Self::WriteStream))
    }

    /**
     * Reads a block of data from the data node.
     *
     *  @param request - DataReadRequest containing block ID.
     *  @return Result<Response<DataReadResponse>> - Response containing data block or error.
     */
    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> ServiceResult<Response<ReadStream>> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(8);
        let out = ReceiverStream::new(rx);
        let msg_size = self.message_size;

        let mut buf = vec![0u8; msg_size];
        let data_mgr = self.data_mgr.clone();
        let logger = self.log_mgr.clone();

        tokio::spawn(async move {
            let mut reader = match data_mgr.read_buf(&req.block_id, msg_size, req.offset).await {
                Ok(reader) => {
                    reader
                },
                Err(e) => {
                    logger.write_status(&e);
                    let _ = tx.send(Err(e.clone())).await;
                    return Err(e);
                },
            };

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        logger.write(LogLevel::Info, || {
                            format!("Completed read for block {}", req.block_id)
                        });
                        break;
                    },
                    Ok(n @ 1..) => {
                        let res = ReadResponse {
                            block_id: req.block_id.clone(),
                            data: buf[..n].to_vec(),
                        };

                        tx.send(Ok(res))
                            .await
                            .map_err(|e| {
                                let err = status_err_sending_client(e);
                                logger.write_status(&err);
                                err
                            })?;
                    }
                    Err(e) => {
                        let err = status_err_reading(&req.block_id, e);
                        logger.write_status(&err);
                        let _ = tx.send(Err(err)).await;
                        break;
                    }
                }
            }

            Ok::<(), Status>(())
        });

        
        Ok(Response::new(Box::pin(out) as Self::ReadStream))
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
            data_nodes: Arc::new(DataNodeManager::new(logger.clone())),
            data_mgr: BlockManager::new(&config.data_node.data_dir, &logger)?,
            log_mgr: logger,
            message_size: config.message_size.as_usize(),
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
        .register(RegisterRequest {
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

fn status_err_writing(err: IoError) -> Status {
    let str = format!("Error writing block: {}", err);
    Status::internal(str)
}

fn status_err_internal_channel(e: SendError<Option<ReplicaNode>>) -> Status {
    let str = format!("Error sending to internal channel: {}", e);
    Status::internal(str)
}

fn status_err_joining_write_ops(e: JoinError) -> Status {
    let str = format!("Error joining write tasks: {}", e);
    Status::internal(str)
}

fn status_err_forwarding(e: SendError<WriteRequest>) -> Status {
    let str = format!("Error forwarding to replica: {}", e);
    Status::internal(str)
}

fn status_err_reading(block: &str, err: IoError) -> Status {
    let str = format!("Encountered IoError reading block {}: {}", block, err);
    Status::internal(str)
}

fn status_err_sending_client<T>(e: SendError<ServiceResult<T>>) -> Status {
    let str = format!("Error sending response to client: {}", e);
    Status::internal(str)
}
