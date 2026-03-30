use futures::future::TryFutureExt;
use futures::stream::Stream;
use rustdfs_proto::data::write_request::ReplicaNode;
use std::io::Error as IoError;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::vec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::join;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self};
use tokio::task::{JoinError, JoinSet};
use tokio_retry2::strategy::{ExponentialBackoff, MaxInterval, jitter};
use tokio_retry2::{Retry, RetryError};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};
use tonic_health::server::{self as health_server, HealthReporter};

use crate::conn::DataNodeManager;
use rustdfs_proto::data::data_node_server::{DataNode, DataNodeServer};
use rustdfs_proto::data::{ReadRequest, ReadResponse, WriteRequest};
use rustdfs_proto::name::name_node_client::NameNodeClient;
use rustdfs_proto::name::{HeartbeatRequest, RegisterRequest};
use rustdfs_shared::config::RustDFSConfig;
use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::host::HostAddr;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_shared::result::{Result, ServiceResult};

use crate::args::RustDFSArgs;
use crate::blocks::BlockManager;

type WriteStream = Pin<Box<dyn Stream<Item = ServiceResult<()>> + Send>>;
type ReadStream = Pin<Box<dyn Stream<Item = ServiceResult<ReadResponse>> + Send>>;
type RetryResult<T> = std::result::Result<T, RetryError<RustDFSError>>;

/**
 * Data Node gRPC service implementation.
 *
 * Handles streaming block writes (with replication chaining),
 * streaming block reads, and lifecycle management (health, Name Node registration).
 *
 *  @field host - [HostAddr] this data node listens on.
 *  @field name_host - [HostAddr] of the Name Node to register with.
 *  @field data_nodes - Shared [DataNodeManager] for replication connections.
 *  @field data_mgr - [BlockManager] for local block I/O.
 *  @field log_mgr - [LogManager] for logging.
 *  @field message_size - Max gRPC streaming message size in bytes.
 *  @field heartbeat_interval - Seconds between heartbeat RPCs.
 *  @field replica_connection_ttl - Seconds before an idle replication connection is evicted.
 */
#[derive(Debug)]
pub struct DataNodeService {
    host: HostAddr,
    name_host: HostAddr,
    data_nodes: Arc<DataNodeManager>,
    data_mgr: BlockManager,
    log_mgr: LogManager,
    message_size: usize,
    heartbeat_interval: u64,
    replica_connection_ttl: u64,
}

#[tonic::async_trait]
impl DataNode for DataNodeService {
    type WriteStream = WriteStream;
    type ReadStream = ReadStream;

    /**
     * Handles a streaming block write.
     *  => Returns the response stream immediately, then processes
     *     input in spawned tasks.
     *
     * Task architecture (managed by [JoinSet]):
     *  1. Consumer — reads each [WriteRequest] from the client, writes
     *     the chunk to disk via [BlockManager], and forwards it to the
     *     replication channel (if replicas are specified).
     *  2. Replicator — establishes a gRPC connection to the next replica
     *     node in the chain and streams forwarded chunks. Sends a per-chunk
     *     acknowledgement back to the client.
     *  3. Supervisor (tokio::spawn) — joins on the JoinSet; on any task
     *     error it shuts down remaining tasks and forwards the error to
     *     the client stream.
     *
     *  @param request - Streaming [WriteRequest] messages.
     *  @return ServiceResult<Response<WriteStream>> - Acknowledgement stream.
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
                        let mut writer = block_mgr.write_buf(&req.block_id, msg_size).await?;

                        writer.write(&req.data).await.map_err(status_err_writing)?;

                        writer.flush().await.map_err(status_err_writing)?;

                        match req.replicas.len() {
                            0 => {
                                if !tx_init_a.is_closed() {
                                    tx_init_a
                                        .send(None)
                                        .await
                                        .map_err(status_err_internal_channel)?;
                                }

                                tx_client_a
                                    .send(Ok(()))
                                    .await
                                    .map_err(status_err_sending_client)?;
                            }
                            _ => {
                                let repl = WriteRequest {
                                    block_id: req.block_id.clone(),
                                    data: req.data,
                                    replicas: req.replicas[1..].to_vec(),
                                };

                                if !tx_init_a.is_closed() {
                                    tx_init_a
                                        .send(Some(req.replicas[0].clone()))
                                        .await
                                        .map_err(status_err_internal_channel)?;
                                }

                                tx_repl.send(repl).await.map_err(status_err_forwarding)?;
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
                Some(Some(repl)) => {
                    drop(rx_init);
                    repl
                }
                Some(None) => {
                    drop(rx_init);
                    return Ok(());
                }
                None => {
                    drop(rx_init);
                    return Ok(());
                }
            };

            if !data_nodes.has_conn(&repl.host).await {
                data_nodes.add_conn(&repl.host, repl.port as u16).await?;
            }

            let conn = data_nodes.get_conn(&repl.host).await?;
            let mut repl_stream = conn.write(ReceiverStream::new(rx_repl)).await?;

            while let Some(next) = repl_stream.next().await {
                match next {
                    Ok(_) => {
                        tx_client_b
                            .send(Ok(()))
                            .await
                            .map_err(status_err_sending_client)?;
                    }
                    Err(err) => {
                        let _ = tx_client_b.send(Err(err.clone())).await;
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
                        Ok(_) => {}
                        Err(err) => {
                            tasks.shutdown().await;

                            if let Some(tx_client) = tx_client.take() {
                                let _ = tx_client.send(Err(err)).await;
                            }

                            break;
                        }
                    },
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
     * Handles a streaming block read.
     * Opens the block file at the requested offset and streams
     * data chunks back to the client through a channel.
     *
     *  @param request - [ReadRequest] with block ID and byte offset.
     *  @return ServiceResult<Response<ReadStream>> - Stream of data chunks.
     */
    async fn read(&self, request: Request<ReadRequest>) -> ServiceResult<Response<ReadStream>> {
        let req = request.into_inner();
        let (tx, rx) = mpsc::channel(8);
        let out = ReceiverStream::new(rx);
        let msg_size = self.message_size;

        let mut buf = vec![0u8; msg_size];
        let data_mgr = self.data_mgr.clone();
        let logger = self.log_mgr.clone();

        tokio::spawn(async move {
            let mut reader = match data_mgr.read_buf(&req.block_id, msg_size, req.offset).await {
                Ok(reader) => reader,
                Err(e) => {
                    logger.write_status(&e);
                    let _ = tx.send(Err(e.clone())).await;
                    return Err(e);
                }
            };

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        logger.write(LogLevel::Info, || {
                            format!("Completed read for block {}", req.block_id)
                        });
                        break;
                    }
                    Ok(n @ 1..) => {
                        let res = ReadResponse {
                            block_id: req.block_id.clone(),
                            data: buf[..n].to_vec(),
                        };

                        tx.send(Ok(res)).await.map_err(|e| {
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
     * Creates a new [DataNodeService].
     * Resolves the local hostname, configures logging and block storage.
     *
     *  @param args - CLI arguments (port, log level, silent mode).
     *  @param config - [RustDFSConfig] with cluster-wide settings.
     *  @return Result<DataNodeService> - Initialized service or error.
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
            heartbeat_interval: config.data_node.heartbeat_interval,
            replica_connection_ttl: config.data_node.replica_connection_ttl,
        })
    }

    /**
     * Starts the Data Node gRPC server and concurrently registers
     * with the Name Node (with exponential-backoff retries).
     *
     *  @return Result<()> - Resolves when the server shuts down.
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
        let heartbeat_interval = self.heartbeat_interval;

        // Spawn the replication connection TTL reaper task.
        let reaper_nodes = Arc::clone(&self.data_nodes);
        let reaper_ttl = self.replica_connection_ttl;
        tokio::spawn(async move {
            // Recheck at half the TTL, at least every 30 seconds.
            let recheck = (reaper_ttl / 2).clamp(1, 30);
            let mut interval = tokio::time::interval(Duration::from_secs(recheck));
            loop {
                interval.tick().await;
                reaper_nodes.remove_stale(reaper_ttl).await;
            }
        });

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
            name_node_lifecycle(
                logger.clone(),
                health_rep.clone(),
                host,
                name_host,
                heartbeat_interval,
            ),
        );

        health_rep
            .set_not_serving::<DataNodeServer<DataNodeService>>()
            .await;

        res.0?;
        Ok(())
    }
}

/**
 * Manages the full Name Node lifecycle: connect, register, heartbeat, re-register.
 * Runs in an infinite loop — on heartbeat failure, re-enters the registration path.
 *
 *  @param logger - LogManager for logging.
 *  @param health_rep - HealthReporter for service health status.
 *  @param host - HostAddr of this Data Node.
 *  @param name_host - HostAddr of the Name Node.
 *  @param heartbeat_interval - Seconds between heartbeat RPCs.
 */
async fn name_node_lifecycle(
    logger: LogManager,
    health_rep: HealthReporter,
    host: HostAddr,
    name_host: HostAddr,
    heartbeat_interval: u64,
) {
    loop {
        // Phase 1: Connect and register with exponential backoff.
        let retry_strat = ExponentialBackoff::from_millis(100)
            .factor(1)
            .max_delay_millis(1000)
            .max_interval(10000)
            .map(jitter);

        let client = Retry::spawn(retry_strat, || {
            connect_and_register(
                logger.clone(),
                health_rep.clone(),
                host.clone(),
                name_host.clone(),
            )
        })
        .await;

        let mut client = match client {
            Ok(c) => c,
            Err(e) => {
                logger.write(LogLevel::Error, || {
                    format!("Registration failed permanently: {}", e)
                });
                return;
            }
        };

        // Phase 2: Heartbeat loop — runs until the name node becomes unreachable.
        logger.write(LogLevel::Info, || {
            format!(
                "Starting heartbeat loop (interval={}s) to {}:{}",
                heartbeat_interval, name_host.hostname, name_host.port
            )
        });

        let mut interval = tokio::time::interval(Duration::from_secs(heartbeat_interval));
        loop {
            interval.tick().await;
            let result = client
                .heartbeat(HeartbeatRequest {
                    host: host.hostname.clone(),
                    port: host.port as u32,
                })
                .await;

            if let Err(e) = result {
                logger.write(LogLevel::Error, || {
                    format!(
                        "Heartbeat to {}:{} failed: {}. Re-registering...",
                        name_host.hostname, name_host.port, e
                    )
                });

                health_rep
                    .set_not_serving::<DataNodeServer<DataNodeService>>()
                    .await;

                break;
            }
        }
    }
}

/**
 * Connects to the Name Node and sends a registration request.
 * Returns the connected client for reuse in the heartbeat loop.
 *
 *  @param logger - LogManager for logging.
 *  @param health_rep - HealthReporter for service health status.
 *  @param host - HostAddr of this Data Node.
 *  @param name_host - HostAddr of the Name Node.
 *  @return RetryResult<NameNodeClient<Channel>> - Connected client or transient error.
 */
async fn connect_and_register(
    logger: LogManager,
    health_rep: HealthReporter,
    host: HostAddr,
    name_host: HostAddr,
) -> RetryResult<NameNodeClient<Channel>> {
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

    let mut client = client.unwrap();
    let res = client
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

    logger.write(LogLevel::Info, || {
        format!(
            "Registered with NameNode at {}:{}",
            name_host.hostname, name_host.port
        )
    });

    Ok(client)
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
