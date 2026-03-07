use futures::future::join_all;
use std::collections::HashMap;
use std::net::SocketAddr;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tonic_health::server as health_server;
use tonic_reflection::server::Builder;

use rustdfs_shared::args::RustDFSArgs;
use rustdfs_shared::config::RustDFSConfig;
use rustdfs_shared::data_conn::{DataNodeConn, DataNodeManager};
use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_shared::node::GenericNode;
use rustdfs_shared::proto::DATA_FILE_DESCRIPTOR_SET;
use rustdfs_shared::proto::data_node_server::{DataNode, DataNodeServer};
use rustdfs_shared::proto::{
    DataReadRequest, DataReadResponse, DataWriteRequest, DataWriteResponse,
};
use rustdfs_shared::result::{Result, ServiceResult};

use crate::data_mgr::DataDirManager;

/**
 * Data Node service implementation for RustDFS.
 *
 * Handles read and write requests for data blocks,
 * and manages replication to other data nodes.
 */
#[derive(Debug)]
pub struct DataNodeService {
    id: String,
    self_node: GenericNode,
    data_nodes: DataNodeManager,
    data_mgr: DataDirManager,
    log_mgr: LogManager,
}

#[tonic::async_trait]
impl DataNode for DataNodeService {
    /**
     * Writes a block of data to the data node.
     * Also replicates the block to other data nodes as specified.
     *
     *  @param request - DataWriteRequest containing block ID, data, and replica node IDs.
     *  @return Result<Response<DataWriteResponse>> - Response indicating success or failure.
     */
    async fn write(
        &self,
        request: Request<DataWriteRequest>,
    ) -> ServiceResult<Response<DataWriteResponse>> {
        let request_ref = request.get_ref();
        let mut repls = Vec::new();
        let mut ids = Vec::new();

        self.data_mgr
            .write_block(&request_ref.block_id, &request_ref.data)?;

        for id in request_ref.replica_node_ids.iter() {
            if id == &self.id {
                continue;
            }

            ids.push(id.clone());

            repls.push(self.data_nodes.get_conn(id)?.write(DataWriteRequest {
                block_id: request_ref.block_id.clone(),
                data: request_ref.data.clone(),
                replica_node_ids: vec![],
            }));
        }

        let failed = join_all(repls)
            .await
            .into_iter()
            .enumerate()
            .filter(|(_, r)| r.is_err() || !r.as_ref().unwrap().success)
            .map(|(i, _)| ids[i].clone())
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

        Ok(Response::new(DataWriteResponse { success: true }))
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
        let mut data_dir = None;
        let mut data_nodes = HashMap::new();
        let mut log_file = None;
        let mut node = None;

        for (k, dn_config) in config.data_nodes {
            if args.id == k {
                node = Some(GenericNode {
                    host: dn_config.host,
                    port: dn_config.port,
                });
                data_dir = Some(dn_config.data_dir);
                log_file = Some(dn_config.log_file);
                continue;
            }

            data_nodes.insert(
                k.clone(),
                DataNodeConn::new(k.clone(), dn_config.host, dn_config.port),
            );
        }

        if node.is_none() || data_dir.is_none() || log_file.is_none() {
            return Err(err_misconfigured_svc());
        }

        let logger = LogManager::new(
            log_file.clone().unwrap(),
            args.log_level,
            args.silent,
        )?;

        Ok(DataNodeService {
            id: args.id,
            self_node: node.unwrap(),
            data_nodes: DataNodeManager::new(data_nodes, logger.clone()),
            data_mgr: DataDirManager::new(&data_dir.unwrap(), logger.clone())?,
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
        let addr = Into::<Result<SocketAddr>>::into(&self.self_node)?;
        let logger = self.log_mgr.clone();

        // should remove this or make it optional via config
        // only added this for testing
        let svc_reflection = Builder::configure()
            .register_encoded_file_descriptor_set(DATA_FILE_DESCRIPTOR_SET)
            .build_v1()
            .unwrap();

        logger.write(LogLevel::Info, || {
            format!(
                "Starting DataNodeServer with ID {} at {} on port {}",
                self.id,
                addr.ip(),
                addr.port()
            )
        });

        health_rep
            .set_serving::<DataNodeServer<DataNodeService>>()
            .await;

        let res = Server::builder()
            .add_service(health_svc)
            .add_service(svc_reflection)
            .add_service(DataNodeServer::new(self))
            .serve(addr)
            .await
            .map_err(|e| {
                let err = RustDFSError::TonicError(e);
                logger.write_err(&err);
                err
            });

        health_rep
            .set_not_serving::<DataNodeServer<DataNodeService>>()
            .await;

        res
    }
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

// Helper functions for error statuses

fn err_misconfigured_svc() -> RustDFSError {
    RustDFSError::CustomError("Misconfigured Data Node service".to_string())
}

fn status_err_forwarding(nodes: &[String]) -> Status {
    let ids_str = nodes.join(",");
    let log = format!("Error forwarding to data nodes: {}", ids_str);
    Status::internal(log)
}
