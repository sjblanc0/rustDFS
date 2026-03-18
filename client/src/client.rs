use futures::StreamExt;
use futures::io::Write;
use tokio::sync::Mutex;
use futures::stream;
use prost::Name;
use prost::bytes::Buf;
use tokio_stream::wrappers::ReceiverStream;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::join;
use tokio::sync::mpsc;
use std::sync::Arc;
use core::str;
use uuid::Uuid;
use tonic::transport::Channel;

use rustdfs_proto::name::name_node_client::NameNodeClient;
use rustdfs_proto::data::WriteRequest;
use rustdfs_proto::data::write_request::ReplicaNode;
use rustdfs_proto::name::Block;
use rustdfs_proto::name::WriteEndRequest;
use rustdfs_proto::name::ReadRequest as NameReadRequest;
use rustdfs_proto::data::ReadRequest as DataReadRequest;
use rustdfs_proto::name::block::Node;
use rustdfs_proto::data::data_node_client::DataNodeClient;
use rustdfs_proto::data::data_node_server::DataNode;
use rustdfs_proto::name::WriteStartRequest;

use crate::error::RustDFSError;
use crate::out::OutManager;
use crate::result::Result;
use crate::args::{Operation, RustDFSArgs};
use crate::host::{self, HostAddr};
use crate::out::Verbosity;

const CHANNEL_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub struct RustDFSClient {
    host: HostAddr,
    source: String,
    dest: String,
    out: OutManager,
}

impl RustDFSClient {

    pub async fn new(args: RustDFSArgs) -> Result<Self> {
        Ok(RustDFSClient {
            host: HostAddr::from_str(&args.host)?,
            source: args.source,
            dest: args.dest,
            out: OutManager { verbosity: args.verbosity },
        })
    }

    pub async fn run(&mut self, op: Operation) -> Result<()> {
        match op {
            Operation::Write => {
                match self.write().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            },
            Operation::Read => {
                match self.read().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            },
        }
    }

    async fn write(&mut self) -> Result<()> {
        let op_id = Uuid::new_v4().to_string();
        let mut name = name_client(&self.host, &self.out).await?;
        let start_req = write_start_req(&self.source, &self.dest, &op_id).await?;
        let start_res = name.write_start(start_req)
            .await
            .map_err(|e| RustDFSError::TonicStatusError(e))?
            .into_inner();

        let mut err = None;
        let reader = reader(&self.source, start_res.message_size as usize).await?;

        'outer: for block in &start_res.blocks {
            let data_host = to_host_addr(&block.nodes[0]);
            let (tx, rx) = mpsc::channel::<WriteRequest>(CHANNEL_SIZE);
            let in_stream = ReceiverStream::new(rx);
            let reader = reader.clone();
            let mut data = data_client(&data_host).await?;
            let out_a = self.out.clone();
            let out_b = self.out.clone();

            let mut out_stream = data.write(in_stream)
                .await
                .map_err(|e| RustDFSError::TonicStatusError(e))?
                .into_inner();

            match join!(
                async move {
                    let reader_ref = reader.clone();
                    let mut reader = reader_ref.lock().await;
                    let msg_size = start_res.message_size as usize;
                    let mut buf = vec![0u8; msg_size];
                    let mut sent: u64 = 0;

                    loop {
                        let remain = (block.block_size - sent) as usize;
                        let n = remain.min(buf.len());

                        if n == 0 {
                            break;
                        }

                        match reader.read(&mut buf[..n]).await {
                            Ok(0) => {
                                break;
                            },
                            Ok(n) => {
                                sent += n as u64;

                                let req = WriteRequest {
                                    block_id: block.block_id.clone(),
                                    data: buf[..n].to_vec(),
                                    replicas: to_replica_nodes(&block.nodes[1..]),
                                };

                                tx.send(req)
                                    .await
                                    .map_err(|e| {
                                        let err = RustDFSError::DataWriteError(e);
                                        out_a.write_err(&err);
                                        err
                                    })?;
                            },
                            Err(e) => {
                                let err = RustDFSError::IoError(e);
                                out_a.write_err(&err);
                                return Err(err);
                            },
                        }
                    }

                    Ok::<(), RustDFSError>(())
                },
                async move {
                    while let Some(res) = out_stream.next().await {
                        match res {
                            Ok(_) => {},
                            Err(e) => {
                                let err = RustDFSError::TonicStatusError(e);
                                out_b.write_err(&err);
                                return Err(err);
                            }
                        }
                    }

                    Ok::<(), RustDFSError>(())
                }
            ) {
                (Err(e), _) | (_, Err(e)) => {
                    err = Some(e);
                    break 'outer;
                },
                _ => {},
            }
        }

        let end_req = write_end_req(&self.dest, &op_id, err.is_none());
        name.write_end(end_req)
            .await
            .map_err(|e| {
                let err = RustDFSError::TonicStatusError(e);
                self.out.write_err(&err);
                err
            })?;
        
        match err {
            Some(e) => {
                Err(e)
            },
            None => {
                self.out.write(Verbosity::Info, || {
                    format!("Finished writing file: {}", self.dest)
                });
                Ok(())
            },
        }
    }

    async fn read(&mut self) -> Result<()> {
        let mut name = name_client(&self.host, &self.out).await?;
        let name_req = name_read_req(&self.source);
        let name_res = name.read(name_req)
            .await
            .map_err(|e| RustDFSError::TonicStatusError(e))?
            .into_inner();

        let mut writer = writer(&self.dest, name_res.message_size as usize)
            .await?;

        for block in name_res.blocks {
            let node_count = block.nodes.len();
            let mut offset = 0u64;

            'retry: for (i, node) in block.nodes.iter().enumerate() {
                let host = to_host_addr(node);
                let req = data_read_req(&block.block_id, offset);
                let mut data = data_client(&host).await?;

                let mut stream = data.read(req)
                    .await
                    .map_err(|e| {
                        let err = RustDFSError::TonicStatusError(e);
                        self.out.write_err(&err);
                        err
                    })?
                    .into_inner();

                while let Some(res) = stream.next().await {
                    match res {
                        Ok(msg) => {
                            offset += msg.data.len() as u64;

                            writer.write(&msg.data)
                                .await
                                .map_err(|e| {
                                    let err = RustDFSError::IoError(e);
                                    self.out.write_err(&err);
                                    err
                                })?;

                            writer.flush()
                                .await
                                .map_err(|e| {
                                    let err = RustDFSError::IoError(e);
                                    self.out.write_err(&err);
                                    err
                                })?;
                        },
                        Err(e) => {
                            if i == node_count - 1 {
                                let str = format!("Read failed for block {}", block.block_id);
                                let err = RustDFSError::CustomError(str.clone());
                                self.out.write_err(&err);
                                return Err(err);
                            } else {
                                let err = RustDFSError::TonicStatusError(e);
                                self.out.write_err(&err);
                                continue 'retry;
                            }
                        }
                    }
                }

                break;
            }
        }

        self.out.write(Verbosity::Info, || {
            format!("Finished reading file: {}", self.dest)
        });
        Ok(())
    }
}

async fn name_client(host: &HostAddr, out: &OutManager) -> Result<NameNodeClient<Channel>> {
    out.write(Verbosity::Info, || {
        format!("Connecting to name node at {}:{}", host.hostname, host.port)
    });
    
    let endpoint = host.to_endpoint()?;

    NameNodeClient::connect(endpoint)
        .await
        .map_err(|e| {
            let err = RustDFSError::TonicError(e);
            err
        })
}

async fn data_client(host: &HostAddr) -> Result<DataNodeClient<Channel>> {
    let endpoint = host.to_endpoint()?;

    DataNodeClient::connect(endpoint)
        .await
        .map_err(|e| {
            let err = RustDFSError::TonicError(e);
            err
        })
}

async fn write_start_req(source: &str, dest: &str, id: &str) -> Result<WriteStartRequest> {
    let req = WriteStartRequest {
        file_name: dest.to_string(),
        operation_id: id.to_string(),
        file_size: fs::metadata(source)
            .await
            .map_err(|e| RustDFSError::IoError(e))?
            .len(),
    };
    Ok(req)
}

fn write_end_req(dest: &str, operation_id: &str, success: bool) -> WriteEndRequest {
    WriteEndRequest {
        file_name: dest.to_string(),
        operation_id: operation_id.to_string(),
        success,
    }
}

fn name_read_req(source: &str) -> NameReadRequest {
    NameReadRequest {
        file_name: source.to_string(),
    }
}

fn data_read_req(block_id: &str, offset: u64) -> DataReadRequest {
    DataReadRequest {
        block_id: block_id.to_string(),
        offset: offset,
    }
}

fn to_host_addr(node: &Node) -> HostAddr {
    HostAddr {
        hostname: node.host.clone(),
        port: node.port as u16,
    }
}

fn to_replica_nodes(nodes: &[Node]) -> Vec<ReplicaNode> {
    nodes.iter()
        .map(|n| ReplicaNode {
            host: n.host.clone(),
            port: n.port,
        })
        .collect()
}

async fn reader(
    fp: &str,
    size: usize,
) -> Result<Arc<Mutex<BufReader<File>>>> {
    let file = File::open(fp)
        .await
        .map_err(|e| RustDFSError::IoError(e))?;
    let reader = BufReader::with_capacity(size, file);
    Ok(Arc::new(Mutex::new(reader)))
}

async fn writer(
    fp: &str,
    size: usize,
) -> Result<BufWriter<File>> {
    let file = File::create(fp)
        .await
        .map_err(|e| RustDFSError::IoError(e))?;
    let writer = BufWriter::with_capacity(size, file);
    Ok(writer)
}
