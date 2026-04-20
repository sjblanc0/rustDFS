use core::str;
use futures::StreamExt;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::Channel;
use uuid::Uuid;

use rustdfs_proto::data::ReadRequest as DataReadRequest;
use rustdfs_proto::data::ReadResponse as DataReadResponse;
use rustdfs_proto::data::WriteRequest;
use rustdfs_proto::data::data_node_client::DataNodeClient;
use rustdfs_proto::data::write_request::ReplicaNode;
use rustdfs_proto::name::AddBlockRequest;
use rustdfs_proto::name::Block;
use rustdfs_proto::name::ReadRequest as NameReadRequest;
use rustdfs_proto::name::RenewLeaseRequest;
use rustdfs_proto::name::WriteEndRequest;
use rustdfs_proto::name::WriteStartRequest;
use rustdfs_proto::name::block::Node;
use rustdfs_proto::name::name_node_client::NameNodeClient;

use crate::error::RustDFSError;
use crate::host::HostAddr;
use crate::result::Result;

const CHANNEL_SIZE: usize = 8;

/**
 * Filesystem handle — connection to the name node.
 *
 *  @field name - gRPC client to the Name Node.
 */
pub struct RustDFSClient {
    name: NameNodeClient<Channel>,
}

impl RustDFSClient {
    /**
     * Connect to the name node.
     *
     *  @param host - Host string in "host:port" format.
     *  @param port - Port number.
     *  @return Result<Self>
     */
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let host_addr = HostAddr {
            hostname: host.to_string(),
            port,
        };
        let name = name_client(&host_addr).await?;
        Ok(RustDFSClient { name })
    }

    /**
     * Connect to the name node from a "host:port" string.
     *
     *  @param host_str - Host string in "host:port" format.
     *  @return Result<Self>
     */
    pub async fn connect_from_str(host_str: &str) -> Result<Self> {
        let host_addr = HostAddr::from_str(host_str)?;
        let name = name_client(&host_addr).await?;

        Ok(RustDFSClient { name })
    }

    /**
     * Disconnect and release resources.
     */
    pub async fn disconnect(self) {
        drop(self);
    }

    /**
     * Open a file for reading or writing.
     *
     *  @param path - Remote file path.
     *  @param flags - [O_RDONLY] or [O_WRONLY].
     *  @return Result<RustDFSFile>
     */
    pub async fn open(&mut self, path: &str, flags: i32) -> Result<RustDFSFile> {
        if flags == crate::O_RDONLY {
            self.open_read(path).await
        } else if flags == crate::O_WRONLY {
            self.open_write(path).await
        } else {
            Err(RustDFSError::Custom(format!("Invalid flags: {}", flags)))
        }
    }

    async fn open_read(&mut self, path: &str) -> Result<RustDFSFile> {
        let name_req = NameReadRequest {
            file_name: path.to_string(),
        };
        let name_res = self
            .name
            .read(name_req)
            .await
            .map_err(RustDFSError::TonicStatus)?
            .into_inner();

        let blocks = name_res.blocks;

        Ok(RustDFSFile {
            state: FileState::Read(ReadState {
                blocks,
                current_block_idx: 0,
                stream: None,
                offset: 0,
                overflow: Vec::new(),
                overflow_pos: 0,
                eof: false,
            }),
        })
    }

    async fn open_write(&mut self, path: &str) -> Result<RustDFSFile> {
        let op_id = Uuid::new_v4().to_string();
        let start_req = WriteStartRequest {
            file_name: path.to_string(),
            operation_id: op_id.clone(),
        };
        let start_res = self
            .name
            .write_start(start_req)
            .await
            .map_err(RustDFSError::TonicStatus)?
            .into_inner();

        let block_size = start_res.block_size;
        let msg_size = start_res.message_size as usize;

        let lease_handle = spawn_lease_renewal(
            self.name.clone(),
            path.to_string(),
            op_id.clone(),
            start_res.expire,
        );

        Ok(RustDFSFile {
            state: FileState::Write(WriteState {
                name: self.name.clone(),
                file_name: path.to_string(),
                op_id,
                block_size,
                msg_size,
                current_block: None,
                lease_handle,
                closed: false,
            }),
        })
    }
}

/**
 * File handle — an open file for reading or writing.
 * Internally tracks either [ReadState] or [WriteState].
 */
pub struct RustDFSFile {
    state: FileState,
}

enum FileState {
    Read(ReadState),
    Write(WriteState),
}

struct ReadState {
    blocks: Vec<Block>,
    current_block_idx: usize,
    stream: Option<Streaming<DataReadResponse>>,
    offset: u64,
    overflow: Vec<u8>,
    overflow_pos: usize,
    eof: bool,
}

struct CurrentBlock {
    tx: mpsc::Sender<WriteRequest>,
    block_id: String,
    nodes: Vec<Node>,
    ack_handle: JoinHandle<std::result::Result<(), RustDFSError>>,
    sent: u64,
}

struct WriteState {
    name: NameNodeClient<Channel>,
    file_name: String,
    op_id: String,
    block_size: u64,
    msg_size: usize,
    current_block: Option<CurrentBlock>,
    lease_handle: JoinHandle<()>,
    closed: bool,
}

impl RustDFSFile {
    /**
     * Read up to buf.len() bytes. Returns bytes read, 0 on EOF.
     * Only valid for files opened with [O_RDONLY].
     *
     *  @param buf - Buffer to read into.
     *  @return Result<usize> - Bytes read.
     */
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let state = match &mut self.state {
            FileState::Read(s) => s,
            FileState::Write(_) => {
                return Err(RustDFSError::Custom(
                    "Cannot read from a write handle".to_string(),
                ));
            }
        };

        if state.eof || buf.is_empty() {
            return Ok(0);
        }

        let mut written = 0usize;

        // Drain overflow buffer first
        if state.overflow_pos < state.overflow.len() {
            let avail = state.overflow.len() - state.overflow_pos;
            let n = avail.min(buf.len());
            buf[..n].copy_from_slice(&state.overflow[state.overflow_pos..state.overflow_pos + n]);
            state.overflow_pos += n;
            written += n;

            if state.overflow_pos >= state.overflow.len() {
                state.overflow.clear();
                state.overflow_pos = 0;
            }

            if written >= buf.len() {
                return Ok(written);
            }
        }

        // Read from blocks
        loop {
            if written >= buf.len() {
                break;
            }

            // Open stream for current block if needed
            if state.stream.is_none() {
                if state.current_block_idx >= state.blocks.len() {
                    state.eof = true;
                    break;
                }

                let block = &state.blocks[state.current_block_idx];
                let node_count = block.nodes.len();

                let mut opened = false;
                for (i, node) in block.nodes.iter().enumerate() {
                    let host = to_host_addr(node);
                    let req = DataReadRequest {
                        block_id: block.block_id.clone(),
                        offset: state.offset,
                    };
                    match data_client(&host).await {
                        Ok(mut client) => match client.read(req).await {
                            Ok(response) => {
                                state.stream = Some(response.into_inner());
                                opened = true;
                                break;
                            }
                            Err(e) => {
                                let err = RustDFSError::TonicStatus(e);
                                if i == node_count - 1 {
                                    return Err(err);
                                }
                            }
                        },
                        Err(e) => {
                            if i == node_count - 1 {
                                return Err(e);
                            }
                        }
                    }
                }

                if !opened {
                    let msg = format!(
                        "Read failed for block {}",
                        state.blocks[state.current_block_idx].block_id
                    );
                    return Err(RustDFSError::Custom(msg));
                }
            }

            // Read from active stream
            if let Some(stream) = &mut state.stream {
                match stream.next().await {
                    Some(Ok(msg)) => {
                        state.offset += msg.data.len() as u64;
                        let remaining = buf.len() - written;
                        if msg.data.len() <= remaining {
                            buf[written..written + msg.data.len()].copy_from_slice(&msg.data);
                            written += msg.data.len();
                        } else {
                            buf[written..written + remaining]
                                .copy_from_slice(&msg.data[..remaining]);
                            written += remaining;
                            state.overflow = msg.data[remaining..].to_vec();
                            state.overflow_pos = 0;
                        }
                    }
                    Some(Err(e)) => {
                        // Stream error — try next replica
                        state.stream = None;
                        let block = &state.blocks[state.current_block_idx];
                        let node_count = block.nodes.len();

                        // Find next replica to try
                        let err = RustDFSError::TonicStatus(e);

                        // We can't easily track which replica we were on,
                        // so re-open from offset (failover handled by opening again)
                        if node_count <= 1 {
                            return Err(err);
                        }
                        // Will retry on next loop iteration
                    }
                    None => {
                        // Block stream ended — move to next block
                        state.stream = None;
                        state.current_block_idx += 1;
                        state.offset = 0;
                    }
                }
            }
        }

        Ok(written)
    }

    /**
     * Write data. Returns bytes written.
     * Only valid for files opened with [O_WRONLY].
     * The library handles block boundaries, replication, and lease renewal.
     *
     *  @param data - Data to write.
     *  @return Result<usize> - Bytes written.
     */
    pub async fn write(&mut self, data: &[u8]) -> Result<usize> {
        let state = match &mut self.state {
            FileState::Write(s) => s,
            FileState::Read(_) => {
                return Err(RustDFSError::Custom(
                    "Cannot write to a read handle".to_string(),
                ));
            }
        };

        if data.is_empty() {
            return Ok(0);
        }

        let mut total_written = 0usize;

        while total_written < data.len() {
            // Allocate a new block if needed
            if state.current_block.is_none() {
                allocate_block(state).await?;
            }

            let cb = state.current_block.as_ref().unwrap();
            let remaining_in_block = (state.block_size - cb.sent) as usize;

            if remaining_in_block == 0 {
                // Current block is full — finalize it and allocate a new one
                finalize_current_block(state).await?;
                allocate_block(state).await?;
                continue;
            }

            let remaining_data = &data[total_written..];
            let chunk_len = remaining_data.len().min(remaining_in_block);
            let mut pos = 0;

            while pos < chunk_len {
                let end = (pos + state.msg_size).min(chunk_len);
                let chunk = &remaining_data[pos..end];

                let cb = state.current_block.as_ref().unwrap();
                let req = WriteRequest {
                    block_id: cb.block_id.clone(),
                    data: chunk.to_vec(),
                    replicas: to_replica_nodes(&cb.nodes[1..]),
                };

                cb.tx.send(req).await.map_err(RustDFSError::DataWrite)?;

                pos += chunk.len();
            }

            // Update sent count
            let cb = state.current_block.as_mut().unwrap();
            cb.sent += chunk_len as u64;
            total_written += chunk_len;
        }

        Ok(total_written)
    }

    /**
     * Flush buffered write data to the current data node.
     * Only valid for files opened with [O_WRONLY].
     *
     *  @return Result<()>
     */
    pub async fn flush(&mut self) -> Result<()> {
        match &mut self.state {
            FileState::Write(_) => {
                // Data is sent immediately through the channel,
                // so flush is a no-op at this level.
                Ok(())
            }
            FileState::Read(_) => Err(RustDFSError::Custom(
                "Cannot flush a read handle".to_string(),
            )),
        }
    }

    /**
     * Close the file. For writes: sends WriteEnd, releases lease.
     *
     *  @return Result<()>
     */
    pub async fn close(mut self) -> Result<()> {
        match &mut self.state {
            FileState::Read(state) => {
                state.stream = None;
                state.eof = true;
                Ok(())
            }
            FileState::Write(state) => {
                if state.closed {
                    return Ok(());
                }
                state.closed = true;

                let mut err = None;

                // Finalize current block if one is open
                if state.current_block.is_some()
                    && let Err(e) = finalize_current_block(state).await
                {
                    err = Some(e);
                }

                // Abort lease renewal
                state.lease_handle.abort();

                // Send WriteEnd
                let end_req = WriteEndRequest {
                    file_name: state.file_name.clone(),
                    operation_id: state.op_id.clone(),
                    success: err.is_none(),
                };
                state
                    .name
                    .write_end(end_req)
                    .await
                    .map_err(RustDFSError::TonicStatus)?;

                match err {
                    Some(e) => Err(e),
                    None => Ok(()),
                }
            }
        }
    }
}

/**
 * Allocates a new block for the write state by calling AddBlock
 * on the name node.
 */
async fn allocate_block(state: &mut WriteState) -> Result<()> {
    let add_req = AddBlockRequest {
        file_name: state.file_name.clone(),
        operation_id: state.op_id.clone(),
    };

    let add_res = state
        .name
        .add_block(add_req)
        .await
        .map_err(RustDFSError::TonicStatus)?
        .into_inner();

    let block = add_res.block.unwrap();
    let data_host = to_host_addr(&block.nodes[0]);
    let mut data = data_client(&data_host).await?;

    let (tx, rx) = mpsc::channel::<WriteRequest>(CHANNEL_SIZE);
    let in_stream = ReceiverStream::new(rx);

    let mut out_stream = data
        .write(in_stream)
        .await
        .map_err(RustDFSError::TonicStatus)?
        .into_inner();

    // Spawn a background task to drain acknowledgements
    let ack_handle = tokio::spawn(async move {
        while let Some(res) = out_stream.next().await {
            match res {
                Ok(_) => {}
                Err(e) => {
                    return Err(RustDFSError::TonicStatus(e));
                }
            }
        }
        Ok(())
    });

    state.current_block = Some(CurrentBlock {
        tx,
        block_id: block.block_id.clone(),
        nodes: block.nodes,
        ack_handle,
        sent: 0,
    });

    Ok(())
}

/**
 * Finalizes the current write block by dropping the sender
 * and waiting for the ack task to complete.
 */
async fn finalize_current_block(state: &mut WriteState) -> Result<()> {
    if let Some(cb) = state.current_block.take() {
        // Drop sender to signal end of stream
        drop(cb.tx);

        // Wait for ack task to complete
        match cb.ack_handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(RustDFSError::Custom(format!("Ack task panicked: {}", e))),
        }
    } else {
        Ok(())
    }
}

// ── Helper functions ─────────────────────────────────────────────────────────

async fn name_client(host: &HostAddr) -> Result<NameNodeClient<Channel>> {
    let endpoint = host.to_endpoint()?;
    NameNodeClient::connect(endpoint)
        .await
        .map_err(RustDFSError::Tonic)
}

async fn data_client(host: &HostAddr) -> Result<DataNodeClient<Channel>> {
    let endpoint = host.to_endpoint()?;
    DataNodeClient::connect(endpoint)
        .await
        .map_err(RustDFSError::Tonic)
}

fn to_host_addr(node: &Node) -> HostAddr {
    HostAddr {
        hostname: node.host.clone(),
        port: node.port as u16,
    }
}

fn to_replica_nodes(nodes: &[Node]) -> Vec<ReplicaNode> {
    nodes
        .iter()
        .map(|n| ReplicaNode {
            host: n.host.clone(),
            port: n.port,
        })
        .collect()
}

fn spawn_lease_renewal(
    mut name: NameNodeClient<Channel>,
    file_name: String,
    operation_id: String,
    expire: u64,
) -> JoinHandle<()> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let lease_secs = expire.saturating_sub(now);
    let interval = Duration::from_secs(lease_secs / 2).max(Duration::from_secs(1));

    tokio::spawn(async move {
        loop {
            time::sleep(interval).await;

            let req = RenewLeaseRequest {
                file_name: file_name.clone(),
                operation_id: operation_id.clone(),
            };

            match name.renew_lease(req).await {
                Ok(_) => {}
                Err(_) => break,
            }
        }
    })
}
