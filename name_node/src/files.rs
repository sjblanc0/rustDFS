use std::collections::VecDeque;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use std::collections::HashMap;
use futures::future;
use rand::seq::SliceRandom;
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use rustdfs_shared::{host::HostAddr, result::ServiceResult};
use rustdfs_shared::conn::DataNodeManager;
use rustdfs_shared::logging::LogManager;

/**
 * Manages the namespace for the distributed file system.
 *  => Keeps track of files and their associated blocks and data nodes.
 *  => RwLock is used to ensure thread-safe access to the namespace data.
 */
#[derive(Debug)]
pub struct FileManager {
    files: RwLock<HashMap<String, VecDeque<FileDescriptor>>>,
    log_mgr: LogManager,
    lease_duration: u32,
    replica_ct: usize,
    block_size: usize,
}

#[derive(Debug, Clone)]
pub struct FileDescriptor {
    pub status: WriteStatus,
    pub blocks: Vec<BlockDescriptor>,
}

#[derive(Debug, Clone)]
pub struct BlockDescriptor {
    pub id: String,
    pub size: u64,
    pub nodes: Vec<HostAddr>,
}

#[derive(Debug, Clone)]
pub enum WriteStatus {
    InProgress { 
        id: String,
        expire: u64,
    },
    Complete,
}

impl FileManager {
    /**
     * Creates a new FileManager instance.
     */
    pub fn new(
        log_mgr: LogManager,
        lease_duration: u32,
        replica_ct: usize,
        block_size: usize,
    ) -> Self {
        FileManager {
            files: RwLock::new(HashMap::new()),
            log_mgr,
            lease_duration,
            replica_ct,
            block_size,
        }
    }

    /**
     * Adds a new file and its block descriptors to the namespace.
     *
     *  @param file_name - Name of the file.
     *  @param blocks - Vector of BlockDescriptor for the file.
     */
    pub async fn init_write(
        &self, 
        operation_id: &str,
        file_name: &str, 
        file_size: u64, 
        data_nodes: &DataNodeManager,
    ) -> ServiceResult<(FileDescriptor, u64)> {
        let time = self.get_time_sec()?;
        let count = (file_size as f64 / self.block_size as f64).ceil() as u32;
        let mut files = self.files.write().await;

        let deque = files
            .entry(file_name.to_string())
            .or_insert(VecDeque::new());

        if let Some(desc) = deque.back() { match &desc.status {
            WriteStatus::InProgress { expire , .. } if time < *expire => {
                let err = status_file_locked(file_name);
                self.log_mgr.write_status(&err);
                return Err(err);
            }
            WriteStatus::InProgress { .. } => {
                deque.pop_back();
            }
            _ => {
                if deque.len() == 2 {
                    deque.pop_front();
                }
            }
        } }

        let desc = FileDescriptor {
            status: WriteStatus::InProgress { 
                id: operation_id.to_string(),
                expire: time + self.lease_duration as u64,
            },
            blocks: self.allocate_blocks(count, data_nodes).await?,
        };

        deque.push_back(desc.clone());
        Ok((desc, time + self.lease_duration as u64))
    }

    pub async fn complete_write(
        &self, 
        file_name: &str,
        operation_id: &str,
    ) -> ServiceResult<()> {
        let mut files = self.files.write().await;

        let desc = files.get_mut(file_name)
            .ok_or_else(|| {
                let err = status_file_not_found(file_name);
                self.log_mgr.write_status(&err);
                err
            })?
            .back_mut()
            .ok_or_else(|| {
                let err = status_file_not_found(file_name);
                self.log_mgr.write_status(&err);
                err
            })?;

        match desc.status {
            WriteStatus::InProgress { ref id, .. } if id == operation_id => {
                desc.status = WriteStatus::Complete;
                Ok(())
            }
            _ => {
                let err = status_lease_expired(file_name);
                self.log_mgr.write_status(&err);
                Err(err)
            }
        }
    }

    pub async fn renew_lease(
        &self, 
        file_name: &str,
        operation_id: &str,
    ) -> ServiceResult<u64> {
        let time = self.get_time_sec()?;
        let mut files = self.files.write().await;

        let desc = files.get_mut(file_name)
            .ok_or_else(|| {
                let err = status_file_not_found(file_name);
                self.log_mgr.write_status(&err);
                err
            })?
            .back_mut()
            .ok_or_else(|| {
                let err = status_file_not_found(file_name);
                self.log_mgr.write_status(&err);
                err
            })?;

        match &mut desc.status {
            WriteStatus::InProgress { expire, id, .. } if id == operation_id => {
                *expire = time + self.lease_duration as u64;
                Ok(*expire)
            }
            _ => {
                let err = status_lease_expired(file_name);
                self.log_mgr.write_status(&err);
                Err(err)
            }
        }
    }

    /**
     * Retrieves the block descriptors for a given file.
     *
     *  @param file_name - Name of the file.
     *  @return ServiceResult<Vec<BlockDescriptor>> - Vector of BlockDescriptor or error.
     */
    pub async fn read(
        &self, 
        file_name: &str
    ) -> ServiceResult<FileDescriptor> {
        let files = self.files.read().await;

        let deque = files.get(file_name)
            .ok_or_else(|| {
                let err = status_file_not_found(file_name);
                self.log_mgr.write_status(&err);
                err
            })?;
        
        for desc in deque.iter().rev() {
            if let WriteStatus::Complete = &desc.status {
                return Ok(desc.clone());
            }
        }

        let err = status_file_not_found(file_name);
        self.log_mgr.write_status(&err);
        Err(err)
    }

    async fn allocate_blocks(
        &self, 
        count: u32,
        data_nodes: &DataNodeManager,
    ) -> ServiceResult<Vec<BlockDescriptor>> {
        let futs = future::join_all(
            (0..count)
                .map(|_| async {
                    Ok::<BlockDescriptor, Status>(
                        BlockDescriptor {
                            id: Uuid::new_v4().to_string(),
                            size: self.block_size as u64,
                            nodes: self.select_nodes(data_nodes).await.unwrap(),
                        }
                    )
                })
                .collect::<Vec<_>>()
        ); 
        
        futs.await
                .into_iter()
                .collect::<ServiceResult<Vec<BlockDescriptor>>>()
    }

    // randomly selects a primary data node and replica nodes
    // for write operation
    async fn select_nodes(
        &self, 
        data_nodes: &DataNodeManager
    ) -> ServiceResult<Vec<HostAddr>> {
        let mut keys = data_nodes.get_hosts().await;

        if keys.len() <= self.replica_ct {
            let err = status_not_enough_nodes(keys.len(), self.replica_ct);
            self.log_mgr.write_status(&err);
            return Err(err);
        }

        let (selected, _) = keys.partial_shuffle(&mut rand::rng(), self.replica_ct + 1);

        Ok(selected.to_vec())
    }

    fn get_time_sec(&self) -> ServiceResult<u64> {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| {
                let err = status_system_time(e);
                self.log_mgr.write_status(&err);
                err
            })?
            .as_secs();
        Ok(time)
    }
}

// Status helpers

fn status_system_time(e: SystemTimeError) -> Status {
    let msg = format!("System time error: {}", e);
    Status::internal(msg)
}

fn status_file_locked(file: &str) -> Status {
    let msg = format!("File is currently locked for write: {}", file);
    Status::already_exists(msg)
}

fn status_lease_expired(file: &str) -> Status {
    let msg = format!("Lease expired for file: {}", file);
    Status::failed_precondition(msg)
}

fn status_not_enough_nodes(node_ct: usize, replica_ct: usize) -> Status {
    let msg = format!(
        "Not enough data nodes for allocate: available {}, required {}",
        node_ct,
        replica_ct + 1
    );
    Status::internal(msg)
}

fn status_file_not_found(file_name: &str) -> Status {
    let msg = format!("File {} not found in namespace", file_name);
    Status::not_found(msg)
}
