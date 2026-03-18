use futures::future;
use rand::seq::SliceRandom;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use rustdfs_shared::conn::DataNodeManager;
use rustdfs_shared::logging::LogManager;
use rustdfs_shared::{host::HostAddr, result::ServiceResult};

/**
 * Manages the file namespace for the name node.
 * Tracks file-to-block mappings, write leases, and file versioning.
 *  => Uses a [VecDeque] per file path to keep up to two versions
 *     (the current completed version and an in-progress write).
 *  => Thread-safe via [RwLock] on the file map.
 *
 *  @field files - Map of file path to versioned [FileDescriptor] deque.
 *  @field log_mgr - [LogManager] for logging file operations.
 *  @field lease_duration - Write lease duration in seconds.
 *  @field replica_ct - Number of replica copies per block.
 *  @field block_size - Maximum block size in bytes.
 */
#[derive(Debug)]
pub struct FileManager {
    files: RwLock<HashMap<String, VecDeque<FileDescriptor>>>,
    log_mgr: LogManager,
    lease_duration: u32,
    replica_ct: usize,
    block_size: usize,
}

/**
 * Describes a file version, including its block layout and write status.
 *
 *  @field status - Current [WriteStatus] (InProgress or Complete).
 *  @field blocks - Ordered list of [BlockDescriptor]s comprising this file.
 */
#[derive(Debug, Clone)]
pub struct FileDescriptor {
    pub status: WriteStatus,
    pub blocks: Vec<BlockDescriptor>,
}

/**
 * Describes a single data block within a file.
 *
 *  @field id - UUID string identifying this block.
 *  @field size - Maximum size of the block in bytes.
 *  @field nodes - Ordered list of [HostAddr]s: first is primary, rest are replicas.
 */
#[derive(Debug, Clone)]
pub struct BlockDescriptor {
    pub id: String,
    pub size: u64,
    pub nodes: Vec<HostAddr>,
}

/**
 * Tracks the write lifecycle of a file version.
 *
 *  @variant InProgress - Write lease active; holds the operation ID and expiry.
 *  @variant Complete - Write is finalized; the file is readable.
 */
#[derive(Debug, Clone)]
pub enum WriteStatus {
    InProgress { id: String, expire: u64 },
    Complete,
}

impl FileManager {
    /**
     * Creates a new [FileManager].
     *
     *  @param log_mgr - Shared [LogManager] instance.
     *  @param lease_duration - Lease duration in seconds.
     *  @param replica_ct - Number of replica copies per block.
     *  @param block_size - Maximum block size in bytes.
     *  @return FileManager
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
     * Initializes a file write by allocating blocks and acquiring a lease.
     * If an in-progress lease has expired it is replaced; if the file
     * already has two versions the oldest is evicted.
     *
     *  @param operation_id - Unique ID for this write operation.
     *  @param file_name - Target file path in the namespace.
     *  @param file_size - Total file size in bytes to compute block count.
     *  @param data_nodes - [DataNodeManager] used to select storage nodes.
     *  @return ServiceResult<(FileDescriptor, u64)> - Block assignments and lease expiry.
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

        if let Some(desc) = deque.back() {
            match &desc.status {
                WriteStatus::InProgress { expire, .. } if time < *expire => {
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
            }
        }

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

    /**
     * Marks a file write as complete, transitioning its status to [WriteStatus::Complete].
     * Fails if the operation ID does not match the active lease.
     *
     *  @param file_name - File path in the namespace.
     *  @param operation_id - ID of the write operation that started the lease.
     *  @return ServiceResult<()> - Success or error.
     */
    pub async fn complete_write(&self, file_name: &str, operation_id: &str) -> ServiceResult<()> {
        let mut files = self.files.write().await;

        let desc = files
            .get_mut(file_name)
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

    /**
     * Renews the write lease for a file, extending the expiry timestamp.
     *
     *  @param file_name - File path in the namespace.
     *  @param operation_id - ID of the write operation holding the lease.
     *  @return ServiceResult<u64> - New lease expiry in epoch seconds.
     */
    pub async fn renew_lease(&self, file_name: &str, operation_id: &str) -> ServiceResult<u64> {
        let time = self.get_time_sec()?;
        let mut files = self.files.write().await;

        let desc = files
            .get_mut(file_name)
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
     * Reads the latest completed file descriptor for a given file.
     * Skips any in-progress versions and returns the most recent
     * completed one.
     *
     *  @param file_name - File path in the namespace.
     *  @return ServiceResult<FileDescriptor> - File metadata or not-found error.
     */
    pub async fn read(&self, file_name: &str) -> ServiceResult<FileDescriptor> {
        let files = self.files.read().await;

        let deque = files.get(file_name).ok_or_else(|| {
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

    /**
     * Allocates the specified number of blocks, each assigned
     * to randomly selected data nodes (primary + replicas).
     *
     *  @param count - Number of blocks to allocate.
     *  @param data_nodes - [DataNodeManager] for selecting storage nodes.
     *  @return ServiceResult<Vec<BlockDescriptor>> - Allocated block descriptors.
     */
    async fn allocate_blocks(
        &self,
        count: u32,
        data_nodes: &DataNodeManager,
    ) -> ServiceResult<Vec<BlockDescriptor>> {
        let futs = future::join_all(
            (0..count)
                .map(|_| async {
                    Ok::<BlockDescriptor, Status>(BlockDescriptor {
                        id: Uuid::new_v4().to_string(),
                        size: self.block_size as u64,
                        nodes: self.select_nodes(data_nodes).await.unwrap(),
                    })
                })
                .collect::<Vec<_>>(),
        );

        futs.await
            .into_iter()
            .collect::<ServiceResult<Vec<BlockDescriptor>>>()
    }

    /**
     * Randomly selects a primary data node plus `replica_ct` replicas.
     * Returns an error if not enough data nodes are registered.
     */
    // randomly selects a primary data node and replica nodes
    // for write operation
    async fn select_nodes(&self, data_nodes: &DataNodeManager) -> ServiceResult<Vec<HostAddr>> {
        let mut keys = data_nodes.get_hosts().await;

        if keys.len() <= self.replica_ct {
            let err = status_not_enough_nodes(keys.len(), self.replica_ct);
            self.log_mgr.write_status(&err);
            return Err(err);
        }

        let (selected, _) = keys.partial_shuffle(&mut rand::rng(), self.replica_ct + 1);

        Ok(selected.to_vec())
    }

    /**
     * Returns the current time in epoch seconds.
     */
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
