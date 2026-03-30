use futures::future;
use prost::Message;
use rand::seq::SliceRandom;
use rustdfs_shared::error::RustDFSError;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::Status;
use uuid::Uuid;

use rustdfs_proto::persist::journal_entry::Op;
use rustdfs_proto::persist::{
    BlockEntry, Checkpoint, FileEntry, FileStatus, JournalEntry, WriteCompleteEntry,
    WriteStartEntry,
};
use rustdfs_shared::conn::DataNodeManager;
use rustdfs_shared::host::HostAddr;
use rustdfs_shared::logging::{LogLevel, LogManager};
use rustdfs_shared::result::{Result, ServiceResult};

const CHECKPOINT_FILE: &str = "checkpoint";
const JOURNAL_FILE: &str = "journal";
const CHECKPOINT_TMP: &str = "checkpoint.tmp";

/**
 * Manages the file namespace for the name node.
 * Tracks file-to-block mappings, write leases, and file versioning.
 *  => Uses a [VecDeque] per file path to keep up to two versions
 *     (the current completed version and an in-progress write).
 *  => Thread-safe via [RwLock] on the file map.
 *  => Persists mutations to a journal and periodically writes checkpoints.
 *
 *  @field files - Map of file path to versioned [FileDescriptor] deque.
 *  @field log_mgr - [LogManager] for logging file operations.
 *  @field lease_duration - Write lease duration in seconds.
 *  @field replica_ct - Number of replica copies per block.
 *  @field block_size - Maximum block size in bytes.
 *  @field name_dir - Directory containing checkpoint and journal files.
 *  @field txn_id - Monotonically increasing transaction counter.
 *  @field journal_txn_count - Number of journal entries since last checkpoint.
 *  @field checkpoint_txns - Max journal entries before forcing a checkpoint.
 *  @field checkpoint_period - Max seconds between checkpoints.
 *  @field last_checkpoint - Epoch seconds of the most recent checkpoint.
 */
#[derive(Debug)]
pub struct FileManager {
    files: RwLock<HashMap<String, VecDeque<FileDescriptor>>>,
    log_mgr: LogManager,
    lease_duration: u32,
    replica_ct: usize,
    block_size: usize,
    name_dir: PathBuf,
    txn_id: RwLock<u64>,
    journal_txn_count: RwLock<u64>,
    checkpoint_txns: u64,
    checkpoint_period: u64,
    last_checkpoint: RwLock<u64>,
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

/**
 * Internal struct for loading checkpoint, including
 * file map and last txn_id.
 */
#[derive(Debug)]
struct CheckpointRecord {
    pub files: HashMap<String, VecDeque<FileDescriptor>>,
    pub txn_id: u64,
}

impl FileManager {
    /**
     * Loads or creates a [FileManager], restoring state from checkpoint + journal.
     *
     *  @param log_mgr - Shared [LogManager] instance.
     *  @param lease_duration - Lease duration in seconds.
     *  @param replica_ct - Number of replica copies per block.
     *  @param block_size - Maximum block size in bytes.
     *  @param name_dir - Directory for checkpoint and journal files.
     *  @param checkpoint_txns - Max journal entries before a checkpoint.
     *  @param checkpoint_period - Max seconds between checkpoints.
     *  @return Result<FileManager>
     */
    pub fn load(
        log_mgr: LogManager,
        lease_duration: u32,
        replica_ct: usize,
        block_size: usize,
        name_dir: String,
        checkpoint_txns: u64,
        checkpoint_period: u64,
    ) -> Result<Self> {
        let name_dir = PathBuf::from(&name_dir);
        fs::create_dir_all(&name_dir).ok();

        let checkpoint_path = name_dir.join(CHECKPOINT_FILE);
        let journal_path = name_dir.join(JOURNAL_FILE);
        let mut records = Self::load_checkpoint(&checkpoint_path, &log_mgr)?;
        let journal_txn_id = Self::replay_journal(&mut records.files, &journal_path, &log_mgr)?;

        if journal_txn_id > records.txn_id {
            records.txn_id = journal_txn_id;
        }

        let file_count: usize = records.files.len();
        log_mgr.write(LogLevel::Info, || {
            format!(
                "Loaded namespace: {} files, txn_id={}",
                file_count, records.txn_id
            )
        });

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(rustdfs_shared::error::RustDFSError::SystemTimeError)?
            .as_secs();

        Ok(FileManager {
            files: RwLock::new(records.files),
            log_mgr,
            lease_duration,
            replica_ct,
            block_size,
            name_dir,
            txn_id: RwLock::new(records.txn_id),
            journal_txn_count: RwLock::new(0),
            checkpoint_txns,
            checkpoint_period,
            last_checkpoint: RwLock::new(now),
        })
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

        let mut evicted: Option<FileDescriptor> = None;
        let mut evicted_front = false;

        if let Some(back) = deque.back() {
            match &back.status {
                WriteStatus::InProgress { expire, .. } if time < *expire => {
                    let err = status_file_locked(file_name);
                    self.log_mgr.write_status(&err);
                    return Err(err);
                }
                WriteStatus::InProgress { .. } => {
                    evicted = deque.pop_back();
                }
                _ => {
                    if deque.len() == 2 {
                        evicted = deque.pop_front();
                        evicted_front = true;
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

        let journal_entry = JournalEntry {
            txn_id: 0,
            op: Some(Op::WriteStart(WriteStartEntry {
                file_name: file_name.to_string(),
                operation_id: operation_id.to_string(),
                expire: time + self.lease_duration as u64,
                blocks: desc.blocks.iter().map(block_to_entry).collect(),
            })),
        };

        match self.persist_journal(journal_entry).await {
            Ok(should_checkpoint) => {
                drop(files);
                self.maybe_checkpoint(should_checkpoint).await;
                Ok((desc, time + self.lease_duration as u64))
            }
            Err(e) => {
                // roll-back current operation if journal write fails
                deque.pop_back();

                if let Some(old) = evicted {
                    if evicted_front {
                        deque.push_front(old);
                    } else {
                        deque.push_back(old);
                    }
                }

                Err(e)
            }
        }
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

        let old_status = desc.status.clone();
        match &old_status {
            WriteStatus::InProgress { id, .. } if id == operation_id => {
                desc.status = WriteStatus::Complete;

                let journal_entry = JournalEntry {
                    txn_id: 0,
                    op: Some(Op::WriteComplete(WriteCompleteEntry {
                        file_name: file_name.to_string(),
                        operation_id: operation_id.to_string(),
                    })),
                };

                match self.persist_journal(journal_entry).await {
                    Ok(should_checkpoint) => {
                        drop(files);
                        self.maybe_checkpoint(should_checkpoint).await;
                        Ok(())
                    }
                    Err(e) => {
                        // roll-back current operation if journal write fails
                        desc.status = old_status;
                        Err(e)
                    }
                }
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

    // Persistence logic

    /**
     * Persists a journal entry to the journal file.
     * Returns whether a checkpoint should be triggered afterward.
     *
     * This method does NOT acquire the files lock, so it is safe to call
     * while holding it. On I/O failure the txn_id is rolled back.
     */
    async fn persist_journal(&self, mut entry: JournalEntry) -> ServiceResult<bool> {
        let mut txn_id = self.txn_id.write().await;
        *txn_id += 1;
        entry.txn_id = *txn_id;

        let journal_path = self.name_dir.join(JOURNAL_FILE);
        let buf = entry.encode_length_delimited_to_vec();

        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&journal_path)
            .and_then(|mut f| f.write_all(&buf))
            .map_err(|e| {
                *txn_id -= 1;
                let err = status_writing_journal(e);
                self.log_mgr.write_status(&err);
                err
            })?;

        let mut count = self.journal_txn_count.write().await;
        *count += 1;

        let should_checkpoint = *count >= self.checkpoint_txns || {
            let last = *self.last_checkpoint.read().await;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            now.saturating_sub(last) >= self.checkpoint_period
        };

        Ok(should_checkpoint)
    }

    /**
     * Triggers a checkpoint if the threshold was reached.
     * Failure-tolerant operation since the journal entry has already been persisted.
     */
    async fn maybe_checkpoint(&self, should_checkpoint: bool) {
        if should_checkpoint && let Err(e) = self.write_checkpoint().await {
            self.log_mgr
                .write(LogLevel::Error, || format!("Checkpoint failed: {}", e));
        }
    }

    /**
     * Writes current namespace to disk and truncates journal
     */
    async fn write_checkpoint(&self) -> ServiceResult<()> {
        let txn_id = *self.txn_id.read().await;
        let files = self.files.read().await;

        let checkpoint = Checkpoint {
            txn_id,
            files: files
                .iter()
                .flat_map(|(name, deque)| {
                    deque
                        .iter()
                        .map(|desc| file_desc_to_entry(name, desc))
                        .collect::<Vec<_>>()
                })
                .collect(),
        };

        drop(files);
        let tmp_path = self.name_dir.join(CHECKPOINT_TMP);
        let journal_path = self.name_dir.join(JOURNAL_FILE);
        let checkpoint_path = self.name_dir.join(CHECKPOINT_FILE);
        let buf = checkpoint.encode_to_vec();

        fs::write(&tmp_path, &buf)
            .and_then(|_| fs::rename(&tmp_path, &checkpoint_path))
            .map_err(|e| {
                let err = status_writing_checkpoint(e);
                self.log_mgr.write_status(&err);
                err
            })?;

        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&journal_path)
            .map_err(|e| {
                let err = status_truncating_journal(e);
                self.log_mgr.write_status(&err);
                err
            })?;

        *self.journal_txn_count.write().await = 0;
        *self.last_checkpoint.write().await = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.log_mgr.write(LogLevel::Info, || {
            format!("Checkpoint written at txn_id={}", txn_id)
        });

        Ok(())
    }

    /**
     * Loads the checkpoint file and returns the file map and last txn_id.
     *
     *  @param path - Path to the checkpoint file.
     *  @param log_mgr - [LogManager] for logging any errors during loading.
     *  @return Result<CheckpointRecord> - Loaded file map and txn_id, or error if decoding fails.
     */
    fn load_checkpoint(path: &Path, log_mgr: &LogManager) -> Result<CheckpointRecord> {
        let bytes = match fs::read(path) {
            Ok(b) if !b.is_empty() => b,
            _ => {
                return Ok(CheckpointRecord {
                    files: HashMap::new(),
                    txn_id: 0,
                });
            }
        };

        let checkpoint = match Checkpoint::decode(&*bytes) {
            Ok(c) => c,
            Err(e) => {
                let err = RustDFSError::DecodeError(e);
                log_mgr.write_err(&err);
                return Err(err);
            }
        };

        let mut files = HashMap::new();

        for entry in checkpoint.files {
            let desc = entry_to_file_desc(&entry);

            files
                .entry(entry.file_name)
                .or_insert_with(VecDeque::new)
                .push_back(desc);
        }

        Ok(CheckpointRecord {
            files,
            txn_id: checkpoint.txn_id,
        })
    }

    /**
     * Replays journal entries from the journal file, applying each to the files map.
     * Returns the highest txn_id seen in the journal.
     *
     *  @param files - Mutable reference to the file map to apply journal entries to.
     *  @param path - Path to the journal file.
     *  @param log_mgr - [LogManager] for logging any errors during replay.
     *  @return Result<u64> - Highest txn_id seen in the journal, or error if decoding fails.
     */
    fn replay_journal(
        files: &mut HashMap<String, VecDeque<FileDescriptor>>,
        path: &Path,
        log_mgr: &LogManager,
    ) -> Result<u64> {
        let bytes = match fs::read(path) {
            Ok(b) if !b.is_empty() => b,
            _ => return Ok(0),
        };

        let mut cursor = &bytes[..];
        let mut max_txn: u64 = 0;
        let mut replayed: u64 = 0;

        while !cursor.is_empty() {
            let entry = match JournalEntry::decode_length_delimited(&mut cursor) {
                Ok(e) => e,
                Err(e) => {
                    let err = RustDFSError::DecodeError(e);
                    log_mgr.write_err(&err);
                    return Err(err);
                }
            };

            if entry.txn_id > max_txn {
                max_txn = entry.txn_id;
            }

            match entry.op {
                Some(Op::WriteStart(ws)) => {
                    let desc = FileDescriptor {
                        status: WriteStatus::InProgress {
                            id: ws.operation_id,
                            expire: ws.expire,
                        },
                        blocks: ws.blocks.iter().map(entry_to_block_desc).collect(),
                    };

                    let deque = files.entry(ws.file_name).or_default();

                    if let Some(back) = deque.back() {
                        match &back.status {
                            WriteStatus::InProgress { .. } => {
                                deque.pop_back();
                            }
                            WriteStatus::Complete if deque.len() == 2 => {
                                deque.pop_front();
                            }
                            _ => {}
                        }
                    }
                    deque.push_back(desc);
                }
                Some(Op::WriteComplete(wc)) => match files.get_mut(&wc.file_name) {
                    Some(deque) => match deque.back_mut() {
                        Some(desc) => match desc.status {
                            WriteStatus::InProgress { ref id, .. } if id == &wc.operation_id => {
                                desc.status = WriteStatus::Complete;
                            }
                            _ => {
                                let err = err_invalid_journal(&wc.file_name);
                                log_mgr.write_err(&err);
                                return Err(err);
                            }
                        },
                        None => {
                            let err = err_replay_nonexistent_file(&wc.file_name);
                            log_mgr.write_err(&err);
                            return Err(err);
                        }
                    },
                    None => {
                        let err = err_replay_nonexistent_file(&wc.file_name);
                        log_mgr.write_err(&err);
                        return Err(err);
                    }
                },
                None => {}
            }

            replayed += 1;
        }

        if replayed > 0 {
            log_mgr.write(LogLevel::Info, || {
                format!("Replayed {} journal entries", replayed)
            });
        }

        Ok(max_txn)
    }
}

// conversion helpers

fn file_desc_to_entry(name: &str, desc: &FileDescriptor) -> FileEntry {
    let (status, operation_id, expire) = match &desc.status {
        WriteStatus::Complete => (FileStatus::Complete.into(), String::new(), 1),
        WriteStatus::InProgress { id, expire } => {
            (FileStatus::InProgress.into(), id.clone(), *expire)
        }
    };

    FileEntry {
        file_name: name.to_string(),
        status,
        operation_id,
        expire,
        blocks: desc.blocks.iter().map(block_to_entry).collect(),
    }
}

fn entry_to_file_desc(entry: &FileEntry) -> FileDescriptor {
    let status = if entry.status == FileStatus::InProgress as i32 {
        WriteStatus::InProgress {
            id: entry.operation_id.clone(),
            expire: entry.expire,
        }
    } else {
        WriteStatus::Complete
    };
    FileDescriptor {
        status,
        blocks: entry.blocks.iter().map(entry_to_block_desc).collect(),
    }
}

fn block_to_entry(b: &BlockDescriptor) -> BlockEntry {
    BlockEntry {
        block_id: b.id.clone(),
        block_size: b.size,
    }
}

fn entry_to_block_desc(e: &BlockEntry) -> BlockDescriptor {
    BlockDescriptor {
        id: e.block_id.clone(),
        size: e.block_size,
        nodes: Vec::new(), // populated later from BlockReport RPCs
    }
}

// Status helpers

fn err_invalid_journal(file_name: &str) -> RustDFSError {
    let msg = format!("Invalid journal entry for file: {}", file_name);
    RustDFSError::CustomError(msg)
}

fn err_replay_nonexistent_file(file_name: &str) -> RustDFSError {
    let msg = format!(
        "Replayed journal entry for non-existent file: {}",
        file_name
    );
    RustDFSError::CustomError(msg)
}

fn status_writing_journal(e: std::io::Error) -> Status {
    let msg = format!("Failed to write journal entry: {}", e);
    Status::internal(msg)
}

fn status_truncating_journal(e: std::io::Error) -> Status {
    let msg = format!("Failed to truncate journal: {}", e);
    Status::internal(msg)
}

fn status_writing_checkpoint(e: std::io::Error) -> Status {
    let msg = format!("Failed to write checkpoint: {}", e);
    Status::internal(msg)
}

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
