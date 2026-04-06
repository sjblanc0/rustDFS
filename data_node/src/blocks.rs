use std::fs::{self};
use std::io::Error as IoError;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom};

use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::logging::LogManager;
use rustdfs_shared::result::{Result, ServiceResult};
use tonic::Status;

// Bytes written at start of block to identify format
const BLOCK_HDR: &[u8; 4] = b"RDFS";

/**
 * Manages local block storage for the data node.
 * Reads and writes data blocks as files under a configured directory.
 *
 *  @field path - Root directory for block files.
 *  @field log_mgr - [LogManager] for logging I/O events.
 */
#[derive(Debug, Clone)]
pub struct BlockManager {
    path: String,
    log_mgr: LogManager,
}

impl BlockManager {
    /**
     * Creates a new [BlockManager].
     * Ensures the data directory exists, creating it if necessary.
     *
     *  @param path_str - Path to the block storage directory.
     *  @param log_mgr - [LogManager] for logging.
     *  @return Result<BlockManager> - Initialized manager or error.
     */
    pub fn new(path_str: &str, log_mgr: &LogManager) -> Result<Self> {
        let path = Path::new(path_str);

        if path.exists() && !path.is_dir() {
            let err = err_invalid_dir(path_str);
            log_mgr.write_err(&err);
            return Err(err);
        } else {
            fs::create_dir_all(path).map_err(|e| {
                let err = RustDFSError::IoError(e);
                log_mgr.write_err(&err);
                err
            })?;
        }

        Ok(BlockManager {
            path: path_str.to_string(),
            log_mgr: log_mgr.clone(),
        })
    }

    /**
     * Opens a block file for reading with a buffered reader.
     * Skips past the magic header, then seeks to the specified
     * byte offset within the block data.
     *
     *  @param path - Block ID (file name within the data directory).
     *  @param buf_size - Buffer capacity in bytes.
     *  @param offset - Byte offset into block data (0 = first data byte after header).
     *  @return ServiceResult<BufReader<File>> - Buffered reader or I/O error.
     */
    pub async fn read_buf(
        &self,
        path: &str,
        buf_size: usize,
        offset: u64,
    ) -> ServiceResult<BufReader<File>> {
        let block_path = format!("{}/{}", self.path, path);
        let mut file = OpenOptions::new()
            .read(true)
            .open(&block_path)
            .await
            .map_err(|e| {
                let err = status_err_reading(path, e);
                self.log_mgr.write_status(&err);
                err
            })?;

        file.seek(SeekFrom::Start(BLOCK_HDR.len() as u64 + offset))
            .await
            .map_err(|e| {
                let err = status_err_reading(path, e);
                self.log_mgr.write_status(&err);
                err
            })?;

        Ok(BufReader::with_capacity(buf_size, file))
    }

    /**
     * Opens (or creates) a block file for writing with a buffered writer.
     * If the file is newly created (empty), writes the magic header first.
     * Subsequent calls append data after the header.
     *
     *  @param block_id - Block ID (file name within the data directory).
     *  @param buf_size - Buffer capacity in bytes.
     *  @return ServiceResult<BufWriter<File>> - Buffered writer or I/O error.
     */
    pub async fn write_buf(
        &self,
        block_id: &str,
        buf_size: usize,
    ) -> ServiceResult<BufWriter<File>> {
        let block_path = format!("{}/{}", self.path, block_id);

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&block_path)
            .await
            .map_err(|e| {
                let err = status_err_writing(block_id, e);
                self.log_mgr.write_status(&err);
                err
            })?;

        let is_new = file.metadata().await.map(|m| m.len() == 0).unwrap_or(false);

        let mut writer = BufWriter::with_capacity(buf_size, file);

        if is_new {
            writer
                .write_all(BLOCK_HDR)
                .await
                .map_err(|e| status_err_writing(block_id, e))?;
        }

        Ok(writer)
    }

    /**
     * Scans the data directory and returns the block ID (file name)
     * of every file that matches the RDFS magic header.
     *
     *  @return Vec<String> - Verified block IDs found on disk.
     */
    pub fn scan_blocks(&self) -> Vec<String> {
        use std::io::Read;

        let mut results = Vec::new();
        let entries = match fs::read_dir(&self.path) {
            Ok(e) => e,
            Err(_) => return results,
        };

        for entry in entries.flatten() {
            if !entry.path().is_file() {
                continue;
            }

            let ok = fs::File::open(entry.path())
                .and_then(|mut f| {
                    let mut magic = [0u8; 4];
                    f.read_exact(&mut magic)?;
                    Ok(magic == *BLOCK_HDR)
                })
                .unwrap_or(false);

            if ok && let Some(name) = entry.file_name().to_str() {
                results.push(name.to_string());
            }
        }

        results
    }

    /**
     * Deletes a block file from the data directory.
     *
     *  @param block_id - Block ID (file name) to remove.
     */
    pub fn delete_block(&self, block_id: &str) {
        let block_path = format!("{}/{}", self.path, block_id);

        if let Err(e) = fs::remove_file(&block_path) {
            self.log_mgr.write_err(&RustDFSError::IoError(e));
        }
    }
}

// Helper functions for error statuses

fn err_invalid_dir(path: &str) -> RustDFSError {
    let str = format!("Invalid data directory path: {}", path);
    RustDFSError::CustomError(str)
}

fn status_err_writing(block: &str, err: IoError) -> Status {
    let str = format!("Encountered IoError writing block {}: {}", block, err);
    Status::internal(str)
}

fn status_err_reading(block: &str, err: IoError) -> Status {
    let str = format!("Encountered IoError reading block {}: {}", block, err);
    Status::internal(str)
}
