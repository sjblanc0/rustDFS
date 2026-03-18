use std::fs::{self};
use std::io::Error as IoError;
use std::path::Path;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, BufReader, BufWriter, SeekFrom};

use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::logging::LogManager;
use rustdfs_shared::result::{Result, ServiceResult};
use tonic::Status;

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
     * Seeks to the specified byte offset before returning.
     *
     *  @param path - Block ID (file name within the data directory).
     *  @param buf_size - Buffer capacity in bytes.
     *  @param offset - Byte offset to begin reading from.
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

        file.seek(SeekFrom::Start(offset)).await.map_err(|e| {
            let err = status_err_reading(path, e);
            self.log_mgr.write_status(&err);
            err
        })?;

        Ok(BufReader::with_capacity(buf_size, file))
    }

    /**
     * Opens (or creates) a block file for writing with a buffered writer.
     * Writes are appended to the file.
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

        Ok(BufWriter::with_capacity(buf_size, file))
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
