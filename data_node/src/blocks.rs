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
 * Manages data directory operations for the data node.
 * Responsible for reading and writing data blocks to the local filesystem.
 */
#[derive(Debug, Clone)]
pub struct BlockManager {
    path: String,
    log_mgr: LogManager,
}

impl BlockManager {
    /**
     * Creates a new BlockManager instance.
     * Ensures the data directory exists or creates it.
     *
     *  @param path_str - Path to the data directory.
     *  @param log_mgr - LogManager for logging operations.
     *  @return ServiceResult<BlockManager> - Initialized BlockManager instance or error.
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
