use std::fs::{self, OpenOptions};
use std::io::{Error as IoError, Write};
use std::path::Path;

use rustdfs_shared::error::RustDFSError;
use rustdfs_shared::logging::LogManager;
use rustdfs_shared::result::{Result, ServiceResult};
use tonic::Status;

/**
 * Manages data directory operations for the data node.
 * Responsible for reading and writing data blocks to the local filesystem.
 */
#[derive(Debug)]
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

    /**
     * Writes a block of data to the data node.
     *
     *  @param block_id - Identifier for the data block.
     *  @param data - Byte slice containing the data to write.
     *  @return ServiceResult<()> - Result indicating success or failure.
     */
    pub fn write_block(&self, block_id: &str, data: &[u8]) -> ServiceResult<()> {
        let block_path = format!("{}/{}", self.path, block_id);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&block_path)
            .map_err(|e| {
                let err = status_err_writing(block_id, e);
                self.log_mgr.write_status(&err);
                err
            })?;

        file.write_all(data).map_err(|e| {
            let err = status_err_writing(block_id, e);
            self.log_mgr.write_status(&err);
            err
        })?;

        Ok(())
    }

    /**
     * Reads a block of data from the data node.
     *
     *  @param block_id - Identifier for the data block.
     *  @return ServiceResult<Vec<u8>> - Byte vector containing data or error.
     */
    pub fn read_block(&self, block_id: &str) -> ServiceResult<Vec<u8>> {
        let block_path = format!("{}/{}", self.path, block_id);

        fs::read(block_path).map_err(|e| {
            let err = status_err_reading(block_id, e);
            self.log_mgr.write_status(&err);
            err
        })
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
