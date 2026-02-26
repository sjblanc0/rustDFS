use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use rustdfs_shared::base::error::RustDFSError;
use rustdfs_shared::base::result::Result;

#[derive(Debug)]
pub struct DataDirManager {
    path: String,
}

impl DataDirManager {

    pub fn new(
        path_str: &str
    ) -> Result<Self> {
        let path = Path::new(path_str);

        if path.exists() {
            if !path.is_dir() {
                return Err(
                    RustDFSError::err_invalid_data_dir(path_str)
                );
            }
        } else {
            fs::create_dir_all(path)
                .map_err(|e| RustDFSError::err_create_data_dir(path_str, e))?;
        }

        Ok(DataDirManager {
            path: path_str.to_string(),
        })
    }

    pub fn write_block(
        &self, 
        block_id: &str, 
        data: &[u8]
    ) -> Result<()> {
        let block_path = format!("{}/{}", self.path, block_id);

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&block_path)
            .map_err(|e| RustDFSError::err_write_block(&self.path, e))?;

        file.write_all(data)
            .map_err(|e| RustDFSError::err_write_block(&self.path, e))?;

        Ok(())
    }

    pub fn read_block(
        &self, 
        block_id: &str
    ) -> Result<Vec<u8>> {
        let block_path = format!("{}/{}", self.path, block_id);

        fs::read(block_path)
            .map_err(|e| RustDFSError::err_read_block(&self.path, e))   
    }
}
