use std::path::{Path, PathBuf};

use crate::rustdfs::shared::error::RustDFSError;

#[derive(Debug)]
pub struct DataDir {
    pub safe_path_str: String,
    pub path: PathBuf,
}

impl DataDir {

    pub fn new(path_str: &str) -> Result<Self, RustDFSError> {
        let path = Path::new(path_str);

        if path.is_relative() {
            return Err(RustDFSError::err_invalid_data_dir(path_str));
        }

        if path.is_file() {
            return Err(RustDFSError::err_invalid_data_dir(path_str));
        }

        if path.is_symlink() {
            return Err(RustDFSError::err_invalid_data_dir(path_str));
        }

        let path_str_sanitized = path.to_str()
            .ok_or(RustDFSError::err_invalid_data_dir(path_str))?
            .to_string();

        std::fs::create_dir_all(&path_str_sanitized)
            .map_err(|_| RustDFSError::err_invalid_data_dir(path_str))?;

        Ok(DataDir {
            safe_path_str: path_str_sanitized,
            path: path.to_path_buf(),
        })
    }
}
