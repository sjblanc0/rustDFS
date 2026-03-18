use std::fmt::{Display, Formatter};
use tonic::Status;
use tonic::transport::Error as TonicError;
use std::io::Error as IoError;
use tokio::sync::mpsc::error::SendError;

use rustdfs_proto::data::WriteRequest as DataWriteRequest;

#[derive(Debug)]
pub enum RustDFSError {
    IoError(IoError),
    TonicError(TonicError),
    TonicStatusError(Status),
    DataWriteError(SendError<DataWriteRequest>),
    CustomError(String),
}

impl Display for RustDFSError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RustDFSError::IoError(e) => {
                write!(f, "IO Error: {}", e)
            }
            RustDFSError::TonicError(e) => {
                write!(f, "Tonic Error: {}", e)
            }
            RustDFSError::TonicStatusError(e) => {
                write!(f, "Tonic Status Error: {}", e)
            }
            RustDFSError::DataWriteError(e) => {
                write!(f, "Data Write Error: {}", e)
            }
            RustDFSError::CustomError(msg) => {
                write!(f, "RustDFS Error: {}", msg)
            }
        }
    }
}
