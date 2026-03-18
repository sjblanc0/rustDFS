use std::fmt::{Display, Formatter};
use std::io::Error as IoError;
use tokio::sync::mpsc::error::SendError;
use tonic::Status;
use tonic::transport::Error as TonicError;

use rustdfs_proto::data::WriteRequest as DataWriteRequest;

/**
 * Custom error type for the RustDFS client.
 *
 *  @variant Io - Standard I/O errors.
 *  @variant Tonic - gRPC transport errors.
 *  @variant TonicStatus - gRPC status (application-level) errors.
 *  @variant DataWrite - Error sending a [WriteRequest] through a channel.
 *  @variant Custom - Ad-hoc error message.
 */
#[derive(Debug)]
pub enum RustDFSError {
    Io(IoError),
    Tonic(TonicError),
    TonicStatus(Status),
    DataWrite(SendError<DataWriteRequest>),
    Custom(String),
}

impl Display for RustDFSError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RustDFSError::Io(e) => {
                write!(f, "IO Error: {}", e)
            }
            RustDFSError::Tonic(e) => {
                write!(f, "Tonic Error: {}", e)
            }
            RustDFSError::TonicStatus(e) => {
                write!(f, "Tonic Status Error: {}", e)
            }
            RustDFSError::DataWrite(e) => {
                write!(f, "Data Write Error: {}", e)
            }
            RustDFSError::Custom(msg) => {
                write!(f, "RustDFS Error: {}", msg)
            }
        }
    }
}
