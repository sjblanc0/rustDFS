use std::fmt::{Display, Formatter, Result};
use std::io::Error as IoError;
use std::time::SystemTimeError;
use toml::de::Error as TomlError;
use tonic::Status;
use tonic::transport::Error as TonicError;

/**
 * Custom error type for RustDFS.
 *
 *  @variant IoError - Represents I/O related errors.
 *  @variant TonicError - Represents errors from the Tonic gRPC library.
 *  @variant TonicStatusError - Represents gRPC status errors.
 *  @variant TomlError - Represents errors during TOML parsing.
 *  @variant SystemTimeError - Represents system clock errors.
 *  @variant CustomError - Represents custom error messages.
 */
#[derive(Debug)]
pub enum RustDFSError {
    IoError(IoError),
    TonicError(TonicError),
    TonicStatusError(Status),
    TomlError(TomlError),
    SystemTimeError(SystemTimeError),
    CustomError(String),
}

impl Display for RustDFSError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
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
            RustDFSError::TomlError(e) => {
                write!(f, "TOML Error: {}", e)
            }
            RustDFSError::SystemTimeError(e) => {
                write!(f, "System Time Error: {}", e)
            }
            RustDFSError::CustomError(msg) => {
                write!(f, "RustDFS Error: {}", msg)
            }
        }
    }
}
