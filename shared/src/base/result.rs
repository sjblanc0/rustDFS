use tonic::Status;

use super::error::RustDFSError;

pub type Result<T> = std::result::Result<T, RustDFSError>;
pub type ServiceResult<T> = std::result::Result<T, Status>;
