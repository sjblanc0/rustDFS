use tonic::Status;

use super::error::RustDFSError;

/**
 * Result type alias for non-serving operations.
 * Returns [RustDFSError] on failure.
 */
pub type Result<T> = std::result::Result<T, RustDFSError>;

/**
 * Result type alias for gRPC service operations.
 * Returns tonic [Status] on failure.
 */
pub type ServiceResult<T> = std::result::Result<T, Status>;
