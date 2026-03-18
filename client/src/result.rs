use crate::error::RustDFSError;

/**
 * Result type alias for client operations.
 * Returns [RustDFSError] on failure.
 */
pub type Result<T> = std::result::Result<T, RustDFSError>;
