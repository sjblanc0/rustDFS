use std::cell::RefCell;
use std::ffi::CString;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;
use tokio::sync::mpsc::error::SendError;
use tonic::Status;
use tonic::transport::Error as TonicError;

use rustdfs_proto::data::WriteRequest as DataWriteRequest;

/**
 * Integer error codes for FFI consumers.
 */
#[repr(i32)]
#[derive(Debug, Clone, Copy)]
pub enum ErrorCode {
    Ok = 0,
    Io = -1,
    Connection = -2,
    Protocol = -3,
    LeaseLost = -4,
    NotFound = -5,
    Unknown = -99,
}

impl From<&RustDFSError> for ErrorCode {
    fn from(err: &RustDFSError) -> Self {
        match err {
            RustDFSError::Io(_) => ErrorCode::Io,
            RustDFSError::Tonic(_) => ErrorCode::Connection,
            RustDFSError::TonicStatus(_) => ErrorCode::Protocol,
            RustDFSError::DataWrite(_) => ErrorCode::Io,
            RustDFSError::Custom(_) => ErrorCode::Unknown,
        }
    }
}

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

// ── Thread-local error storage for FFI ───────────────────────────────────────

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

/**
 * Stores an error message in thread-local storage for retrieval
 * by [get_last_error].
 */
pub fn set_last_error(err: &RustDFSError) {
    let msg = err.to_string();
    // CString::new will fail if msg contains interior nulls; replace them.
    let safe_msg = msg.replace('\0', "");
    if let Ok(c) = CString::new(safe_msg) {
        LAST_ERROR.with(|cell| {
            *cell.borrow_mut() = Some(c);
        });
    }
}

/**
 * Returns a pointer to the last error message, or null if none.
 * The pointer is valid until the next [set_last_error] call
 * on this thread.
 */
pub fn get_last_error() -> *const std::ffi::c_char {
    LAST_ERROR.with(|cell| {
        let borrow = cell.borrow();
        match &*borrow {
            Some(c) => c.as_ptr(),
            None => std::ptr::null(),
        }
    })
}
