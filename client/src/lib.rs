/**
 * rustDFS client library.
 *
 * Provides [RustDFSClient] for connecting to a rustDFS cluster
 * and [RustDFSFile] for reading and writing files. Also exposes
 * a C-compatible FFI through [ffi].
 */
mod client;
pub mod error;
pub mod ffi;
mod host;
pub mod result;

pub use client::{RustDFSClient, RustDFSFile};

/// Open for reading (mirrors O_RDONLY).
pub const O_RDONLY: i32 = 0x0001;
/// Open for writing (mirrors O_WRONLY).
pub const O_WRONLY: i32 = 0x0002;
