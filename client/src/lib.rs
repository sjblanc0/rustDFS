/**
 * rustDFS client library.
 *
 * Provides [RdfsClient] for connecting to a rustDFS cluster
 * and [RdfsFile] for reading and writing files. Also exposes
 * a C-compatible FFI through [ffi].
 */
mod client;
pub mod error;
pub mod ffi;
mod host;
pub mod out;
pub mod result;

pub use client::{RdfsClient, RdfsFile};

/// Open for reading (mirrors O_RDONLY).
pub const O_RDONLY: i32 = 0x0001;
/// Open for writing (mirrors O_WRONLY).
pub const O_WRONLY: i32 = 0x0002;
