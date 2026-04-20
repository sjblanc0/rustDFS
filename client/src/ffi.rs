use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::Arc;

use crate::client::{RustDFSClient, RustDFSFile};
use crate::error::{RustDFSError, set_last_error};
use crate::{O_RDONLY, O_WRONLY};

/**
 * Opaque filesystem handle for the C FFI.
 * Wraps [RustDFSClient] + a tokio [Runtime] so callers
 * don't need to manage async themselves.
 */
pub struct RustDFSClientHandle {
    runtime: Arc<tokio::runtime::Runtime>,
    inner: RustDFSClient,
}

/**
 * Opaque file handle for the C FFI.
 * Wraps [RustDFSFile] + an [Arc<Runtime>] for block_on().
 */
pub struct RustDFSFileHandle {
    runtime: Arc<tokio::runtime::Runtime>,
    inner: RustDFSFile,
}

// FFI entrypoints

/// Connect to the name node.
/// Returns 0 on success, -1 on error.
///
/// # Safety
/// `host` must be a valid null-terminated C string.
/// `fs_out` must be a valid pointer to a pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rdfs_connect(
    host: *const c_char,
    port: u16,
    fs_out: *mut *mut RustDFSClientHandle,
) -> c_int {
    if host.is_null() || fs_out.is_null() {
        let err = RustDFSError::Custom("Null pointer argument".to_string());
        set_last_error(&err);
        return -1;
    }

    let host_str = match unsafe { CStr::from_ptr(host) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            let err = RustDFSError::Custom("Invalid UTF-8 in host string".to_string());
            set_last_error(&err);
            return -1;
        }
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => Arc::new(rt),
        Err(e) => {
            let err = RustDFSError::Custom(format!("Failed to create runtime: {}", e));
            set_last_error(&err);
            return -1;
        }
    };

    let client = match runtime.block_on(RustDFSClient::connect(&host_str, port)) {
        Ok(c) => c,
        Err(e) => {
            set_last_error(&e);
            return -1;
        }
    };

    let handle = Box::new(RustDFSClientHandle {
        runtime,
        inner: client,
    });

    unsafe {
        *fs_out = Box::into_raw(handle);
    }
    0
}

/// Disconnect and free the filesystem handle.
/// The handle is freed even on error.
/// Returns 0 on success, -1 on error.
///
/// # Safety
/// `fs` must be a valid pointer returned by [rdfs_connect], or null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rdfs_disconnect(fs: *mut RustDFSClientHandle) -> c_int {
    if fs.is_null() {
        let err = RustDFSError::Custom("Null filesystem handle".to_string());
        set_last_error(&err);
        return -1;
    }

    let handle = unsafe { Box::from_raw(fs) };
    handle.runtime.block_on(handle.inner.disconnect());
    0
}

/// Open a file for reading or writing.
///
/// flags: RDFS_O_RDONLY (0x0001) or RDFS_O_WRONLY (0x0002).
/// Returns 0 on success, -1 on error.
///
/// # Safety
/// `fs` must be a valid [RustDFSClientHandle] pointer.
/// `path` must be a valid null-terminated C string.
/// `file_out` must be a valid pointer to a pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rdfs_open(
    fs: *mut RustDFSClientHandle,
    path: *const c_char,
    flags: c_int,
    file_out: *mut *mut RustDFSFileHandle,
) -> c_int {
    if fs.is_null() || path.is_null() || file_out.is_null() {
        let err = RustDFSError::Custom("Null pointer argument".to_string());
        set_last_error(&err);
        return -1;
    }

    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            let err = RustDFSError::Custom("Invalid UTF-8 in path string".to_string());
            set_last_error(&err);
            return -1;
        }
    };

    if flags != O_RDONLY && flags != O_WRONLY {
        let err = RustDFSError::Custom(format!("Invalid flags: {}", flags));
        set_last_error(&err);
        return -1;
    }

    let handle = unsafe { &mut *fs };
    let file = match handle.runtime.block_on(handle.inner.open(&path_str, flags)) {
        Ok(f) => f,
        Err(e) => {
            set_last_error(&e);
            return -1;
        }
    };

    let file_handle = Box::new(RustDFSFileHandle {
        runtime: handle.runtime.clone(),
        inner: file,
    });

    unsafe {
        *file_out = Box::into_raw(file_handle);
    }
    0
}

/// Read data from an open file.
/// Returns bytes read on success, 0 on EOF, -1 on error.
///
/// # Safety
/// `fs` must be a valid [RustDFSClientHandle] pointer.
/// `file` must be a valid [RustDFSFileHandle] pointer.
/// `buffer` must point to at least `length` writeable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rdfs_read(
    _fs: *mut RustDFSClientHandle,
    file: *mut RustDFSFileHandle,
    buffer: *mut c_void,
    length: i64,
) -> i64 {
    if file.is_null() || buffer.is_null() {
        let err = RustDFSError::Custom("Null pointer argument".to_string());
        set_last_error(&err);
        return -1;
    }

    if length <= 0 {
        return 0;
    }

    let file_handle = unsafe { &mut *file };
    let buf = unsafe { std::slice::from_raw_parts_mut(buffer as *mut u8, length as usize) };

    match file_handle.runtime.block_on(file_handle.inner.read(buf)) {
        Ok(n) => n as i64,
        Err(e) => {
            set_last_error(&e);
            -1
        }
    }
}

/// Write data to an open file.
/// Returns bytes written on success, -1 on error.
///
/// # Safety
/// `fs` must be a valid [RustDFSClientHandle] pointer.
/// `file` must be a valid [RustDFSFileHandle] pointer.
/// `buffer` must point to at least `length` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rdfs_write(
    _fs: *mut RustDFSClientHandle,
    file: *mut RustDFSFileHandle,
    buffer: *const c_void,
    length: i64,
) -> i64 {
    if file.is_null() || buffer.is_null() {
        let err = RustDFSError::Custom("Null pointer argument".to_string());
        set_last_error(&err);
        return -1;
    }

    if length <= 0 {
        return 0;
    }

    let file_handle = unsafe { &mut *file };
    let data = unsafe { std::slice::from_raw_parts(buffer as *const u8, length as usize) };

    match file_handle.runtime.block_on(file_handle.inner.write(data)) {
        Ok(n) => n as i64,
        Err(e) => {
            set_last_error(&e);
            -1
        }
    }
}

/// Flush buffered write data.
/// Returns 0 on success, -1 on error.
///
/// # Safety
/// `fs` must be a valid [RustDFSClientHandle] pointer.
/// `file` must be a valid [RustDFSFileHandle] pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rdfs_flush(
    _fs: *mut RustDFSClientHandle,
    file: *mut RustDFSFileHandle,
) -> c_int {
    if file.is_null() {
        let err = RustDFSError::Custom("Null pointer argument".to_string());
        set_last_error(&err);
        return -1;
    }

    let file_handle = unsafe { &mut *file };

    match file_handle.runtime.block_on(file_handle.inner.flush()) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&e);
            -1
        }
    }
}

/// Close an open file.
/// For writes: finalizes the write and releases the lease.
/// The file handle is freed even on error.
/// Returns 0 on success, -1 on error.
///
/// # Safety
/// `fs` must be a valid [RustDFSClientHandle] pointer.
/// `file` must be a valid [RustDFSFileHandle] pointer returned by [rdfs_open].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn rdfs_close(
    _fs: *mut RustDFSClientHandle,
    file: *mut RustDFSFileHandle,
) -> c_int {
    if file.is_null() {
        let err = RustDFSError::Custom("Null file handle".to_string());
        set_last_error(&err);
        return -1;
    }

    let file_handle = unsafe { Box::from_raw(file) };

    match file_handle.runtime.block_on(file_handle.inner.close()) {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(&e);
            -1
        }
    }
}

/// Get a human-readable error string for the last failed operation.
/// The returned pointer is valid until the next rdfs_* call on the
/// same thread.
///
/// Returns NULL if no error information is available.
#[unsafe(no_mangle)]
pub extern "C" fn rdfs_get_last_error() -> *const c_char {
    crate::error::get_last_error()
}
