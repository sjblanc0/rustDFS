/**
 * Shared library for RustDFS.
 *
 * Provides common types, configuration, networking, logging,
 * and error handling used across the name node, data node, and client.
 */
pub mod bytesize;
pub mod config;
pub mod conn;
pub mod error;
pub mod host;
pub mod logging;
pub mod result;
