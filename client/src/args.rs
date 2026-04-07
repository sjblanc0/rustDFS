use clap::{Parser, ValueEnum};

use crate::out::Verbosity;

/**
 * Supported operations for the RustDFS client.
 *
 *  @variant Write - Upload a local file to the cluster.
 *  @variant Read - Download a file from the cluster.
 */
#[derive(Debug, Clone, ValueEnum)]
pub enum Operation {
    Write,
    Read,
}

/**
 * Command line arguments for the RustDFS client.
 *
 *  @field op - [Operation] to perform (read or write).
 *  @field host - Name Node address in "host:port" format.
 *  @field source - Source path (local file for write, remote file for read).
 *  @field dest - Destination path (remote file for write, local file for read).
 *  @field verbosity - Console output [Verbosity] level.
 *
 * CLI Usage:
 *  rustdfs-client <write|read> <host:port> <source> <dest> [-v <VERBOSITY>]
 */
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct RustDFSArgs {
    #[arg(value_enum)]
    pub op: Operation,

    pub host: String,

    pub source: String,

    pub dest: String,

    #[arg(short, long, value_enum, default_value_t = Verbosity::Error)]
    pub verbosity: Verbosity,
}

impl RustDFSArgs {
    /**
     * Parses CLI arguments from the process invocation.
     */
    pub fn new() -> Self {
        Self::parse()
    }
}
