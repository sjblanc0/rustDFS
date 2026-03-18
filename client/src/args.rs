use clap::{Parser, ValueEnum};

use crate::out::Verbosity;

/**
 * Supported operations for RustDFS client.
 */
#[derive(Debug, Clone, ValueEnum)]
pub enum Operation {
    Write,
    Read,
}

/**
 * Command line arguments for RustDFS client.
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
    pub fn new() -> Self {
        Self::parse()
    }
}
