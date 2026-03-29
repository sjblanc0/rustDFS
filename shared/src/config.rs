use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use toml;

use crate::bytesize::ByteSize;
use crate::error::RustDFSError;
use crate::result::Result;

const CONFIG_FILE_GLOBAL: &str = "/etc/rustdfs/rdfsconf.toml";
const LEASE_DURATION_GLOBAL: u32 = 120;
const LOG_FILE_GLOBAL: &str = "/var/log/rustdfs";
const DATA_DIR_GLOBAL: &str = "/var/lib/rustdfs/data";
const NAME_DIR_GLOBAL: &str = "/var/lib/rustdfs/names";
const CHECKPOINT_TXNS_GLOBAL: u64 = 1000;
const CHECKPOINT_PERIOD_GLOBAL: u64 = 3600;

/**
 * Configuration structure for RustDFS. Deserialized from a TOML config file.
 *
 *  @field replica_count - Number of replicas per data block.
 *  @field lease_duration - Write lease duration in seconds.
 *  @field message_size - Max gRPC message size for streaming (e.g. "64KB").
 *  @field block_size - Max size of a single data block (e.g. "32MB").
 *  @field name_node - Network config for the name node.
 *  @field data_node - Shared config for data nodes.
 *
 * Sample TOML structure:
 *
 * ```toml
 *  replica-count = 2
 *  lease-duration = 120
 *  message-size = "64KB"
 *  block-size = "32MB"
 *
 *  [name-node]
 *  host = "namenode1"
 *  port = 5000
 *  name-dir = "/var/lib/rustdfs/names"
 *  log-file = "/var/log/rustdfs/namenode.log"
 *  checkpoint-transactions = 1000
 *  checkpoint-period = 3600
 *
 *  [data-node]
 *  data-dir = "/var/lib/rustdfs/data"
 *  log-file = "/var/log/rustdfs/datanode.log"
 * ```
 */
#[derive(Deserialize)]
pub struct RustDFSConfig {
    #[serde(rename = "replica-count", default)]
    pub replica_count: u32,

    #[serde(rename = "lease-duration", default = "default_lease_duration")]
    pub lease_duration: u32,

    #[serde(rename = "message-size", default = "default_message_size")]
    pub message_size: ByteSize,

    #[serde(rename = "block-size", default = "default_block_size")]
    pub block_size: ByteSize,

    #[serde(rename = "name-node")]
    pub name_node: NameNodeConfig,

    #[serde(rename = "data-node")]
    pub data_node: DataNodeConfig,
}

/**
 * Network, logging, and persistence configuration for the Name Node.
 *
 *  @field host - Hostname or IP address of the name node.
 *  @field port - Port number for the name node gRPC service.
 *  @field name_dir - Directory for checkpoint and journal files.
 *  @field log_file - Path to the log file.
 *  @field checkpoint_txns - Max journal entries before forcing a checkpoint.
 *  @field checkpoint_period - Max seconds between checkpoints.
 */
#[derive(Deserialize)]
pub struct NameNodeConfig {
    #[serde(rename = "host")]
    pub host: String,

    #[serde(rename = "port")]
    pub port: u16,

    #[serde(rename = "name-dir", default = "default_name_dir")]
    pub name_dir: String,

    #[serde(rename = "log-file", default = "default_log_file")]
    pub log_file: String,

    #[serde(rename = "checkpoint-transactions", default = "default_checkpoint_txns")]
    pub checkpoint_txns: u64,

    #[serde(rename = "checkpoint-period", default = "default_checkpoint_period")]
    pub checkpoint_period: u64,
}

/**
 * Storage and logging configuration for Data Nodes.
 *
 *  @field data_dir - Directory path for storing data blocks on disk.
 *  @field log_file - Path to the log file.
 */
#[derive(Deserialize)]
pub struct DataNodeConfig {
    #[serde(rename = "data-dir", default = "default_data_dir")]
    pub data_dir: String,

    #[serde(rename = "log-file", default = "default_log_file")]
    pub log_file: String,
}

impl RustDFSConfig {
    /**
     * Loads the RustDFS configuration from the default global config file.
     *
     *  @return Result<RustDFSConfig> - Loaded configuration or error.
     */
    pub fn new() -> Result<Self> {
        Self::extract_to_config(CONFIG_FILE_GLOBAL)
    }

    // Extracts and parses the configuration from the specified file path.
    fn extract_to_config(path: &str) -> Result<Self> {
        let contents: String = Self::extract_to_string(path)?;
        let res: Self = toml::from_str(&contents).map_err(RustDFSError::TomlError)?;

        Ok(res)
    }

    // Reads the entire content of the file at the specified path into a string.
    fn extract_to_string(path: &str) -> Result<String> {
        let mut contents: String = String::new();

        File::open(path)
            .map_err(RustDFSError::IoError)?
            .read_to_string(&mut contents)
            .map_err(RustDFSError::IoError)?;

        Ok(contents)
    }
}

// Default fields

fn default_message_size() -> ByteSize {
    ByteSize(1024 * 64) // 64 KB
}

fn default_block_size() -> ByteSize {
    ByteSize(1024 * 1024 * 32) // 32 MB
}

fn default_lease_duration() -> u32 {
    LEASE_DURATION_GLOBAL
}

fn default_log_file() -> String {
    LOG_FILE_GLOBAL.to_string()
}

fn default_data_dir() -> String {
    DATA_DIR_GLOBAL.to_string()
}

fn default_name_dir() -> String {
    NAME_DIR_GLOBAL.to_string()
}

fn default_checkpoint_txns() -> u64 {
    CHECKPOINT_TXNS_GLOBAL
}

fn default_checkpoint_period() -> u64 {
    CHECKPOINT_PERIOD_GLOBAL
}
