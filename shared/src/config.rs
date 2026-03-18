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

/**
 * Configuration structure for RustDFS. Corresponds to TOML config file.
 *
 *  @field replica_count - Number of replicas for each data block.
 *  @field lease_duration - Duration of lease in seconds.
 *  @field name_node - Config for name node. Identifies location on network.
 *  @field data_node - Shared config for data nodes.
 *
 * Sample TOML structure:
 *
 * ```toml
 *  replica-count = 0
 *
 *  [name-node]
 *  host = namenode1
 *  port = 50051
 *  log-file = "/path/to/logfile"
 *
 *  [data-node]
 *  data-dir = "/path/to/datadir"
 *  log-file = "/path/to/logfile"
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
 * Configuration for a Name Node.
 *
 *  @field host - Hostname or IP address of the name node.
 *  @field port - Port number for the name node service.
 *  @field log_file - Path to the log file for logging.
 */
#[derive(Deserialize)]
pub struct NameNodeConfig {
    #[serde(rename = "host")]
    pub host: String,

    #[serde(rename = "port")]
    pub port: u16,

    #[serde(rename = "log-file", default = "default_log_file")]
    pub log_file: String,
}

/**
 * Configuration for a Data Node.
 *
 *  @field data_dir - Directory path for storing data blocks.
 *  @field log_file - Path to the log file for logging.
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
