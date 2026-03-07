use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use toml;

use super::error::RustDFSError;
use super::result::Result;

const CONFIG_FILE_GLOBAL: &str = "/etc/rustdfs/rdfsconf.toml";
const LOG_FILE_GLOBAL: &str = "/var/log/rustdfs";
const NAME_FILE_GLOBAL: &str = "/var/lib/rustdfs/data";
const DATA_DIR_GLOBAL: &str = "/var/lib/rustdfs/data";

/**
 * Configuration structure for RustDFS. Corresponds to TOML config file.
 *
 *  @field replica_count - Number of replicas for each data block.
 *  @field name_nodes - Mapping of name node IDs to their configurations.
 *  @field data_nodes - Mapping of data node IDs to their configurations.
 *
 * Sample TOML structure:
 *
 * ```toml
 *  replica-count = 0
 *
 *  [name-node.nn1]
 *  host = namenode1
 *  port = 50051
 *  name-file = "/path/to/namefile"
 *  log-file = "/path/to/logfile"
 *
 *  [data-node.dn1]
 *  host = datanode1
 *  port = 50052
 *  data-dir = "/path/to/datadir"
 *  log-file = "/path/to/logfile"
 * ```
 */
#[derive(Deserialize)]
pub struct RustDFSConfig {
    #[serde(rename = "replica-count", default)]
    pub replica_count: u32,

    #[serde(rename = "name-node")]
    pub name_nodes: HashMap<String, NameNodeConfig>,

    #[serde(rename = "data-node")]
    pub data_nodes: HashMap<String, DataNodeConfig>,
}

/**
 * Configuration for a Name Node.
 *
 *  @field host - Hostname or IP address of the name node.
 *  @field port - Port number for the name node service.
 *  @field name_file - Path to the name file for persistence.
 *  @field log_file - Path to the log file for logging.
 */
#[derive(Deserialize)]
pub struct NameNodeConfig {
    #[serde(rename = "host")]
    pub host: String,

    #[serde(rename = "port")]
    pub port: u16,

    #[serde(rename = "name-file", default = "default_name_file")]
    pub name_file: String,

    #[serde(rename = "log-file", default = "default_log_file")]
    pub log_file: String,
}

/**
 * Configuration for a Data Node.
 *
 *  @field host - Hostname or IP address of the data node.
 *  @field port - Port number for the data node service.
 *  @field data_dir - Directory path for storing data blocks.
 *  @field log_file - Path to the log file for logging.
 */
#[derive(Deserialize)]
pub struct DataNodeConfig {
    #[serde(rename = "host")]
    pub host: String,

    #[serde(rename = "port")]
    pub port: u16,

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
        let res: Self =
            toml::from_str(&contents).map_err(RustDFSError::TomlError)?;

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

fn default_log_file() -> String {
    LOG_FILE_GLOBAL.to_string()
}

fn default_name_file() -> String {
    NAME_FILE_GLOBAL.to_string()
}

fn default_data_dir() -> String {
    DATA_DIR_GLOBAL.to_string()
}
