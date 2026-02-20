use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use serde::Deserialize;
use toml;

use super::error::RustDFSError;
use super::result::Result;

const CONFIG_FILE_GLOBAL: &str = "/etc/rustdfs/rdfsconf.toml";

#[derive(Deserialize)]
pub struct RustDFSConfig {
    #[serde(rename = "name-node")]
    pub name_nodes: HashMap<String, NameNodeConfig>,
    #[serde(rename = "data-node")]
    pub data_nodes: HashMap<String, DataNodeConfig>,
}

#[derive(Deserialize)]
pub struct NameNodeConfig {
    #[serde(rename = "host")]
    pub host: String,
    #[serde(rename = "port")]
    pub port: u16,
    #[serde(rename = "log-file", default = "default_log_file")]
    pub log_file: String,
}

#[derive(Deserialize)]
pub struct DataNodeConfig {
    #[serde(rename = "host")]
    pub host: String,
    #[serde(rename = "port")]
    pub port: u16,
    #[serde(rename = "data-dir")]
    pub data_dir: String,
    #[serde(rename = "log-file", default = "default_log_file")]
    pub log_file: String,
}

impl RustDFSConfig {

    pub fn new() -> Result<Self> {
        return Self::extract_to_config(CONFIG_FILE_GLOBAL);
    }

    fn extract_to_config(path: &str) -> Result<Self> {
        let contents: String = Self::extract_to_string(path)?;
        let res: Self = toml::from_str(&contents)
            .map_err(|e| { RustDFSError::err_toml_parse(e) })?;

        return Ok(res);
    }

    fn extract_to_string(path: &str) -> Result<String> {
        let mut contents: String = String::new();

        File::open(path)
            .map_err(|_| { RustDFSError::err_config_open(path) })?
            .read_to_string(&mut contents)
            .map_err(|_| { RustDFSError::err_config_read(path) })?;

        return Ok(contents);
    }
}

fn default_log_file() -> String {
    "/var/log/rustdfs".to_string()
}
