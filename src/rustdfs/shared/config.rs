use super::node::Node;
use crate::rustdfs::shared::error::RustDFSError;

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use serde::Deserialize;
use toml;

const CONFIG_FILE_GLOBAL: &str = "/etc/rustdfs/rdfsconf.toml";

const DEFAULT_NODES: Vec<Node> = Vec::new();

#[derive(Deserialize)]
pub struct RustDFSConfig {
    #[serde(rename = "self")]
    pub self_config: SelfConfig,
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
}

#[derive(Deserialize)]
pub struct DataNodeConfig {
    #[serde(rename = "host")]
    pub host: String,
    #[serde(rename = "port")]
    pub port: u16,
    #[serde(rename = "data-dir")]
    pub data_dir: String,
}

#[derive(Deserialize)]
pub struct SelfConfig {
    #[serde(rename = "host")]
    pub host: String,
    #[serde(rename = "port")]
    pub port: u16,
}

impl RustDFSConfig {

    pub fn new() -> Result<Self, RustDFSError> {
        return Self::extract_to_config(CONFIG_FILE_GLOBAL);
    }

    fn extract_to_config(path: &str) -> Result<Self, RustDFSError> {
        let contents: String = Self::extract_to_string(path)?;
        let res: Self = toml::from_str(&contents)
            .map_err(|e| { RustDFSError::err_toml_parse(e) })?;

        return Ok(res);
    }

    fn extract_to_string(path: &str) -> Result<String, RustDFSError> {
        let mut contents: String = String::new();

        File::open(path)
            .map_err(|_| { RustDFSError::err_config_open(path) })?
            .read_to_string(&mut contents)
            .map_err(|_| { RustDFSError::err_config_read(path) })?;

        return Ok(contents);
    }
}
