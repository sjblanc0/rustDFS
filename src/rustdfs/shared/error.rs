use tonic::transport::Error as TonicError;
use toml::de::Error as TomlError;
use std::io::Error as IoError;

#[derive(Debug)]
pub struct RustDFSError {
    pub kind: Kind,
    pub message: String,
}

#[derive(Debug)]
pub enum Kind {
    ConfigError,
    DataNodeServiceError,
}

impl RustDFSError {

    pub fn err_misconfigured_svc() -> Self {
        RustDFSError {
            kind: Kind::DataNodeServiceError,
            message: "Self host and port do not match any configured data node".to_string(),
        }
    }

    pub fn err_serving(e: TonicError) -> Self {
        RustDFSError {
            kind: Kind::DataNodeServiceError,
            message: format!("Error serving Data Node service: {}", e),
        }
    }

    pub fn err_config_open(path: &str) -> Self {
        return RustDFSError {
            kind: Kind::ConfigError,
            message: format!("Error opening config file at {}", path),
        };
    }

    pub fn err_config_read(path: &str) -> Self {
        return RustDFSError {
            kind: Kind::ConfigError,
            message: format!("Error reading config file at {}", path),
        };
    }

    pub fn err_toml_parse(e: TomlError) -> Self {
        return RustDFSError {
            kind: Kind::ConfigError,
            message: e.message().to_string(),
        };
    }

    pub fn err_invalid_addr(e: IoError) -> Self {
        RustDFSError {
            kind: Kind::ConfigError,
            message: format!("{}", e),
        }
    }
}
