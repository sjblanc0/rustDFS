use tonic::Status;
use tonic::transport::Error as TonicError;
use toml::de::Error as TomlError;
use std::io::Error as IoError;
use std::sync::PoisonError;

#[derive(Debug)]
pub struct RustDFSError {
    pub kind: Kind,
    pub message: String,
}

#[derive(Debug)]
pub enum Kind {
    ConfigError,
    DataNodeServiceError,
    AsyncLockError,
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

    pub fn err_forwarding(e: Status) -> Self {
        RustDFSError {
            kind: Kind::DataNodeServiceError,
            message: format!("Error forwarding write to Data Node: {}", e),
        }
    }

    pub fn err_invalid_data_dir(path: &str) -> Self {
        RustDFSError {
            kind: Kind::ConfigError,
            message: format!("Invalid data directory path: {}", path),
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

    pub fn err_client_lock<T>(e: PoisonError<T>) -> Self {
        RustDFSError {
            kind: Kind::AsyncLockError,
            message: format!("{}", e),
        }
    }
}
