use tonic::Status;
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
    NameNodeServiceError,
}

impl RustDFSError {

    pub fn err_misconfigured_svc_name() -> Self {
        RustDFSError {
            kind: Kind::NameNodeServiceError,
            message: "Misconfigured Name Node".to_string(),
        }
    }

    pub fn err_serving_name(e: TonicError) -> Self {
        RustDFSError {
            kind: Kind::NameNodeServiceError,
            message: format!("Error serving Name Node service: {}", e),
        }
    }

    pub fn err_writing(e: Status) -> Self {
        RustDFSError {
            kind: Kind::NameNodeServiceError,
            message: format!("Error writing to Data Node: {}", e),
        }
    }

    pub fn err_misconfigured_svc_data() -> Self {
        RustDFSError {
            kind: Kind::DataNodeServiceError,
            message: "Misconfigured Data Node".to_string(),
        }
    }

    pub fn err_serving_data(e: TonicError) -> Self {
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
            kind: Kind::DataNodeServiceError,
            message: format!("Invalid data directory path: {}", path),
        }
    }

    pub fn err_create_data_dir(path: &str, e: IoError) -> Self {
        RustDFSError {
            kind: Kind::DataNodeServiceError,
            message: format!("Failed to create data directory '{}': {}", path, e),
        }
    }

    pub fn err_write_block(path: &str, e: IoError) -> Self {
        RustDFSError {
            kind: Kind::DataNodeServiceError,
            message: format!("Error writing block to '{}': {}", path, e),
        }
    }

    pub fn err_read_block(path: &str, e: IoError) -> Self {
        RustDFSError {
            kind: Kind::DataNodeServiceError,
            message: format!("Error reading block from '{}': {}", path, e),
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

    pub fn err_invalid_addr(host: &str, port: u16) -> Self {
        RustDFSError {
            kind: Kind::ConfigError,
            message: format!("Invalid address: {}:{}", host, port),
        }
    }

    pub fn err_invalid_addr_io(e: IoError) -> Self {
        RustDFSError {
            kind: Kind::ConfigError,
            message: format!("Invalid address: {}", e),
        }
    }

    pub fn err_invalid_addr_tonic(e: TonicError) -> Self {
        RustDFSError {
            kind: Kind::ConfigError,
            message: format!("{}", e),
        }
    }

    pub fn err_create_log_dir(path: &str, e: IoError) -> Self {
        RustDFSError {
            kind: Kind::ConfigError,
            message: format!("Failed to create log directory '{}': {}", path, e),
        }
    }
}
