use super::error::{RustDFSError, Kind};

use std::net::{SocketAddr, ToSocketAddrs};

#[derive(Clone, Debug)]
pub struct Node {
    pub host: String,
    pub port: u16,
}

impl Node {

    pub fn to_socket_addr(&self) -> Result<SocketAddr, RustDFSError> {
        let err = || {
            RustDFSError{
                kind: Kind::ConfigError,
                message: format!("Invalid address: {}:{}", self.host, self.port),
            }
        };

        let addr_str = format!("{}:{}", self.host, self.port);
        addr_str.to_socket_addrs()
            .map_err(|e| RustDFSError::err_invalid_addr(e))?
            .next()
            .ok_or_else(|| err())
    }
}
