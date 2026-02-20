use super::error::{RustDFSError, Kind};
use super::result::Result;

use std::net::{SocketAddr, ToSocketAddrs};
use tokio::sync::RwLock;
use tonic::transport::Endpoint;

#[derive(Debug)]
pub struct Node<T> {
    pub host: String,
    pub port: u16,
    pub client_ref: RwLock<Option<T>>,
}

impl <T> Node<T> {

    pub fn to_socket_addr(&self) -> Result<SocketAddr> {
        let err = || {
            RustDFSError {
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

    pub fn to_endpoint(&self) -> Result<Endpoint> {
        let socket_addr = self.to_socket_addr()?;
        let endpoint = format!("http://{}", socket_addr);
        Ok(Endpoint::from_shared(endpoint)
            .map_err(|e| RustDFSError::err_invalid_addr_tonic(e))?)
    }
}
