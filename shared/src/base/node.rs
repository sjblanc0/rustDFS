use std::net::{SocketAddr, ToSocketAddrs};

use super::error::RustDFSError;
use super::result::Result;

pub struct GenericNode {
    pub host: String,
    pub port: u16,
}

pub trait Node {
    fn to_socket_addr(&self) -> Result<SocketAddr>;
}

impl Node for GenericNode {

    fn to_socket_addr(
        &self
    ) -> Result<SocketAddr> {
        let addr_str = format!("{}:{}", self.host, self.port);
        addr_str.to_socket_addrs()
            .map_err(|e| RustDFSError::err_invalid_addr_io(e))?
            .next()
            .ok_or_else(|| RustDFSError::err_invalid_addr(&self.host, self.port))
    }
}

