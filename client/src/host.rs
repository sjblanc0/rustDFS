use std::fmt::Display;
use std::net::ToSocketAddrs;

use tonic::transport::Endpoint;

use crate::error::RustDFSError;
use crate::result::Result;

const HTTP_PREFIX: &str = "https://";

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct HostAddr {
    pub hostname: String,
    pub port: u16,
}

impl HostAddr {
    pub fn from_str(host_str: &str) -> Result<Self> {
        let parts: Vec<&str> = host_str.split(':').collect();

        if parts.len() != 2 {
            return Err(err_invalid_host_str(host_str));
        }

        let hostname = parts[0].to_string();
        let port = parts[1]
            .parse::<u16>()
            .map_err(|_| err_invalid_port(host_str))?;

        Ok(HostAddr { hostname, port })
    }

    pub fn to_endpoint(&self) -> Result<Endpoint> {
        let addr_str = format!("{}:{}", self.hostname, self.port);
        let mut addrs_iter = addr_str
            .to_socket_addrs()
            .map_err(|e| err_not_resolve(&self.hostname, self.port, Some(e)))?;

        let addr = addrs_iter.next().ok_or_else(|| {
            type R = RustDFSError;

            err_not_resolve::<R>(&self.hostname, self.port, None)
        })?;

        Endpoint::from_shared(format!("{}{}", HTTP_PREFIX, addr))
            .map_err(|e| err_not_resolve(&self.hostname, self.port, Some(e)))
    }
}

// Error helpers

fn err_invalid_host_str(host: &str) -> RustDFSError {
    let msg = format!("Invalid host string: {}", host);
    RustDFSError::CustomError(msg)
}

fn err_invalid_port(host: &str) -> RustDFSError {
    let msg = format!("Invalid port: {}", host);
    RustDFSError::CustomError(msg)
}

fn err_not_resolve<D>(host: &str, port: u16, err: Option<D>) -> RustDFSError
where
    D: Display,
{
    match err {
        Some(e) => {
            let msg = format!("Invalid address {}:{}. Error: {}", host, port, e);
            RustDFSError::CustomError(msg)
        }
        None => {
            let msg = format!("Invalid address {}:{}", host, port);
            RustDFSError::CustomError(msg)
        }
    }
}
