use std::fmt::Display;
use std::net::{SocketAddr, ToSocketAddrs};
use tonic::Status;
use tonic::transport::Endpoint;

use crate::error::RustDFSError;
use crate::logging::LogManager;
use crate::result::{Result, ServiceResult};

const HTTP_PREFIX: &str = "https://";

/**
 * Represents a host address with hostname and port.
 *  => converts to [SocketAddr] or [Endpoint] with hostname
 *     resolution
 *
 *  @field hostname - Hostname or IP address.
 *  @field port - Port number.
 */
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct HostAddr {
    pub hostname: String,
    pub port: u16,
}

impl HostAddr {
    /**
     * Converts the [HostAddr] to a [SocketAddr] with hostname resolution.
     * Returns [RustDFSError] for errors outside of serving.
     *
     *  @param logger - [LogManager] for logging errors.
     *  @return Result<SocketAddr> - Result containing the SocketAddr or an error.
     */
    pub fn to_socket_addr(&self, logger: &LogManager) -> Result<SocketAddr> {
        let addr_str = format!("{}:{}", self.hostname, self.port);
        let mut addrs_iter = addr_str.to_socket_addrs().map_err(|e| {
            let err = err_not_resolve(&self.hostname, self.port, Some(e));
            logger.write_err(&err);
            err
        })?;

        addrs_iter.next().ok_or_else(|| {
            type R = RustDFSError;
            let err = err_not_resolve::<R>(&self.hostname, self.port, None);
            logger.write_err(&err);
            err
        })
    }

    /**
     * Converts the [HostAddr] to an [Endpoint] with hostname resolution.
     * Returns [RustDFSError] for errors outside of serving.
     *
     *  @param logger - [LogManager] for logging errors.
     *  @return Result<Endpoint> - Result containing the Endpoint or an error.
     */
    pub fn to_endpoint(&self, logger: &LogManager) -> Result<Endpoint> {
        let addr = self.to_socket_addr(logger)?;

        Endpoint::from_shared(format!("{}{}", HTTP_PREFIX, addr)).map_err(|e| {
            let err = err_not_resolve(&self.hostname, self.port, Some(e));
            logger.write_err(&err);
            err
        })
    }

    /**
     * Converts the [HostAddr] to a [SocketAddr] with hostname resolution.
     * Returns [Status] for error RPC result.
     *
     *  @param logger - [LogManager] for logging errors.
     *  @return ServiceResult<SocketAddr> - Result containing the SocketAddr or an error.
     */
    pub fn to_socket_addr_serving(&self, logger: &LogManager) -> ServiceResult<SocketAddr> {
        let addr_str = format!("{}:{}", self.hostname, self.port);
        let mut addrs_iter = addr_str.to_socket_addrs().map_err(|e| {
            let err = status_not_resolve(&self.hostname, self.port, Some(e));
            logger.write_status(&err);
            err
        })?;

        addrs_iter.next().ok_or_else(|| {
            type R = RustDFSError;
            let err = status_not_resolve::<R>(&self.hostname, self.port, None);
            logger.write_status(&err);
            err
        })
    }

    /**
     * Converts the [HostAddr] to an [Endpoint] with hostname resolution.
     * Returns [Status] for error RPC result.
     *
     *  @param logger - [LogManager] for logging errors.
     *  @return ServiceResult<Endpoint> - Result containing the Endpoint or an error.
     */
    pub fn to_endpoint_serving(&self, logger: &LogManager) -> ServiceResult<Endpoint> {
        let addr = self.to_socket_addr_serving(logger)?;

        Endpoint::from_shared(format!("{}{}", HTTP_PREFIX, addr)).map_err(|e| {
            let err = status_not_resolve(&self.hostname, self.port, Some(e));
            logger.write_status(&err);
            err
        })
    }
}

// Error helpers

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

fn status_not_resolve<D>(host: &str, port: u16, err: Option<D>) -> Status
where
    D: Display,
{
    match err {
        Some(e) => {
            let msg = format!("Invalid address {}:{}. Error: {}", host, port, e);
            Status::internal(msg)
        }
        None => {
            let msg = format!("Invalid address {}:{}", host, port);
            Status::internal(msg)
        }
    }
}
