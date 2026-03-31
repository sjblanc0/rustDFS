use futures::future::FutureExt;
use tonic::IntoStreamingRequest;
use tonic::Streaming;
use tonic::transport::Channel;

use crate::host::HostAddr;
use crate::result::ServiceResult;
use rustdfs_proto::data::data_node_client::DataNodeClient;
use rustdfs_proto::data::{ReadRequest, ReadResponse, WriteRequest};

/**
 * Represents a gRPC client connection to a single data node.
 * Cloneable — the underlying [DataNodeClient] uses a shared channel.
 *
 *  @field host - [HostAddr] of the connected data node.
 *  @field client - gRPC [DataNodeClient] for issuing read/write RPCs.
 */
#[derive(Debug, Clone)]
pub struct DataNodeConn {
    pub host: HostAddr,
    client: DataNodeClient<Channel>,
}

impl DataNodeConn {
    /**
     * Creates a new [DataNodeConn] instance.
     *
     *  @param host - [HostAddr] of the data node.
     *  @param client - Connected [DataNodeClient].
     *  @return DataNodeConn
     */
    pub fn new(host: HostAddr, client: DataNodeClient<Channel>) -> Self {
        DataNodeConn { host, client }
    }

    /**
     * Sends a streaming write request to the connected data node.
     * Returns a response stream of per-chunk acknowledgements.
     *
     *  @param request - Streaming [WriteRequest] messages (block ID, data, replica info).
     *  @return ServiceResult<Streaming<()>> - Stream of acknowledgements or error.
     */
    pub async fn write(
        self,
        request: impl IntoStreamingRequest<Message = WriteRequest>,
    ) -> ServiceResult<Streaming<()>> {
        let stream = self
            .client
            .clone()
            .write(request)
            .map(|res| match res {
                Ok(response) => Ok(response.into_inner()),
                Err(e) => Err(e),
            })
            .await?;

        Ok(stream)
    }

    /**
     * Sends a read request and returns a response stream of data chunks.
     *
     *  @param request - [ReadRequest] containing block ID and byte offset.
     *  @return ServiceResult<Streaming<ReadResponse>> - Stream of data chunks or error.
     */
    pub async fn read(self, request: ReadRequest) -> ServiceResult<Streaming<ReadResponse>> {
        let response = self.client.clone().read(request).await?;
        Ok(response.into_inner())
    }
}
