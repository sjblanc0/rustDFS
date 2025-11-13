use tonic::{transport::Server, Request, Response, Status};
use tonic_reflection::{server::Builder as ReflectionBuilder};

use peer::peer_node_server::{PeerNode, PeerNodeServer};
use peer::{HandshakeRequest, HandshakeResponse, Ping};

pub mod peer {
    use tonic::{include_proto, include_file_descriptor_set};

    include_proto!("peer");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] = include_file_descriptor_set!("peer_descriptor");
}

#[derive(Debug, Default)]
pub struct IPeerNode {}

#[tonic::async_trait]
impl PeerNode for IPeerNode {

    async fn handshake(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeResponse>, Status> {
        println!("Got a handshake: {:?}", request);

        let reply = peer::HandshakeResponse {
            accepted: true,
        };

        Ok(Response::new(reply))
    }

    async fn peer_ping(
        &self,
        request: Request<Ping>,
    ) -> Result<Response<Ping>, Status> {
        println!("Got a ping: {:?}", request);

        let reply = peer::Ping {
            peer_id: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,

        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: std::net::SocketAddr = "[::1]:50051".parse()?;
    let peer_node = IPeerNode::default();

    // should remove this or make it optional via config
    // only added this for testing
    let svc_reflection = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(peer::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    println!("PeerNodeServer listening on {}", addr);

    Server::builder()
        .add_service(svc_reflection)
        .add_service(PeerNodeServer::new(peer_node))
        .serve(addr)
        .await?;

    Ok(())
}

