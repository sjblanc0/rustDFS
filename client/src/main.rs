mod args;
mod client;
mod proto;

use std::net::ToSocketAddrs;
    
use args::{RustDFSArgs, Operation};
use client::RustDFSClient;
use proto::name_node_client::NameNodeClient;

#[tokio::main]
async fn main() {
    let args = RustDFSArgs::new();
    let addr = args.host.to_socket_addrs().unwrap().next().unwrap();
    let addr_str = format!("http://{}", addr);

    println!("Connecting to NameNode at {}", addr_str);

    let mut client = NameNodeClient::connect(addr_str)
        .await
        .unwrap();

    match args.op {
        Operation::Write => client.client_write(args).await,
        Operation::Read => client.client_read(args).await,
    }
}
