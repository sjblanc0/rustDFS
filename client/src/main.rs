mod args;
mod client;
mod host;
mod error;
mod result;
mod out;

use args::RustDFSArgs;
use client::RustDFSClient;

#[tokio::main]
async fn main() {
    let args = RustDFSArgs::new();
    let op = args.op.clone();

    RustDFSClient::new(args)
        .await
        .unwrap()
        .run(op)
        .await
        .unwrap();
}
