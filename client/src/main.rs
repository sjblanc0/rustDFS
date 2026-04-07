/**
 * RustDFS Client binary.
 *
 * Parses CLI args, connects to the Name Node, and
 * performs the requested read or write operation.
 */
mod args;
mod client;
mod error;
mod host;
mod out;
mod result;

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
