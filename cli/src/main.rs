/**
 * RustDFS Client CLI binary.
 *
 * Parses CLI args, connects to the Name Node via the
 * rustdfs-client library, and performs the requested
 * read or write operation.
 */
mod args;

use args::{Operation, RustDFSArgs};
use rustdfs_client::{O_RDONLY, O_WRONLY, RustDFSClient};

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

#[tokio::main]
async fn main() {
    let args = RustDFSArgs::new();
    let op = args.op.clone();
    let host = args.host.clone();
    let source = args.source.clone();
    let dest = args.dest.clone();

    // Parse host:port
    let parts: Vec<&str> = host.split(':').collect();
    if parts.len() != 2 {
        eprintln!("Invalid host format. Expected host:port, got: {}", host);
        std::process::exit(1);
    }
    let hostname = parts[0];
    let port: u16 = parts[1].parse().unwrap_or_else(|_| {
        eprintln!("Invalid port: {}", parts[1]);
        std::process::exit(1);
    });

    let mut client = RustDFSClient::connect(hostname, port).await.unwrap_or_else(|e| {
        eprintln!("Failed to connect: {}", e);
        std::process::exit(1);
    });

    // Set verbosity from CLI args
    client.set_verbosity(args.verbosity.into());

    match op {
        Operation::Write => {
            // Open local file for reading
            let local_file = File::open(&source).await.unwrap_or_else(|e| {
                eprintln!("Failed to open local file {}: {}", source, e);
                std::process::exit(1);
            });
            let mut reader = BufReader::new(local_file);

            // Open remote file for writing
            let mut remote_file = client.open(&dest, O_WRONLY).await.unwrap_or_else(|e| {
                eprintln!("Failed to open remote file {}: {}", dest, e);
                std::process::exit(1);
            });

            // Stream data from local to remote
            let mut buf = vec![0u8; 16 * 1024]; // 16 KB buffer
            loop {
                let n = reader.read(&mut buf).await.unwrap_or_else(|e| {
                    eprintln!("Failed to read local file: {}", e);
                    std::process::exit(1);
                });
                if n == 0 {
                    break;
                }
                remote_file.write(&buf[..n]).await.unwrap_or_else(|e| {
                    eprintln!("Failed to write to remote file: {}", e);
                    std::process::exit(1);
                });
            }

            remote_file.close().await.unwrap_or_else(|e| {
                eprintln!("Failed to close remote file: {}", e);
                std::process::exit(1);
            });
        }
        Operation::Read => {
            // Open remote file for reading
            let mut remote_file = client.open(&source, O_RDONLY).await.unwrap_or_else(|e| {
                eprintln!("Failed to open remote file {}: {}", source, e);
                std::process::exit(1);
            });

            // Open local file for writing
            let local_file = File::create(&dest).await.unwrap_or_else(|e| {
                eprintln!("Failed to create local file {}: {}", dest, e);
                std::process::exit(1);
            });
            let mut writer = BufWriter::new(local_file);

            // Stream data from remote to local
            let mut buf = vec![0u8; 16 * 1024]; // 16 KB buffer
            loop {
                let n = remote_file.read(&mut buf).await.unwrap_or_else(|e| {
                    eprintln!("Failed to read remote file: {}", e);
                    std::process::exit(1);
                });
                if n == 0 {
                    break;
                }
                writer.write_all(&buf[..n]).await.unwrap_or_else(|e| {
                    eprintln!("Failed to write local file: {}", e);
                    std::process::exit(1);
                });
            }

            writer.flush().await.unwrap_or_else(|e| {
                eprintln!("Failed to flush local file: {}", e);
                std::process::exit(1);
            });

            remote_file.close().await.unwrap_or_else(|e| {
                eprintln!("Failed to close remote file: {}", e);
                std::process::exit(1);
            });
        }
    }
}
