use std::io::{Read, Write};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::fs::File;
use tonic::transport::Channel;
use futures::stream;
use futures::StreamExt;

use crate::args::RustDFSArgs;
use crate::proto::NameWriteRequest;
use crate::proto::name_node_client::NameNodeClient;
use crate::proto::{NameReadRequest};

const BLOCK_SIZE: usize = 1024 * 1024 * 2; // 2 MB

pub trait RustDFSClient {
    async fn client_write(&mut self, args: RustDFSArgs);
    async fn client_read(&mut self, args: RustDFSArgs);
}

impl RustDFSClient for NameNodeClient<Channel> {

    async fn client_write(
        &mut self, 
        args: RustDFSArgs
    ) {
        let rs = stream::unfold(
            (
                args.dest, 
                File::open(&args.source).await.unwrap()
            ), 
            |(name, mut file)| async move {

                let mut buf = vec![0u8; BLOCK_SIZE];
                let n = file.read(&mut buf).await.unwrap();

                if n == 0 {
                    return None;
                }

                Some((
                    NameWriteRequest {
                        file_name: name.clone(),
                        data: buf[..n].to_vec(),
                    },
                    (name, file),
                ))
        });

        self.write(rs)
            .await
            .unwrap();
    }

    async fn client_read(
        &mut self, 
        args: RustDFSArgs
    ) {
        let mut stream = self.read(
            NameReadRequest {
                file_name: args.source,
            }
        )
        .await
        .unwrap()
        .into_inner();

        let mut file = File::create(&args.dest)
            .await
            .unwrap();

        while let Some(res) = stream.next().await {
            file.write_all(&res.unwrap().data)
                .await
                .unwrap();
        }
    }
}
