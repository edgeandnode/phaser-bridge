/// Client for Erigon's custom BlockDataBackend service
use crate::error::ErigonBridgeError;
use crate::proto::custom::{
    block_data_backend_client::BlockDataBackendClient, BlockBatch, BlockRangeRequest, ReceiptBatch,
    TransactionBatch,
};
use std::time::Duration;
use tonic::transport::Channel;
use tonic::Streaming;
use tracing::{debug, error, info};

pub struct BlockDataClient {
    client: BlockDataBackendClient<Channel>,
    endpoint: String,
}

impl BlockDataClient {
    /// Connect to Erigon's BlockDataBackend service
    pub async fn connect(endpoint: String) -> Result<Self, ErigonBridgeError> {
        info!("Connecting to Erigon BlockDataBackend at {}", endpoint);

        // Parse endpoint to determine if it's IPC or TCP
        let channel = if endpoint.starts_with('/') || endpoint.starts_with("./") {
            // Unix socket (IPC)
            info!("Using IPC connection to Erigon BlockDataBackend");

            // We need to use a dummy URI for IPC
            let uri = "http://[::]:50051".to_string();
            let path = endpoint.clone();

            Channel::builder(uri.parse()?)
                .timeout(Duration::from_secs(300)) // 5 minute timeout for long-running operations
                .http2_keep_alive_interval(Duration::from_secs(10)) // Send keepalive ping every 10s
                .keep_alive_timeout(Duration::from_secs(20)) // Wait 20s for keepalive response
                .keep_alive_while_idle(true) // Send pings even when no active requests
                .tcp_keepalive(Some(Duration::from_secs(60))) // TCP-level keepalive
                .connect_with_connector(tower::service_fn(move |_| {
                    let socket_path = path.clone();
                    async move {
                        let stream = tokio::net::UnixStream::connect(socket_path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }))
                .await?
        } else {
            // TCP connection
            info!("Using TCP connection to Erigon BlockDataBackend");
            info!("Note: Custom Erigon must be running with BlockDataBackend service enabled");

            let uri = if !endpoint.starts_with("http://") {
                format!("http://{}", endpoint)
            } else {
                endpoint.clone()
            };

            Channel::builder(uri.parse()?)
                .timeout(Duration::from_secs(300)) // 5 minute timeout for long-running operations
                .http2_keep_alive_interval(Duration::from_secs(10)) // Send keepalive ping every 10s
                .keep_alive_timeout(Duration::from_secs(20)) // Wait 20s for keepalive response
                .keep_alive_while_idle(true) // Send pings even when no active requests
                .tcp_keepalive(Some(Duration::from_secs(60))) // TCP-level keepalive
                .connect()
                .await?
        };

        // Configure message size limits (128MB to handle large transaction batches)
        const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

        let client = BlockDataBackendClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);

        Ok(Self {
            client,
            endpoint: endpoint.clone(),
        })
    }

    /// Stream RLP-encoded block headers for a range (fast: from snapshots/DB)
    pub async fn stream_blocks(
        &mut self,
        from_block: u64,
        to_block: u64,
        batch_size: u32,
    ) -> Result<Streaming<BlockBatch>, ErigonBridgeError> {
        let request = BlockRangeRequest {
            from_block,
            to_block,
            batch_size: if batch_size > 0 { batch_size } else { 1000 },
        };

        debug!(
            "Starting block header stream (blocks {}-{}, batch_size: {})",
            from_block, to_block, batch_size
        );

        let response = self.client.stream_blocks(request).await?;
        Ok(response.into_inner())
    }

    /// Stream RLP-encoded transactions for a range (fast: from blocks, no receipts)
    pub async fn stream_transactions(
        &mut self,
        from_block: u64,
        to_block: u64,
        batch_size: u32,
    ) -> Result<Streaming<TransactionBatch>, ErigonBridgeError> {
        let request = BlockRangeRequest {
            from_block,
            to_block,
            batch_size: if batch_size > 0 { batch_size } else { 1000 },
        };

        debug!(
            "Starting transaction stream (blocks {}-{}, batch_size: {})",
            from_block, to_block, batch_size
        );

        let response = self.client.stream_transactions(request).await?;
        Ok(response.into_inner())
    }

    /// Execute blocks and stream RLP-encoded receipts (slow: executes blocks, generates receipts)
    /// Receipts contain logs - decode RLP to extract log data
    pub async fn execute_blocks(
        &mut self,
        from_block: u64,
        to_block: u64,
        batch_size: u32,
    ) -> Result<Streaming<ReceiptBatch>, ErigonBridgeError> {
        let request = BlockRangeRequest {
            from_block,
            to_block,
            batch_size: if batch_size > 0 { batch_size } else { 1000 },
        };

        debug!(
            "Starting receipt stream via block execution (blocks {}-{}, batch_size: {})",
            from_block, to_block, batch_size
        );

        let response = self.client.execute_blocks(request).await?;
        Ok(response.into_inner())
    }

    /// Test if the BlockDataBackend service is available
    pub async fn test_connection(&mut self) -> Result<(), ErigonBridgeError> {
        info!("Testing BlockDataBackend service connection...");

        // Try to stream a single block (genesis)
        match self.stream_blocks(0, 0, 1).await {
            Ok(mut stream) => match stream.message().await {
                Ok(Some(batch)) => {
                    info!(
                        "BlockDataBackend service is available! Received {} blocks",
                        batch.blocks.len()
                    );
                    Ok(())
                }
                Ok(None) => {
                    error!("BlockDataBackend service returned no data");
                    Err(ErigonBridgeError::ErigonClient(Box::new(tonic::Status::not_found(
                        "No blocks returned",
                    ))))
                }
                Err(e) => {
                    error!("BlockDataBackend service error: {}", e);
                    Err(ErigonBridgeError::from(e))
                }
            },
            Err(e) => {
                // Check if it's an "Unimplemented" error
                if let ErigonBridgeError::ErigonClient(status) = &e {
                    if status.code() == tonic::Code::Unimplemented {
                        error!(
                            "BlockDataBackend service is NOT implemented in this Erigon instance. \
                            You need to run a custom Erigon build with BlockDataBackend support."
                        );
                    }
                }
                Err(e)
            }
        }
    }
}

impl Clone for BlockDataClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            endpoint: self.endpoint.clone(),
        }
    }
}
