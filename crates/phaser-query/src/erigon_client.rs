use anyhow::Result;
use tonic::transport::Channel;
use tracing::{info, debug, error, warn};
use futures::StreamExt;

use crate::proto::{
    EthbackendClient, Event, SubscribeRequest, SubscribeReply,
    BlockRequest, BlockReply, LogsFilterRequest, SubscribeLogsReply,
};
use crate::proto::types::{H256, H160};

/// Client for connecting to Erigon's gRPC interface
pub struct ErigonClient {
    client: EthbackendClient<Channel>,
    endpoint: String,
}

impl ErigonClient {
    /// Create a new ErigonClient and connect to the given endpoint
    pub async fn connect(endpoint: String) -> Result<Self> {
        info!("Connecting to Erigon gRPC at {}", endpoint);
        info!("Note: Erigon must be running with --private.api.addr={}", endpoint);

        let channel = Channel::from_shared(format!("http://{}", endpoint))?
            .connect()
            .await
            .map_err(|e| {
                error!("Failed to connect to Erigon at {}: {}", endpoint, e);
                error!("Make sure Erigon is running with --private.api.addr flag");
                e
            })?;

        let client = EthbackendClient::new(channel);

        info!("Successfully connected to Erigon");

        Ok(Self { client, endpoint })
    }

    /// Test the connection by fetching the client version
    pub async fn test_connection(&mut self) -> Result<()> {
        use crate::proto::remote::ClientVersionRequest;

        let request = tonic::Request::new(ClientVersionRequest {});
        let response = self.client.client_version(request).await?;

        info!("Connected to Erigon node: {}", response.into_inner().node_name);

        Ok(())
    }

    /// Get the current syncing status
    pub async fn syncing_status(&mut self) -> Result<()> {
        let request = tonic::Request::new(());
        let response = self.client.syncing(request).await?;
        let sync_info = response.into_inner();

        info!("Syncing status:");
        info!("  Last new block: {}", sync_info.last_new_block_seen);
        info!("  Frozen blocks: {}", sync_info.frozen_blocks);
        info!("  Current block: {}", sync_info.current_block);
        info!("  Is syncing: {}", sync_info.syncing);

        if !sync_info.stages.is_empty() {
            info!("  Stages:");
            for stage in &sync_info.stages {
                info!("    {} at block {}", stage.stage_name, stage.block_number);
            }
        }

        Ok(())
    }

    /// Subscribe to block headers
    pub async fn subscribe_headers(&mut self) -> Result<()> {
        info!("Subscribing to block headers...");

        let request = tonic::Request::new(SubscribeRequest {
            r#type: Event::Header as i32,
        });

        let mut stream = self.client.subscribe(request).await?.into_inner();

        info!("Subscription established, waiting for blocks...");

        // Listen to a few blocks for testing
        let mut count = 0;
        while let Some(reply) = stream.next().await {
            match reply {
                Ok(msg) => {
                    debug!("Received event type: {:?}", msg.r#type);
                    debug!("Data length: {} bytes", msg.data.len());
                    count += 1;

                    if count >= 5 {
                        info!("Received {} blocks, stopping test", count);
                        break;
                    }
                }
                Err(e) => {
                    error!("Stream error: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Get a specific block by number
    pub async fn get_block(&mut self, block_number: u64) -> Result<()> {
        info!("Fetching block {}", block_number);

        let request = tonic::Request::new(BlockRequest {
            block_height: block_number,
            block_hash: None,
        });

        let response = self.client.block(request).await?;
        let block = response.into_inner();

        info!("Block {} retrieved:", block_number);
        info!("  RLP size: {} bytes", block.block_rlp.len());
        info!("  Senders size: {} bytes", block.senders.len());

        Ok(())
    }

    /// Get the latest block number
    pub async fn get_latest_block(&mut self) -> Result<u64> {
        let request = tonic::Request::new(());
        let response = self.client.syncing(request).await?;
        let sync_info = response.into_inner();

        Ok(sync_info.current_block)
    }
}