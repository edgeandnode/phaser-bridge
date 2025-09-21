use anyhow::Result;
use tonic::transport::Channel;
use tracing::{info, debug, error};
use futures::StreamExt;

use crate::proto::{
    EthbackendClient, Event, SubscribeRequest,
    BlockRequest, BlockReply,
};
use crate::proto::remote::SyncingReply;

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

        let mut client = EthbackendClient::new(channel);

        // Test the connection
        let request = tonic::Request::new(crate::proto::remote::ClientVersionRequest {});
        let response = client.client_version(request).await?;
        info!("Connected to Erigon node: {}", response.into_inner().node_name);

        Ok(Self { client, endpoint })
    }

    /// Get the current syncing status
    pub async fn syncing_status(&mut self) -> Result<SyncingReply> {
        let request = tonic::Request::new(());
        let response = self.client.syncing(request).await?;
        Ok(response.into_inner())
    }

    /// Subscribe to block headers
    pub async fn subscribe_headers(&mut self) -> Result<tonic::Streaming<crate::proto::remote::SubscribeReply>> {
        info!("Subscribing to block headers...");

        let request = tonic::Request::new(SubscribeRequest {
            r#type: Event::Header as i32,
        });

        let stream = self.client.subscribe(request).await?.into_inner();
        info!("Subscription established");

        Ok(stream)
    }

    // TODO: Implement subscribe_logs when needed

    /// Get the latest block number
    pub async fn get_latest_block(&mut self) -> Result<u64> {
        let sync_info = self.syncing_status().await?;
        Ok(sync_info.current_block)
    }

    /// Get a full block by number
    pub async fn get_block(&mut self, block_number: u64) -> Result<BlockReply> {
        debug!("Fetching full block #{}", block_number);

        let request = tonic::Request::new(BlockRequest {
            block_height: block_number,
            block_hash: None,
        });

        let response = self.client.block(request).await?;
        let block = response.into_inner();

        debug!("Received block #{} - RLP size: {} bytes, senders: {} bytes",
               block_number, block.block_rlp.len(), block.senders.len());

        Ok(block)
    }

    /// Clone the client (for moving into async tasks)
    pub fn clone(&self) -> ErigonClient {
        ErigonClient {
            client: self.client.clone(),
            endpoint: self.endpoint.clone(),
        }
    }
}