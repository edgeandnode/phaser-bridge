use anyhow::Result;
use arrow_array::RecordBatch;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, error, debug};

use crate::client::ErigonClient;
use crate::converter::ErigonDataConverter;

/// Stateless service that streams from Erigon
pub struct StreamingService {
    client: Arc<tokio::sync::Mutex<ErigonClient>>,
    // Broadcast channel for real-time blocks
    block_sender: broadcast::Sender<RecordBatch>,
}

impl StreamingService {
    pub fn new(client: ErigonClient) -> Self {
        let (block_sender, _) = broadcast::channel(100);

        Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            block_sender,
        }
    }

    /// Start streaming blocks from Erigon (runs continuously)
    pub async fn start_streaming(self: Arc<Self>) -> Result<()> {
        info!("Starting continuous streaming from Erigon");

        let client = self.client.clone();
        let sender = self.block_sender.clone();

        // Spawn background task that always streams
        tokio::spawn(async move {
            loop {
                match Self::stream_blocks(client.clone(), sender.clone()).await {
                    Ok(_) => {
                        error!("Stream ended unexpectedly, restarting in 5 seconds");
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                    Err(e) => {
                        error!("Stream error: {}, restarting in 5 seconds", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });

        Ok(())
    }

    async fn stream_blocks(
        client: Arc<tokio::sync::Mutex<ErigonClient>>,
        sender: broadcast::Sender<RecordBatch>,
    ) -> Result<()> {
        let mut client = client.lock().await;
        let mut stream = client.subscribe_headers().await?;

        info!("Subscription established, streaming blocks continuously");

        while let Some(msg) = stream.message().await? {
            debug!("Received block event - type: {}, data_len: {}", msg.r#type, msg.data.len());

            // Convert actual Erigon data to RecordBatch
            let batch = match ErigonDataConverter::convert_subscribe_reply(&msg) {
                Ok(batch) => {
                    info!("Converted block header to RecordBatch with {} rows", batch.num_rows());
                    batch
                }
                Err(e) => {
                    error!("Failed to convert block header: {}", e);
                    continue; // Skip this block on conversion error
                }
            };

            // Broadcast to all active subscribers
            let _ = sender.send(batch); // Ignore if no receivers
        }

        Ok(())
    }

    /// Subscribe to the block stream
    pub fn subscribe(&self) -> broadcast::Receiver<RecordBatch> {
        self.block_sender.subscribe()
    }
}