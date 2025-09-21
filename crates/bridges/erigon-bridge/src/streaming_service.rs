use anyhow::Result;
use arrow_array::RecordBatch;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, error, debug};
use alloy_rlp::Decodable;

use crate::client::ErigonClient;
use crate::converter::ErigonDataConverter;

#[derive(Clone)]
pub enum StreamDataType {
    Headers,      // Header batches only
    Transactions, // Transaction batches only
    Both,        // Both header and transaction batches
}

/// Stateless service that streams from Erigon
pub struct StreamingService {
    client: Arc<tokio::sync::Mutex<ErigonClient>>,
    // Broadcast channel for real-time blocks
    block_sender: broadcast::Sender<RecordBatch>,
    // Configuration for what data to stream
    stream_type: StreamDataType,
}

impl StreamingService {
    pub fn new(client: ErigonClient) -> Self {
        let (block_sender, _) = broadcast::channel(100);

        Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            block_sender,
            stream_type: StreamDataType::Both, // Default to both
        }
    }

    pub fn with_stream_type(client: ErigonClient, stream_type: StreamDataType) -> Self {
        let (block_sender, _) = broadcast::channel(100);

        Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            block_sender,
            stream_type,
        }
    }

    /// Start streaming blocks from Erigon (runs continuously)
    pub async fn start_streaming(self: Arc<Self>) -> Result<()> {
        info!("Starting continuous streaming from Erigon");

        let client = self.client.clone();
        let sender = self.block_sender.clone();
        let stream_type = self.stream_type.clone();

        // Spawn background task that always streams
        tokio::spawn(async move {
            loop {
                match Self::stream_blocks(client.clone(), sender.clone(), stream_type.clone()).await {
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
        stream_type: StreamDataType,
    ) -> Result<()> {
        let mut client_guard = client.lock().await;
        let mut stream = client_guard.subscribe_headers().await?;
        drop(client_guard); // Release lock after getting stream

        info!("Subscription established, streaming blocks continuously");

        while let Some(msg) = stream.message().await? {
            debug!("Received block event - type: {}, data_len: {}", msg.r#type, msg.data.len());

            // Skip non-header events or empty data
            if msg.r#type != 0 || msg.data.is_empty() {
                debug!("Skipping non-header event or empty data");
                continue;
            }

            // First, decode the header to get block number
            let header = match alloy_consensus::Header::decode(&mut &msg.data[..]) {
                Ok(h) => h,
                Err(e) => {
                    error!("Failed to decode header: {}", e);
                    continue;
                }
            };

            let block_num = header.number;

            match stream_type {
                StreamDataType::Headers => {
                    // Just convert and send the header
                    let batch = match ErigonDataConverter::convert_subscribe_reply(&msg) {
                        Ok(batch) => {
                            info!("Converted block #{} header to RecordBatch", block_num);
                            batch
                        }
                        Err(e) => {
                            error!("Failed to convert block header: {}", e);
                            continue;
                        }
                    };
                    let _ = sender.send(batch);
                },
                StreamDataType::Both | StreamDataType::Transactions => {
                    // Fetch the full block
                    let mut client_guard = client.lock().await;
                    let block_reply = match client_guard.get_block(block_num).await {
                        Ok(reply) => reply,
                        Err(e) => {
                            error!("Failed to fetch full block #{}: {}. Falling back to header only.", block_num, e);
                            drop(client_guard);

                            // Fall back to just sending the header if we're in Both mode
                            if matches!(stream_type, StreamDataType::Both) {
                                let batch = match ErigonDataConverter::convert_subscribe_reply(&msg) {
                                    Ok(batch) => batch,
                                    Err(e) => {
                                        error!("Failed to convert block header: {}", e);
                                        continue;
                                    }
                                };
                                let _ = sender.send(batch);
                            }
                            continue;
                        }
                    };
                    drop(client_guard);

                    // Convert the full block
                    let (header_batch, tx_batch) = match ErigonDataConverter::convert_full_block(&block_reply) {
                        Ok(batches) => batches,
                        Err(e) => {
                            error!("Failed to convert full block #{}: {}", block_num, e);
                            continue;
                        }
                    };

                    // Send based on stream type
                    match stream_type {
                        StreamDataType::Both => {
                            info!("Block #{}: sending header batch", block_num);
                            let _ = sender.send(header_batch);

                            if let Some(tx_batch) = tx_batch {
                                info!("Block #{}: sending {} transactions", block_num, tx_batch.num_rows());
                                let _ = sender.send(tx_batch);
                            }
                        },
                        StreamDataType::Transactions => {
                            if let Some(tx_batch) = tx_batch {
                                info!("Block #{}: sending {} transactions", block_num, tx_batch.num_rows());
                                let _ = sender.send(tx_batch);
                            }
                        },
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    /// Subscribe to the block stream
    pub fn subscribe(&self) -> broadcast::Receiver<RecordBatch> {
        self.block_sender.subscribe()
    }
}