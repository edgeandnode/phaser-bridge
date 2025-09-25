use alloy_rlp::Decodable;
use anyhow::Result;
use arrow_array::RecordBatch;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::client::ErigonClient;
use crate::converter::ErigonDataConverter;

/// Service that streams different data types from Erigon
/// Each data type has its own broadcast channel to maintain schema consistency
pub struct StreamingService {
    client: Arc<tokio::sync::Mutex<ErigonClient>>,
    // Separate broadcast channels for each data type
    block_sender: broadcast::Sender<RecordBatch>,
    tx_sender: broadcast::Sender<RecordBatch>,
    log_sender: broadcast::Sender<RecordBatch>,
}

impl StreamingService {
    pub fn new(client: ErigonClient) -> Self {
        // Create separate channels for each data type
        let (block_sender, _) = broadcast::channel(100);
        let (tx_sender, _) = broadcast::channel(100);
        let (log_sender, _) = broadcast::channel(100);

        Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            block_sender,
            tx_sender,
            log_sender,
        }
    }

    /// Start streaming all data types from Erigon
    pub async fn start_streaming(self: Arc<Self>) -> Result<()> {
        info!("Starting streaming services from Erigon");

        // Start block/transaction streaming
        let blocks_service = self.clone();
        tokio::spawn(async move {
            loop {
                match blocks_service.stream_blocks_and_transactions().await {
                    Ok(_) => {
                        error!("Block stream ended unexpectedly, restarting in 5 seconds");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Err(e) => {
                        error!("Block stream error: {}, restarting in 5 seconds", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });

        // Start log streaming
        let logs_service = self.clone();
        tokio::spawn(async move {
            loop {
                match logs_service.stream_logs().await {
                    Ok(_) => {
                        error!("Log stream ended unexpectedly, restarting in 5 seconds");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Err(e) => {
                        error!("Log stream error: {}, restarting in 5 seconds", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Stream blocks and transactions from Erigon
    async fn stream_blocks_and_transactions(&self) -> Result<()> {
        let mut client_guard = self.client.lock().await;
        let mut stream = client_guard.subscribe_headers().await?;
        drop(client_guard); // Release lock after getting stream

        info!("Block/transaction subscription established");

        while let Some(msg) = stream.message().await? {
            debug!(
                "Received block event - type: {}, data_len: {}",
                msg.r#type,
                msg.data.len()
            );

            // Skip non-header events or empty data
            if msg.r#type != 0 || msg.data.is_empty() {
                debug!("Skipping non-header event or empty data");
                continue;
            }

            // Check if we have any subscribers for blocks or transactions
            let block_subscribers = self.block_sender.receiver_count();
            let tx_subscribers = self.tx_sender.receiver_count();

            if block_subscribers == 0 && tx_subscribers == 0 {
                info!("No subscribers for blocks or transactions, skipping processing");
                continue;
            }

            // Decode the header
            let header = match alloy_consensus::Header::decode(&mut &msg.data[..]) {
                Ok(h) => h,
                Err(e) => {
                    error!("Failed to decode header: {}", e);
                    continue;
                }
            };

            let block_num = header.number;
            let block_hash = header.hash_slow();

            debug!(
                "Processing block #{} - subscribers: {} blocks, {} txs",
                block_num, block_subscribers, tx_subscribers
            );

            // If we only have block subscribers and no tx subscribers, just send the header
            if block_subscribers > 0 && tx_subscribers == 0 {
                match ErigonDataConverter::convert_subscribe_reply(&msg) {
                    Ok(batch) => {
                        info!(
                            "Sending block #{} header to {} subscribers",
                            block_num, block_subscribers
                        );
                        let _ = self.block_sender.send(batch);
                    }
                    Err(e) => {
                        error!("Failed to convert block header: {}", e);
                    }
                }
                continue;
            }

            // If we have transaction subscribers (or both), fetch the full block
            if tx_subscribers > 0 || block_subscribers > 0 {
                info!(
                    "Fetching full block #{} with hash 0x{}",
                    block_num,
                    hex::encode(block_hash.as_slice())
                );

                let mut client_guard = self.client.lock().await;
                let block_reply = match client_guard.get_block(block_num, Some(&block_hash.0)).await
                {
                    Ok(reply) => {
                        info!(
                            "Successfully fetched block #{} - RLP: {} bytes, senders: {} bytes",
                            block_num,
                            reply.block_rlp.len(),
                            reply.senders.len()
                        );
                        reply
                    }
                    Err(e) => {
                        error!("Failed to fetch full block #{}: {}", block_num, e);
                        drop(client_guard);

                        // If we have block subscribers, send just the header as fallback
                        if block_subscribers > 0 {
                            match ErigonDataConverter::convert_subscribe_reply(&msg) {
                                Ok(batch) => {
                                    info!("Sending block #{} header as fallback", block_num);
                                    let _ = self.block_sender.send(batch);
                                }
                                Err(e) => {
                                    error!("Failed to convert block header: {}", e);
                                }
                            }
                        }
                        continue;
                    }
                };
                drop(client_guard);

                // Convert the full block
                match ErigonDataConverter::convert_full_block(&block_reply) {
                    Ok((header_batch, tx_batch)) => {
                        // Send header to block subscribers
                        if block_subscribers > 0 {
                            info!(
                                "Sending block #{} header to {} subscribers",
                                block_num, block_subscribers
                            );
                            let _ = self.block_sender.send(header_batch);
                        }

                        // Send transactions to transaction subscribers
                        if let Some(tx_batch) = tx_batch {
                            if tx_subscribers > 0 {
                                info!(
                                    "Sending {} transactions from block #{} to {} subscribers",
                                    tx_batch.num_rows(),
                                    block_num,
                                    tx_subscribers
                                );
                                let _ = self.tx_sender.send(tx_batch);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to convert full block #{}: {}", block_num, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Stream logs from Erigon
    async fn stream_logs(&self) -> Result<()> {
        let mut client_guard = self.client.lock().await;
        // Subscribe to all logs (no filters for now)
        let mut stream = client_guard
            .subscribe_logs(
                true,   // all_addresses
                vec![], // no specific addresses
                true,   // all_topics
                vec![], // no specific topics
            )
            .await?;
        drop(client_guard); // Release lock after getting stream

        info!("Log subscription established");

        // Buffer logs by block for batching
        let mut current_block_logs: Vec<crate::proto::remote::SubscribeLogsReply> = Vec::new();
        let mut current_block_num: Option<u64> = None;
        let mut current_block_timestamp: Option<i64> = None;

        while let Some(log) = stream.message().await? {
            debug!(
                "Received log - block: {}, tx_index: {}, log_index: {}",
                log.block_number, log.transaction_index, log.log_index
            );

            // Check if we have any subscribers
            let subscriber_count = self.log_sender.receiver_count();
            if subscriber_count == 0 {
                debug!("No log subscribers, skipping processing");
                continue;
            }

            // If this is a new block, send the previous block's logs
            if let Some(block_num) = current_block_num {
                if log.block_number != block_num && !current_block_logs.is_empty() {
                    // Convert and send the batch
                    let timestamp = current_block_timestamp.unwrap_or(0);
                    match ErigonDataConverter::convert_logs_to_batch(&current_block_logs, timestamp)
                    {
                        Ok(batch) => {
                            info!(
                                "Sending {} logs from block #{} to {} subscribers",
                                current_block_logs.len(),
                                block_num,
                                subscriber_count
                            );
                            let _ = self.log_sender.send(batch);
                        }
                        Err(e) => {
                            error!("Failed to convert logs for block #{}: {}", block_num, e);
                        }
                    }

                    // Clear for next block
                    current_block_logs.clear();
                }
            }

            // Update current block tracking
            if current_block_num != Some(log.block_number) {
                current_block_num = Some(log.block_number);
                // TODO: Get actual block timestamp - for now use unix timestamp in nanos
                current_block_timestamp = Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("System clock is set before Unix epoch")
                        .as_nanos() as i64,
                );
            }

            // Add log to current block's collection
            current_block_logs.push(log);
        }

        // Send any remaining logs
        if !current_block_logs.is_empty() {
            if let Some(block_num) = current_block_num {
                let timestamp = current_block_timestamp.unwrap_or(0);
                let subscriber_count = self.log_sender.receiver_count();

                match ErigonDataConverter::convert_logs_to_batch(&current_block_logs, timestamp) {
                    Ok(batch) => {
                        info!(
                            "Sending final {} logs from block #{} to {} subscribers",
                            current_block_logs.len(),
                            block_num,
                            subscriber_count
                        );
                        let _ = self.log_sender.send(batch);
                    }
                    Err(e) => {
                        error!(
                            "Failed to convert final logs for block #{}: {}",
                            block_num, e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Subscribe to block header stream
    pub fn subscribe_blocks(&self) -> broadcast::Receiver<RecordBatch> {
        self.block_sender.subscribe()
    }

    /// Subscribe to transaction stream
    pub fn subscribe_transactions(&self) -> broadcast::Receiver<RecordBatch> {
        self.tx_sender.subscribe()
    }

    /// Subscribe to log stream
    pub fn subscribe_logs(&self) -> broadcast::Receiver<RecordBatch> {
        self.log_sender.subscribe()
    }
}
