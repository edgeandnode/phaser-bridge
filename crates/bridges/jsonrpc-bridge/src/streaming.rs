use crate::client::JsonRpcClient;
use crate::converter::JsonRpcConverter;
use alloy_rpc_types_eth::{BlockNumberOrTag, Filter};
use anyhow::Result;
use arrow_array::RecordBatch;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

/// Service for streaming blockchain data
pub struct StreamingService {
    client: Arc<JsonRpcClient>,
    blocks_tx: broadcast::Sender<RecordBatch>,
    txs_tx: broadcast::Sender<RecordBatch>,
    logs_tx: broadcast::Sender<RecordBatch>,
}

impl StreamingService {
    /// Create a new streaming service
    pub fn new(client: Arc<JsonRpcClient>) -> Self {
        // Create broadcast channels with larger buffer sizes to prevent lagging
        let (blocks_tx, _) = broadcast::channel(1000);
        let (txs_tx, _) = broadcast::channel(10000);
        let (logs_tx, _) = broadcast::channel(10000);

        Self {
            client,
            blocks_tx,
            txs_tx,
            logs_tx,
        }
    }

    /// Subscribe to block stream
    pub fn subscribe_blocks(&self) -> broadcast::Receiver<RecordBatch> {
        self.blocks_tx.subscribe()
    }

    /// Subscribe to transaction stream
    pub fn subscribe_transactions(&self) -> broadcast::Receiver<RecordBatch> {
        self.txs_tx.subscribe()
    }

    /// Subscribe to logs stream
    pub fn subscribe_logs(&self) -> broadcast::Receiver<RecordBatch> {
        self.logs_tx.subscribe()
    }

    /// Start streaming data
    pub async fn start_streaming(&self) -> Result<()> {
        info!("Starting streaming service");

        if self.client.supports_subscriptions() {
            info!("Using subscription mode (WebSocket/IPC)");
            self.start_subscription_mode().await
        } else {
            info!("Using polling mode (HTTP)");
            self.start_polling_mode().await
        }
    }

    /// Start subscription-based streaming (WebSocket/IPC)
    async fn start_subscription_mode(&self) -> Result<()> {
        // Subscribe to new blocks
        let mut block_sub = self.client.subscribe_new_heads().await?;

        let client = self.client.clone();
        let blocks_tx = self.blocks_tx.clone();
        let txs_tx = self.txs_tx.clone();

        tokio::spawn(async move {
            info!("Block subscription task started");

            loop {
                let header = match block_sub.recv().await {
                    Ok(h) => h,
                    Err(e) => {
                        error!("Block subscription error: {}", e);
                        // Try to recover from lagged channel by continuing
                        if e.to_string().contains("lagged") {
                            warn!("Block subscription lagged, continuing...");
                            continue;
                        }
                        break;
                    }
                };
                let block_num = header.number;
                debug!("Received new block header: #{}", block_num);

                // Fetch full block with transactions
                match client
                    .get_block_with_txs(BlockNumberOrTag::Number(block_num))
                    .await
                {
                    Ok(Some(block)) => {
                        // Convert and emit block header
                        match evm_common::rpc_conversions::convert_any_header(&block.header) {
                            Ok(batch) => {
                                debug!("Emitting block batch for block #{}", block_num);
                                // Only send if there are receivers
                                if blocks_tx.receiver_count() > 0 {
                                    if let Err(e) = blocks_tx.send(batch) {
                                        warn!("Failed to send block batch: {}", e);
                                    }
                                } else {
                                    debug!("No receivers for block batch, skipping");
                                }
                            }
                            Err(e) => {
                                error!("Failed to convert block header: {}", e);
                            }
                        }

                        // Convert and emit transactions
                        if !block.transactions.is_empty() {
                            match JsonRpcConverter::convert_transactions(&block) {
                                Ok(batch) => {
                                    debug!(
                                        "Emitting {} transactions for block #{}",
                                        block.transactions.len(),
                                        block_num
                                    );
                                    // Only send if there are receivers
                                    if txs_tx.receiver_count() > 0 {
                                        if let Err(e) = txs_tx.send(batch) {
                                            warn!("Failed to send transaction batch: {}", e);
                                        }
                                    } else {
                                        debug!("No receivers for transaction batch, skipping");
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to convert transactions: {}", e);
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        warn!("Block #{} not found", block_num);
                    }
                    Err(e) => {
                        error!("Failed to fetch block #{}: {}", block_num, e);
                    }
                }
            }

            warn!("Block subscription ended");
        });

        // Subscribe to logs
        let filter = Filter::new(); // Empty filter for all logs
        let mut log_sub = self.client.subscribe_logs(filter).await?;

        let logs_tx = self.logs_tx.clone();

        tokio::spawn(async move {
            info!("Log subscription task started");
            let mut batch_buffer = Vec::new();
            let mut last_block = 0u64;

            loop {
                let log = match log_sub.recv().await {
                    Ok(l) => l,
                    Err(e) => {
                        error!("Log subscription error: {}", e);
                        // Try to recover from lagged channel by continuing
                        if e.to_string().contains("lagged") {
                            warn!("Log subscription lagged, continuing...");
                            continue;
                        }
                        break;
                    }
                };
                let block_num = log.block_number.unwrap_or(0);
                let block_hash = log.block_hash.unwrap_or_default();

                // Batch logs by block
                if block_num != last_block && !batch_buffer.is_empty() {
                    // Emit the batch
                    match JsonRpcConverter::convert_logs(&batch_buffer, last_block, block_hash, 0) {
                        Ok(batch) => {
                            debug!(
                                "Emitting {} logs for block #{}",
                                batch_buffer.len(),
                                last_block
                            );
                            // Only send if there are receivers
                            if logs_tx.receiver_count() > 0 {
                                if let Err(e) = logs_tx.send(batch) {
                                    warn!("Failed to send log batch: {}", e);
                                }
                            } else {
                                debug!("No receivers for log batch, skipping");
                            }
                        }
                        Err(e) => {
                            error!("Failed to convert logs: {}", e);
                        }
                    }
                    batch_buffer.clear();
                }

                batch_buffer.push(log);
                last_block = block_num;
            }

            warn!("Log subscription ended");
        });

        Ok(())
    }

    /// Start polling-based streaming (HTTP)
    async fn start_polling_mode(&self) -> Result<()> {
        let client = self.client.clone();
        let blocks_tx = self.blocks_tx.clone();
        let txs_tx = self.txs_tx.clone();
        let logs_tx = self.logs_tx.clone();

        tokio::spawn(async move {
            info!("Polling task started");
            let mut last_block = match client.get_block_number().await {
                Ok(n) => n,
                Err(e) => {
                    error!("Failed to get initial block number: {}", e);
                    return;
                }
            };

            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                // Get current block number
                let current_block = match client.get_block_number().await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("Failed to get block number: {}", e);
                        continue;
                    }
                };

                // Process any new blocks
                for block_num in (last_block + 1)..=current_block {
                    debug!("Polling: fetching block #{}", block_num);

                    match client
                        .get_block_with_txs(BlockNumberOrTag::Number(block_num))
                        .await
                    {
                        Ok(Some(block)) => {
                            // Convert and emit block header
                            if let Ok(batch) =
                                evm_common::rpc_conversions::convert_any_header(&block.header)
                            {
                                if blocks_tx.receiver_count() > 0 {
                                    let _ = blocks_tx.send(batch);
                                }
                            }

                            // Convert and emit transactions
                            if !block.transactions.is_empty() {
                                if let Ok(batch) = JsonRpcConverter::convert_transactions(&block) {
                                    if txs_tx.receiver_count() > 0 {
                                        let _ = txs_tx.send(batch);
                                    }
                                }
                            }

                            // Fetch and emit logs for this block
                            let filter = Filter::new().from_block(block_num).to_block(block_num);

                            if let Ok(logs) = client.get_logs(filter).await {
                                if !logs.is_empty() {
                                    let block_hash = block.header.hash;
                                    if let Ok(batch) = JsonRpcConverter::convert_logs(
                                        &logs,
                                        block_num,
                                        block_hash,
                                        block.header.timestamp,
                                    ) {
                                        if logs_tx.receiver_count() > 0 {
                                            let _ = logs_tx.send(batch);
                                        }
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            warn!("Block #{} not found", block_num);
                        }
                        Err(e) => {
                            error!("Failed to fetch block #{}: {}", block_num, e);
                        }
                    }
                }

                last_block = current_block;
            }
        });

        Ok(())
    }
}
