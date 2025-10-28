use crate::parquet_writer::ParquetWriter;
use crate::trie_writer::TrieWriter;
use anyhow::Result;
use arrow::array::UInt64Array;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use phaser_bridge::{
    descriptors::{BlockchainDescriptor, StreamType},
    FlightBridgeClient,
};
use rocksdb::DB;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

/// Enhanced streaming service with parquet writing capabilities
pub struct StreamingServiceWithWriter {
    bridges: Vec<FlightBridgeClient>,
    data_dir: PathBuf,
    max_file_size_mb: u64,
    segment_size: u64,
    db: Option<Arc<DB>>,
    chain_id: u64,
    bridge_name: String,
    live_state: Option<Arc<crate::LiveStreamingState>>,
}

impl StreamingServiceWithWriter {
    pub async fn new(
        bridge_endpoints: Vec<String>,
        data_dir: PathBuf,
        max_file_size_mb: u64,
        segment_size: u64,
        chain_id: u64,
        bridge_name: String,
        live_state: Arc<crate::LiveStreamingState>,
    ) -> Result<Self> {
        let mut bridges = Vec::new();

        for endpoint in bridge_endpoints {
            info!("Connecting to bridge at {}", endpoint);
            let client = FlightBridgeClient::connect(endpoint).await?;
            bridges.push(client);
        }

        // Create data directory
        std::fs::create_dir_all(&data_dir)?;

        Ok(Self {
            bridges,
            data_dir,
            max_file_size_mb,
            segment_size,
            db: None,
            chain_id,
            bridge_name,
            live_state: Some(live_state),
        })
    }

    /// Spawn a generic parquet writer task for any data type
    fn spawn_writer_task(
        data_dir: PathBuf,
        max_file_size_mb: u64,
        segment_size: u64,
        data_type: &'static str,
        mut receiver: mpsc::Receiver<RecordBatch>,
    ) {
        tokio::spawn(async move {
            let mut writer = match ParquetWriter::with_config_and_mode(
                data_dir,
                max_file_size_mb,
                segment_size,
                data_type.to_string(),
                None, // no custom parquet config for live streaming
                true, // is_live = true for live streaming
            ) {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to create {} parquet writer: {}", data_type, e);
                    return;
                }
            };

            while let Some(batch) = receiver.recv().await {
                if let Err(e) = writer.write_batch(batch).await {
                    error!("Failed to write {} batch to parquet: {}", data_type, e);
                }
            }

            if let Err(e) = writer.finalize_current_file() {
                error!("Failed to finalize {} parquet file: {}", data_type, e);
            }
            info!("{} parquet writer task completed", data_type);
        });
    }

    /// Spawn a stream processor task for any stream type
    fn spawn_stream_processor(
        stream_type: StreamType,
        mut stream: impl StreamExt<Item = Result<RecordBatch, arrow_flight::error::FlightError>>
            + Send
            + Unpin
            + 'static,
        sender: mpsc::Sender<RecordBatch>,
        live_state: Option<Arc<crate::LiveStreamingState>>,
        chain_id: u64,
        bridge_name: String,
    ) {
        let stream_name = format!("{:?}", stream_type).to_lowercase();
        let mut first_block_received = false;

        tokio::spawn(async move {
            while let Some(batch_result) = stream.next().await {
                match batch_result {
                    Ok(batch) => {
                        // Special logging for blocks to show block number
                        let _block_number = if matches!(stream_type, StreamType::Blocks) {
                            let block_num = batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .and_then(|a| {
                                    if !a.is_empty() {
                                        Some(a.value(0))
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(0);

                            info!(
                                "Received {} batch with {} rows, block #{}",
                                stream_name,
                                batch.num_rows(),
                                block_num
                            );

                            // Set live streaming boundary on first block
                            if !first_block_received && block_num > 0 {
                                if let Some(ref state) = live_state {
                                    let id = crate::ChainBridgeId::new(chain_id, &bridge_name);
                                    info!(
                                        "Live streaming started for chain {} bridge '{}' at block {}",
                                        chain_id, bridge_name, block_num
                                    );
                                    state.set_boundary(&id, block_num).await;
                                    first_block_received = true;
                                }
                            }

                            Some(block_num)
                        } else {
                            info!(
                                "Received {} batch with {} rows",
                                stream_name,
                                batch.num_rows()
                            );
                            None
                        };

                        if let Err(e) = sender.send(batch).await {
                            error!("Failed to send {} batch to writer: {}", stream_name, e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Error receiving {} batch: {}", stream_name, e);
                    }
                }
            }
            info!("{} stream ended", stream_name);
        });
    }

    /// Start streaming all data types from bridges with parquet persistence
    pub async fn start_streaming(&mut self) -> Result<()> {
        // Mark live streaming as enabled for this chain/bridge
        if let Some(ref state) = self.live_state {
            let id = crate::ChainBridgeId::new(self.chain_id, &self.bridge_name);
            state.mark_enabled(&id).await;
            info!(
                "Live streaming enabled for chain {} bridge '{}'",
                self.chain_id, self.bridge_name
            );
        }

        // Create channels for each data type
        let (blocks_tx, blocks_rx) = mpsc::channel::<RecordBatch>(100);
        let (txs_tx, txs_rx) = mpsc::channel::<RecordBatch>(100);
        let (logs_tx, logs_rx) = mpsc::channel::<RecordBatch>(100);

        // Spawn writer tasks for each data type
        Self::spawn_writer_task(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "blocks",
            blocks_rx,
        );

        Self::spawn_writer_task(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "transactions",
            txs_rx,
        );

        Self::spawn_writer_task(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "logs",
            logs_rx,
        );

        // Start streaming from each bridge
        for bridge in &mut self.bridges {
            // Check bridge health
            if !bridge.health_check().await? {
                error!("Bridge health check failed");
                continue;
            }

            // Subscribe to blocks
            let blocks_descriptor = BlockchainDescriptor::live(StreamType::Blocks);
            info!("Subscribing to blocks from bridge");
            let blocks_stream = bridge.subscribe(&blocks_descriptor).await?;
            Self::spawn_stream_processor(
                StreamType::Blocks,
                blocks_stream,
                blocks_tx.clone(),
                self.live_state.clone(),
                self.chain_id,
                self.bridge_name.clone(),
            );

            // Subscribe to transactions
            let txs_descriptor = BlockchainDescriptor::live(StreamType::Transactions);
            info!("Subscribing to transactions from bridge");
            let txs_stream = bridge.subscribe(&txs_descriptor).await?;
            Self::spawn_stream_processor(
                StreamType::Transactions,
                txs_stream,
                txs_tx.clone(),
                self.live_state.clone(),
                self.chain_id,
                self.bridge_name.clone(),
            );

            // Subscribe to logs
            let logs_descriptor = BlockchainDescriptor::live(StreamType::Logs);
            info!("Subscribing to logs from bridge");
            let logs_stream = bridge.subscribe(&logs_descriptor).await?;
            Self::spawn_stream_processor(
                StreamType::Logs,
                logs_stream,
                logs_tx.clone(),
                self.live_state.clone(),
                self.chain_id,
                self.bridge_name.clone(),
            );
        }

        Ok(())
    }

    /// Fetch and write historical data from bridges (blocks only for now)
    pub async fn fetch_historical(&mut self, start_block: u64, end_block: u64) -> Result<()> {
        let mut writer = ParquetWriter::new(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "blocks".to_string(),
        )?;

        for bridge in &mut self.bridges {
            let descriptor =
                BlockchainDescriptor::historical(StreamType::Blocks, start_block, end_block);

            info!(
                "Fetching historical blocks {} to {}",
                start_block, end_block
            );
            let batches = bridge.stream_data(&descriptor).await?;

            for batch in batches {
                info!("Processing historical batch with {} rows", batch.num_rows());
                writer.write_batch(batch).await?;
            }
        }

        writer.finalize_current_file()?;
        info!("Historical data fetch completed");

        Ok(())
    }

    /// Set the RocksDB instance for trie storage
    pub fn set_db(&mut self, db: Arc<DB>) {
        self.db = Some(db);
    }

    /// Spawn a trie writer task that writes to RocksDB
    fn spawn_trie_writer_task(db: Arc<DB>, mut receiver: mpsc::Receiver<RecordBatch>) {
        tokio::spawn(async move {
            let mut writer = match TrieWriter::new(db, crate::index::cf::TRIE.to_string()) {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to create trie writer: {}", e);
                    return;
                }
            };

            while let Some(batch) = receiver.recv().await {
                if let Err(e) = writer.write_batch(batch) {
                    error!("Failed to write trie batch to RocksDB: {}", e);
                }
            }

            info!(
                "Trie writer task completed, wrote {} nodes",
                writer.nodes_written()
            );
        });
    }

    /// Start streaming trie data from bridges
    pub async fn start_trie_streaming(&mut self) -> Result<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("RocksDB not set for trie storage"))?
            .clone();

        // Create channel for trie data
        let (trie_tx, trie_rx) = mpsc::channel::<RecordBatch>(100);

        // Spawn trie writer task
        Self::spawn_trie_writer_task(db, trie_rx);

        // Start streaming from each bridge
        for bridge in &mut self.bridges {
            // Check bridge health
            if !bridge.health_check().await? {
                error!("Bridge health check failed");
                continue;
            }

            // Subscribe to trie stream
            let trie_descriptor = BlockchainDescriptor::live(StreamType::Trie);
            info!("Subscribing to trie data from bridge");

            match bridge.subscribe(&trie_descriptor).await {
                Ok(trie_stream) => {
                    info!("Successfully subscribed to trie stream");
                    Self::spawn_stream_processor(
                        StreamType::Trie,
                        trie_stream,
                        trie_tx.clone(),
                        self.live_state.clone(),
                        self.chain_id,
                        self.bridge_name.clone(),
                    );
                }
                Err(e) => {
                    error!("Failed to subscribe to trie stream: {}", e);
                }
            }
        }

        Ok(())
    }
}
