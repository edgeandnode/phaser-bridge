use crate::parquet_writer::ParquetWriter;
use crate::ParquetConfig;
use anyhow::{Context, Result};
use futures::StreamExt;
use phaser_bridge::client::FlightBridgeClient;
use phaser_bridge::descriptors::{BlockchainDescriptor, StreamType};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Worker progress tracking
#[derive(Debug, Clone)]
pub struct WorkerProgress {
    pub worker_id: u32,
    pub from_block: u64,
    pub to_block: u64,
    pub current_phase: String,
    pub blocks_completed: bool,
    pub transactions_completed: bool,
    pub logs_completed: bool,
}

pub type ProgressTracker = Arc<RwLock<HashMap<u32, WorkerProgress>>>;

/// A worker that syncs a specific block range from erigon-bridge
pub struct SyncWorker {
    worker_id: u32,
    bridge_endpoint: String,
    data_dir: PathBuf,
    from_block: u64,
    to_block: u64,
    segment_size: u64,
    max_file_size_mb: u64,
    _batch_size: u32,
    parquet_config: Option<ParquetConfig>,
    progress_tracker: Option<ProgressTracker>,
}

impl SyncWorker {
    pub fn new(
        worker_id: u32,
        bridge_endpoint: String,
        data_dir: PathBuf,
        from_block: u64,
        to_block: u64,
        segment_size: u64,
        max_file_size_mb: u64,
        batch_size: u32,
        parquet_config: Option<ParquetConfig>,
    ) -> Self {
        Self {
            worker_id,
            bridge_endpoint,
            data_dir,
            from_block,
            to_block,
            segment_size,
            max_file_size_mb,
            _batch_size: batch_size,
            parquet_config,
            progress_tracker: None,
        }
    }

    pub fn with_progress_tracker(mut self, tracker: ProgressTracker) -> Self {
        self.progress_tracker = Some(tracker);
        self
    }

    async fn update_progress(
        &self,
        phase: &str,
        blocks_done: bool,
        txs_done: bool,
        logs_done: bool,
    ) {
        if let Some(tracker) = &self.progress_tracker {
            let mut tracker_lock = tracker.write().await;
            tracker_lock.insert(
                self.worker_id,
                WorkerProgress {
                    worker_id: self.worker_id,
                    from_block: self.from_block,
                    to_block: self.to_block,
                    current_phase: phase.to_string(),
                    blocks_completed: blocks_done,
                    transactions_completed: txs_done,
                    logs_completed: logs_done,
                },
            );
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(
            "Worker {} starting sync of blocks {}-{} from {}",
            self.worker_id, self.from_block, self.to_block, self.bridge_endpoint
        );

        // Initialize progress
        self.update_progress("blocks", false, false, false).await;

        // Connect to bridge via Arrow Flight
        let mut client = FlightBridgeClient::connect(self.bridge_endpoint.clone())
            .await
            .context("Failed to connect to bridge")?;

        info!("Worker {} connected to bridge", self.worker_id);

        // Sync blocks, transactions, and logs
        self.sync_blocks(&mut client).await?;
        self.update_progress("transactions", true, false, false)
            .await;

        self.sync_transactions(&mut client).await?;
        self.update_progress("logs", true, true, false).await;

        self.sync_logs(&mut client).await?;
        self.update_progress("completed", true, true, true).await;

        info!("Worker {} completed sync successfully", self.worker_id);
        Ok(())
    }

    async fn sync_blocks(&mut self, client: &mut FlightBridgeClient) -> Result<()> {
        info!(
            "Worker {} syncing blocks {}-{}",
            self.worker_id, self.from_block, self.to_block
        );

        let mut writer = ParquetWriter::with_config(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "blocks".to_string(),
            self.parquet_config.clone(),
        )?;

        // Create historical query descriptor
        let descriptor =
            BlockchainDescriptor::historical(StreamType::Blocks, self.from_block, self.to_block);

        // Subscribe to the block stream (returns Arrow RecordBatches)
        let mut stream = client
            .subscribe(&descriptor)
            .await
            .context("Failed to subscribe to block stream")?;

        let mut blocks_processed = 0u64;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to receive block batch")?;

            debug!(
                "Worker {} received block batch with {} rows",
                self.worker_id,
                batch.num_rows()
            );

            // Write Arrow RecordBatch directly to parquet
            writer
                .write_batch(batch)
                .await
                .context("Failed to write block batch")?;

            blocks_processed += 1;
        }

        writer.finalize_current_file()?;

        info!(
            "Worker {} completed block sync ({} batches)",
            self.worker_id, blocks_processed
        );
        Ok(())
    }

    async fn sync_transactions(&mut self, client: &mut FlightBridgeClient) -> Result<()> {
        info!(
            "Worker {} syncing transactions {}-{}",
            self.worker_id, self.from_block, self.to_block
        );

        let mut writer = ParquetWriter::with_config(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "transactions".to_string(),
            self.parquet_config.clone(),
        )?;

        // Create historical query descriptor for transactions
        let descriptor = BlockchainDescriptor::historical(
            StreamType::Transactions,
            self.from_block,
            self.to_block,
        );

        // Subscribe to the transaction stream (returns Arrow RecordBatches)
        let mut stream = client
            .subscribe(&descriptor)
            .await
            .context("Failed to subscribe to transaction stream")?;

        let mut batches_processed = 0u64;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to receive transaction batch")?;

            debug!(
                "Worker {} received transaction batch with {} rows",
                self.worker_id,
                batch.num_rows()
            );

            // Write Arrow RecordBatch directly to parquet
            writer
                .write_batch(batch)
                .await
                .context("Failed to write transaction batch")?;

            batches_processed += 1;
        }

        writer.finalize_current_file()?;

        info!(
            "Worker {} completed transaction sync ({} batches)",
            self.worker_id, batches_processed
        );
        Ok(())
    }

    async fn sync_logs(&mut self, client: &mut FlightBridgeClient) -> Result<()> {
        info!(
            "Worker {} syncing logs {}-{}",
            self.worker_id, self.from_block, self.to_block
        );

        let mut writer = ParquetWriter::with_config(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "logs".to_string(),
            self.parquet_config.clone(),
        )?;

        // Create historical query descriptor for logs (uses ExecuteBlocks)
        let descriptor =
            BlockchainDescriptor::historical(StreamType::Logs, self.from_block, self.to_block);

        // Subscribe to the log stream (returns Arrow RecordBatches)
        let mut stream = client
            .subscribe(&descriptor)
            .await
            .context("Failed to subscribe to log stream")?;

        let mut batches_processed = 0u64;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to receive log batch")?;

            debug!(
                "Worker {} received log batch with {} rows",
                self.worker_id,
                batch.num_rows()
            );

            // Write Arrow RecordBatch directly to parquet
            writer
                .write_batch(batch)
                .await
                .context("Failed to write log batch")?;

            batches_processed += 1;
        }

        writer.finalize_current_file()?;

        info!(
            "Worker {} completed log sync ({} batches)",
            self.worker_id, batches_processed
        );
        Ok(())
    }
}
