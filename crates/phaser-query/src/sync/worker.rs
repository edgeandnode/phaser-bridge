use crate::parquet_writer::ParquetWriter;
use crate::ParquetConfig;
use anyhow::{Context, Result};
use futures::StreamExt;
use phaser_bridge::client::FlightBridgeClient;
use phaser_bridge::descriptors::{BlockchainDescriptor, StreamType};
use std::path::PathBuf;
use tracing::{debug, info};

/// A worker that syncs a specific block range from erigon-bridge
pub struct SyncWorker {
    worker_id: u32,
    bridge_endpoint: String,
    data_dir: PathBuf,
    from_block: u64,
    to_block: u64,
    segment_size: u64,
    max_file_size_mb: u64,
    batch_size: u32,
    parquet_config: Option<ParquetConfig>,
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
            batch_size,
            parquet_config,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(
            "Worker {} starting sync of blocks {}-{} from {}",
            self.worker_id, self.from_block, self.to_block, self.bridge_endpoint
        );

        // Connect to bridge via Arrow Flight
        let mut client = FlightBridgeClient::connect(self.bridge_endpoint.clone())
            .await
            .context("Failed to connect to bridge")?;

        info!("Worker {} connected to bridge", self.worker_id);

        // Sync blocks, transactions, and logs
        self.sync_blocks(&mut client).await?;
        self.sync_transactions(&mut client).await?;
        // Note: logs require ExecuteBlocks which is not yet implemented via Flight
        // self.sync_logs(&mut client).await?;

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
        let descriptor = BlockchainDescriptor::historical(
            StreamType::Blocks,
            self.from_block,
            self.to_block,
        );

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

            // TODO: Report progress
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

}
