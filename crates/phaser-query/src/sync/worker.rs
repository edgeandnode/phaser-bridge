use crate::parquet_writer::ParquetWriter;
use crate::sync::data_scanner::DataScanner;
use crate::ParquetConfig;
use anyhow::{Context, Result};
use arrow::array as arrow_array;
use evm_common::proof::{generate_transaction_proof, MerkleProofRecord};
use evm_common::transaction::TransactionRecord;
use futures::StreamExt;
use phaser_bridge::client::FlightBridgeClient;
use phaser_bridge::descriptors::{BlockchainDescriptor, StreamType, ValidationStage};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use typed_arrow::prelude::*;

/// Write a 0-byte .empty file to mark a range as checked but containing no data
fn write_empty_marker(
    data_dir: &PathBuf,
    data_type: &str,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let filename = format!("{}_from_{}_to_{}.empty", data_type, from_block, to_block);
    let path = data_dir.join(filename);
    std::fs::File::create(&path)?;
    info!(
        "Created empty marker file for {} blocks {}-{}: {}",
        data_type,
        from_block,
        to_block,
        path.display()
    );
    Ok(())
}

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
    pub started_at: std::time::SystemTime,
    pub current_block: u64,
    pub blocks_processed: u64,
    pub bytes_written: u64,
    pub files_created: u32,
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
    validation_stage: ValidationStage,
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
        validation_stage: ValidationStage,
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
            validation_stage,
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
        current_block: u64,
        blocks_processed: u64,
        bytes_written: u64,
        files_created: u32,
    ) {
        if let Some(tracker) = &self.progress_tracker {
            let mut tracker_lock = tracker.write().await;

            // Get existing started_at or use current time for new worker
            let started_at = tracker_lock
                .get(&self.worker_id)
                .map(|p| p.started_at)
                .unwrap_or_else(std::time::SystemTime::now);

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
                    started_at,
                    current_block,
                    blocks_processed,
                    bytes_written,
                    files_created,
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
        self.update_progress("scanning", false, false, false, self.from_block, 0, 0, 0)
            .await;

        // Scan for missing ranges using DataScanner
        let scanner = DataScanner::new(self.data_dir.clone());
        let missing_blocks =
            scanner.find_missing_ranges("blocks", self.from_block, self.to_block)?;
        let missing_txs =
            scanner.find_missing_ranges("transactions", self.from_block, self.to_block)?;
        let missing_logs = scanner.find_missing_ranges("logs", self.from_block, self.to_block)?;

        let blocks_complete = missing_blocks.is_empty();
        let txs_complete = missing_txs.is_empty();
        let logs_complete = missing_logs.is_empty();

        // Note: We don't skip work even if scanner reports everything complete
        // Empty parquet files may exist but contain no actual data
        // Always attempt to sync missing ranges to verify data exists

        // Connect to bridge via Arrow Flight
        let mut client = FlightBridgeClient::connect(self.bridge_endpoint.clone())
            .await
            .context("Failed to connect to bridge")?;

        info!("Worker {} connected to bridge", self.worker_id);

        let mut total_batches = 0u64;
        let mut total_bytes = 0u64;
        let mut files_created = 0u32;

        // Sync missing blocks ranges
        if !blocks_complete {
            self.update_progress(
                "blocks",
                false,
                txs_complete,
                logs_complete,
                self.from_block,
                total_batches,
                total_bytes,
                files_created,
            )
            .await;

            for range in &missing_blocks {
                info!(
                    "Worker {} syncing blocks {}-{}",
                    self.worker_id, range.start, range.end
                );
                let (batches, bytes) = self
                    .sync_blocks_range(&mut client, range.start, range.end)
                    .await?;
                total_batches += batches;
                total_bytes += bytes;
                files_created += 1;
            }
        }

        // Sync missing transaction ranges
        if !txs_complete {
            self.update_progress(
                "transactions",
                true,
                false,
                logs_complete,
                self.to_block,
                total_batches,
                total_bytes,
                files_created,
            )
            .await;

            for range in &missing_txs {
                info!(
                    "Worker {} syncing transactions {}-{}",
                    self.worker_id, range.start, range.end
                );
                let (batches, bytes) = self
                    .sync_transactions_range(&mut client, range.start, range.end)
                    .await?;
                total_batches += batches;
                total_bytes += bytes;
                files_created += 1;
            }
        }

        // Sync missing log ranges
        if !logs_complete {
            self.update_progress(
                "logs",
                true,
                true,
                false,
                self.to_block,
                total_batches,
                total_bytes,
                files_created,
            )
            .await;

            for range in &missing_logs {
                info!(
                    "Worker {} syncing logs {}-{}",
                    self.worker_id, range.start, range.end
                );
                let (batches, bytes) = self
                    .sync_logs_range(&mut client, range.start, range.end)
                    .await?;
                total_batches += batches;
                total_bytes += bytes;
                files_created += 1;
            }
        }

        self.update_progress(
            "completed",
            true,
            true,
            true,
            self.to_block,
            total_batches,
            total_bytes,
            files_created,
        )
        .await;

        info!("Worker {} completed sync successfully", self.worker_id);
        Ok(())
    }

    async fn sync_blocks_range(
        &mut self,
        client: &mut FlightBridgeClient,
        from_block: u64,
        to_block: u64,
    ) -> Result<(u64, u64)> {
        let mut writer = ParquetWriter::with_config(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "blocks".to_string(),
            self.parquet_config.clone(),
        )?;

        // Create historical query descriptor
        let descriptor = BlockchainDescriptor::historical(StreamType::Blocks, from_block, to_block);

        // Subscribe to the block stream (returns Arrow RecordBatches)
        let mut stream = client
            .subscribe(&descriptor)
            .await
            .context("Failed to subscribe to block stream")?;

        let mut batches_processed = 0u64;
        let mut bytes_written = 0u64;
        let mut first_block_seen: Option<u64> = None;
        let mut last_block_seen: Option<u64> = None;
        let mut schema: Option<arrow::datatypes::SchemaRef> = None;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to receive block batch")?;

            debug!(
                "Worker {} received block batch with {} rows",
                self.worker_id,
                batch.num_rows()
            );

            // Track the block range we actually received
            if batch.num_rows() > 0 {
                if let Some(block_col) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                {
                    let first = block_col.value(0);
                    let last = block_col.value(block_col.len() - 1);

                    if first_block_seen.is_none() {
                        first_block_seen = Some(first);
                    }
                    last_block_seen = Some(last);
                }
            }

            let batch_bytes = batch.get_array_memory_size() as u64;
            bytes_written += batch_bytes;

            // Write Arrow RecordBatch directly to parquet
            writer
                .write_batch(batch)
                .await
                .context("Failed to write block batch")?;

            batches_processed += 1;
        }

        // Validate we got the expected range
        if batches_processed > 0 {
            if let (Some(first), Some(last)) = (first_block_seen, last_block_seen) {
                if first != from_block {
                    anyhow::bail!(
                        "Bridge returned blocks starting at {} but requested range started at {}",
                        first,
                        from_block
                    );
                }
                if last != to_block {
                    anyhow::bail!(
                        "Bridge returned blocks ending at {} but requested range ended at {}",
                        last,
                        to_block
                    );
                }
            }
            writer.finalize_current_file()?;
        } else {
            // No data received - write empty marker to mark range as checked
            write_empty_marker(&self.data_dir, "blocks", from_block, to_block)?;
        }

        Ok((batches_processed, bytes_written))
    }

    async fn sync_transactions_range(
        &mut self,
        client: &mut FlightBridgeClient,
        from_block: u64,
        to_block: u64,
    ) -> Result<(u64, u64)> {
        let mut writer = ParquetWriter::with_config(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "transactions".to_string(),
            self.parquet_config.clone(),
        )?;

        // Check if proof generation is enabled
        let generate_proofs = self
            .parquet_config
            .as_ref()
            .map(|c| c.generate_proofs)
            .unwrap_or(false);

        let mut proof_writer = if generate_proofs {
            info!(
                "Worker {} will generate merkle proofs for transactions",
                self.worker_id
            );
            Some(ParquetWriter::with_config(
                self.data_dir.clone(),
                self.max_file_size_mb,
                self.segment_size,
                "proofs".to_string(),
                self.parquet_config.clone(),
            )?)
        } else {
            None
        };

        // Create historical query descriptor for transactions with configured validation stage
        let descriptor =
            BlockchainDescriptor::historical(StreamType::Transactions, from_block, to_block)
                .with_validation(self.validation_stage);

        // Subscribe to the transaction stream (returns Arrow RecordBatches)
        let mut stream = client
            .subscribe(&descriptor)
            .await
            .context("Failed to subscribe to transaction stream")?;

        let mut batches_processed = 0u64;
        let mut bytes_written = 0u64;
        let mut first_block_seen: Option<u64> = None;
        let mut last_block_seen: Option<u64> = None;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to receive transaction batch")?;

            debug!(
                "Worker {} received transaction batch with {} rows",
                self.worker_id,
                batch.num_rows()
            );

            // Track the block range we actually received
            if batch.num_rows() > 0 {
                if let Some(block_col) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                {
                    let first = block_col.value(0);
                    let last = block_col.value(block_col.len() - 1);

                    if first_block_seen.is_none() {
                        first_block_seen = Some(first);
                    }
                    last_block_seen = Some(last);
                }
            }

            let batch_bytes = batch.get_array_memory_size() as u64;
            bytes_written += batch_bytes;

            // Generate proofs if enabled
            if let Some(ref mut proof_w) = proof_writer {
                if let Ok(proof_batch) = self.generate_proofs_for_batch(&batch) {
                    proof_w
                        .write_batch(proof_batch)
                        .await
                        .context("Failed to write proof batch")?;
                } else {
                    warn!(
                        "Worker {} failed to generate proofs for batch",
                        self.worker_id
                    );
                }
            }

            // Write Arrow RecordBatch directly to parquet
            writer
                .write_batch(batch)
                .await
                .context("Failed to write transaction batch")?;

            batches_processed += 1;
        }

        // Validate we got data within the expected range
        // Note: Early blocks may have no transactions, so first block can be > from_block
        if batches_processed > 0 {
            if let (Some(first), Some(last)) = (first_block_seen, last_block_seen) {
                if first < from_block || first > to_block {
                    anyhow::bail!(
                        "Bridge returned transactions starting at block {} which is outside requested range {}-{}",
                        first,
                        from_block,
                        to_block
                    );
                }
                if last < from_block || last > to_block {
                    anyhow::bail!(
                        "Bridge returned transactions ending at block {} which is outside requested range {}-{}",
                        last,
                        from_block,
                        to_block
                    );
                }
            }
            writer.finalize_current_file()?;
            if let Some(ref mut proof_w) = proof_writer {
                proof_w.finalize_current_file()?;
            }
        } else {
            // No data received - write empty marker to mark range as checked
            write_empty_marker(&self.data_dir, "transactions", from_block, to_block)?;
        }

        Ok((batches_processed, bytes_written))
    }

    fn generate_proofs_for_batch(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<arrow::array::RecordBatch> {
        use alloy_consensus::TxEnvelope;
        use std::collections::BTreeMap;

        // Convert RecordBatch to TransactionRecords
        let views = batch.iter_views::<TransactionRecord>()?;
        let tx_records: Vec<TransactionRecord> = views
            .try_flatten()?
            .into_iter()
            .map(|v| v.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        // Group transactions by block number
        let mut txs_by_block: BTreeMap<u64, Vec<TransactionRecord>> = BTreeMap::new();
        for tx in tx_records {
            txs_by_block.entry(tx.block_num).or_default().push(tx);
        }

        // Generate proofs for each block
        let mut all_proofs = Vec::new();
        for (block_num, txs) in txs_by_block {
            // Convert transactions to RLP
            let tx_rlps: Vec<Vec<u8>> = txs
                .iter()
                .filter_map(|tx| {
                    TxEnvelope::try_from(tx)
                        .ok()
                        .map(|envelope| alloy_rlp::encode(&envelope))
                })
                .collect();

            // Generate proof for each transaction
            for (index, _tx) in txs.iter().enumerate() {
                if let Ok((root, proof_nodes, value)) = generate_transaction_proof(&tx_rlps, index)
                {
                    let proof = MerkleProofRecord::new_transaction_proof(
                        block_num,
                        index as u64,
                        root,
                        proof_nodes,
                        value,
                    );
                    all_proofs.push(proof);
                }
            }
        }

        // Convert proofs to RecordBatch
        let mut builder = <MerkleProofRecord as BuildRows>::new_builders(all_proofs.len());
        builder.append_rows(all_proofs);
        let arrays = builder.finish();
        Ok(arrays.into_record_batch())
    }

    async fn sync_logs_range(
        &mut self,
        client: &mut FlightBridgeClient,
        from_block: u64,
        to_block: u64,
    ) -> Result<(u64, u64)> {
        let mut writer = ParquetWriter::with_config(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "logs".to_string(),
            self.parquet_config.clone(),
        )?;

        // Create historical query descriptor for logs with configured validation stage
        let descriptor = BlockchainDescriptor::historical(StreamType::Logs, from_block, to_block)
            .with_validation(self.validation_stage);

        // Subscribe to the log stream (returns Arrow RecordBatches)
        let mut stream = client
            .subscribe(&descriptor)
            .await
            .context("Failed to subscribe to log stream")?;

        let mut batches_processed = 0u64;
        let mut bytes_written = 0u64;
        let mut first_block_seen: Option<u64> = None;
        let mut last_block_seen: Option<u64> = None;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to receive log batch")?;

            debug!(
                "Worker {} received log batch with {} rows",
                self.worker_id,
                batch.num_rows()
            );

            // Track the block range we actually received
            if batch.num_rows() > 0 {
                if let Some(block_col) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                {
                    let first = block_col.value(0);
                    let last = block_col.value(block_col.len() - 1);

                    if first_block_seen.is_none() {
                        first_block_seen = Some(first);
                    }
                    last_block_seen = Some(last);
                }
            }

            let batch_bytes = batch.get_array_memory_size() as u64;
            bytes_written += batch_bytes;

            // Write Arrow RecordBatch directly to parquet
            writer
                .write_batch(batch)
                .await
                .context("Failed to write log batch")?;

            batches_processed += 1;
        }

        // Validate we got data within the expected range
        // Note: Early blocks may have no logs, so first block can be > from_block
        if batches_processed > 0 {
            if let (Some(first), Some(last)) = (first_block_seen, last_block_seen) {
                if first < from_block || first > to_block {
                    anyhow::bail!(
                        "Bridge returned logs starting at block {} which is outside requested range {}-{}",
                        first,
                        from_block,
                        to_block
                    );
                }
                if last < from_block || last > to_block {
                    anyhow::bail!(
                        "Bridge returned logs ending at block {} which is outside requested range {}-{}",
                        last,
                        from_block,
                        to_block
                    );
                }
            }
            writer.finalize_current_file()?;
        } else {
            // No data received - write empty marker to mark range as checked
            write_empty_marker(&self.data_dir, "logs", from_block, to_block)?;
        }

        Ok((batches_processed, bytes_written))
    }
}
