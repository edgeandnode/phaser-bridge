use crate::parquet_writer::ParquetWriter;
use crate::sync::error::{DataType, ErrorCategory, SyncError};
use crate::ParquetConfig;
use anyhow::{Context, Result};
use arrow::array as arrow_array;
use evm_common::proof::{generate_transaction_proof, MerkleProofRecord};
use evm_common::transaction::TransactionRecord;
use futures::StreamExt;
use phaser_client::sync::SegmentWork;
use phaser_client::{GenericQuery, PhaserClient, ValidationStage};
use phaser_metrics::SegmentMetrics;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use typed_arrow::prelude::*;

/// Check if an error is transient and should trigger a retry
fn is_transient_error(err: &SyncError) -> bool {
    // Use the is_transient() method from phaser-client
    err.is_transient()
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

/// Progress update data for tracking worker state
#[allow(dead_code)]
pub struct ProgressUpdate {
    pub phase: String,
    pub blocks_done: bool,
    pub txs_done: bool,
    pub logs_done: bool,
    pub current_block: u64,
    pub blocks_processed: u64,
    pub bytes_written: u64,
    pub files_created: u32,
}

/// Configuration for creating a SyncWorker
pub struct SyncWorkerConfig {
    pub metrics: super::metrics::SyncMetrics,
    pub bridge_endpoint: String,
    pub data_dir: PathBuf,
    pub from_block: u64,
    pub to_block: u64,
    pub segment_size: u64,
    pub max_file_size_mb: u64,
    pub batch_size: u32,
    pub parquet_config: Option<ParquetConfig>,
    pub validation_stage: ValidationStage,
    pub logs_semaphore: Arc<tokio::sync::Semaphore>,
}

/// A worker that syncs a specific block range from erigon-bridge
pub struct SyncWorker {
    worker_id: u32,
    metrics: super::metrics::SyncMetrics,
    bridge_endpoint: String,
    data_dir: PathBuf,
    from_block: u64,
    to_block: u64,
    segment_size: u64,
    max_file_size_mb: u64,
    _batch_size: u32,
    parquet_config: Option<ParquetConfig>,
    progress_tracker: Option<ProgressTracker>,
    _validation_stage: phaser_client::ValidationStage,
    segment_work: SegmentWork, // Pre-computed missing ranges
    current_progress: Arc<RwLock<WorkerProgress>>, // Real-time progress state
    logs_semaphore: Arc<tokio::sync::Semaphore>,
}

impl SyncWorker {
    pub fn new(
        worker_id: u32,
        config: SyncWorkerConfig,
        segment_work: SegmentWork,
        historical_boundary: Option<u64>,
    ) -> Self {
        // Cap to_block at historical boundary if present
        // This prevents syncing blocks that don't exist yet when near chain tip
        let (effective_to_block, _is_truncated) = if let Some(boundary) = historical_boundary {
            // Don't sync past the block before live streaming starts
            let boundary_limit = boundary.saturating_sub(1);
            if config.to_block > boundary_limit {
                info!(
                    "Worker {} capping segment range {}-{} at historical boundary {} (truncated)",
                    worker_id, config.from_block, config.to_block, boundary_limit
                );
                (boundary_limit, true)
            } else {
                (config.to_block, false)
            }
        } else {
            (config.to_block, false)
        };

        // Initialize progress state
        let current_progress = Arc::new(RwLock::new(WorkerProgress {
            worker_id,
            from_block: config.from_block,
            to_block: effective_to_block,
            current_phase: "initializing".to_string(),
            blocks_completed: false,
            transactions_completed: false,
            logs_completed: false,
            started_at: std::time::SystemTime::now(),
            current_block: config.from_block,
            blocks_processed: 0,
            bytes_written: 0,
            files_created: 0,
        }));

        Self {
            worker_id,
            metrics: config.metrics,
            bridge_endpoint: config.bridge_endpoint,
            data_dir: config.data_dir,
            from_block: config.from_block,
            to_block: effective_to_block,
            segment_size: config.segment_size,
            max_file_size_mb: config.max_file_size_mb,
            _batch_size: config.batch_size,
            parquet_config: config.parquet_config,
            progress_tracker: None,
            _validation_stage: config.validation_stage,
            segment_work,
            current_progress,
            logs_semaphore: config.logs_semaphore,
        }
    }

    pub fn with_progress_tracker(mut self, tracker: ProgressTracker) -> Self {
        self.progress_tracker = Some(tracker);

        // Spawn background progress reporter if tracker is set
        if self.progress_tracker.is_some() {
            let tracker = self.progress_tracker.clone().unwrap();
            let current_progress = self.current_progress.clone();
            let worker_id = self.worker_id;

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                loop {
                    interval.tick().await;

                    // Clone current progress and report it
                    let progress = current_progress.read().await.clone();
                    let mut tracker_lock = tracker.write().await;
                    tracker_lock.insert(worker_id, progress);
                }
            });
        }

        self
    }

    /// Update the current progress state (call frequently during batch processing)
    async fn update_current_progress(&self, bytes_delta: u64) {
        let mut progress = self.current_progress.write().await;
        progress.bytes_written += bytes_delta;
    }

    /// Update the current stage
    #[allow(dead_code)]
    async fn update_stage(&self, stage: &str) {
        let mut progress = self.current_progress.write().await;
        progress.current_phase = stage.to_string();
    }

    #[allow(dead_code)]
    async fn update_progress(&self, update: ProgressUpdate) {
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
                    current_phase: update.phase,
                    blocks_completed: update.blocks_done,
                    transactions_completed: update.txs_done,
                    logs_completed: update.logs_done,
                    started_at,
                    current_block: update.current_block,
                    blocks_processed: update.blocks_processed,
                    bytes_written: update.bytes_written,
                    files_created: update.files_created,
                },
            );
        }
    }

    pub async fn run(&mut self) -> Result<(), SyncError> {
        debug!(
            "Worker {} starting sync of blocks {}-{} from {}",
            self.worker_id, self.from_block, self.to_block, self.bridge_endpoint
        );

        // Use pre-computed missing ranges from segment_work (no file I/O needed!)
        // Convert from generic Range to BlockRange for backwards compat
        let missing_blocks: Vec<crate::sync::data_scanner::BlockRange> =
            self.segment_work.get_missing_ranges("blocks").to_vec();
        let missing_txs: Vec<crate::sync::data_scanner::BlockRange> = self
            .segment_work
            .get_missing_ranges("transactions")
            .to_vec();
        let missing_logs: Vec<crate::sync::data_scanner::BlockRange> =
            self.segment_work.get_missing_ranges("logs").to_vec();

        debug!(
            "Worker {} segment work: blocks={} ranges, txs={} ranges, logs={} ranges",
            self.worker_id,
            missing_blocks.len(),
            missing_txs.len(),
            missing_logs.len()
        );

        // Spawn all data types in parallel
        let blocks_fut = self.sync_all_blocks(missing_blocks);
        let txs_fut = self.sync_all_transactions(missing_txs);
        let logs_fut = self.sync_all_logs(missing_logs);

        // Wait for all to complete and collect results
        let (blocks_res, txs_res, logs_res) = tokio::join!(blocks_fut, txs_fut, logs_fut);

        // Collect all errors
        let mut errors = Vec::new();
        if let Err(e) = blocks_res {
            errors.push((DataType::new("blocks"), e));
        }
        if let Err(e) = txs_res {
            errors.push((DataType::new("transactions"), e));
        }
        if let Err(e) = logs_res {
            errors.push((DataType::new("logs"), e));
        }

        // If any failed, return MultipleDataTypeErrors
        if !errors.is_empty() {
            let multi_err = crate::sync::error::MultipleDataTypeErrors {
                from_block: self.from_block,
                to_block: self.to_block,
                errors,
            };

            error!(
                "Worker {} failed with {} data type error(s): {}",
                self.worker_id,
                multi_err.errors.len(),
                multi_err
            );

            return Err(multi_err.into());
        }

        debug!("Worker {} completed sync successfully", self.worker_id);
        Ok(())
    }

    /// Sync all blocks ranges
    async fn sync_all_blocks(
        &self,
        missing_blocks: Vec<crate::sync::data_scanner::BlockRange>,
    ) -> Result<(), SyncError> {
        if missing_blocks.is_empty() {
            return Ok(());
        }

        // Connect to bridge
        let mut client = PhaserClient::connect(self.bridge_endpoint.clone())
            .await
            .map_err(|e| {
                SyncError::from_anyhow_with_context(
                    DataType::new("blocks"),
                    self.from_block,
                    self.to_block,
                    "Failed to connect to bridge",
                    e.into(),
                )
            })?;

        for range in &missing_blocks {
            debug!(
                "Worker {} syncing blocks {}-{}",
                self.worker_id, range.start, range.end
            );
            self.sync_blocks_range(&mut client, range.start, range.end)
                .await?;
        }

        Ok(())
    }

    /// Sync all transactions ranges
    async fn sync_all_transactions(
        &self,
        missing_txs: Vec<crate::sync::data_scanner::BlockRange>,
    ) -> Result<(), SyncError> {
        if missing_txs.is_empty() {
            return Ok(());
        }

        // Connect to bridge
        let mut client = PhaserClient::connect(self.bridge_endpoint.clone())
            .await
            .map_err(|e| {
                SyncError::from_anyhow_with_context(
                    DataType::new("transactions"),
                    self.from_block,
                    self.to_block,
                    "Failed to connect to bridge",
                    e.into(),
                )
            })?;

        for range in &missing_txs {
            debug!(
                "Worker {} syncing transactions {}-{}",
                self.worker_id, range.start, range.end
            );
            self.sync_transactions_range(&mut client, range.start, range.end)
                .await?;
        }

        Ok(())
    }

    /// Sync all logs ranges
    async fn sync_all_logs(
        &self,
        missing_logs: Vec<crate::sync::data_scanner::BlockRange>,
    ) -> Result<(), SyncError> {
        if missing_logs.is_empty() {
            return Ok(());
        }

        // Acquire 1 permit for this entire segment
        // This limits how many segments can be processing logs concurrently
        debug!(
            "Worker {} acquiring log semaphore permit for segment (blocks {}-{})",
            self.worker_id, self.from_block, self.to_block
        );

        let _permit = self
            .logs_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| {
                SyncError::from_anyhow_with_context(
                    DataType::new("logs"),
                    self.from_block,
                    self.to_block,
                    "Failed to acquire log semaphore permit for segment",
                    anyhow::anyhow!("{}", e),
                )
            })?;

        debug!(
            "Worker {} acquired permit, starting log sync for {} ranges",
            self.worker_id,
            missing_logs.len()
        );

        // Connect to bridge
        let mut client = PhaserClient::connect(self.bridge_endpoint.clone())
            .await
            .map_err(|e| {
                SyncError::from_anyhow_with_context(
                    DataType::new("logs"),
                    self.from_block,
                    self.to_block,
                    "Failed to connect to bridge",
                    e.into(),
                )
            })?;

        // Process all ranges sequentially while holding all permits
        for range in &missing_logs {
            debug!(
                "Worker {} syncing logs {}-{}",
                self.worker_id, range.start, range.end
            );
            self.sync_logs_range(&mut client, range.start, range.end)
                .await?;
        }

        Ok(())
    }

    async fn sync_blocks_range(
        &self,
        client: &mut PhaserClient,
        from_block: u64,
        to_block: u64,
    ) -> Result<(), SyncError> {
        // Track phase
        self.metrics.active_workers_inc("blocks");
        let metrics = self.metrics.clone();
        let _phase_guard = scopeguard::guard(metrics, |m| {
            m.active_workers_dec("blocks");
        });

        let mut writer = ParquetWriter::with_config_and_mode(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "blocks".to_string(),
            self.parquet_config.clone(),
            false, // historical sync worker
        )?;

        // Calculate segment boundaries (logical 500K segments)
        let segment_start = (self.from_block / self.segment_size) * self.segment_size;
        let segment_end = segment_start + self.segment_size - 1;

        // Set both segment range (for filename) and responsibility range (for metadata)
        writer.set_ranges(segment_start, segment_end, self.from_block, self.to_block);

        // Retry with resume logic
        const INITIAL_BACKOFF_SECS: u64 = 1;
        const MAX_BACKOFF_SECS: u64 = 60;
        const MAX_RETRIES_WITHOUT_PROGRESS: u32 = 5;
        let mut retry_count = 0u32;
        let mut retries_without_progress = 0u32;
        let mut resume_from = from_block;
        let mut last_resume_from = resume_from;

        loop {
            match self
                .try_sync_blocks_stream(client, resume_from, to_block, &mut writer, from_block)
                .await
            {
                Ok(()) => break,
                Err(e) if is_transient_error(&e) => {
                    // Get the last block we successfully wrote
                    let last_written = writer.last_written_block();

                    let new_resume_from = if let Some(last) = last_written {
                        // We wrote some data, resume from the next block
                        last + 1
                    } else {
                        // No data written - this means the stream failed before sending any batches
                        // Keep resume_from unchanged to retry the same range
                        resume_from
                    };

                    // Detect if we're stuck in an infinite loop (no progress being made)
                    if new_resume_from == last_resume_from {
                        retries_without_progress += 1;
                        if retries_without_progress >= MAX_RETRIES_WITHOUT_PROGRESS {
                            return Err(SyncError::new(
                                DataType::new("blocks"),
                                ErrorCategory::StuckWorker,
                                resume_from,
                                to_block,
                                format!(
                                    "Worker {} failed to make progress on blocks {}-{} after {} retries. \
                                    Last error: {}. This may indicate missing or corrupted data in the source.",
                                    self.worker_id, resume_from, to_block, MAX_RETRIES_WITHOUT_PROGRESS, e
                                ),
                            ));
                        }
                    } else {
                        // Made progress, reset the no-progress counter
                        retries_without_progress = 0;
                    }

                    last_resume_from = resume_from;
                    resume_from = new_resume_from;

                    if resume_from > to_block {
                        // We actually completed, the error was after all data
                        debug!(
                            "Worker {} completed blocks {}-{} before stream error",
                            self.worker_id, from_block, to_block
                        );
                        break; // Already finalized
                    }

                    retry_count += 1;
                    let backoff_secs = (INITIAL_BACKOFF_SECS
                        * 2u64.pow(retry_count.saturating_sub(1)))
                    .min(MAX_BACKOFF_SECS);

                    let progress_msg = if let Some(last) = last_written {
                        format!("last written block: {last}")
                    } else {
                        "no data received yet".to_string()
                    };

                    warn!(
                        "Worker {} blocks stream failed ({}) - attempt {}: {}. Resuming from block {} in {}s... (retries without progress: {})",
                        self.worker_id, progress_msg, retry_count, e, resume_from, backoff_secs, retries_without_progress
                    );

                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn try_sync_blocks_stream(
        &self,
        client: &mut PhaserClient,
        from_block: u64,
        to_block: u64,
        writer: &mut ParquetWriter,
        original_from: u64,
    ) -> Result<(), SyncError> {
        // Create historical query using GenericQuery
        let query = GenericQuery::historical("blocks", from_block, to_block);

        // Subscribe to the block stream with metadata (returns RecordBatch + responsibility range)
        let stream = client.query_with_metadata(query).await.map_err(|e| {
            SyncError::from_anyhow_with_context(
                DataType::new("blocks"),
                from_block,
                to_block,
                "Failed to subscribe to block stream",
                e.into(),
            )
        })?;
        let mut stream = Box::pin(stream);

        let mut batches_processed = 0u64;
        let mut first_block_seen: Option<u64> = None;
        let mut last_block_seen: Option<u64> = None;
        let mut max_responsibility_end: Option<u64> = None;

        while let Some(batch_result) = stream.next().await {
            let (batch, metadata) = match batch_result {
                Ok(data) => data,
                Err(e) => {
                    // Preserve the full gRPC/Flight error chain from bridge/erigon
                    return Err(SyncError::from_error_with_context(
                        DataType::new("blocks"),
                        from_block,
                        to_block,
                        "Failed to receive block batch",
                        e,
                    ));
                }
            };

            // Track the maximum responsibility end from batch metadata
            let resp_end = metadata.responsibility_range.end_block;
            max_responsibility_end = Some(
                max_responsibility_end
                    .map(|current| current.max(resp_end))
                    .unwrap_or(resp_end),
            );
            // Update writer's responsibility range from bridge metadata
            writer.update_responsibility_end(resp_end);

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

            // Write Arrow RecordBatch directly to parquet and get actual bytes written
            let batch_bytes = writer.write_batch(batch).await.map_err(|e| {
                SyncError::from_anyhow_with_context(
                    DataType::new("blocks"),
                    from_block,
                    to_block,
                    "Failed to write block batch to parquet",
                    e,
                )
            })?;

            // Update progress with actual disk bytes
            self.update_current_progress(batch_bytes).await;

            batches_processed += 1;
        }

        // Validate we got the expected range (only if this is not a resume)
        if batches_processed > 0 {
            if let (Some(first), Some(last)) = (first_block_seen, last_block_seen) {
                if from_block == original_from && first != from_block {
                    return Err(SyncError::validation_error(
                        DataType::new("blocks"),
                        from_block,
                        to_block,
                        format!(
                            "Bridge returned blocks starting at {first} but requested range started at {from_block}"
                        ),
                    ));
                }
                if last != to_block {
                    return Err(SyncError::validation_error(
                        DataType::new("blocks"),
                        from_block,
                        to_block,
                        format!(
                            "Bridge returned blocks ending at {last} but requested range ended at {to_block}"
                        ),
                    ));
                }
            }
            // Finalize with the requested end block to ensure proper filename
            writer.finalize_current_file().map_err(|e| {
                SyncError::from_anyhow_with_context(
                    DataType::new("blocks"),
                    from_block,
                    to_block,
                    "Failed to finalize parquet file",
                    e,
                )
            })?;
        } else {
            // No batches received from bridge
            // This is valid when a block range legitimately has no blocks (shouldn't happen but handle gracefully).
            // Arrow Flight doesn't transmit 0-row RecordBatches, so receiving 0 batches
            // means the range was processed successfully but contains no data.
            debug!(
                "Worker {} received ZERO batches for blocks {}-{}. \
                Range processed successfully with no blocks.",
                self.worker_id, from_block, to_block
            );
            // Treat as success with no data
        }

        Ok(())
    }

    async fn sync_transactions_range(
        &self,
        client: &mut PhaserClient,
        from_block: u64,
        to_block: u64,
    ) -> Result<(), SyncError> {
        // Track phase
        self.metrics.active_workers_inc("transactions");
        let metrics = self.metrics.clone();
        let _phase_guard = scopeguard::guard(metrics, |m| {
            m.active_workers_dec("transactions");
        });

        let mut writer = ParquetWriter::with_config_and_mode(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "transactions".to_string(),
            self.parquet_config.clone(),
            false, // historical sync worker
        )?;

        // Calculate segment boundaries (logical 500K segments)
        let segment_start = (self.from_block / self.segment_size) * self.segment_size;
        let segment_end = segment_start + self.segment_size - 1;

        // Set both segment range (for filename) and responsibility range (for metadata)
        writer.set_ranges(segment_start, segment_end, self.from_block, self.to_block);

        // Check if proof generation is enabled
        let generate_proofs = self
            .parquet_config
            .as_ref()
            .map(|c| c.generate_proofs)
            .unwrap_or(false);

        let mut proof_writer = if generate_proofs {
            debug!(
                "Worker {} will generate merkle proofs for transactions",
                self.worker_id
            );
            let mut pw = ParquetWriter::with_config_and_mode(
                self.data_dir.clone(),
                self.max_file_size_mb,
                self.segment_size,
                "proofs".to_string(),
                self.parquet_config.clone(),
                false, // historical sync worker
            )?;

            // Calculate segment boundaries (logical 500K segments)
            let segment_start = (self.from_block / self.segment_size) * self.segment_size;
            let segment_end = segment_start + self.segment_size - 1;

            // Set both segment range (for filename) and responsibility range (for metadata)
            pw.set_ranges(segment_start, segment_end, self.from_block, self.to_block);
            Some(pw)
        } else {
            None
        };

        // Retry with resume logic
        const INITIAL_BACKOFF_SECS: u64 = 1;
        const MAX_BACKOFF_SECS: u64 = 60;
        const MAX_RETRIES_WITHOUT_PROGRESS: u32 = 5;
        let mut retry_count = 0u32;
        let mut retries_without_progress = 0u32;
        let mut resume_from = from_block;
        let mut last_resume_from = resume_from;

        loop {
            match self
                .try_sync_transactions_stream(
                    client,
                    resume_from,
                    to_block,
                    &mut writer,
                    &mut proof_writer,
                    from_block,
                )
                .await
            {
                Ok(()) => break,
                Err(e) if is_transient_error(&e) => {
                    // Get the last block we successfully wrote
                    let last_written = writer.last_written_block();

                    let new_resume_from = if let Some(last) = last_written {
                        // We wrote some data, resume from the next block
                        last + 1
                    } else {
                        // No data written - this means the stream failed before sending any batches
                        // Keep resume_from unchanged to retry the same range
                        resume_from
                    };

                    // Detect if we're stuck in an infinite loop (no progress being made)
                    if new_resume_from == last_resume_from {
                        retries_without_progress += 1;
                        if retries_without_progress >= MAX_RETRIES_WITHOUT_PROGRESS {
                            return Err(SyncError::new(
                                DataType::new("transactions"),
                                ErrorCategory::StuckWorker,
                                resume_from,
                                to_block,
                                format!(
                                    "Worker {} failed to make progress on transactions {}-{} after {} retries. \
                                    Last error: {}. This may indicate missing or corrupted data in the source.",
                                    self.worker_id, resume_from, to_block, MAX_RETRIES_WITHOUT_PROGRESS, e
                                ),
                            ));
                        }
                    } else {
                        // Made progress, reset the no-progress counter
                        retries_without_progress = 0;
                    }

                    last_resume_from = resume_from;
                    resume_from = new_resume_from;

                    if resume_from > to_block {
                        // We actually completed, the error was after all data
                        debug!(
                            "Worker {} completed transactions {}-{} before stream error",
                            self.worker_id, from_block, to_block
                        );
                        break; // Already finalized
                    }

                    retry_count += 1;
                    let backoff_secs = (INITIAL_BACKOFF_SECS
                        * 2u64.pow(retry_count.saturating_sub(1)))
                    .min(MAX_BACKOFF_SECS);

                    let progress_msg = if let Some(last) = last_written {
                        format!("last written block: {last}")
                    } else {
                        "no data received yet".to_string()
                    };

                    warn!(
                        "Worker {} transactions stream failed ({}) - attempt {}: {}. Resuming from block {} in {}s... (retries without progress: {})",
                        self.worker_id, progress_msg, retry_count, e, resume_from, backoff_secs, retries_without_progress
                    );

                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn try_sync_transactions_stream(
        &self,
        client: &mut PhaserClient,
        from_block: u64,
        to_block: u64,
        writer: &mut ParquetWriter,
        proof_writer: &mut Option<ParquetWriter>,
        _original_from: u64,
    ) -> Result<(), SyncError> {
        // Create historical query using GenericQuery
        let query = GenericQuery::historical("transactions", from_block, to_block);

        debug!(
            "Worker {} requesting transactions for blocks {}-{} (segment {})",
            self.worker_id,
            from_block,
            to_block,
            from_block / 500000,
        );

        // Subscribe to the transaction stream with metadata (returns RecordBatch + responsibility range)
        let stream = client
            .query_with_metadata(query)
            .await
            .context("Failed to subscribe to transaction stream")?;
        let mut stream = Box::pin(stream);

        let mut batches_processed = 0u64;
        let mut first_block_seen: Option<u64> = None;
        let mut last_block_seen: Option<u64> = None;
        let mut max_responsibility_end: Option<u64> = None;

        while let Some(batch_result) = stream.next().await {
            let (batch, metadata) = match batch_result {
                Ok(data) => data,
                Err(e) => {
                    // Log the full error details from the bridge before wrapping
                    error!(
                        "Worker {} received error from bridge for transactions {}-{}: {:?}",
                        self.worker_id, from_block, to_block, e
                    );
                    // Preserve the full gRPC/Flight error chain from bridge/erigon
                    return Err(SyncError::from_error_with_context(
                        DataType::new("transactions"),
                        from_block,
                        to_block,
                        "Failed to receive transaction batch",
                        e,
                    ));
                }
            };

            // Track the maximum responsibility end from batch metadata
            let resp_end = metadata.responsibility_range.end_block;
            max_responsibility_end = Some(
                max_responsibility_end
                    .map(|current| current.max(resp_end))
                    .unwrap_or(resp_end),
            );
            // Update writer's responsibility range from bridge metadata
            writer.update_responsibility_end(resp_end);
            if let Some(ref mut proof_w) = proof_writer {
                proof_w.update_responsibility_end(resp_end);
            }

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

                    // Log what we received from bridge
                    debug!(
                        "PHASER RECEIVED: Worker {} transactions batch {}, blocks {}-{} ({} rows)",
                        self.worker_id,
                        batches_processed + 1,
                        first,
                        last,
                        batch.num_rows()
                    );

                    last_block_seen = Some(last);
                }
            }

            // Generate proofs if enabled
            if let Some(ref mut proof_w) = proof_writer {
                if let Ok(proof_batch) = self.generate_proofs_for_batch(&batch) {
                    proof_w.write_batch(proof_batch).await.map_err(|e| {
                        SyncError::disk_io_error(
                            DataType::new("transactions"),
                            from_block,
                            to_block,
                            format!("Failed to write proof batch: {e}"),
                        )
                    })?;
                } else {
                    warn!(
                        "Worker {} failed to generate proofs for batch",
                        self.worker_id
                    );
                }
            }

            // Write Arrow RecordBatch directly to parquet and get actual bytes written
            let batch_bytes = writer.write_batch(batch).await.map_err(|e| {
                SyncError::disk_io_error(
                    DataType::new("transactions"),
                    from_block,
                    to_block,
                    format!("Failed to write transaction batch: {e}"),
                )
            })?;

            // Update progress with actual disk bytes
            self.update_current_progress(batch_bytes).await;

            batches_processed += 1;
        }

        debug!(
            "Worker {} received {} batches for transactions {}-{} (first_block: {:?}, last_block: {:?})",
            self.worker_id,
            batches_processed,
            from_block,
            to_block,
            first_block_seen,
            last_block_seen
        );

        // Validate we got data within the expected range
        // Note: Early blocks may have no transactions, so first block can be > from_block
        if batches_processed > 0 {
            if let (Some(first), Some(last)) = (first_block_seen, last_block_seen) {
                if first < from_block || first > to_block {
                    return Err(SyncError::validation_error(
                        DataType::new("transactions"),
                        from_block,
                        to_block,
                        format!(
                            "Bridge returned transactions starting at block {first} which is outside requested range {from_block}-{to_block}"
                        ),
                    ));
                }
                if last < from_block || last > to_block {
                    return Err(SyncError::validation_error(
                        DataType::new("transactions"),
                        from_block,
                        to_block,
                        format!(
                            "Bridge returned transactions ending at block {last} which is outside requested range {from_block}-{to_block}"
                        ),
                    ));
                }
            }
            // Finalize with the requested end block to ensure proper filename
            writer.finalize_current_file().map_err(|e| {
                SyncError::from_anyhow_with_context(
                    DataType::new("transactions"),
                    from_block,
                    to_block,
                    "Failed to finalize parquet file",
                    e,
                )
            })?;
            if let Some(ref mut proof_w) = proof_writer {
                proof_w.finalize_current_file().map_err(|e| {
                    SyncError::from_anyhow_with_context(
                        DataType::new("transactions"),
                        from_block,
                        to_block,
                        "Failed to finalize proof parquet file",
                        e,
                    )
                })?;
            }
        } else {
            // No batches received from bridge
            // This is valid when a block range legitimately has no transactions.
            // Arrow Flight doesn't transmit 0-row RecordBatches, so receiving 0 batches
            // means the range was processed successfully but contains no data.
            debug!(
                "Worker {} received ZERO batches for transactions {}-{}. \
                Range processed successfully with no transactions.",
                self.worker_id, from_block, to_block
            );
            // Treat as success with no data
        }

        Ok(())
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
        &self,
        client: &mut PhaserClient,
        from_block: u64,
        to_block: u64,
    ) -> Result<(), SyncError> {
        // Track phase
        self.metrics.active_workers_inc("logs");
        let metrics = self.metrics.clone();
        let _phase_guard = scopeguard::guard(metrics, |m| {
            m.active_workers_dec("logs");
        });

        let mut writer = ParquetWriter::with_config_and_mode(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
            "logs".to_string(),
            self.parquet_config.clone(),
            false, // historical sync worker
        )?;

        // Calculate segment boundaries (logical 500K segments)
        let segment_start = (self.from_block / self.segment_size) * self.segment_size;
        let segment_end = segment_start + self.segment_size - 1;

        // Set both segment range (for filename) and responsibility range (for metadata)
        writer.set_ranges(segment_start, segment_end, self.from_block, self.to_block);

        // Retry with resume logic
        const INITIAL_BACKOFF_SECS: u64 = 1;
        const MAX_BACKOFF_SECS: u64 = 60;
        const MAX_RETRIES_WITHOUT_PROGRESS: u32 = 5;
        let mut retry_count = 0u32;
        let mut retries_without_progress = 0u32;
        let mut resume_from = from_block;
        let mut last_resume_from = resume_from;

        loop {
            match self
                .try_sync_logs_stream(client, resume_from, to_block, &mut writer, from_block)
                .await
            {
                Ok(()) => break,
                Err(e) if is_transient_error(&e) => {
                    // Get the last block we successfully wrote
                    let last_written = writer.last_written_block();

                    let new_resume_from = if let Some(last) = last_written {
                        // We wrote some data, resume from the next block
                        last + 1
                    } else {
                        // No data written - this means the stream failed before sending any batches
                        // Keep resume_from unchanged to retry the same range
                        resume_from
                    };

                    // Detect if we're stuck in an infinite loop (no progress being made)
                    if new_resume_from == last_resume_from {
                        retries_without_progress += 1;
                        if retries_without_progress >= MAX_RETRIES_WITHOUT_PROGRESS {
                            return Err(SyncError::new(
                                DataType::new("logs"),
                                ErrorCategory::StuckWorker,
                                resume_from,
                                to_block,
                                format!(
                                    "Worker {} failed to make progress on logs {}-{} after {} retries. \
                                    Last error: {}. This may indicate missing or corrupted data in the source.",
                                    self.worker_id, resume_from, to_block, MAX_RETRIES_WITHOUT_PROGRESS, e
                                ),
                            ));
                        }
                    } else {
                        // Made progress, reset the no-progress counter
                        retries_without_progress = 0;
                    }

                    last_resume_from = resume_from;
                    resume_from = new_resume_from;

                    if resume_from > to_block {
                        // We actually completed, the error was after all data
                        debug!(
                            "Worker {} completed logs {}-{} before stream error",
                            self.worker_id, from_block, to_block
                        );
                        break; // Already finalized
                    }

                    retry_count += 1;
                    let backoff_secs = (INITIAL_BACKOFF_SECS
                        * 2u64.pow(retry_count.saturating_sub(1)))
                    .min(MAX_BACKOFF_SECS);

                    let progress_msg = if let Some(last) = last_written {
                        format!("last written block: {last}")
                    } else {
                        "no data received yet".to_string()
                    };

                    warn!(
                        "Worker {} logs stream failed ({}) - attempt {}: {}. Resuming from block {} in {}s... (retries without progress: {})",
                        self.worker_id, progress_msg, retry_count, e, resume_from, backoff_secs, retries_without_progress
                    );

                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn try_sync_logs_stream(
        &self,
        client: &mut PhaserClient,
        from_block: u64,
        to_block: u64,
        writer: &mut ParquetWriter,
        _original_from: u64,
    ) -> Result<(), SyncError> {
        // Create historical query using GenericQuery
        // Note: enable_traces can be passed as a filter if needed
        let query = GenericQuery::historical("logs", from_block, to_block)
            .with_filter("enable_traces", serde_json::json!(true));

        // Subscribe to the log stream with metadata (returns RecordBatch + responsibility range)
        let stream = client.query_with_metadata(query).await.map_err(|e| {
            SyncError::from_anyhow_with_context(
                DataType::new("logs"),
                from_block,
                to_block,
                "Failed to subscribe to log stream",
                e.into(),
            )
        })?;
        let mut stream = Box::pin(stream);

        let mut batches_processed = 0u64;
        let mut first_block_seen: Option<u64> = None;
        let mut last_block_seen: Option<u64> = None;
        let mut max_responsibility_end: Option<u64> = None;

        while let Some(batch_result) = stream.next().await {
            let (batch, metadata) = match batch_result {
                Ok(data) => data,
                Err(e) => {
                    // Preserve the full gRPC/Flight error chain from bridge/erigon
                    return Err(SyncError::from_error_with_context(
                        DataType::new("logs"),
                        from_block,
                        to_block,
                        "Failed to receive log batch",
                        e,
                    ));
                }
            };

            // Track the maximum responsibility end from batch metadata
            let resp_end = metadata.responsibility_range.end_block;
            max_responsibility_end = Some(
                max_responsibility_end
                    .map(|current| current.max(resp_end))
                    .unwrap_or(resp_end),
            );
            // Update writer's responsibility range from bridge metadata
            writer.update_responsibility_end(resp_end);

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

            // Write Arrow RecordBatch directly to parquet and get actual bytes written
            let batch_bytes = writer.write_batch(batch).await.map_err(|e| {
                SyncError::disk_io_error(
                    DataType::new("logs"),
                    from_block,
                    to_block,
                    format!("Failed to write log batch: {e}"),
                )
            })?;

            // Update progress with actual disk bytes
            self.update_current_progress(batch_bytes).await;

            batches_processed += 1;
        }

        // Validate we got data within the expected range
        // Note: Early blocks may have no logs, so first block can be > from_block
        if batches_processed > 0 {
            if let (Some(first), Some(last)) = (first_block_seen, last_block_seen) {
                if first < from_block || first > to_block {
                    return Err(SyncError::validation_error(
                        DataType::new("logs"),
                        from_block,
                        to_block,
                        format!(
                            "Bridge returned logs starting at block {first} which is outside requested range {from_block}-{to_block}"
                        ),
                    ));
                }
                if last < from_block || last > to_block {
                    return Err(SyncError::validation_error(
                        DataType::new("logs"),
                        from_block,
                        to_block,
                        format!(
                            "Bridge returned logs ending at block {last} which is outside requested range {from_block}-{to_block}"
                        ),
                    ));
                }
            }
            // Finalize with the requested end block to ensure proper filename
            writer.finalize_current_file().map_err(|e| {
                SyncError::from_anyhow_with_context(
                    DataType::new("logs"),
                    from_block,
                    to_block,
                    "Failed to finalize parquet file",
                    e,
                )
            })?;
        } else {
            // No batches received from bridge
            // This is valid when a block range legitimately has no logs.
            // Arrow Flight doesn't transmit 0-row RecordBatches, so receiving 0 batches
            // means the range was processed successfully but contains no data.
            debug!(
                "Worker {} received ZERO batches for logs {}-{}. \
                Range processed successfully with no logs.",
                self.worker_id, from_block, to_block
            );
            // Treat as success with no data
        }

        Ok(())
    }
}
