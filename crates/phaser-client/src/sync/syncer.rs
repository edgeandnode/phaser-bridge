//! PhaserSyncer - High-level orchestration for syncing data from bridges
//!
//! The syncer manages:
//! - Work queue of segments to sync
//! - Parallel worker coordination
//! - Retry logic with backoff
//! - Progress tracking
//!
//! This module is source-agnostic - it works with any data types defined
//! by the caller (e.g., "blocks", "events", "account_updates").

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::{mpsc, Mutex, RwLock, Semaphore};
use tracing::{debug, error, info, warn};

use crate::{GenericQuery, PhaserClient};

use super::config::SyncConfig;
use super::error::{DataType, MultipleDataTypeErrors, SyncError};
use super::progress::{Range, SegmentWork, SyncProgress};
use super::writer::{BatchWriter, WriterFactory};

/// Progress update sent to the caller
#[derive(Debug, Clone)]
pub struct ProgressUpdate {
    /// Current progress snapshot
    pub progress: SyncProgress,
    /// Timestamp of this update
    pub timestamp: std::time::Instant,
}

/// Type alias for the progress sender
pub type ProgressSender = mpsc::Sender<ProgressUpdate>;

/// Type alias for the progress receiver
pub type ProgressReceiver = mpsc::Receiver<ProgressUpdate>;

/// Result of a sync operation
#[derive(Debug)]
pub struct SyncResult {
    /// Number of segments synced
    pub segments_synced: u64,
    /// Total positions synced
    pub positions_synced: u64,
    /// Total bytes written
    pub bytes_written: u64,
    /// Number of retries that occurred
    pub retries: u64,
    /// Number of permanent errors (segments that couldn't be synced)
    pub permanent_errors: u64,
}

/// High-level syncer for data from Phaser bridges
///
/// Coordinates parallel workers to sync data from a Phaser bridge.
/// Source-agnostic - works with any data types defined by the caller.
pub struct PhaserSyncer {
    config: SyncConfig,
    /// Optional progress sender for streaming updates to the caller
    progress_sender: Option<ProgressSender>,
    /// Interval for sending progress updates (default: 1 second)
    progress_interval: Duration,
}

impl PhaserSyncer {
    /// Create a new syncer with the given configuration
    pub fn new(config: SyncConfig) -> Self {
        Self {
            config,
            progress_sender: None,
            progress_interval: Duration::from_secs(1),
        }
    }

    /// Create a progress channel and return the receiver
    ///
    /// Call this before `sync_range()` to receive progress updates.
    /// The syncer will send `ProgressUpdate` messages at the configured interval.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let syncer = PhaserSyncer::new(config);
    /// let mut progress_rx = syncer.subscribe_progress(100);
    ///
    /// // Spawn a task to handle progress
    /// tokio::spawn(async move {
    ///     while let Some(update) = progress_rx.recv().await {
    ///         println!("Progress: {:.1}%", update.progress.completion_percentage());
    ///     }
    /// });
    ///
    /// // Start sync
    /// syncer.sync_range(...).await?;
    /// ```
    pub fn subscribe_progress(&mut self, buffer_size: usize) -> ProgressReceiver {
        let (tx, rx) = mpsc::channel(buffer_size);
        self.progress_sender = Some(tx);
        rx
    }

    /// Set the interval for progress updates (default: 1 second)
    pub fn set_progress_interval(&mut self, interval: Duration) {
        self.progress_interval = interval;
    }

    /// Sync a position range from a bridge endpoint using the provided writer factory
    ///
    /// This is the main entry point for syncing. It:
    /// 1. Takes pre-computed work (which segments need which data types)
    /// 2. Spawns parallel workers
    /// 3. Uses the WriterFactory to create writers for each data type
    /// 4. Manages retries for failed segments
    /// 5. Returns when all segments are synced or have permanently failed
    ///
    /// The `work` parameter defines what needs to be synced. Each `SegmentWork`
    /// contains a map of data type names to missing ranges. The syncer doesn't
    /// know what "blocks" or "events" mean - it just syncs whatever data types
    /// are specified.
    pub async fn sync_range<F>(
        &self,
        bridge_endpoint: &str,
        from_position: u64,
        to_position: u64,
        work: Vec<SegmentWork>,
        writer_factory: Arc<F>,
    ) -> Result<SyncResult, SyncError>
    where
        F: WriterFactory + 'static,
    {
        let total_segments = work.len() as u64;

        if total_segments == 0 {
            info!(
                "No segments need syncing in range {}-{}",
                from_position, to_position
            );
            return Ok(SyncResult {
                segments_synced: 0,
                positions_synced: 0,
                bytes_written: 0,
                retries: 0,
                permanent_errors: 0,
            });
        }

        info!(
            "Starting sync of {} segments from {} ({} workers, positions {}-{})",
            total_segments, bridge_endpoint, self.config.max_workers, from_position, to_position
        );

        // Create work queue
        let segment_queue = Arc::new(Mutex::new(VecDeque::from(work)));
        let job_complete = Arc::new(AtomicBool::new(false));

        // Create semaphore for limiting concurrent "heavy" data type syncs
        let heavy_semaphore = Arc::new(Semaphore::new(
            self.config.max_concurrent_log_segments as usize,
        ));

        // Create shared progress tracker
        let progress = Arc::new(RwLock::new(SyncProgress::new(
            total_segments,
            to_position - from_position + 1,
        )));

        // Spawn workers
        let num_workers = std::cmp::min(self.config.max_workers as u64, total_segments) as u32;
        let mut worker_handles = Vec::with_capacity(num_workers as usize);

        for worker_id in 0..num_workers {
            let bridge_endpoint = bridge_endpoint.to_string();
            let segment_queue = segment_queue.clone();
            let job_complete = job_complete.clone();
            let heavy_semaphore = heavy_semaphore.clone();
            let progress = progress.clone();
            let config = self.config.clone();
            let writer_factory = writer_factory.clone();

            let handle = tokio::spawn(async move {
                Self::worker_loop(
                    worker_id,
                    &bridge_endpoint,
                    segment_queue,
                    job_complete,
                    heavy_semaphore,
                    progress,
                    &config,
                    writer_factory,
                )
                .await
            });

            worker_handles.push(handle);
        }

        // Spawn progress reporter if a subscriber exists
        let progress_handle = if let Some(sender) = &self.progress_sender {
            let sender = sender.clone();
            let progress = progress.clone();
            let job_complete = job_complete.clone();
            let interval = self.progress_interval;

            Some(tokio::spawn(async move {
                loop {
                    // Check if job is complete
                    if job_complete.load(Ordering::Relaxed) {
                        // Send final update
                        let snapshot = progress.read().await.clone();
                        let _ = sender
                            .send(ProgressUpdate {
                                progress: snapshot,
                                timestamp: std::time::Instant::now(),
                            })
                            .await;
                        break;
                    }

                    // Send progress update
                    let snapshot = progress.read().await.clone();
                    if sender
                        .send(ProgressUpdate {
                            progress: snapshot,
                            timestamp: std::time::Instant::now(),
                        })
                        .await
                        .is_err()
                    {
                        // Receiver dropped, stop reporting
                        break;
                    }

                    tokio::time::sleep(interval).await;
                }
            }))
        } else {
            None
        };

        // Wait for all workers to complete
        for (idx, handle) in worker_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(())) => {
                    debug!("Worker {} finished successfully", idx);
                }
                Ok(Err(e)) => {
                    error!("Worker {} failed: {}", idx, e);
                }
                Err(e) => {
                    error!("Worker {} panicked: {}", idx, e);
                }
            }
        }

        // Mark job complete
        job_complete.store(true, Ordering::Relaxed);

        // Wait for progress reporter to finish
        if let Some(handle) = progress_handle {
            let _ = handle.await;
        }

        // Collect final progress
        let final_progress = progress.read().await;
        Ok(SyncResult {
            segments_synced: final_progress.completed_segments,
            positions_synced: final_progress.positions_synced,
            bytes_written: final_progress.bytes_written,
            retries: final_progress.total_retries,
            permanent_errors: final_progress.permanent_errors,
        })
    }

    /// Worker loop - pulls segments from queue and syncs them
    #[allow(clippy::too_many_arguments)]
    async fn worker_loop<F>(
        worker_id: u32,
        bridge_endpoint: &str,
        segment_queue: Arc<Mutex<VecDeque<SegmentWork>>>,
        job_complete: Arc<AtomicBool>,
        heavy_semaphore: Arc<Semaphore>,
        progress: Arc<RwLock<SyncProgress>>,
        config: &SyncConfig,
        writer_factory: Arc<F>,
    ) -> Result<(), SyncError>
    where
        F: WriterFactory,
        F::Writer: 'static,
    {
        loop {
            // Check if job is complete
            if job_complete.load(Ordering::Relaxed) {
                info!(worker_id, "Worker exiting - job complete");
                break;
            }

            // Get next segment
            let work = {
                let mut queue = segment_queue.lock().await;
                queue.pop_front()
            };

            let work = match work {
                Some(w) => w,
                None => {
                    // No work available, check if we should wait or exit
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };

            let segment_num = work.segment_num;
            let retry_count = work.retry_count.unwrap_or(0);

            info!(
                worker_id,
                segment_num,
                from_position = work.segment_start,
                to_position = work.segment_end,
                retry_count,
                missing_types = ?work.missing_types(),
                "Starting segment processing"
            );

            // Mark segment as started
            {
                let mut p = progress.write().await;
                p.mark_segment_started();
            }

            // Process the segment
            match Self::sync_segment(
                worker_id,
                bridge_endpoint,
                &work,
                heavy_semaphore.clone(),
                config,
                &*writer_factory,
            )
            .await
            {
                Ok((positions, bytes)) => {
                    info!(
                        worker_id,
                        segment_num, positions, bytes, "Segment completed successfully"
                    );
                    let mut p = progress.write().await;
                    p.mark_segment_completed(positions, bytes);
                }
                Err(sync_err) => {
                    // Check if error is retryable
                    if sync_err.is_permanent() {
                        error!(
                            worker_id,
                            segment_num,
                            error_type = sync_err.category.as_str(),
                            data_type = sync_err.data_type.as_str(),
                            error = %sync_err,
                            "Segment failed with permanent error"
                        );
                        let mut p = progress.write().await;
                        p.record_permanent_error();
                        continue;
                    }

                    // Retryable error - check if we've exceeded max retries without progress
                    if retry_count >= config.retry_policy.max_retries_without_progress {
                        error!(
                            worker_id,
                            segment_num,
                            retry_count,
                            error = %sync_err,
                            "Segment exceeded max retries without progress"
                        );
                        let mut p = progress.write().await;
                        p.record_permanent_error();
                        continue;
                    }

                    // Calculate backoff
                    let backoff = config.retry_policy.backoff_for_retry(retry_count + 1);

                    warn!(
                        worker_id,
                        segment_num,
                        retry_count,
                        backoff_secs = backoff.as_secs(),
                        error = %sync_err,
                        "Segment failed, will retry"
                    );

                    // Record retry
                    {
                        let mut p = progress.write().await;
                        p.record_retry();
                    }

                    // Sleep for backoff
                    tokio::time::sleep(backoff).await;

                    // Re-queue with incremented retry count
                    let mut retry_work = work;
                    retry_work.retry_count = Some(retry_count + 1);
                    retry_work.last_attempt = std::time::Instant::now();

                    let mut queue = segment_queue.lock().await;
                    queue.push_back(retry_work);
                }
            }
        }

        Ok(())
    }

    /// Sync a single segment (all data types in parallel)
    async fn sync_segment<F>(
        worker_id: u32,
        bridge_endpoint: &str,
        work: &SegmentWork,
        _heavy_semaphore: Arc<Semaphore>,
        config: &SyncConfig,
        writer_factory: &F,
    ) -> Result<(u64, u64), SyncError>
    where
        F: WriterFactory,
        F::Writer: 'static,
    {
        // Get all data types that need syncing
        let data_types: Vec<String> = work.missing_types();

        if data_types.is_empty() {
            return Ok((0, 0));
        }

        // Spawn a task for each data type
        let mut handles = Vec::new();

        for data_type in data_types {
            let ranges = work.get_missing_ranges(&data_type).to_vec();
            let segment_start = work.segment_start;
            let segment_end = work.segment_end;
            let config = config.clone();
            let bridge_endpoint = bridge_endpoint.to_string();
            let dt_clone = data_type.clone();

            // Create writer for this data type
            let writer = match writer_factory.create_writer(
                DataType::new(&data_type),
                segment_start,
                segment_end,
                ranges.first().map(|r| r.start).unwrap_or(segment_start),
                ranges.last().map(|r| r.end).unwrap_or(segment_end),
            ) {
                Ok(w) => w,
                Err(e) => {
                    error!(
                        worker_id,
                        data_type = &data_type,
                        segment_num = work.segment_num,
                        error = %e,
                        "Failed to create writer"
                    );
                    return Err(e);
                }
            };

            let handle = tokio::spawn(async move {
                Self::sync_data_type_ranges(
                    worker_id,
                    &bridge_endpoint,
                    &dt_clone,
                    &ranges,
                    segment_start,
                    segment_end,
                    &config,
                    writer,
                )
                .await
                .map(|result| (dt_clone, result))
            });

            handles.push((data_type, handle));
        }

        // Collect results
        let mut errors = Vec::new();
        let mut total_positions = 0u64;
        let mut total_bytes = 0u64;

        for (data_type, handle) in handles {
            match handle.await {
                Ok(Ok((_, (positions, bytes)))) => {
                    total_positions += positions;
                    total_bytes += bytes;
                }
                Ok(Err(e)) => {
                    errors.push((DataType::new(&data_type), e));
                }
                Err(join_err) => {
                    errors.push((
                        DataType::new(&data_type),
                        SyncError::from_message(
                            DataType::new(&data_type),
                            work.segment_start,
                            work.segment_end,
                            format!("Task panicked: {join_err}"),
                        ),
                    ));
                }
            }
        }

        if !errors.is_empty() {
            return Err(MultipleDataTypeErrors {
                from_block: work.segment_start,
                to_block: work.segment_end,
                errors,
            }
            .into());
        }

        Ok((total_positions, total_bytes))
    }

    /// Sync ranges for a specific data type using the provided writer
    #[allow(clippy::too_many_arguments)]
    async fn sync_data_type_ranges<W>(
        worker_id: u32,
        bridge_endpoint: &str,
        data_type: &str,
        ranges: &[Range],
        segment_start: u64,
        segment_end: u64,
        config: &SyncConfig,
        mut writer: W,
    ) -> Result<(u64, u64), SyncError>
    where
        W: BatchWriter,
    {
        if ranges.is_empty() {
            return Ok((0, 0));
        }

        let dt = DataType::new(data_type);

        // Connect to bridge
        let mut client = PhaserClient::connect(bridge_endpoint.to_string())
            .await
            .map_err(|e| {
                SyncError::from_message(
                    dt.clone(),
                    segment_start,
                    segment_end,
                    format!("Failed to connect to bridge: {e}"),
                )
            })?;

        let mut total_positions = 0u64;
        let mut total_bytes = 0u64;

        for range in ranges {
            debug!(
                worker_id,
                data_type,
                from = range.start,
                to = range.end,
                "Syncing range"
            );

            let (positions, bytes) = Self::sync_single_range(
                worker_id,
                &mut client,
                data_type,
                range.start,
                range.end,
                config,
                &mut writer,
            )
            .await?;

            total_positions += positions;
            total_bytes += bytes;
        }

        // Finalize the writer
        writer.finalize().map_err(|e| {
            SyncError::from_message(
                dt.clone(),
                segment_start,
                segment_end,
                format!("Failed to finalize writer: {e}"),
            )
        })?;

        Ok((total_positions, total_bytes))
    }

    /// Sync a single range with retry/resume logic
    async fn sync_single_range<W>(
        worker_id: u32,
        client: &mut PhaserClient,
        data_type: &str,
        from_position: u64,
        to_position: u64,
        config: &SyncConfig,
        writer: &mut W,
    ) -> Result<(u64, u64), SyncError>
    where
        W: BatchWriter,
    {
        let dt = DataType::new(data_type);
        let mut resume_from = from_position;
        let mut last_resume_from = resume_from;
        let mut retries_without_progress = 0u32;
        let mut retry_count = 0u32;
        let mut total_bytes = 0u64;

        loop {
            match Self::try_sync_stream(client, data_type, resume_from, to_position, config, writer)
                .await
            {
                Ok(bytes) => {
                    total_bytes += bytes;
                    let positions = to_position - from_position + 1;
                    return Ok((positions, total_bytes));
                }
                Err(e) if e.is_transient() => {
                    // Get last written position from writer for resume
                    let new_resume_from = writer
                        .last_written_block()
                        .map(|b| b + 1)
                        .unwrap_or(resume_from);

                    // Detect stuck loop
                    if new_resume_from == last_resume_from {
                        retries_without_progress += 1;
                        if retries_without_progress
                            >= config.retry_policy.max_retries_without_progress
                        {
                            return Err(SyncError::stuck_worker(
                                dt.clone(),
                                resume_from,
                                to_position,
                                format!(
                                    "Worker {} failed to make progress on {} {}-{} after {} retries. Last error: {}",
                                    worker_id, data_type, resume_from, to_position,
                                    config.retry_policy.max_retries_without_progress, e
                                ),
                            ));
                        }
                    } else {
                        retries_without_progress = 0;
                    }

                    last_resume_from = resume_from;
                    resume_from = new_resume_from;

                    if resume_from > to_position {
                        // Completed before error
                        break;
                    }

                    retry_count += 1;
                    let backoff = config.retry_policy.backoff_for_retry(retry_count);

                    warn!(
                        worker_id,
                        data_type,
                        from = resume_from,
                        to = to_position,
                        retry_count,
                        backoff_secs = backoff.as_secs(),
                        error = %e,
                        "Stream failed, will retry"
                    );

                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        let positions = to_position - from_position + 1;
        Ok((positions, total_bytes))
    }

    /// Try to sync a stream (single attempt), writing batches through the writer
    async fn try_sync_stream<W>(
        client: &mut PhaserClient,
        data_type: &str,
        from_position: u64,
        to_position: u64,
        config: &SyncConfig,
        writer: &mut W,
    ) -> Result<u64, SyncError>
    where
        W: BatchWriter,
    {
        let dt = DataType::new(data_type);

        // Build query using the data type as the table name
        let mut query = GenericQuery::historical(data_type, from_position, to_position);

        // Add trace filter for logs if enabled (EVM-specific, but harmless for others)
        if data_type == "logs" && config.enable_traces {
            query = query.with_filter("enable_traces", serde_json::json!(true));
        }

        // Get stream with metadata
        let stream = client.query_with_metadata(query).await.map_err(|e| {
            SyncError::from_message(
                dt.clone(),
                from_position,
                to_position,
                format!("Failed to start stream: {e}"),
            )
        })?;
        let mut stream = Box::pin(stream);

        let mut batches_processed = 0u64;
        let mut total_bytes = 0u64;

        while let Some(batch_result) = stream.next().await {
            let (batch, metadata) = match batch_result {
                Ok(data) => data,
                Err(e) => {
                    return Err(SyncError::from_message(
                        dt.clone(),
                        from_position,
                        to_position,
                        format!("Stream error: {e}"),
                    ));
                }
            };

            // Update writer with responsibility range from metadata
            writer.update_responsibility_end(metadata.responsibility_range.end_block);

            // Write the batch through the writer
            let bytes = writer.write_batch(batch).await.map_err(|e| {
                SyncError::from_message(
                    dt.clone(),
                    from_position,
                    to_position,
                    format!("Write error: {e}"),
                )
            })?;

            total_bytes += bytes;
            batches_processed += 1;
        }

        debug!(
            data_type,
            from = from_position,
            to = to_position,
            batches = batches_processed,
            bytes = total_bytes,
            "Stream completed"
        );

        Ok(total_bytes)
    }
}
