use crate::proto::admin::sync_service_server::{SyncService, SyncServiceServer};
use crate::proto::admin::*;
use crate::sync::data_scanner::DataScanner;
use crate::sync::metrics;
use crate::sync::worker::{ProgressTracker, SyncWorker, SyncWorkerConfig};
use crate::PhaserConfig;
use anyhow::Result;
use core_executor::ThreadPoolExecutor;
use phaser_bridge::FlightBridgeClient;
use phaser_metrics::SegmentMetrics;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::proto::admin::{
    DataProgress as ProtoDataProgress, DataTypeProgress as ProtoDataTypeProgress,
    FileStatistics as ProtoFileStatistics, GapAnalysis as ProtoGapAnalysis, IncompleteSegment,
};
use crate::sync::data_scanner::{
    DataProgress, DataTypeProgress, FileStatistics, GapAnalysis as DataGapAnalysis,
};

/// Convert internal GapAnalysis to proto
fn gap_analysis_to_proto(analysis: &DataGapAnalysis, segment_size: u64) -> ProtoGapAnalysis {
    let incomplete_details = analysis
        .segments_needing_work
        .iter()
        .map(|work| {
            let from_block = work.segment_num * segment_size;
            let to_block = from_block + segment_size - 1;

            // Convert BlockRange vectors to proto
            let missing_blocks_ranges = work
                .missing_blocks
                .iter()
                .map(|r| crate::proto::admin::BlockRange {
                    start: r.start,
                    end: r.end,
                })
                .collect();

            let missing_transactions_ranges = work
                .missing_transactions
                .iter()
                .map(|r| crate::proto::admin::BlockRange {
                    start: r.start,
                    end: r.end,
                })
                .collect();

            let missing_logs_ranges = work
                .missing_logs
                .iter()
                .map(|r| crate::proto::admin::BlockRange {
                    start: r.start,
                    end: r.end,
                })
                .collect();

            IncompleteSegment {
                segment_num: work.segment_num,
                from_block,
                to_block,
                missing_data_types: work.missing_types(),
                missing_blocks_ranges,
                missing_transactions_ranges,
                missing_logs_ranges,
            }
        })
        .collect();

    ProtoGapAnalysis {
        total_segments: analysis.total_segments,
        complete_segments: analysis.complete_count() as u64,
        missing_segments: analysis.missing_count() as u64,
        completion_percentage: analysis.completion_percentage(),
        cleaned_temp_files: analysis.cleaned_temp_files as u64,
        segments_to_sync: analysis
            .segments_needing_work
            .iter()
            .map(|w| w.segment_num)
            .collect(),
        incomplete_details,
    }
}

/// Convert internal DataProgress to proto
fn data_progress_to_proto(progress: &DataProgress) -> ProtoDataProgress {
    ProtoDataProgress {
        blocks: Some(data_type_progress_to_proto(&progress.blocks)),
        transactions: Some(data_type_progress_to_proto(&progress.transactions)),
        logs: Some(data_type_progress_to_proto(&progress.logs)),
        file_stats: Some(file_statistics_to_proto(&progress.file_stats)),
    }
}

/// Convert internal DataTypeProgress to proto
fn data_type_progress_to_proto(progress: &DataTypeProgress) -> ProtoDataTypeProgress {
    ProtoDataTypeProgress {
        blocks_on_disk: progress.blocks_on_disk,
        gap_count: progress.gap_count,
        coverage_percentage: progress.coverage_percentage,
        highest_continuous: progress.highest_continuous,
    }
}

/// Convert internal FileStatistics to proto
fn file_statistics_to_proto(stats: &FileStatistics) -> ProtoFileStatistics {
    ProtoFileStatistics {
        total_files: stats.total_files,
        blocks_files: stats.blocks_files,
        transactions_files: stats.transactions_files,
        logs_files: stats.logs_files,
        proofs_files: stats.proofs_files,
        total_disk_bytes: stats.total_disk_bytes,
        blocks_disk_bytes: stats.blocks_disk_bytes,
        transactions_disk_bytes: stats.transactions_disk_bytes,
        logs_disk_bytes: stats.logs_disk_bytes,
        proofs_disk_bytes: stats.proofs_disk_bytes,
    }
}

/// Job state for tracking sync progress
#[derive(Debug, Clone)]
struct SyncJobState {
    job_id: String,
    chain_id: u64,
    bridge_name: String,
    from_block: u64,
    to_block: u64,
    status: i32, // SyncStatus enum
    current_block: u64,
    blocks_synced: u64,
    active_workers: u32,
    error: Option<String>,
    progress_tracker: ProgressTracker,
}

/// Configuration for a sync job
struct SyncJobConfig {
    chain_id: u64,
    bridge_name: String,
    bridge_endpoint: String,
    from_block: u64,
    to_block: u64,
    historical_boundary: Option<u64>,
}

/// Server implementation for the sync admin service
pub struct SyncServer {
    config: Arc<PhaserConfig>,
    jobs: Arc<RwLock<HashMap<String, SyncJobState>>>,
    live_state: Arc<crate::LiveStreamingState>,
    executor: Arc<Mutex<ThreadPoolExecutor>>,
}

impl SyncServer {
    pub fn new(
        config: Arc<PhaserConfig>,
        live_state: Arc<crate::LiveStreamingState>,
        executor: Arc<Mutex<ThreadPoolExecutor>>,
    ) -> Self {
        Self {
            config,
            jobs: Arc::new(RwLock::new(HashMap::new())),
            live_state,
            executor,
        }
    }

    pub fn into_grpc_service(self) -> SyncServiceServer<Self> {
        SyncServiceServer::new(self)
    }

    pub async fn start(self, port: u16) -> Result<()> {
        let addr = format!("0.0.0.0:{}", port).parse()?;
        info!("Starting sync admin gRPC server on {}", addr);

        tonic::transport::Server::builder()
            .add_service(self.into_grpc_service())
            .serve(addr)
            .await?;

        Ok(())
    }

    async fn run_sync_job(
        config: Arc<PhaserConfig>,
        jobs: Arc<RwLock<HashMap<String, SyncJobState>>>,
        job_id: String,
        job_config: SyncJobConfig,
        progress_tracker: ProgressTracker,
        executor: Arc<Mutex<ThreadPoolExecutor>>,
    ) -> Result<()> {
        // Update status to RUNNING
        {
            let mut jobs_lock = jobs.write().await;
            if let Some(job) = jobs_lock.get_mut(&job_id) {
                job.status = SyncStatus::Running as i32;
                job.active_workers = config.sync_parallelism;
            }
        }

        info!(
            "Starting sync job {} with {} workers: blocks {}-{}",
            job_id, config.sync_parallelism, job_config.from_block, job_config.to_block
        );

        let segment_size = config.segment_size;

        // Find which segments need to be synced (supports resume)
        let data_dir = config.bridge_data_dir(job_config.chain_id, &job_config.bridge_name);
        let scanner = DataScanner::new(data_dir.clone(), executor);

        // Use boundary from LiveStreamingState (already computed in start_sync)
        // This is more reliable than scanning temp files

        // Analyze what needs syncing
        let mut analysis = scanner
            .analyze_sync_range(job_config.from_block, job_config.to_block, segment_size)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to analyze sync range: {}", e))?;

        // Filter out segments >= live sync boundary to avoid cleaning active live streaming temp files
        let segments_to_clean: Vec<u64> = if let Some(boundary) = job_config.historical_boundary {
            let live_segment = (boundary + 1) / segment_size;
            analysis
                .segments_needing_work
                .iter()
                .filter(|w| w.segment_num < live_segment)
                .map(|w| w.segment_num)
                .collect()
        } else {
            analysis
                .segments_needing_work
                .iter()
                .map(|w| w.segment_num)
                .collect()
        };

        // Clean only temp files that conflict with segments we're about to sync (excluding live sync segments)
        info!("Cleaning conflicting temp files in {:?}", data_dir);
        let cleaned_count = scanner
            .clean_conflicting_temp_files(&segments_to_clean, segment_size)
            .map_err(|e| anyhow::anyhow!("Failed to clean temp files: {}", e))?;

        analysis.cleaned_temp_files = cleaned_count;

        // Log summary for CLI/API consumers
        info!(
            "Gap analysis: {}/{} segments complete ({:.1}%), {} need syncing",
            analysis.complete_count(),
            analysis.total_segments,
            analysis.completion_percentage(),
            analysis.missing_count()
        );

        if !analysis.needs_sync() {
            info!(
                "All segments already synced for range {}-{}",
                job_config.from_block, job_config.to_block
            );
            // Mark job as complete
            let mut jobs_lock = jobs.write().await;
            if let Some(job) = jobs_lock.get_mut(&job_id) {
                job.status = SyncStatus::Completed as i32;
                job.blocks_synced = job_config.to_block - job_config.from_block + 1;
                job.current_block = job_config.to_block;
            }
            return Ok(());
        }

        // Use the segments_needing_work from analysis
        let total_segments = analysis.missing_count() as u64;
        let segment_queue = Arc::new(tokio::sync::Mutex::new(VecDeque::from(
            analysis.segments_needing_work,
        )));
        let job_complete = Arc::new(AtomicBool::new(false));

        info!(
            "Found {} segments to sync ({} blocks per segment)",
            total_segments, segment_size
        );

        // Spawn worker tasks that pull segments from the queue
        let mut worker_handles = vec![];
        let num_workers = std::cmp::min(config.sync_parallelism as u64, total_segments) as u32;
        let max_file_size_mb = config.max_file_size_mb;

        // Constants for retry logic
        const MAX_RETRIES_WITHOUT_PROGRESS: u32 = 5;
        const MIN_BACKOFF_SECS: u64 = 1;
        const MAX_BACKOFF_SECS: u64 = 10;

        // Create metrics helper once
        let sync_metrics = metrics::SyncMetrics::new(
            "phaser_query",
            job_config.chain_id,
            job_config.bridge_name.clone(),
        );

        for worker_id in 0..num_workers {
            let bridge_endpoint = job_config.bridge_endpoint.clone();
            let data_dir = data_dir.clone();
            let segment_queue = segment_queue.clone();
            let progress_tracker = progress_tracker.clone();
            let parquet_config = config.parquet.clone();
            let validation_stage = config.validation_stage;
            let job_complete = job_complete.clone();
            let historical_boundary = job_config.historical_boundary;
            let metrics = sync_metrics.clone();

            let handle: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
                loop {
                    // Check if job is complete
                    if job_complete.load(Ordering::Relaxed) {
                        info!(worker_id = worker_id, "Worker exiting - job complete");
                        break;
                    }

                    // Get next segment work item
                    let work = {
                        let mut queue = segment_queue.lock().await;

                        // Update queue depth metric
                        metrics.sync_queue_depth("pending", queue.len() as i64);

                        queue.pop_front()
                    };

                    let work = match work {
                        Some(w) => w,
                        None => {
                            // No work available - wait before checking again
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }
                    };

                    let segment_num = work.segment_num;
                    let segment_from = work.segment_from;
                    let segment_to = work.segment_to;
                    let retry_count = work.retry_count.unwrap_or(0);
                    let segment_start = std::time::Instant::now();

                    info!(
                        worker_id = worker_id,
                        segment_num = segment_num,
                        from_block = segment_from,
                        to_block = segment_to,
                        retry_count = retry_count,
                        missing_blocks = work.missing_blocks.len(),
                        missing_txs = work.missing_transactions.len(),
                        missing_logs = work.missing_logs.len(),
                        "Starting segment processing"
                    );

                    // Reset retry metric on first attempt
                    if retry_count == 0 {
                        metrics.segment_sync_retries(segment_num, 0);
                    }

                    // Create and run worker for this segment
                    let config = SyncWorkerConfig {
                        metrics: metrics.clone(),
                        bridge_endpoint: bridge_endpoint.clone(),
                        data_dir: data_dir.clone(),
                        from_block: segment_from,
                        to_block: segment_to,
                        segment_size,
                        max_file_size_mb,
                        batch_size: 1000,
                        parquet_config: parquet_config.clone(),
                        validation_stage,
                    };

                    let mut worker =
                        SyncWorker::new(worker_id, config, work.clone(), historical_boundary)
                            .with_progress_tracker(progress_tracker.clone());

                    match worker.run().await {
                        Ok(()) => {
                            let duration = segment_start.elapsed();

                            // Metrics
                            metrics.segment_attempts("success");
                            metrics.segment_duration("all", duration.as_secs_f64());

                            // Record total duration for this segment (from first attempt to completion)
                            metrics.segment_total_duration(segment_num, duration.as_secs() as i64);

                            // Set final retry count for this sync run
                            metrics.segment_sync_retries(segment_num, retry_count as i64);

                            // Clear current retry count gauge (no longer needed)
                            metrics.segment_retry_count_remove(segment_num);

                            info!(
                                worker_id = worker_id,
                                segment_num = segment_num,
                                duration_secs = duration.as_secs(),
                                retry_count = retry_count,
                                "Segment completed successfully"
                            );
                        }
                        Err(sync_err) => {
                            // We now have a rich SyncError with all context preserved
                            let error_category = sync_err.category.as_str();
                            let data_type = sync_err.data_type.as_str();

                            // Metrics
                            metrics.sync_errors(error_category, data_type);

                            // Check if error is retryable
                            use crate::sync::error::ErrorCategory;
                            let is_retryable = matches!(
                                sync_err.category,
                                ErrorCategory::Connection
                                    | ErrorCategory::Timeout
                                    | ErrorCategory::Cancelled
                            );

                            if !is_retryable {
                                // Non-retryable error - fail immediately
                                metrics.segment_attempts("failure");
                                error!(
                                    worker_id = worker_id,
                                    segment_num = segment_num,
                                    error_type = error_category,
                                    data_type = data_type,
                                    from_block = sync_err.from_block,
                                    to_block = sync_err.to_block,
                                    error = %sync_err,
                                    "Segment failed with non-retryable error, not retrying"
                                );
                                // Don't re-queue, move to next segment
                                continue;
                            }

                            // Retryable error - proceed with retry logic
                            metrics.segment_attempts("retry");

                            // Update retry count metric (gauge for current retry count)
                            metrics.segment_retry_count(segment_num, (retry_count + 1) as i64);

                            // Calculate backoff: exponential up to 5 retries, then cap at 10s
                            let backoff_secs = if retry_count < MAX_RETRIES_WITHOUT_PROGRESS {
                                (MIN_BACKOFF_SECS * 2u64.pow(retry_count)).min(MAX_BACKOFF_SECS)
                            } else {
                                MAX_BACKOFF_SECS
                            };

                            // Log level escalates after detection threshold
                            if retry_count < MAX_RETRIES_WITHOUT_PROGRESS {
                                warn!(
                                    worker_id = worker_id,
                                    segment_num = segment_num,
                                    retry_count = retry_count,
                                    backoff_secs = backoff_secs,
                                    error_type = error_category,
                                    data_type = data_type,
                                    from_block = sync_err.from_block,
                                    to_block = sync_err.to_block,
                                    error = %sync_err,
                                    "Segment failed, will retry"
                                );
                            } else {
                                error!(
                                    worker_id = worker_id,
                                    segment_num = segment_num,
                                    retry_count = retry_count,
                                    backoff_secs = backoff_secs,
                                    error_type = error_category,
                                    data_type = data_type,
                                    from_block = sync_err.from_block,
                                    to_block = sync_err.to_block,
                                    error = %sync_err,
                                    "Segment persistently failing - continuing to retry with {}s backoff",
                                    MAX_BACKOFF_SECS
                                );
                            }

                            // Sleep for backoff period BEFORE re-queuing
                            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;

                            // Re-queue for retry
                            let mut work_with_retry = work.clone();
                            work_with_retry.retry_count = Some(retry_count + 1);
                            work_with_retry.last_attempt = std::time::Instant::now();

                            // Put back at END of queue so other segments get processed first
                            segment_queue.lock().await.push_back(work_with_retry);
                        }
                    }
                }

                Ok(())
            });

            worker_handles.push(handle);
        }

        // Wait for all workers to complete
        for (idx, handle) in worker_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(())) => {
                    info!("Worker {} finished", idx);
                }
                Ok(Err(e)) => {
                    error!("Worker {} failed: {}", idx, e);
                }
                Err(e) => {
                    error!("Worker {} panicked: {}", idx, e);
                }
            }
        }

        // Mark job as complete
        job_complete.store(true, Ordering::Relaxed);

        // Update final status
        {
            let mut jobs_lock = jobs.write().await;
            if let Some(job) = jobs_lock.get_mut(&job_id) {
                job.status = SyncStatus::Completed as i32;
                job.blocks_synced = job_config.to_block - job_config.from_block + 1;
                job.current_block = job_config.to_block;
                job.active_workers = 0;
            }
        }

        info!("Sync job {} completed successfully", job_id);
        Ok(())
    }
}

#[tonic::async_trait]
impl SyncService for SyncServer {
    async fn start_sync(
        &self,
        request: Request<SyncRequest>,
    ) -> Result<Response<SyncResponse>, Status> {
        let req = request.into_inner();

        info!(
            "Received sync request: chain_id={}, bridge={}, from={}, to={}",
            req.chain_id, req.bridge_name, req.from_block, req.to_block
        );

        // Validate request (allow single-block syncs where from == to)
        if req.from_block > req.to_block {
            return Err(Status::invalid_argument(
                "from_block must be less than or equal to to_block",
            ));
        }

        // Wait for live streaming to initialize (if it's enabled)
        // This gives us the exact boundary where historical sync should stop
        let id = crate::ChainBridgeId::new(req.chain_id, &req.bridge_name);
        let historical_boundary = if self.live_state.is_enabled(&id).await {
            info!("Live streaming is enabled, waiting for boundary...");
            // Wait with longer timeout when streaming is enabled (120 seconds)
            let boundary = self.live_state.wait_for_boundary(&id, 120).await;

            if boundary.is_none() {
                return Err(Status::internal(
                    "Live streaming is enabled but failed to initialize within timeout",
                ));
            }

            boundary
        } else {
            // No live streaming enabled
            info!("Live streaming not enabled, using full requested range");
            None
        };

        // Determine final to_block based on live streaming boundary
        let to_block = if let Some(boundary_block) = historical_boundary {
            // Live streaming has started - historical sync goes right up to where it started
            let safe_boundary = boundary_block.saturating_sub(1);

            if req.to_block > safe_boundary {
                info!(
                    "Live streaming detected at block {}. Adjusting to_block from {} to {}",
                    boundary_block, req.to_block, safe_boundary
                );
                safe_boundary
            } else {
                req.to_block
            }
        } else {
            req.to_block
        };

        // Check if bridge is configured
        let bridge = self
            .config
            .get_bridge(req.chain_id, &req.bridge_name)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Bridge '{}' for chain {} not found in configuration",
                    req.bridge_name, req.chain_id
                ))
            })?;

        // Validate that to_block doesn't exceed chain tip (only for historical syncs without live streaming)
        if historical_boundary.is_none() {
            // Connect to bridge to check chain tip
            let mut client = FlightBridgeClient::connect(bridge.endpoint.clone())
                .await
                .map_err(|e| Status::unavailable(format!("Failed to connect to bridge: {}", e)))?;

            let bridge_info = client
                .get_info()
                .await
                .map_err(|e| Status::internal(format!("Failed to get bridge info: {}", e)))?;

            if bridge_info.current_block > 0 && to_block > bridge_info.current_block {
                return Err(Status::invalid_argument(format!(
                    "Requested to_block ({}) exceeds chain tip ({}). \
                    Historical sync cannot request blocks that don't exist yet. \
                    Current chain tip is at block {}.",
                    to_block, bridge_info.current_block, bridge_info.current_block
                )));
            }
        }

        // Generate job ID
        let job_id = Uuid::new_v4().to_string();

        // Perform gap analysis before starting job
        let data_dir = self.config.bridge_data_dir(req.chain_id, &req.bridge_name);
        let scanner = DataScanner::new(data_dir.clone(), self.executor.clone());

        // Analyze what needs syncing
        let mut gap_analysis = scanner
            .analyze_sync_range(req.from_block, to_block, self.config.segment_size)
            .await
            .map_err(|e| Status::internal(format!("Failed to analyze sync range: {}", e)))?;

        // Filter out segments >= live sync boundary to avoid cleaning active live streaming temp files
        let segments_to_clean: Vec<u64> = if let Some(boundary_block) = historical_boundary {
            let live_segment = boundary_block / self.config.segment_size;
            gap_analysis
                .segments_needing_work
                .iter()
                .filter(|w| w.segment_num < live_segment)
                .map(|w| w.segment_num)
                .collect()
        } else {
            // No live streaming - safe to clean all missing segments
            gap_analysis
                .segments_needing_work
                .iter()
                .map(|w| w.segment_num)
                .collect()
        };

        // Clean only temp files that conflict with segments we're about to sync (excluding live sync segments)
        let cleaned_count = scanner
            .clean_conflicting_temp_files(&segments_to_clean, self.config.segment_size)
            .map_err(|e| Status::internal(format!("Failed to clean temp files: {}", e)))?;

        gap_analysis.cleaned_temp_files = cleaned_count;

        // Create progress tracker
        let progress_tracker = Arc::new(RwLock::new(HashMap::new()));

        // Create job state
        let job_state = SyncJobState {
            job_id: job_id.clone(),
            chain_id: req.chain_id,
            bridge_name: req.bridge_name.clone(),
            from_block: req.from_block,
            to_block,
            status: SyncStatus::Pending as i32,
            current_block: req.from_block,
            blocks_synced: 0,
            active_workers: 0,
            error: None,
            progress_tracker: progress_tracker.clone(),
        };

        // Store job state
        {
            let mut jobs = self.jobs.write().await;
            jobs.insert(job_id.clone(), job_state.clone());
        }

        // Spawn sync job with workers
        let config = self.config.clone();
        let jobs = self.jobs.clone();
        let job_id_clone = job_id.clone();
        let executor = self.executor.clone();

        let job_config = SyncJobConfig {
            chain_id: req.chain_id,
            bridge_name: req.bridge_name.clone(),
            bridge_endpoint: bridge.endpoint.clone(),
            from_block: req.from_block,
            to_block,
            historical_boundary,
        };

        tokio::spawn(async move {
            if let Err(e) = Self::run_sync_job(
                config,
                jobs,
                job_id_clone,
                job_config,
                progress_tracker,
                executor,
            )
            .await
            {
                error!("Sync job failed: {}", e);
            }
        });

        info!("Created sync job {}", job_id);

        let message = if to_block != req.to_block {
            format!(
                "Sync job created for blocks {}-{} on chain {} via bridge '{}' (adjusted from {} to avoid live sync overlap)",
                req.from_block, to_block, req.chain_id, req.bridge_name, req.to_block
            )
        } else {
            format!(
                "Sync job created for blocks {}-{} on chain {} via bridge '{}'",
                req.from_block, to_block, req.chain_id, req.bridge_name
            )
        };

        Ok(Response::new(SyncResponse {
            job_id,
            message,
            accepted: true,
            gap_analysis: Some(gap_analysis_to_proto(
                &gap_analysis,
                self.config.segment_size,
            )),
        }))
    }

    async fn get_sync_status(
        &self,
        request: Request<SyncStatusRequest>,
    ) -> Result<Response<SyncStatusResponse>, Status> {
        let req = request.into_inner();

        let jobs = self.jobs.read().await;
        let job = jobs
            .get(&req.job_id)
            .ok_or_else(|| Status::not_found(format!("Job {} not found", req.job_id)))?;

        // Scan disk for ground truth - what's actually completed
        let data_dir = self.config.bridge_data_dir(job.chain_id, &job.bridge_name);
        let scanner = DataScanner::new(data_dir, self.executor.clone());
        let analysis = scanner
            .analyze_sync_range(job.from_block, job.to_block, self.config.segment_size)
            .await
            .map_err(|e| Status::internal(format!("Failed to analyze sync progress: {}", e)))?;

        // Calculate actual progress from disk
        let complete_segments = analysis.complete_segments.len() as u64;
        let blocks_synced = complete_segments * self.config.segment_size;

        // Find highest completed block from complete segments
        let max_completed_block = analysis
            .complete_segments
            .iter()
            .max()
            .map(|&seg| {
                let seg_end = (seg + 1) * self.config.segment_size - 1;
                std::cmp::min(seg_end, job.to_block)
            })
            .unwrap_or(job.from_block);

        // Count active workers from progress tracker (for UX visibility)
        let progress = job.progress_tracker.read().await;
        let active_workers = progress.len() as u32;

        // Calculate download rate from active workers
        let now = std::time::SystemTime::now();
        let download_rate_bytes_per_sec = progress
            .values()
            .filter_map(|p| {
                if let Ok(elapsed) = now.duration_since(p.started_at) {
                    let secs = elapsed.as_secs_f64();
                    if secs > 0.0 {
                        return Some(p.bytes_written as f64 / secs);
                    }
                }
                None
            })
            .sum();

        let total_blocks = job.to_block - job.from_block + 1;

        // Calculate detailed progress metrics
        let data_progress = scanner
            .calculate_data_progress(job.from_block, job.to_block)
            .await
            .ok()
            .map(|dp| data_progress_to_proto(&dp));

        Ok(Response::new(SyncStatusResponse {
            job_id: job.job_id.clone(),
            status: job.status,
            current_block: max_completed_block,
            total_blocks,
            blocks_synced,
            error: job.error.clone().unwrap_or_default(),
            active_workers,
            chain_id: job.chain_id,
            bridge_name: job.bridge_name.clone(),
            from_block: job.from_block,
            to_block: job.to_block,
            gap_analysis: Some(gap_analysis_to_proto(&analysis, self.config.segment_size)),
            data_progress,
            download_rate_bytes_per_sec,
        }))
    }

    async fn list_sync_jobs(
        &self,
        request: Request<ListSyncJobsRequest>,
    ) -> Result<Response<ListSyncJobsResponse>, Status> {
        let req = request.into_inner();

        let jobs = self.jobs.read().await;

        let mut job_list = Vec::new();

        for job in jobs.values() {
            // Apply status filter if provided
            if let Some(status_filter) = req.status_filter {
                if job.status != status_filter {
                    continue;
                }
            }

            // Scan disk for actual progress
            let data_dir = self.config.bridge_data_dir(job.chain_id, &job.bridge_name);
            let scanner = DataScanner::new(data_dir, self.executor.clone());
            let (blocks_synced, max_completed_block, gap_analysis, data_progress) = match scanner
                .analyze_sync_range(job.from_block, job.to_block, self.config.segment_size)
                .await
            {
                Ok(analysis) => {
                    let complete_segments = analysis.complete_segments.len() as u64;
                    let blocks = complete_segments * self.config.segment_size;
                    let max_block = analysis
                        .complete_segments
                        .iter()
                        .max()
                        .map(|&seg| {
                            let seg_end = (seg + 1) * self.config.segment_size - 1;
                            std::cmp::min(seg_end, job.to_block)
                        })
                        .unwrap_or(job.from_block);

                    // Calculate detailed progress
                    let data_progress = scanner
                        .calculate_data_progress(job.from_block, job.to_block)
                        .await
                        .ok()
                        .map(|dp| data_progress_to_proto(&dp));

                    (
                        blocks,
                        max_block,
                        Some(gap_analysis_to_proto(&analysis, self.config.segment_size)),
                        data_progress,
                    )
                }
                Err(_) => (0, job.from_block, None, None),
            };

            // Count active workers and calculate download rate
            let progress = job.progress_tracker.read().await;
            let active_workers = progress.len() as u32;

            // Calculate download rate from active workers
            let now = std::time::SystemTime::now();
            let download_rate_bytes_per_sec = progress
                .values()
                .filter_map(|p| {
                    if let Ok(elapsed) = now.duration_since(p.started_at) {
                        let secs = elapsed.as_secs_f64();
                        if secs > 0.0 {
                            return Some(p.bytes_written as f64 / secs);
                        }
                    }
                    None
                })
                .sum();

            job_list.push(SyncStatusResponse {
                job_id: job.job_id.clone(),
                status: job.status,
                current_block: max_completed_block,
                total_blocks: job.to_block - job.from_block + 1,
                blocks_synced,
                error: job.error.clone().unwrap_or_default(),
                active_workers,
                chain_id: job.chain_id,
                bridge_name: job.bridge_name.clone(),
                from_block: job.from_block,
                to_block: job.to_block,
                gap_analysis,
                data_progress,
                download_rate_bytes_per_sec,
            });
        }

        Ok(Response::new(ListSyncJobsResponse { jobs: job_list }))
    }

    async fn cancel_sync(
        &self,
        request: Request<CancelSyncRequest>,
    ) -> Result<Response<CancelSyncResponse>, Status> {
        let req = request.into_inner();

        let mut jobs = self.jobs.write().await;
        let job = jobs
            .get_mut(&req.job_id)
            .ok_or_else(|| Status::not_found(format!("Job {} not found", req.job_id)))?;

        // Check if job can be cancelled
        if job.status == SyncStatus::Completed as i32
            || job.status == SyncStatus::Failed as i32
            || job.status == SyncStatus::Cancelled as i32
        {
            return Ok(Response::new(CancelSyncResponse {
                success: false,
                message: format!("Job {} cannot be cancelled (already finished)", req.job_id),
            }));
        }

        // TODO: Actually cancel the running workers
        job.status = SyncStatus::Cancelled as i32;

        Ok(Response::new(CancelSyncResponse {
            success: true,
            message: format!("Job {} cancelled", req.job_id),
        }))
    }

    async fn analyze_gaps(
        &self,
        request: Request<AnalyzeGapsRequest>,
    ) -> Result<Response<AnalyzeGapsResponse>, Status> {
        let req = request.into_inner();

        // Verify bridge is configured
        let _bridge = self
            .config
            .get_bridge(req.chain_id, &req.bridge_name)
            .ok_or_else(|| {
                Status::not_found(format!(
                    "Bridge '{}' for chain {} not found in configuration",
                    req.bridge_name, req.chain_id
                ))
            })?;

        // Perform gap analysis
        let data_dir = self.config.bridge_data_dir(req.chain_id, &req.bridge_name);
        let scanner = DataScanner::new(data_dir.clone(), self.executor.clone());

        // Analyze what needs syncing
        let mut gap_analysis = scanner
            .analyze_sync_range(req.from_block, req.to_block, self.config.segment_size)
            .await
            .map_err(|e| Status::internal(format!("Failed to analyze sync range: {}", e)))?;

        // Get historical boundary from LiveStreamingState to avoid cleaning live streaming temp files
        let id = crate::ChainBridgeId::new(req.chain_id, &req.bridge_name);
        let historical_boundary = self.live_state.get_boundary(&id).await;

        // Filter out segments >= live sync boundary to avoid cleaning active live streaming temp files
        let segments_to_clean: Vec<u64> = if let Some(boundary_block) = historical_boundary {
            let live_segment = boundary_block / self.config.segment_size;
            gap_analysis
                .segments_needing_work
                .iter()
                .filter(|w| w.segment_num < live_segment)
                .map(|w| w.segment_num)
                .collect()
        } else {
            // No live streaming - safe to clean all missing segments
            gap_analysis
                .segments_needing_work
                .iter()
                .map(|w| w.segment_num)
                .collect()
        };

        // Clean only temp files that conflict with segments we're analyzing (excluding live sync segments)
        let cleaned_count = scanner
            .clean_conflicting_temp_files(&segments_to_clean, self.config.segment_size)
            .map_err(|e| Status::internal(format!("Failed to clean temp files: {}", e)))?;

        gap_analysis.cleaned_temp_files = cleaned_count;

        let message = if gap_analysis.needs_sync() {
            format!(
                "{}/{} segments complete ({:.1}%), {} need syncing",
                gap_analysis.complete_count(),
                gap_analysis.total_segments,
                gap_analysis.completion_percentage(),
                gap_analysis.missing_count()
            )
        } else {
            format!("All {} segments complete", gap_analysis.total_segments)
        };

        Ok(Response::new(AnalyzeGapsResponse {
            gap_analysis: Some(gap_analysis_to_proto(
                &gap_analysis,
                self.config.segment_size,
            )),
            message,
        }))
    }

    type StreamSyncProgressStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<SyncProgressUpdate, Status>> + Send>>;

    async fn stream_sync_progress(
        &self,
        request: Request<SyncProgressRequest>,
    ) -> Result<Response<Self::StreamSyncProgressStream>, Status> {
        let req = request.into_inner();

        // Verify job exists
        {
            let jobs = self.jobs.read().await;
            if !jobs.contains_key(&req.job_id) {
                return Err(Status::not_found(format!("Job {} not found", req.job_id)));
            }
        }

        // Stream actual progress from disk + active worker state
        let jobs = self.jobs.clone();
        let job_id = req.job_id.clone();
        let config = self.config.clone();
        let executor = self.executor.clone();

        let stream = async_stream::stream! {
            loop {
                // Read current job state
                let update = {
                    let jobs_lock = jobs.read().await;
                    if let Some(job) = jobs_lock.get(&job_id) {
                        // Scan disk for actual progress
                        let data_dir = config.bridge_data_dir(job.chain_id, &job.bridge_name);
                        let scanner = DataScanner::new(data_dir, executor.clone());
                        let total_blocks_synced = match scanner.analyze_sync_range(
                            job.from_block,
                            job.to_block,
                            config.segment_size,
                        ).await {
                            Ok(analysis) => analysis.complete_segments.len() as u64 * config.segment_size,
                            Err(_) => 0, // Fallback if scan fails
                        };

                        // Calculate detailed progress
                        let data_progress = scanner
                            .calculate_data_progress(job.from_block, job.to_block)
                            .await
                            .ok()
                            .map(|dp| data_progress_to_proto(&dp));

                        // Read worker progress for UX visibility
                        let progress_lock = job.progress_tracker.read().await;

                        // Calculate download rate from active workers
                        let now = std::time::SystemTime::now();
                        let download_rate_bytes_per_sec = progress_lock
                            .values()
                            .filter_map(|p| {
                                if let Ok(elapsed) = now.duration_since(p.started_at) {
                                    let secs = elapsed.as_secs_f64();
                                    if secs > 0.0 {
                                        return Some(p.bytes_written as f64 / secs);
                                    }
                                }
                                None
                            })
                            .sum();

                        let workers: Vec<WorkerProgress> = progress_lock
                            .values()
                            .map(|p| {
                                let started_at = p.started_at
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs() as i64;

                                let elapsed = p.started_at.elapsed().unwrap_or_default().as_secs();
                                let rate = if elapsed > 0 {
                                    p.blocks_processed as f64 / elapsed as f64
                                } else {
                                    0.0
                                };

                                WorkerProgress {
                                    worker_id: p.worker_id,
                                    stage: p.current_phase.clone(),
                                    from_block: p.from_block,
                                    to_block: p.to_block,
                                    current_block: p.current_block,
                                    blocks_processed: p.blocks_processed,
                                    rate,
                                    bytes_written: p.bytes_written,
                                    files_created: p.files_created,
                                    started_at,
                                }
                            })
                            .collect();

                        Some(SyncProgressUpdate {
                            job_id: job.job_id.clone(),
                            status: job.status,
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as i64,
                            workers,
                            total_blocks_synced,
                            total_blocks: job.to_block - job.from_block + 1,
                            overall_rate: 0.0,
                            total_bytes_written: 0,
                            data_progress,
                            download_rate_bytes_per_sec,
                        })
                    } else {
                        None
                    }
                };

                match update {
                    Some(u) => {
                        yield Ok(u);

                        // Check if job is finished
                        let jobs_lock = jobs.read().await;
                        if let Some(job) = jobs_lock.get(&job_id) {
                            if job.status == SyncStatus::Completed as i32
                                || job.status == SyncStatus::Failed as i32
                                || job.status == SyncStatus::Cancelled as i32
                            {
                                break;
                            }
                        }
                    }
                    None => break,
                }

                // Wait before sending next update
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }
}
