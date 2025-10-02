use crate::proto::admin::sync_service_server::{SyncService, SyncServiceServer};
use crate::proto::admin::*;
use crate::sync::data_scanner::DataScanner;
use crate::sync::worker::{ProgressTracker, SyncWorker};
use crate::PhaserConfig;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{error, info};
use uuid::Uuid;

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

/// Server implementation for the sync admin service
pub struct SyncServer {
    config: Arc<PhaserConfig>,
    jobs: Arc<RwLock<HashMap<String, SyncJobState>>>,
}

impl SyncServer {
    pub fn new(config: Arc<PhaserConfig>) -> Self {
        Self {
            config,
            jobs: Arc::new(RwLock::new(HashMap::new())),
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
        chain_id: u64,
        bridge_name: String,
        bridge_endpoint: String,
        from_block: u64,
        to_block: u64,
        progress_tracker: ProgressTracker,
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
            job_id, config.sync_parallelism, from_block, to_block
        );

        let segment_size = config.segment_size;

        // Find which segments need to be synced (supports resume)
        let data_dir = config.bridge_data_dir(chain_id, &bridge_name);
        let scanner = DataScanner::new(data_dir.clone());

        // Clean up any stale temp files from previous crashes
        info!("Scanning for stale temp files in {:?}", data_dir);
        let cleaned_count = scanner
            .clean_stale_temp_files()
            .map_err(|e| anyhow::anyhow!("Failed to clean temp files: {}", e))?;

        // Analyze what needs syncing
        let mut analysis = scanner
            .analyze_sync_range(from_block, to_block, segment_size)
            .map_err(|e| anyhow::anyhow!("Failed to analyze sync range: {}", e))?;

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
                from_block, to_block
            );
            // Mark job as complete
            let mut jobs_lock = jobs.write().await;
            if let Some(job) = jobs_lock.get_mut(&job_id) {
                job.status = SyncStatus::Completed as i32;
                job.blocks_synced = to_block - from_block + 1;
                job.current_block = to_block;
            }
            return Ok(());
        }

        let missing_segments = analysis.missing_segments;
        let total_segments = missing_segments.len() as u64;

        info!(
            "Found {} segments to sync ({} blocks per segment)",
            total_segments, segment_size
        );

        // Convert missing segments to a shared queue
        let segment_queue = Arc::new(tokio::sync::Mutex::new(missing_segments));

        // Spawn worker tasks that pull segments from the queue
        let mut worker_handles = vec![];
        let num_workers = std::cmp::min(config.sync_parallelism as u64, total_segments) as u32;
        let max_file_size_mb = config.max_file_size_mb;

        // Track failed segments for potential retry
        let failed_segments = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        for worker_id in 0..num_workers {
            let bridge_endpoint = bridge_endpoint.clone();
            let data_dir = data_dir.clone();
            let segment_queue = segment_queue.clone();
            let progress_tracker = progress_tracker.clone();
            let parquet_config = config.parquet.clone();
            let failed_segments = failed_segments.clone();

            let handle = tokio::spawn(async move {
                let mut worker_errors = 0u32;

                loop {
                    // Get next segment to process
                    let segment_num = {
                        let mut queue = segment_queue.lock().await;
                        if queue.is_empty() {
                            info!(
                                "Worker {} completed all assigned segments (errors: {})",
                                worker_id, worker_errors
                            );
                            break;
                        }
                        queue.remove(0)
                    };

                    // Calculate block range for this segment
                    let segment_from = segment_num * segment_size;
                    let segment_to = segment_from + segment_size - 1;

                    // Ensure we don't go past the requested to_block
                    let segment_to = std::cmp::min(segment_to, to_block);

                    info!(
                        "Worker {} processing segment {} (blocks {}-{})",
                        worker_id, segment_num, segment_from, segment_to
                    );

                    // Create and run worker for this segment with timeout
                    let mut worker = SyncWorker::new(
                        worker_id,
                        bridge_endpoint.clone(),
                        data_dir.clone(),
                        segment_from,
                        segment_to,
                        segment_size,
                        max_file_size_mb,
                        1000, // batch_size
                        parquet_config.clone(),
                    )
                    .with_progress_tracker(progress_tracker.clone());

                    // Add 10 minute timeout per segment
                    let result =
                        tokio::time::timeout(std::time::Duration::from_secs(600), worker.run())
                            .await;

                    match result {
                        Ok(Ok(())) => {
                            info!("Worker {} completed segment {}", worker_id, segment_num);
                        }
                        Ok(Err(e)) => {
                            error!(
                                "Worker {} failed on segment {}: {}",
                                worker_id, segment_num, e
                            );
                            worker_errors += 1;
                            failed_segments.lock().await.push(segment_num);

                            // Continue to next segment instead of stopping worker
                        }
                        Err(_) => {
                            error!(
                                "Worker {} timeout on segment {} after 10 minutes",
                                worker_id, segment_num
                            );
                            worker_errors += 1;
                            failed_segments.lock().await.push(segment_num);
                        }
                    }
                }

                if worker_errors > 0 {
                    Err(anyhow::anyhow!(
                        "Worker {} had {} errors",
                        worker_id,
                        worker_errors
                    ))
                } else {
                    Ok(())
                }
            });

            worker_handles.push(handle);
        }

        // Wait for all workers to complete
        let mut has_error = false;
        let mut error_msg = String::new();

        for (idx, handle) in worker_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(())) => {
                    info!("Worker {} finished all segments", idx);
                }
                Ok(Err(e)) => {
                    error!("Worker {} failed: {}", idx, e);
                    has_error = true;
                    if error_msg.is_empty() {
                        error_msg = format!("Worker {} failed: {}", idx, e);
                    }
                }
                Err(e) => {
                    error!("Worker {} panicked: {}", idx, e);
                    has_error = true;
                    if error_msg.is_empty() {
                        error_msg = format!("Worker {} panicked: {}", idx, e);
                    }
                }
            }
        }

        // Check for failed segments
        let failed = failed_segments.lock().await;
        if !failed.is_empty() {
            error!(
                "Sync job {} had {} failed segments: {:?}",
                job_id,
                failed.len(),
                failed
            );
            has_error = true;
            error_msg = format!("{} segments failed: {:?}", failed.len(), failed);
        }

        // Update final status
        {
            let mut jobs_lock = jobs.write().await;
            if let Some(job) = jobs_lock.get_mut(&job_id) {
                if has_error {
                    job.status = SyncStatus::Failed as i32;
                    job.error = Some(error_msg);
                } else {
                    job.status = SyncStatus::Completed as i32;
                    job.blocks_synced = to_block - from_block + 1;
                    job.current_block = to_block;
                }
                job.active_workers = 0;
            }
        }

        if has_error {
            return Err(anyhow::anyhow!("Sync job failed"));
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

        // Validate request
        if req.from_block >= req.to_block {
            return Err(Status::invalid_argument(
                "from_block must be less than to_block",
            ));
        }

        // Get data directory for this bridge to scan for existing data
        let data_dir = self.config.bridge_data_dir(req.chain_id, &req.bridge_name);
        let scanner = DataScanner::new(data_dir);

        // Find where live sync data starts (if any)
        let historical_boundary = scanner
            .find_historical_boundary(self.config.segment_size)
            .map_err(|e| Status::internal(format!("Failed to scan existing data: {}", e)))?;

        // Determine final to_block
        let to_block = if let Some(boundary) = historical_boundary {
            // Live sync data detected, ensure we don't overlap
            if req.to_block > boundary {
                info!(
                    "Live sync detected at block {}. Adjusting to_block from {} to {}",
                    boundary + 1,
                    req.to_block,
                    boundary
                );
                boundary
            } else {
                req.to_block
            }
        } else {
            // No live sync data, use requested to_block
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

        // Generate job ID
        let job_id = Uuid::new_v4().to_string();

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
        let bridge_endpoint = bridge.endpoint.clone();
        let bridge_name = req.bridge_name.clone();
        let chain_id = req.chain_id;
        let from_block = req.from_block;

        tokio::spawn(async move {
            if let Err(e) = Self::run_sync_job(
                config,
                jobs,
                job_id_clone,
                chain_id,
                bridge_name,
                bridge_endpoint,
                from_block,
                to_block,
                progress_tracker,
            )
            .await
            {
                error!("Sync job failed: {}", e);
            }
        });

        info!("Created sync job {}", job_id);

        Ok(Response::new(SyncResponse {
            job_id,
            message: format!(
                "Sync job created for blocks {}-{} on chain {} via bridge '{}'",
                req.from_block, to_block, req.chain_id, req.bridge_name
            ),
            accepted: true,
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

        // Aggregate worker progress
        let progress = job.progress_tracker.read().await;
        let total_blocks = job.to_block - job.from_block + 1;

        // Count completed items from all workers across all 3 data types
        let mut blocks_synced = 0u64;
        let mut max_completed_block = job.from_block;

        for worker in progress.values() {
            let worker_blocks = worker.to_block - worker.from_block + 1;

            // Calculate completion for this worker
            // Each worker processes 3 data types: blocks, transactions, logs
            // Progress is sum of completed phases
            let phase_progress = if worker.logs_completed {
                worker_blocks // All 3 phases complete
            } else if worker.transactions_completed {
                (worker_blocks * 2) / 3 // 2 of 3 phases complete
            } else if worker.blocks_completed {
                worker_blocks / 3 // 1 of 3 phases complete
            } else {
                0
            };

            blocks_synced += phase_progress;

            // Track highest fully completed segment (all 3 data types done)
            if worker.logs_completed && worker.to_block > max_completed_block {
                max_completed_block = worker.to_block;
            }
        }

        Ok(Response::new(SyncStatusResponse {
            job_id: job.job_id.clone(),
            status: job.status,
            current_block: max_completed_block,
            total_blocks,
            blocks_synced,
            error: job.error.clone().unwrap_or_default(),
            active_workers: job.active_workers,
            chain_id: job.chain_id,
            bridge_name: job.bridge_name.clone(),
            from_block: job.from_block,
            to_block: job.to_block,
        }))
    }

    async fn list_sync_jobs(
        &self,
        request: Request<ListSyncJobsRequest>,
    ) -> Result<Response<ListSyncJobsResponse>, Status> {
        let req = request.into_inner();

        let jobs = self.jobs.read().await;

        let job_list: Vec<SyncStatusResponse> = jobs
            .values()
            .filter(|job| {
                // Apply status filter if provided
                if let Some(status_filter) = req.status_filter {
                    job.status == status_filter
                } else {
                    true
                }
            })
            .map(|job| SyncStatusResponse {
                job_id: job.job_id.clone(),
                status: job.status,
                current_block: job.current_block,
                total_blocks: job.to_block - job.from_block + 1,
                blocks_synced: job.blocks_synced,
                error: job.error.clone().unwrap_or_default(),
                active_workers: job.active_workers,
                chain_id: job.chain_id,
                bridge_name: job.bridge_name.clone(),
                from_block: job.from_block,
                to_block: job.to_block,
            })
            .collect();

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

        // TODO: Implement actual progress streaming from workers
        // For now, return a stub stream that sends periodic updates
        let jobs = self.jobs.clone();
        let job_id = req.job_id.clone();

        let stream = async_stream::stream! {
            loop {
                // Read current job state
                let update = {
                    let jobs_lock = jobs.read().await;
                    if let Some(job) = jobs_lock.get(&job_id) {
                        Some(SyncProgressUpdate {
                            job_id: job.job_id.clone(),
                            status: job.status,
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as i64,
                            workers: vec![], // TODO: Actual worker progress
                            total_blocks_synced: job.blocks_synced,
                            total_blocks: job.to_block - job.from_block + 1,
                            overall_rate: 0.0,
                            total_bytes_written: 0,
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
