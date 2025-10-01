use crate::proto::admin::sync_service_server::{SyncService, SyncServiceServer};
use crate::proto::admin::*;
use crate::sync::worker::SyncWorker;
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

        // Calculate block range per worker
        let total_blocks = to_block - from_block + 1;
        let blocks_per_worker =
            (total_blocks + config.sync_parallelism as u64 - 1) / config.sync_parallelism as u64;

        // Get data directory for this bridge
        let data_dir = config.bridge_data_dir(chain_id, &bridge_name);

        // Spawn workers
        let mut worker_handles = vec![];
        for worker_id in 0..config.sync_parallelism {
            let worker_from = from_block + (worker_id as u64 * blocks_per_worker);
            let worker_to = std::cmp::min(worker_from + blocks_per_worker - 1, to_block);

            // Skip if this worker has no blocks to process
            if worker_from > to_block {
                break;
            }

            let mut worker = SyncWorker::new(
                worker_id,
                bridge_endpoint.clone(),
                data_dir.clone(),
                worker_from,
                worker_to,
                config.segment_size,
                config.max_file_size_mb,
                1000, // batch_size
                config.parquet.clone(),
            );

            let handle = tokio::spawn(async move { worker.run().await });

            worker_handles.push(handle);
        }

        // Wait for all workers to complete
        let mut has_error = false;
        let mut error_msg = String::new();

        for (idx, handle) in worker_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(())) => {
                    info!("Worker {} completed successfully", idx);
                }
                Ok(Err(e)) => {
                    error!("Worker {} failed: {}", idx, e);
                    has_error = true;
                    error_msg = format!("Worker {} failed: {}", idx, e);
                }
                Err(e) => {
                    error!("Worker {} panicked: {}", idx, e);
                    has_error = true;
                    error_msg = format!("Worker {} panicked: {}", idx, e);
                }
            }
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
                    job.blocks_synced = total_blocks;
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

        // Create job state
        let job_state = SyncJobState {
            job_id: job_id.clone(),
            chain_id: req.chain_id,
            bridge_name: req.bridge_name.clone(),
            from_block: req.from_block,
            to_block: req.to_block,
            status: SyncStatus::Pending as i32,
            current_block: req.from_block,
            blocks_synced: 0,
            active_workers: 0,
            error: None,
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
        let to_block = req.to_block;

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
                req.from_block, req.to_block, req.chain_id, req.bridge_name
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

        Ok(Response::new(SyncStatusResponse {
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
