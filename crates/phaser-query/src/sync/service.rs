use crate::proto::admin::sync_service_server::{SyncService, SyncServiceServer};
use crate::proto::admin::*;
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

        // TODO: Spawn sync job with workers
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
        }))
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
}
