use crate::error::ValidationError;
use crate::executor::ValidationExecutor;
use crate::validation::validate_transactions_root;
use async_trait::async_trait;
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;

/// Tokio-based validation executor
///
/// Uses tokio::spawn_blocking for CPU-bound validation work.
/// Best for low to medium throughput scenarios.
pub struct TokioExecutor {
    max_concurrent: usize,
}

impl TokioExecutor {
    /// Create a new Tokio executor
    ///
    /// # Arguments
    /// * `max_concurrent` - Maximum number of concurrent validation tasks
    pub fn new(max_concurrent: usize) -> Self {
        Self { max_concurrent }
    }
}

#[async_trait]
impl ValidationExecutor for TokioExecutor {
    async fn validate_block(
        &self,
        block: BlockRecord,
        transactions: Vec<TransactionRecord>,
    ) -> Result<(), ValidationError> {
        tokio::task::spawn_blocking(move || validate_transactions_root(&block, &transactions))
            .await?
    }

    async fn validate_batch(
        &self,
        blocks: Vec<(BlockRecord, Vec<TransactionRecord>)>,
    ) -> Vec<Result<(), ValidationError>> {
        use futures::stream::{self, StreamExt};

        // Process blocks concurrently with a limit
        let tasks = blocks.into_iter().map(|(block, txs)| {
            tokio::task::spawn_blocking(move || validate_transactions_root(&block, &txs))
        });

        // Collect results with concurrency limit
        stream::iter(tasks)
            .buffer_unordered(self.max_concurrent)
            .map(|join_result| match join_result {
                Ok(validation_result) => validation_result,
                Err(join_err) => Err(ValidationError::TaskJoinError(join_err.to_string())),
            })
            .collect::<Vec<_>>()
            .await
    }
}
