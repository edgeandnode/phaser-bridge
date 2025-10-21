use crate::error::ValidationError;
use crate::executor::ValidationExecutor;
use crate::validation::{
    validate_receipts_rlp, validate_transactions_rlp, validate_transactions_root,
};
use alloy_primitives::{Bytes, B256};
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
    async fn spawn_validate_rlp(
        &self,
        expected_root: B256,
        transaction_rlps: Vec<Bytes>,
    ) -> Result<(), ValidationError> {
        tokio::task::spawn_blocking(move || {
            let rlp_refs: Vec<_> = transaction_rlps.iter().map(|b| b.as_ref()).collect();
            validate_transactions_rlp(expected_root, &rlp_refs)
        })
        .await?
    }

    async fn spawn_validate_receipts_rlp(
        &self,
        expected_root: B256,
        receipt_rlps: Vec<Bytes>,
    ) -> Result<(), ValidationError> {
        tokio::task::spawn_blocking(move || {
            let rlp_refs: Vec<_> = receipt_rlps.iter().map(|b| b.as_ref()).collect();
            validate_receipts_rlp(expected_root, &rlp_refs)
        })
        .await?
    }

    async fn spawn_validate_records(
        &self,
        block: BlockRecord,
        transactions: Vec<TransactionRecord>,
    ) -> Result<(), ValidationError> {
        tokio::task::spawn_blocking(move || validate_transactions_root(&block, &transactions))
            .await?
    }

    async fn spawn_validate_batch(
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
