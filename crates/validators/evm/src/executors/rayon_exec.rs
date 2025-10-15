use crate::error::ValidationError;
use crate::executor::ValidationExecutor;
use crate::validation::validate_transactions_root;
use async_trait::async_trait;
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;
use rayon::prelude::*;

/// Rayon-based parallel validation executor
///
/// Uses rayon's work-stealing thread pool for CPU-bound validation work.
/// Best for high-throughput scenarios with many blocks to validate.
pub struct RayonExecutor {
    num_threads: Option<usize>,
}

impl RayonExecutor {
    /// Create a new Rayon executor
    ///
    /// # Arguments
    /// * `num_threads` - Number of threads to use (None = use all cores)
    pub fn new(num_threads: Option<usize>) -> Self {
        Self { num_threads }
    }

    /// Configure the rayon thread pool if needed
    fn configure_pool(&self) {
        if let Some(threads) = self.num_threads {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build_global()
                .ok(); // Ignore if already configured
        }
    }
}

#[async_trait]
impl ValidationExecutor for RayonExecutor {
    async fn validate_block(
        &self,
        block: BlockRecord,
        transactions: Vec<TransactionRecord>,
    ) -> Result<(), ValidationError> {
        self.configure_pool();

        // Move to blocking context for CPU-bound work
        tokio::task::spawn_blocking(move || validate_transactions_root(&block, &transactions))
            .await?
    }

    async fn validate_batch(
        &self,
        blocks: Vec<(BlockRecord, Vec<TransactionRecord>)>,
    ) -> Vec<Result<(), ValidationError>> {
        self.configure_pool();

        // Capture length before moving
        let block_count = blocks.len();

        // Move entire batch to blocking context, then use rayon parallel iteration
        let results = tokio::task::spawn_blocking(move || {
            blocks
                .par_iter()
                .map(|(block, txs)| validate_transactions_root(block, txs))
                .collect::<Vec<_>>()
        })
        .await;

        match results {
            Ok(validation_results) => validation_results,
            Err(join_err) => {
                // If the task panicked, return error for all blocks
                let err = ValidationError::TaskJoinError(join_err.to_string());
                vec![Err(err); block_count]
            }
        }
    }
}
