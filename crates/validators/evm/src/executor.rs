use crate::error::ValidationError;
use crate::executors::{CoreExecutor, RayonExecutor, TokioExecutor};
use async_trait::async_trait;
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;
use serde::{Deserialize, Serialize};

/// Trait for executing validation work with different parallelization strategies
#[async_trait]
pub trait ValidationExecutor: Send + Sync {
    /// Validate a single block's transactions against its transactions_root
    async fn validate_block(
        &self,
        block: BlockRecord,
        transactions: Vec<TransactionRecord>,
    ) -> Result<(), ValidationError>;

    /// Validate multiple blocks in parallel
    /// Returns a vector of results, one per block
    async fn validate_batch(
        &self,
        blocks: Vec<(BlockRecord, Vec<TransactionRecord>)>,
    ) -> Vec<Result<(), ValidationError>>;
}

/// Configuration for validation executor
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ExecutorConfig {
    /// Rayon-based parallel executor (best for high throughput)
    Rayon {
        #[serde(default)]
        num_threads: Option<usize>,
    },
    /// Tokio-based executor (best for low-medium throughput)
    Tokio {
        #[serde(default = "default_max_concurrent")]
        max_concurrent: usize,
    },
    /// core-executor based (experimental)
    Core {
        #[serde(default = "default_num_workers")]
        num_workers: usize,
    },
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        ExecutorConfig::Rayon { num_threads: None }
    }
}

impl ExecutorConfig {
    /// Build an executor from this configuration
    pub fn build(&self) -> Box<dyn ValidationExecutor> {
        match self {
            ExecutorConfig::Rayon { num_threads } => Box::new(RayonExecutor::new(*num_threads)),
            ExecutorConfig::Tokio { max_concurrent } => {
                Box::new(TokioExecutor::new(*max_concurrent))
            }
            ExecutorConfig::Core { num_workers } => Box::new(CoreExecutor::new(*num_workers)),
        }
    }
}

fn default_max_concurrent() -> usize {
    100
}

fn default_num_workers() -> usize {
    num_cpus::get()
}
