use crate::error::ValidationError;
use crate::executors::{CoreExecutor, TokioExecutor};
use async_trait::async_trait;
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Executor type enum for CLI and configuration
#[derive(Debug, Clone, Copy)]
pub enum ExecutorType {
    Tokio,
    Core,
}

impl ExecutorType {
    /// Build an ExecutorConfig from this executor type
    ///
    /// # Arguments
    /// * `threads` - Number of threads/workers (will use num_cpus::get() if None)
    pub fn build_config(self, threads: Option<usize>) -> ExecutorConfig {
        let threads = threads.unwrap_or_else(num_cpus::get);

        match self {
            ExecutorType::Tokio => ExecutorConfig::Tokio {
                max_concurrent: threads,
            },
            ExecutorType::Core => ExecutorConfig::Core {
                num_workers: threads,
            },
        }
    }
}

impl FromStr for ExecutorType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tokio" => Ok(ExecutorType::Tokio),
            "core" => Ok(ExecutorType::Core),
            _ => Err(format!(
                "invalid executor type '{}', expected 'tokio' or 'core'",
                s
            )),
        }
    }
}

impl fmt::Display for ExecutorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorType::Tokio => write!(f, "tokio"),
            ExecutorType::Core => write!(f, "core"),
        }
    }
}

/// Trait for executing validation work with different parallelization strategies
///
/// # Execution Model
///
/// All `spawn_*` methods use an **eager-on-first-poll** execution model:
/// - When the returned future is **first polled**, validation work is immediately
///   dispatched to the underlying executor's thread pool
/// - Subsequent polls check if the work has completed
/// - The returned future is an accessor to retrieve the result, not the work itself
///
/// This design enables overlapping I/O operations (e.g., block collection) with
/// CPU-bound validation work when futures are created during collection and polled later.
#[async_trait]
pub trait ValidationExecutor: Send + Sync {
    /// Spawn validation of transactions from RLP (canonical ingestion validation)
    ///
    /// Validates data authenticity from the node/network by computing merkle root
    /// from RLP-encoded transactions and comparing with expected root.
    ///
    /// # Execution Model
    ///
    /// When the returned future is first polled, validation work is immediately
    /// dispatched to the underlying executor. This allows overlapping collection
    /// with validation when futures are created eagerly.
    ///
    /// # Arguments
    /// * `expected_root` - The transactions_root from block header
    /// * `transaction_rlps` - RLP-encoded transactions (must be in correct order)
    ///
    /// # Returns
    /// Future that resolves when validation completes. Work starts on first poll.
    async fn spawn_validate_rlp(
        &self,
        expected_root: alloy_primitives::B256,
        transaction_rlps: Vec<alloy_primitives::Bytes>,
    ) -> Result<(), ValidationError>;

    /// Spawn validation of receipts from RLP (canonical ingestion validation)
    ///
    /// Validates data authenticity from the node/network by computing merkle root
    /// from RLP-encoded receipts and comparing with expected root.
    ///
    /// # Execution Model
    ///
    /// When the returned future is first polled, validation work is immediately
    /// dispatched to the underlying executor. This allows overlapping collection
    /// with validation when futures are created eagerly.
    ///
    /// # Arguments
    /// * `expected_root` - The receipts_root from block header
    /// * `receipt_rlps` - RLP-encoded receipts (must be in correct order)
    ///
    /// # Returns
    /// Future that resolves when validation completes. Work starts on first poll.
    async fn spawn_validate_receipts_rlp(
        &self,
        expected_root: alloy_primitives::B256,
        receipt_rlps: Vec<alloy_primitives::Bytes>,
    ) -> Result<(), ValidationError>;

    /// Spawn validation of transactions from Arrow records (conversion validation)
    ///
    /// Validates both node data AND our Arrow conversion logic by:
    /// 1. Converting TransactionRecord → TxEnvelope → RLP
    /// 2. Computing merkle root from converted RLP
    /// 3. Comparing with expected root from block header
    ///
    /// # Execution Model
    ///
    /// When the returned future is first polled, validation work is immediately
    /// dispatched to the underlying executor.
    ///
    /// # Arguments
    /// * `block` - Block record with expected transactions_root
    /// * `transactions` - Transaction records to validate
    ///
    /// # Returns
    /// Future that resolves when validation completes. Work starts on first poll.
    async fn spawn_validate_records(
        &self,
        block: BlockRecord,
        transactions: Vec<TransactionRecord>,
    ) -> Result<(), ValidationError>;

    /// Spawn validation of multiple blocks in parallel (batch conversion validation)
    ///
    /// # Execution Model
    ///
    /// All validation work is dispatched to the executor when this method is called.
    /// Returns a vector of futures, one per block.
    ///
    /// # Arguments
    /// * `blocks` - Vector of (block, transactions) pairs to validate
    ///
    /// # Returns
    /// Vector of results, one per block, in same order as input
    async fn spawn_validate_batch(
        &self,
        blocks: Vec<(BlockRecord, Vec<TransactionRecord>)>,
    ) -> Vec<Result<(), ValidationError>>;
}

/// Configuration for validation executor
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ExecutorConfig {
    /// Tokio-based executor (good default for most use cases)
    Tokio {
        #[serde(default = "default_max_concurrent")]
        max_concurrent: usize,
    },
    /// core-executor based (NUMA-aware, for high-throughput production)
    Core {
        #[serde(default = "default_num_workers")]
        num_workers: usize,
    },
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        ExecutorConfig::Tokio {
            max_concurrent: default_max_concurrent(),
        }
    }
}

impl ExecutorConfig {
    /// Build an executor from this configuration
    pub fn build(&self) -> Box<dyn ValidationExecutor> {
        match self {
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
