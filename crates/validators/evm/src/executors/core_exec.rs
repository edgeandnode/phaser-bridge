use crate::error::ValidationError;
use crate::executor::ValidationExecutor;
use crate::validation::{
    validate_receipts_rlp, validate_transactions_rlp, validate_transactions_root,
};
use alloy_primitives::{Bytes, B256};
use async_trait::async_trait;
use core_executor::ThreadPoolExecutor;
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;
use std::sync::Mutex;

/// core-executor based validation executor
///
/// Uses the core-executor library with NUMA-aware work delegation.
/// Automatically detects system topology and distributes validation work
/// across NUMA nodes for optimal memory locality and cache efficiency.
pub struct CoreExecutor {
    executor: Mutex<ThreadPoolExecutor>,
}

impl CoreExecutor {
    /// Create a new core-executor based validator
    ///
    /// Attempts to create a NUMA-aware executor that detects system topology.
    /// Falls back to a regular thread pool if NUMA detection fails.
    ///
    /// # Arguments
    /// * `num_workers` - Number of worker threads (used only for fallback)
    pub fn new(num_workers: usize) -> Self {
        let executor = match ThreadPoolExecutor::new_numa_aware() {
            Ok(exec) => {
                if let Some(topology) = exec.numa_topology() {
                    eprintln!(
                        "CoreExecutor: Using NUMA-aware executor with {} nodes, {} total cores",
                        topology.num_nodes(),
                        topology.total_cores
                    );
                }
                exec
            }
            Err(e) => {
                eprintln!(
                    "CoreExecutor: NUMA detection failed ({e}), using regular thread pool with {num_workers} workers"
                );
                ThreadPoolExecutor::new(num_workers)
            }
        };

        Self {
            executor: Mutex::new(executor),
        }
    }
}

#[async_trait]
impl ValidationExecutor for CoreExecutor {
    async fn spawn_validate_rlp(
        &self,
        expected_root: B256,
        transaction_rlps: Vec<Bytes>,
    ) -> Result<(), ValidationError> {
        // Lock briefly to spawn the task
        let future = {
            let mut executor = self.executor.lock().unwrap();
            executor.spawn_on_any(async move {
                let rlp_refs: Vec<_> = transaction_rlps.iter().map(|b| b.as_ref()).collect();
                validate_transactions_rlp(expected_root, &rlp_refs)
            })
        };
        // Lock is released here

        // Await the future without holding the lock
        future
            .await
            .map_err(|_| ValidationError::TaskJoinError("Task channel closed".to_string()))?
    }

    async fn spawn_validate_receipts_rlp(
        &self,
        expected_root: B256,
        receipt_rlps: Vec<Bytes>,
    ) -> Result<(), ValidationError> {
        // Lock briefly to spawn the task
        let future = {
            let mut executor = self.executor.lock().unwrap();
            executor.spawn_on_any(async move {
                let rlp_refs: Vec<_> = receipt_rlps.iter().map(|b| b.as_ref()).collect();
                validate_receipts_rlp(expected_root, &rlp_refs)
            })
        };
        // Lock is released here

        // Await the future without holding the lock
        future
            .await
            .map_err(|_| ValidationError::TaskJoinError("Task channel closed".to_string()))?
    }

    async fn spawn_validate_records(
        &self,
        block: BlockRecord,
        transactions: Vec<TransactionRecord>,
    ) -> Result<(), ValidationError> {
        // Lock briefly to spawn the task
        let future = {
            let mut executor = self.executor.lock().unwrap();
            executor.spawn_on_any(async move { validate_transactions_root(&block, &transactions) })
        };
        // Lock is released here

        // Await the future without holding the lock
        future
            .await
            .map_err(|_| ValidationError::TaskJoinError("Task channel closed".to_string()))?
    }

    async fn spawn_validate_batch(
        &self,
        blocks: Vec<(BlockRecord, Vec<TransactionRecord>)>,
    ) -> Vec<Result<(), ValidationError>> {
        use futures::future::FutureExt;
        use std::pin::Pin;

        // Spawn all tasks at once, distributing across NUMA nodes if available
        let futures: Vec<Pin<Box<dyn std::future::Future<Output = _> + Send>>> = {
            let mut executor = self.executor.lock().unwrap();
            let num_nodes = executor.numa_topology().map(|t| t.num_nodes()).unwrap_or(1);

            blocks
                .into_iter()
                .enumerate()
                .map(|(idx, (block, txs))| {
                    // Distribute work across NUMA nodes round-robin
                    if num_nodes > 1 {
                        let node_id = idx % num_nodes;
                        // Spawn on specific NUMA node, will return error if node invalid
                        // (shouldn't happen since we calculate node_id correctly)
                        executor
                            .spawn_on_numa_node(node_id, async move {
                                validate_transactions_root(&block, &txs)
                            })
                            .expect("NUMA node should be valid")
                            .boxed()
                    } else {
                        // No NUMA topology, distribute via round-robin spawn_on_any
                        executor
                            .spawn_on_any(async move { validate_transactions_root(&block, &txs) })
                            .boxed()
                    }
                })
                .collect()
        };
        // Lock is released here

        // Await all futures without holding the lock
        let mut results = Vec::with_capacity(futures.len());
        for future in futures {
            let result = future
                .await
                .map_err(|_| ValidationError::TaskJoinError("Task channel closed".to_string()))
                .and_then(|r| r);
            results.push(result);
        }
        results
    }
}
