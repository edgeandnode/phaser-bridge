use alloy_consensus::TxEnvelope;
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;
use serde::Serialize;
use std::time::{Duration, Instant};
use tracing::info;

use crate::loader::DataLoader;

#[derive(Serialize)]
pub struct BenchmarkResults {
    pub stages: Vec<StageResult>,
}

#[derive(Serialize)]
pub struct StageResult {
    pub name: String,
    pub iterations: usize,
    #[serde(with = "duration_ms")]
    pub mean: Duration,
    #[serde(with = "duration_ms")]
    pub median: Duration,
    #[serde(with = "duration_ms")]
    pub std_dev: Duration,
    #[serde(with = "duration_ms")]
    pub min: Duration,
    #[serde(with = "duration_ms")]
    pub max: Duration,
}

pub struct BenchmarkRunner {
    iterations: usize,
}

impl BenchmarkRunner {
    pub fn new(iterations: usize) -> Self {
        Self { iterations }
    }

    pub async fn run_benchmarks(
        &self,
        loader: &DataLoader,
        segments: &[u64],
        tx_counts: &[usize],
    ) -> Result<BenchmarkResults, anyhow::Error> {
        let mut stages = Vec::new();

        // Load and prepare data
        info!("Loading segments for benchmarking...");
        let mut all_blocks = Vec::new();

        for seg_id in segments {
            let segment = loader.load_segment(*seg_id)?;
            for (block_num, block) in segment.blocks {
                if let Some(txs) = segment.transactions_by_block.get(&block_num) {
                    all_blocks.push((block, txs.clone()));
                }
            }
        }

        // Run benchmarks for each target tx count
        for &target_count in tx_counts {
            let blocks_to_bench: Vec<_> = all_blocks
                .iter()
                .filter(|(_, txs)| {
                    let len = txs.len();
                    len >= target_count.saturating_sub(50) && len <= target_count + 50
                })
                .take(5)
                .collect();

            if blocks_to_bench.is_empty() {
                info!("No blocks found with ~{} transactions", target_count);
                continue;
            }

            info!(
                "Benchmarking {} blocks with ~{} transactions",
                blocks_to_bench.len(),
                target_count
            );

            // Benchmark full pipeline
            stages.push(self.benchmark_full_pipeline(&blocks_to_bench, target_count)?);
        }

        Ok(BenchmarkResults { stages })
    }

    fn benchmark_full_pipeline(
        &self,
        blocks: &[&(BlockRecord, Vec<TransactionRecord>)],
        target_tx_count: usize,
    ) -> Result<StageResult, anyhow::Error> {
        let mut timings = Vec::with_capacity(self.iterations);

        for _ in 0..self.iterations {
            for (block, txs) in blocks {
                let start = Instant::now();

                // Full pipeline
                let _header = alloy_consensus::Header::from(block);
                let tx_envelopes: Vec<_> = txs
                    .iter()
                    .map(|tx| TxEnvelope::try_from(tx).unwrap())
                    .collect();
                let tx_rlps: Vec<_> = tx_envelopes.iter().map(alloy_rlp::encode).collect();

                use alloy_primitives::keccak256;
                use alloy_trie::{HashBuilder, Nibbles};

                let mut builder = HashBuilder::default();
                for (idx, tx_rlp) in tx_rlps.iter().enumerate() {
                    let key = alloy_rlp::encode(idx);
                    let key_hash = keccak256(&key);
                    builder.add_leaf(Nibbles::unpack(key_hash), tx_rlp);
                }
                let _root = builder.root();

                timings.push(start.elapsed());
            }
        }

        // Calculate statistics
        timings.sort();
        let mean = timings.iter().sum::<Duration>() / timings.len() as u32;
        let median = timings[timings.len() / 2];
        let min = *timings.first().unwrap();
        let max = *timings.last().unwrap();

        // Calculate std dev
        let variance: f64 = timings
            .iter()
            .map(|t| {
                let diff = t.as_secs_f64() - mean.as_secs_f64();
                diff * diff
            })
            .sum::<f64>()
            / timings.len() as f64;
        let std_dev = Duration::from_secs_f64(variance.sqrt());

        Ok(StageResult {
            name: format!("full_pipeline_{}tx", target_tx_count),
            iterations: timings.len(),
            mean,
            median,
            std_dev,
            min,
            max,
        })
    }
}

// Helper module for Duration serialization in milliseconds
mod duration_ms {
    use serde::Serializer;
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let ms = duration.as_secs_f64() * 1000.0;
        serializer.serialize_f64(ms)
    }
}
