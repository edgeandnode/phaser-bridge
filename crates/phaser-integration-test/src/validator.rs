use alloy_consensus::TxEnvelope;
use alloy_primitives::keccak256;
use alloy_trie::{HashBuilder, Nibbles};
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;
use serde::Serialize;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::loader::DataLoader;

#[derive(Serialize)]
pub struct ValidationResults {
    pub total_blocks: usize,
    pub validated_blocks: usize,
    pub failed_blocks: usize,
    pub failures: Vec<ValidationFailure>,
    #[serde(with = "humantime_serde")]
    pub total_time: Duration,
}

#[derive(Serialize)]
pub struct ValidationFailure {
    pub block_num: u64,
    pub error: String,
}

impl ValidationResults {
    pub fn success_rate(&self) -> f64 {
        if self.total_blocks == 0 {
            return 0.0;
        }
        (self.validated_blocks as f64 / self.total_blocks as f64) * 100.0
    }

    pub fn avg_time_per_block(&self) -> Duration {
        if self.validated_blocks == 0 {
            return Duration::ZERO;
        }
        self.total_time / self.validated_blocks as u32
    }
}

pub struct MerkleValidator;

impl MerkleValidator {
    pub fn new() -> Self {
        Self
    }

    pub async fn validate_segments(
        &self,
        loader: &DataLoader,
        segments: &[u64],
        fail_fast: bool,
    ) -> Result<ValidationResults, anyhow::Error> {
        let start = Instant::now();
        let mut total_blocks = 0;
        let mut validated_blocks = 0;
        let mut failures = Vec::new();

        for segment_id in segments {
            info!("Validating segment {}...", segment_id);

            let segment = loader.load_segment(*segment_id)?;

            for (block_num, block) in &segment.blocks {
                total_blocks += 1;

                if let Some(txs) = segment.transactions_by_block.get(block_num) {
                    match self.validate_block(block, txs) {
                        Ok(()) => {
                            validated_blocks += 1;
                        }
                        Err(e) => {
                            warn!("Block {} validation failed: {}", block_num, e);
                            failures.push(ValidationFailure {
                                block_num: *block_num,
                                error: e.to_string(),
                            });

                            if fail_fast {
                                return Err(e);
                            }
                        }
                    }
                }
            }

            info!(
                "Segment {} complete: {}/{} blocks validated",
                segment_id, validated_blocks, total_blocks
            );
        }

        Ok(ValidationResults {
            total_blocks,
            validated_blocks,
            failed_blocks: failures.len(),
            failures,
            total_time: start.elapsed(),
        })
    }

    fn validate_block(
        &self,
        block: &BlockRecord,
        txs: &[TransactionRecord],
    ) -> Result<(), anyhow::Error> {
        // Convert to Alloy types
        let tx_envelopes: Vec<_> = txs
            .iter()
            .map(|tx| TxEnvelope::try_from(tx))
            .collect::<Result<Vec<_>, _>>()?;

        // RLP encode
        let tx_rlps: Vec<_> = tx_envelopes
            .iter()
            .map(|tx| alloy_rlp::encode(tx))
            .collect();

        // Build merkle tree
        let mut builder = HashBuilder::default();
        for (idx, tx_rlp) in tx_rlps.iter().enumerate() {
            let key = alloy_rlp::encode(idx);
            let key_hash = keccak256(&key);
            builder.add_leaf(Nibbles::unpack(&key_hash), tx_rlp);
        }

        let computed_root = builder.root();

        // Validate
        if computed_root.as_slice() != block.transactions_root.bytes {
            anyhow::bail!(
                "Merkle root mismatch: expected {:?}, computed {:?}",
                block.transactions_root.bytes,
                computed_root
            );
        }

        Ok(())
    }
}

// Helper module for Duration serialization
mod humantime_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{:?}", duration))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Simple parsing - just for completeness
        Ok(Duration::from_secs(0))
    }
}
