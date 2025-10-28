/// Converter for trie data from protobuf to Arrow format
use crate::error::ErigonBridgeError;
use crate::proto::custom::CommitmentNodeBatch;
use arrow_array::RecordBatch;
use evm_common::trie::TrieNodeRecord;
use evm_common::types::Hash32;
use typed_arrow::prelude::*;

/// Convert a batch of commitment nodes from protobuf to Arrow RecordBatch
///
/// This is a simple, efficient conversion that just maps the minimal fields:
/// - hash -> Hash32 (for RocksDB key)
/// - raw_value -> Vec<u8> (for RocksDB value)
/// - step -> u64 (for versioning)
pub fn convert_trie_batch(batch: CommitmentNodeBatch) -> Result<RecordBatch, ErigonBridgeError> {
    // Create builders for the Arrow arrays
    let mut builders = TrieNodeRecord::new_builders(batch.nodes.len());

    for node in batch.nodes {
        // Node now has both key and hash fields
        // key: variable-length trie path from CommitmentDomain
        // hash: 32-byte keccak256 hash of the RLP value

        if node.hash.len() != 32 {
            tracing::error!(
                "Invalid hash length: expected 32, got {} (key={:?}, hash={:?}, rlp_size={}, step={})",
                node.hash.len(),
                hex::encode(&node.key),
                hex::encode(&node.hash),
                node.raw_value.len(),
                node.step
            );
            return Err(ErigonBridgeError::InvalidData(format!(
                "Invalid hash length: expected 32, got {}",
                node.hash.len()
            )));
        }

        // Convert to typed-arrow record using the hash (not the key)
        let hash = Hash32 {
            bytes: node.hash.as_slice().try_into().map_err(|_| {
                ErigonBridgeError::InvalidData("Failed to convert hash to [u8; 32]".into())
            })?,
        };

        let record = TrieNodeRecord::new(hash, node.raw_value, node.step);
        builders.append_row(record);
    }

    // Build the RecordBatch
    let arrays = builders.finish();
    Ok(arrays.into_record_batch())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::custom::CommitmentNode;

    #[test]
    fn test_convert_empty_batch() {
        let batch = CommitmentNodeBatch {
            nodes: vec![],
            total_sent: 0,
            estimated_total: 0,
            next_marker: vec![],
        };

        let result = convert_trie_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_convert_single_node_batch() {
        let node = CommitmentNode {
            key: vec![], // Empty key for test
            hash: vec![0u8; 32],
            raw_value: vec![0x80], // Empty RLP
            step: 42,
        };

        let batch = CommitmentNodeBatch {
            nodes: vec![node],
            total_sent: 1,
            estimated_total: 1,
            next_marker: vec![],
        };

        let result = convert_trie_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn test_invalid_hash_length() {
        let node = CommitmentNode {
            key: vec![],         // Empty key for test
            hash: vec![0u8; 31], // Invalid: not 32 bytes
            raw_value: vec![0x80],
            step: 42,
        };

        let batch = CommitmentNodeBatch {
            nodes: vec![node],
            total_sent: 1,
            estimated_total: 1,
            next_marker: vec![],
        };

        let result = convert_trie_batch(batch);
        assert!(result.is_err());
    }
}
