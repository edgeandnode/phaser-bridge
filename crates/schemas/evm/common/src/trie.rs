/// Minimal trie node schema for transport and storage
use crate::types::Hash32;
use typed_arrow::Record;

/// Trie node record - minimal structure for RocksDB storage
///
/// This schema is designed for direct storage without transformation:
/// - hash becomes the RocksDB key
/// - raw_rlp becomes the RocksDB value
/// - step is used for versioning/consistency
#[derive(Record, Clone, Debug)]
pub struct TrieNodeRecord {
    /// Node hash (32 bytes) - keccak256(raw_rlp)
    /// This becomes the RocksDB key for O(1) lookups
    pub hash: Hash32,

    /// Raw RLP-encoded node data
    /// Contains the complete node information - can be:
    /// - Branch: [child0, child1, ..., child15, value]
    /// - Extension: [encoded_path, next_node]
    /// - Leaf: [encoded_path, value]
    /// - Account: [nonce, balance, storage_root, code_hash]
    pub raw_rlp: Vec<u8>,

    /// Aggregator step number (Erigon-specific versioning)
    /// Allows tracking which aggregation round this node is from
    pub step: u64,
}

impl TrieNodeRecord {
    /// Create a new trie node record
    pub fn new(hash: Hash32, raw_rlp: Vec<u8>, step: u64) -> Self {
        Self {
            hash,
            raw_rlp,
            step,
        }
    }

    /// Get the size of the raw RLP data
    pub fn rlp_size(&self) -> usize {
        self.raw_rlp.len()
    }
}
