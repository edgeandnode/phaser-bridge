/// EVM Common Schemas - Shared types and schemas for all EVM-compatible chains
///
/// This crate provides typed-arrow schemas for blocks, transactions, and logs
/// that are common across all EVM implementations (Ethereum, Arbitrum, Optimism, etc.)
use typed_arrow::schema::SchemaMeta;

pub mod block;
pub mod error;
pub mod log;
pub mod proof;
pub mod rpc_conversions;
pub mod transaction;
pub mod trie;
pub mod types;

/// Version of the schema format
pub const SCHEMA_VERSION: &str = "1.0.0";

/// Get the Arrow schema for blocks
pub fn block_arrow_schema() -> arrow::datatypes::SchemaRef {
    crate::block::BlockRecord::schema()
}

/// Get the Arrow schema for transactions
pub fn transaction_arrow_schema() -> arrow::datatypes::SchemaRef {
    crate::transaction::TransactionRecord::schema()
}

/// Get the Arrow schema for logs
pub fn log_arrow_schema() -> arrow::datatypes::SchemaRef {
    crate::log::LogRecord::schema()
}

/// Get the Arrow schema for trie nodes
pub fn trie_arrow_schema() -> arrow::datatypes::SchemaRef {
    crate::trie::TrieNodeRecord::schema()
}

/// Get the Arrow schema for merkle proofs
pub fn proof_arrow_schema() -> arrow::datatypes::SchemaRef {
    crate::proof::MerkleProofRecord::schema()
}
