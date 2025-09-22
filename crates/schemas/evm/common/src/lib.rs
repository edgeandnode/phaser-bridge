/// EVM Common Schemas - Shared types and schemas for all EVM-compatible chains
///
/// This crate provides typed-arrow schemas for blocks, transactions, and logs
/// that are common across all EVM implementations (Ethereum, Arbitrum, Optimism, etc.)

use typed_arrow::schema::SchemaMeta;

pub mod types;
pub mod block;
pub mod transaction;
pub mod log;

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
