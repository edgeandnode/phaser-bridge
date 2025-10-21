//! EVM transaction validation library
//!
//! Provides validation functions and pluggable executors for verifying
//! Ethereum transaction merkle roots.
//!
//! # Example
//!
//! ```no_run
//! use validators_evm::{ExecutorConfig, ValidationExecutor};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure a Rayon-based executor
//! let config = ExecutorConfig::Rayon { num_threads: None };
//! let executor = config.build();
//!
//! // Validate a block's transactions
//! // executor.validate_block(block, transactions).await?;
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod executor;
pub mod executors;
pub mod validation;

// Re-export main types for convenience
pub use error::ValidationError;
pub use executor::{ExecutorConfig, ExecutorType, ValidationExecutor};
pub use executors::{CoreExecutor, TokioExecutor};
pub use validation::{
    compute_receipts_root_from_rlp, compute_transactions_root, compute_transactions_root_from_rlp,
    validate_receipts_rlp, validate_transactions_rlp, validate_transactions_root,
};
