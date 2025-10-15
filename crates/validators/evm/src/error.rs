use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum ValidationError {
    #[error("Transaction root mismatch for block {block_num}: expected {expected:?}, computed {computed:?}")]
    RootMismatch {
        block_num: u64,
        expected: [u8; 32],
        computed: [u8; 32],
    },

    #[error("Failed to convert transaction at index {index} in block {block_num}: {message}")]
    ConversionError {
        block_num: u64,
        index: usize,
        message: String,
    },

    #[error("Merkle tree error: {0}")]
    MerkleError(String),

    #[error("No transactions provided for validation")]
    NoTransactions,

    #[error("RLP encoding error: {0}")]
    RlpError(#[from] alloy_rlp::Error),

    #[error("Task join error: {0}")]
    TaskJoinError(String),
}

// Allow conversion from tokio join errors
impl From<tokio::task::JoinError> for ValidationError {
    fn from(err: tokio::task::JoinError) -> Self {
        ValidationError::TaskJoinError(err.to_string())
    }
}
