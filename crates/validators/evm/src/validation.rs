use crate::error::ValidationError;
use alloy_primitives::keccak256;
use alloy_trie::{HashBuilder, Nibbles};
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;
use evm_common::types::Hash32;

/// Validate that transactions match the block's transactions_root
///
/// # Arguments
/// * `block` - The block record containing the expected transactions_root
/// * `transactions` - Slice of transaction records to validate
///
/// # Returns
/// Ok(()) if validation succeeds, Err(ValidationError) otherwise
pub fn validate_transactions_root(
    block: &BlockRecord,
    transactions: &[TransactionRecord],
) -> Result<(), ValidationError> {
    let computed_root = compute_transactions_root(transactions)?;

    if computed_root.bytes != block.transactions_root.bytes {
        return Err(ValidationError::RootMismatch {
            block_num: block.block_num,
            expected: block.transactions_root.bytes,
            computed: computed_root.bytes,
        });
    }

    Ok(())
}

/// Compute the merkle root for a list of transactions
///
/// # Arguments
/// * `transactions` - Slice of transaction records
///
/// # Returns
/// The computed merkle root hash
pub fn compute_transactions_root(
    transactions: &[TransactionRecord],
) -> Result<Hash32, ValidationError> {
    if transactions.is_empty() {
        return Err(ValidationError::NoTransactions);
    }

    // Convert TransactionRecords to RLP-encoded transactions
    let tx_rlps: Vec<Vec<u8>> = transactions
        .iter()
        .enumerate()
        .map(|(idx, tx)| {
            alloy_consensus::TxEnvelope::try_from(tx)
                .map(|envelope| alloy_rlp::encode(&envelope))
                .map_err(|e| ValidationError::ConversionError {
                    block_num: tx.block_num,
                    index: idx,
                    message: e.to_string(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Build the merkle patricia trie
    let mut builder = HashBuilder::default();

    for (idx, tx_rlp) in tx_rlps.iter().enumerate() {
        let key = alloy_rlp::encode(idx);
        let key_hash = keccak256(&key);
        builder.add_leaf(Nibbles::unpack(key_hash), tx_rlp);
    }

    let root = builder.root();

    Ok(Hash32 { bytes: root.0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_transactions_fails() {
        let result = compute_transactions_root(&[]);
        assert!(matches!(result, Err(ValidationError::NoTransactions)));
    }
}
