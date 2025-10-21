use crate::error::ValidationError;
use alloy_primitives::B256;
use alloy_trie::{root::ordered_trie_root_with_encoder, HashBuilder};
use evm_common::block::BlockRecord;
use evm_common::transaction::TransactionRecord;
use evm_common::types::Hash32;

/// Compute the empty merkle patricia trie root
/// This is the standard Ethereum empty trie root: keccak256(rlp([]))
fn empty_root() -> B256 {
    HashBuilder::default().root()
}

/// Validate transactions directly from RLP bytes (canonical validation)
///
/// This is the ground truth validation - RLP is the canonical representation.
/// Use this when you have RLP data from the node/network.
///
/// # Arguments
/// * `expected_root` - The transactions_root from the block header
/// * `transaction_rlps` - RLP-encoded transactions
///
/// # Returns
/// Ok(()) if validation succeeds, Err(ValidationError) otherwise
pub fn validate_transactions_rlp(
    expected_root: B256,
    transaction_rlps: &[impl AsRef<[u8]>],
) -> Result<(), ValidationError> {
    // Handle empty transaction list - should match empty trie root
    if transaction_rlps.is_empty() {
        let empty = empty_root();
        if expected_root != empty {
            return Err(ValidationError::RootMismatch {
                block_num: 0,
                expected: format!("0x{}", hex::encode(expected_root.0)),
                computed: format!("0x{}", hex::encode(empty.0)),
            });
        }
        return Ok(());
    }

    let computed_root = compute_transactions_root_from_rlp(transaction_rlps);

    if computed_root != expected_root {
        return Err(ValidationError::RootMismatch {
            block_num: 0, // Don't have block_num in this context
            expected: format!("0x{}", hex::encode(expected_root.0)),
            computed: format!("0x{}", hex::encode(computed_root.0)),
        });
    }

    Ok(())
}

/// Compute merkle root directly from RLP-encoded transactions (canonical)
///
/// # Arguments
/// * `transaction_rlps` - RLP-encoded transactions
///
/// # Returns
/// The computed merkle root hash
pub fn compute_transactions_root_from_rlp(transaction_rlps: &[impl AsRef<[u8]>]) -> B256 {
    // Use alloy's ordered_trie_root_with_encoder which handles the correct key encoding
    // (RLP-encoded index without hashing, with adjust_index_for_rlp, using encode_fixed_size)
    ordered_trie_root_with_encoder(transaction_rlps, |rlp, buf| {
        buf.extend_from_slice(rlp.as_ref());
    })
}

/// Validate that transactions match the block's transactions_root
///
/// This validates both the node data AND our conversion logic.
/// It converts TransactionRecord → TxEnvelope → RLP, then validates.
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
            expected: format!("0x{}", hex::encode(block.transactions_root.bytes)),
            computed: format!("0x{}", hex::encode(computed_root.bytes)),
        });
    }

    Ok(())
}

/// Compute the merkle root for a list of transactions (validates conversion)
///
/// This converts TransactionRecord → TxEnvelope → RLP, so it validates
/// our conversion logic as well as computing the root.
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
    // This tests our conversion logic
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

    let root = compute_transactions_root_from_rlp(&tx_rlps);

    Ok(Hash32 { bytes: root.0 })
}

/// Validate receipts directly from RLP bytes (canonical validation)
///
/// This is the ground truth validation - RLP is the canonical representation.
/// Use this when you have RLP data from the node/network.
///
/// # Arguments
/// * `expected_root` - The receipts_root from the block header
/// * `receipt_rlps` - RLP-encoded receipts
///
/// # Returns
/// Ok(()) if validation succeeds, Err(ValidationError) otherwise
pub fn validate_receipts_rlp(
    expected_root: B256,
    receipt_rlps: &[impl AsRef<[u8]>],
) -> Result<(), ValidationError> {
    // Handle empty receipt list - should match empty trie root
    if receipt_rlps.is_empty() {
        let empty = empty_root();
        if expected_root != empty {
            return Err(ValidationError::RootMismatch {
                block_num: 0,
                expected: format!("0x{}", hex::encode(expected_root.0)),
                computed: format!("0x{}", hex::encode(empty.0)),
            });
        }
        return Ok(());
    }

    let computed_root = compute_receipts_root_from_rlp(receipt_rlps);

    if computed_root != expected_root {
        return Err(ValidationError::RootMismatch {
            block_num: 0, // Don't have block_num in this context
            expected: format!("0x{}", hex::encode(expected_root.0)),
            computed: format!("0x{}", hex::encode(computed_root.0)),
        });
    }

    Ok(())
}

/// Compute merkle root directly from RLP-encoded receipts (canonical)
///
/// # Arguments
/// * `receipt_rlps` - RLP-encoded receipts
///
/// # Returns
/// The computed merkle root hash
pub fn compute_receipts_root_from_rlp(receipt_rlps: &[impl AsRef<[u8]>]) -> B256 {
    // Use alloy's ordered_trie_root_with_encoder which handles the correct key encoding
    // (RLP-encoded index without hashing, with adjust_index_for_rlp, using encode_fixed_size)
    ordered_trie_root_with_encoder(receipt_rlps, |rlp, buf| {
        buf.extend_from_slice(rlp.as_ref());
    })
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
