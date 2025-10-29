use crate::types::Hash32;
use typed_arrow::{List, Record};

/// Type alias for transaction proof generation result
/// Tuple of (root_hash, proof_nodes, rlp_value)
pub type TransactionProofResult = (Hash32, Vec<Vec<u8>>, Vec<u8>);

/// Merkle proof record for transaction or receipt inclusion
#[derive(Record, Clone, Debug)]
pub struct MerkleProofRecord {
    /// Block number this proof belongs to
    pub block_num: u64,

    /// Index of the transaction/receipt in the block
    pub tx_index: u64,

    /// Merkle root hash (transactions_root or receipts_root)
    pub root: Hash32,

    /// RLP-encoded proof nodes forming the path from root to leaf
    pub proof_nodes: List<Vec<u8>>,

    /// The RLP-encoded value being proved (transaction or receipt)
    pub value: Vec<u8>,

    /// Proof type: 0 = transaction, 1 = receipt
    pub proof_type: u8,
}

impl MerkleProofRecord {
    /// Create a new transaction proof
    pub fn new_transaction_proof(
        block_num: u64,
        index: u64,
        root: Hash32,
        proof_nodes: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Self {
        Self {
            block_num,
            tx_index: index,
            root,
            proof_nodes: List::new(proof_nodes),
            value,
            proof_type: 0,
        }
    }

    /// Create a new receipt proof
    pub fn new_receipt_proof(
        block_num: u64,
        index: u64,
        root: Hash32,
        proof_nodes: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Self {
        Self {
            block_num,
            tx_index: index,
            root,
            proof_nodes: List::new(proof_nodes),
            value,
            proof_type: 1,
        }
    }

    /// Check if this is a transaction proof
    pub fn is_transaction_proof(&self) -> bool {
        self.proof_type == 0
    }

    /// Check if this is a receipt proof
    pub fn is_receipt_proof(&self) -> bool {
        self.proof_type == 1
    }
}

/// Generate a merkle proof for a transaction at a specific index
///
/// # Arguments
/// * `transactions` - RLP-encoded transactions in block order
/// * `index` - Index of the transaction to prove
///
/// # Returns
/// Tuple of (root_hash, proof_nodes, rlp_value)
pub fn generate_transaction_proof(
    transactions: &[Vec<u8>],
    index: usize,
) -> Result<TransactionProofResult, anyhow::Error> {
    use alloy_primitives::keccak256;
    use alloy_trie::{HashBuilder, Nibbles};

    if index >= transactions.len() {
        anyhow::bail!(
            "Index {} out of bounds for {} transactions",
            index,
            transactions.len()
        );
    }

    // Compute the nibbles for the target index
    let target_key = alloy_rlp::encode(index);
    let target_key_hash = keccak256(&target_key);
    let target_nibbles = Nibbles::unpack(target_key_hash);

    // Create proof retainer for this specific index
    let proof_retainer = alloy_trie::proof::ProofRetainer::from_iter([target_nibbles]);

    // Build the merkle trie with proof retention
    let mut builder = HashBuilder::default().with_proof_retainer(proof_retainer);

    for (idx, tx_rlp) in transactions.iter().enumerate() {
        let key = alloy_rlp::encode(idx);
        let key_hash = keccak256(&key);
        builder.add_leaf(Nibbles::unpack(key_hash), tx_rlp);
    }

    // Get the root
    let root = builder.root();

    // Extract the proof nodes
    let proof_nodes_map = builder.take_proof_nodes();
    let proof_nodes: Vec<Vec<u8>> = proof_nodes_map
        .into_nodes_sorted()
        .into_iter()
        .map(|(_path, bytes)| bytes.to_vec())
        .collect();

    Ok((
        Hash32 { bytes: root.0 },
        proof_nodes,
        transactions[index].clone(),
    ))
}

/// Verify a merkle proof
///
/// # Arguments
/// * `proof` - The merkle proof record to verify
///
/// # Returns
/// Ok(()) if proof is valid, Err otherwise
pub fn verify_proof(proof: &MerkleProofRecord) -> Result<(), anyhow::Error> {
    use alloy_primitives::{keccak256, Bytes, B256};
    use alloy_trie::Nibbles;

    // Encode the index as the key
    let key = alloy_rlp::encode(proof.tx_index);
    let key_hash = keccak256(&key);
    let nibbles = Nibbles::unpack(key_hash);

    // Convert proof nodes to Bytes
    let proof_nodes: Vec<Bytes> = proof
        .proof_nodes
        .values()
        .iter()
        .map(|node| Bytes::copy_from_slice(node))
        .collect();

    // Convert root to B256
    let root = B256::from(proof.root.bytes);

    // Verify the proof
    alloy_trie::proof::verify_proof(root, nibbles, Some(proof.value.clone()), proof_nodes.iter())?;

    Ok(())
}
