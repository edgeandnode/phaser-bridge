use crate::types::{Address20, Hash32, Wei};
use alloy_consensus::Header;
use typed_arrow::Record;

/// EVM Block schema - common across all EVM chains
#[derive(Record, Clone, Debug)]
pub struct BlockRecord {
    /// Partition key for efficient storage
    pub _block_num: u64,

    /// Block number
    pub block_num: u64,

    /// Unix timestamp in nanoseconds
    pub timestamp: i64,

    /// Block hash
    pub hash: Hash32,

    /// Parent block hash
    pub parent_hash: Hash32,

    /// Ommers/uncles hash
    pub ommers_hash: Hash32,

    /// Block miner/beneficiary address
    pub miner: Address20,

    /// State root hash
    pub state_root: Hash32,

    /// Transactions root hash
    pub transactions_root: Hash32,

    /// Receipts root hash
    pub receipts_root: Hash32,

    /// Logs bloom filter (256 bytes)
    pub logs_bloom: Vec<u8>,

    /// Block difficulty (legacy, pre-merge)
    pub difficulty: Wei,

    /// Gas limit for this block
    pub gas_limit: u64,

    /// Total gas used in this block
    pub gas_used: u64,

    /// Extra data field
    pub extra_data: Vec<u8>,

    /// Mix hash (proof of work)
    pub mix_hash: Hash32,

    /// Nonce (proof of work)
    pub nonce: u64,

    /// Base fee per gas (EIP-1559)
    pub base_fee_per_gas: Option<Wei>,

    /// Withdrawals root (post-merge)
    pub withdrawals_root: Option<Hash32>,

    /// Blob gas used (EIP-4844)
    pub blob_gas_used: Option<u64>,

    /// Excess blob gas (EIP-4844)
    pub excess_blob_gas: Option<u64>,

    /// Parent beacon block root (post-merge)
    pub parent_beacon_root: Option<Hash32>,
}

impl From<&Header> for BlockRecord {
    fn from(header: &Header) -> Self {
        let block_num = header.number;
        let hash = header.hash_slow();

        BlockRecord {
            _block_num: block_num,
            block_num,
            timestamp: header.timestamp as i64 * 1_000_000_000,
            hash: hash.into(),
            parent_hash: header.parent_hash.into(),
            ommers_hash: header.ommers_hash.into(),
            miner: header.beneficiary.into(),
            state_root: header.state_root.into(),
            transactions_root: header.transactions_root.into(),
            receipts_root: header.receipts_root.into(),
            logs_bloom: header.logs_bloom.0.to_vec(),
            difficulty: header.difficulty.into(),
            gas_limit: header.gas_limit,
            gas_used: header.gas_used,
            extra_data: header.extra_data.to_vec(),
            mix_hash: header.mix_hash.into(),
            nonce: u64::from_be_bytes(header.nonce.0),
            base_fee_per_gas: header.base_fee_per_gas.map(Into::into),
            withdrawals_root: header.withdrawals_root.map(Into::into),
            blob_gas_used: header.blob_gas_used,
            excess_blob_gas: header.excess_blob_gas,
            parent_beacon_root: header.parent_beacon_block_root.map(Into::into),
        }
    }
}

