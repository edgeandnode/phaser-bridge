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

/// Reverse conversion: BlockRecord → Header
impl From<&BlockRecord> for Header {
    fn from(record: &BlockRecord) -> Self {
        use alloy_primitives::{Address, Bloom, Bytes, FixedBytes, B64, U256};

        Header {
            parent_hash: FixedBytes::from(record.parent_hash.bytes),
            ommers_hash: FixedBytes::from(record.ommers_hash.bytes),
            beneficiary: Address::from(record.miner.bytes),
            state_root: FixedBytes::from(record.state_root.bytes),
            transactions_root: FixedBytes::from(record.transactions_root.bytes),
            receipts_root: FixedBytes::from(record.receipts_root.bytes),
            logs_bloom: Bloom::from_slice(&record.logs_bloom),
            difficulty: U256::from_le_bytes(record.difficulty.bytes),
            number: record.block_num,
            gas_limit: record.gas_limit,
            gas_used: record.gas_used,
            timestamp: (record.timestamp / 1_000_000_000) as u64,
            extra_data: Bytes::copy_from_slice(&record.extra_data),
            mix_hash: FixedBytes::from(record.mix_hash.bytes),
            nonce: B64::from(record.nonce.to_be_bytes()),
            base_fee_per_gas: record
                .base_fee_per_gas
                .as_ref()
                .map(|w| U256::from_le_bytes(w.bytes).to::<u64>()),
            withdrawals_root: record
                .withdrawals_root
                .as_ref()
                .map(|h| FixedBytes::from(h.bytes)),
            blob_gas_used: record.blob_gas_used,
            excess_blob_gas: record.excess_blob_gas,
            parent_beacon_block_root: record
                .parent_beacon_root
                .as_ref()
                .map(|h| FixedBytes::from(h.bytes)),
            requests_hash: None,
        }
    }
}

impl From<BlockRecord> for Header {
    fn from(record: BlockRecord) -> Self {
        Self::from(&record)
    }
}
