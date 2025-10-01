use crate::types::{Address20, Hash32, Wei};
use alloy_consensus::{TxEip4844Variant, TxEnvelope};
use typed_arrow::Record;

/// EVM Transaction schema - common across all EVM chains
#[derive(Record, Clone, Debug)]
pub struct TransactionRecord {
    /// Partition key for efficient storage
    pub _block_num: u64,

    /// Block hash this transaction is in
    pub block_hash: Hash32,

    /// Block number
    pub block_num: u64,

    /// Block timestamp in nanoseconds
    pub timestamp: i64,

    /// Transaction index within the block
    pub tx_index: u32,

    /// Transaction hash
    pub tx_hash: Hash32,

    /// Sender address (recovered from signature)
    pub from: Address20,

    /// Recipient address (None for contract creation)
    pub to: Option<Address20>,

    /// Transaction nonce
    pub nonce: u64,

    /// Gas price (legacy transactions)
    pub gas_price: Option<Wei>,

    /// Gas limit
    pub gas_limit: u64,

    /// Gas actually used (from receipt)
    pub gas_used: u64,

    /// Value transferred in Wei
    pub value: Wei,

    /// Input data
    pub input: Vec<u8>,

    /// Signature v value
    pub v: Vec<u8>,

    /// Signature r value
    pub r: Vec<u8>,

    /// Signature s value
    pub s: Vec<u8>,

    /// Transaction type (0=Legacy, 1=EIP-2930, 2=EIP-1559, 3=EIP-4844)
    pub tx_type: i32,

    /// Transaction status (from receipt)
    pub status: bool,

    /// Max fee per gas (EIP-1559)
    pub max_fee_per_gas: Option<Wei>,

    /// Max priority fee per gas (EIP-1559)
    pub max_priority_fee_per_gas: Option<Wei>,

    /// Max fee per blob gas (EIP-4844)
    pub max_fee_per_blob_gas: Option<Wei>,
}

/// Context needed to convert a transaction to a record
pub struct TransactionContext<'a> {
    pub tx: &'a TxEnvelope,
    pub block_hash: Hash32,
    pub block_num: u64,
    pub timestamp: i64, // nanos
    pub tx_index: u32,
    pub from: Address20,
    pub gas_used: u64,
    pub status: bool,
}

impl<'a> From<TransactionContext<'a>> for TransactionRecord {
    fn from(ctx: TransactionContext<'a>) -> Self {
        let tx_hash = *ctx.tx.tx_hash();

        // Extract common fields based on transaction type
        let (
            to,
            nonce,
            value,
            input,
            gas_limit,
            gas_price,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            max_fee_per_blob_gas,
            tx_type,
        ) = match ctx.tx {
            TxEnvelope::Legacy(tx) => {
                let inner = tx.tx();
                (
                    inner.to.to().map(|a| (*a).into()),
                    inner.nonce,
                    inner.value.into(),
                    inner.input.to_vec(),
                    inner.gas_limit,
                    Some(inner.gas_price.into()),
                    None,
                    None,
                    None,
                    0,
                )
            }
            TxEnvelope::Eip2930(tx) => {
                let inner = tx.tx();
                (
                    inner.to.to().map(|a| (*a).into()),
                    inner.nonce,
                    inner.value.into(),
                    inner.input.to_vec(),
                    inner.gas_limit,
                    Some(inner.gas_price.into()),
                    None,
                    None,
                    None,
                    1,
                )
            }
            TxEnvelope::Eip1559(tx) => {
                let inner = tx.tx();
                (
                    inner.to.to().map(|a| (*a).into()),
                    inner.nonce,
                    inner.value.into(),
                    inner.input.to_vec(),
                    inner.gas_limit,
                    None, // No gas_price for EIP-1559
                    Some(inner.max_fee_per_gas.into()),
                    Some(inner.max_priority_fee_per_gas.into()),
                    None,
                    2,
                )
            }
            TxEnvelope::Eip4844(tx) => {
                // Extract the inner TxEip4844 from the variant
                let tx_inner = match tx.tx() {
                    TxEip4844Variant::TxEip4844(inner) => inner,
                    TxEip4844Variant::TxEip4844WithSidecar(with_sidecar) => &with_sidecar.tx,
                };
                (
                    Some(tx_inner.to.into()),
                    tx_inner.nonce,
                    tx_inner.value.into(),
                    tx_inner.input.to_vec(),
                    tx_inner.gas_limit,
                    None,
                    Some(tx_inner.max_fee_per_gas.into()),
                    Some(tx_inner.max_priority_fee_per_gas.into()),
                    Some(tx_inner.max_fee_per_blob_gas.into()),
                    3,
                )
            }
            TxEnvelope::Eip7702(tx) => {
                let inner = tx.tx();
                (
                    Some(Address20::from(inner.to)),
                    inner.nonce,
                    inner.value.into(),
                    inner.input.to_vec(),
                    inner.gas_limit,
                    None,
                    Some(inner.max_fee_per_gas.into()),
                    Some(inner.max_priority_fee_per_gas.into()),
                    None,
                    4,
                )
            }
        };

        // Extract signature components
        let (v, r, s) = match ctx.tx {
            TxEnvelope::Legacy(tx) => {
                let sig = tx.signature();
                (
                    vec![if sig.v() { 1u8 } else { 0u8 }],
                    sig.r().to_be_bytes::<32>().to_vec(),
                    sig.s().to_be_bytes::<32>().to_vec(),
                )
            }
            TxEnvelope::Eip2930(tx) => {
                let sig = tx.signature();
                (
                    vec![if sig.v() { 1u8 } else { 0u8 }],
                    sig.r().to_be_bytes::<32>().to_vec(),
                    sig.s().to_be_bytes::<32>().to_vec(),
                )
            }
            TxEnvelope::Eip1559(tx) => {
                let sig = tx.signature();
                (
                    vec![if sig.v() { 1u8 } else { 0u8 }],
                    sig.r().to_be_bytes::<32>().to_vec(),
                    sig.s().to_be_bytes::<32>().to_vec(),
                )
            }
            TxEnvelope::Eip4844(tx) => {
                let sig = tx.signature();
                (
                    vec![if sig.v() { 1u8 } else { 0u8 }],
                    sig.r().to_be_bytes::<32>().to_vec(),
                    sig.s().to_be_bytes::<32>().to_vec(),
                )
            }
            TxEnvelope::Eip7702(tx) => {
                let sig = tx.signature();
                (
                    vec![if sig.v() { 1u8 } else { 0u8 }],
                    sig.r().to_be_bytes::<32>().to_vec(),
                    sig.s().to_be_bytes::<32>().to_vec(),
                )
            }
        };

        TransactionRecord {
            _block_num: ctx.block_num,
            block_hash: ctx.block_hash,
            block_num: ctx.block_num,
            timestamp: ctx.timestamp,
            tx_index: ctx.tx_index,
            tx_hash: tx_hash.into(),
            from: ctx.from,
            to,
            nonce,
            gas_price,
            gas_limit,
            gas_used: ctx.gas_used,
            value,
            input,
            v,
            r,
            s,
            tx_type,
            status: ctx.status,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            max_fee_per_blob_gas,
        }
    }
}

/// Trait for recovering sender address from transaction signatures
pub trait RecoverSender {
    /// Recover the sender address from the transaction signature
    fn recover_sender(&self) -> Option<Address20>;
}

impl RecoverSender for TxEnvelope {
    fn recover_sender(&self) -> Option<Address20> {
        use alloy_consensus::SignableTransaction;

        let sig_hash = match self {
            TxEnvelope::Legacy(tx) => tx.tx().signature_hash(),
            TxEnvelope::Eip2930(tx) => tx.tx().signature_hash(),
            TxEnvelope::Eip1559(tx) => tx.tx().signature_hash(),
            TxEnvelope::Eip4844(tx) => match tx.tx() {
                alloy_consensus::TxEip4844Variant::TxEip4844(inner) => inner.signature_hash(),
                alloy_consensus::TxEip4844Variant::TxEip4844WithSidecar(with_sidecar) => {
                    with_sidecar.tx.signature_hash()
                }
            },
            TxEnvelope::Eip7702(tx) => tx.tx().signature_hash(),
        };

        let signature = match self {
            TxEnvelope::Legacy(tx) => tx.signature(),
            TxEnvelope::Eip2930(tx) => tx.signature(),
            TxEnvelope::Eip1559(tx) => tx.signature(),
            TxEnvelope::Eip4844(tx) => tx.signature(),
            TxEnvelope::Eip7702(tx) => tx.signature(),
        };

        signature
            .recover_address_from_prehash(&sig_hash)
            .ok()
            .map(|addr| addr.into())
    }
}
