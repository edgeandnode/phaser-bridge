use crate::types::{Address20, Hash32, Wei};
use alloy_consensus::{TxEip4844Variant, TxEnvelope};
use typed_arrow::{List, Record};

/// Access list item for EIP-2930+
#[derive(Record, Clone, Debug)]
pub struct AccessListItemRecord {
    pub address: Address20,
    pub storage_keys: List<Hash32>,
}

/// Authorization item for EIP-7702
#[derive(Record, Clone, Debug)]
pub struct AuthorizationRecord {
    pub chain_id: u64,
    pub address: Address20,
    pub nonce: u64,
    pub y_parity: bool,
    pub r: Hash32,
    pub s: Hash32,
}

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

    /// Chain ID
    pub chain_id: Option<u64>,

    /// Access list (EIP-2930+)
    pub access_list: Option<List<AccessListItemRecord>>,

    /// Blob versioned hashes (EIP-4844)
    pub blob_versioned_hashes: Option<List<Hash32>>,

    /// Authorization list (EIP-7702)
    pub authorization_list: Option<List<AuthorizationRecord>>,
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
            chain_id,
            access_list,
            blob_versioned_hashes,
            authorization_list,
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
                    inner.chain_id,
                    None,
                    None,
                    None,
                    0,
                )
            }
            TxEnvelope::Eip2930(tx) => {
                let inner = tx.tx();
                let access_list = Some(List::new(
                    inner
                        .access_list
                        .0
                        .iter()
                        .map(|item| AccessListItemRecord {
                            address: (*item.address).into(),
                            storage_keys: List::new(
                                item.storage_keys.iter().map(|k| (*k).into()).collect(),
                            ),
                        })
                        .collect(),
                ));
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
                    Some(inner.chain_id),
                    access_list,
                    None,
                    None,
                    1,
                )
            }
            TxEnvelope::Eip1559(tx) => {
                let inner = tx.tx();
                let access_list = Some(List::new(
                    inner
                        .access_list
                        .0
                        .iter()
                        .map(|item| AccessListItemRecord {
                            address: (*item.address).into(),
                            storage_keys: List::new(
                                item.storage_keys.iter().map(|k| (*k).into()).collect(),
                            ),
                        })
                        .collect(),
                ));
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
                    Some(inner.chain_id),
                    access_list,
                    None,
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
                let access_list = Some(List::new(
                    tx_inner
                        .access_list
                        .0
                        .iter()
                        .map(|item| AccessListItemRecord {
                            address: (*item.address).into(),
                            storage_keys: List::new(
                                item.storage_keys.iter().map(|k| (*k).into()).collect(),
                            ),
                        })
                        .collect(),
                ));
                let blob_versioned_hashes = Some(List::new(
                    tx_inner
                        .blob_versioned_hashes
                        .iter()
                        .map(|h| (*h).into())
                        .collect(),
                ));
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
                    Some(tx_inner.chain_id),
                    access_list,
                    blob_versioned_hashes,
                    None,
                    3,
                )
            }
            TxEnvelope::Eip7702(tx) => {
                let inner = tx.tx();
                let access_list = Some(List::new(
                    inner
                        .access_list
                        .0
                        .iter()
                        .map(|item| AccessListItemRecord {
                            address: (*item.address).into(),
                            storage_keys: List::new(
                                item.storage_keys.iter().map(|k| (*k).into()).collect(),
                            ),
                        })
                        .collect(),
                ));
                let authorization_list = Some(List::new(
                    inner
                        .authorization_list
                        .iter()
                        .filter_map(|auth| {
                            let sig = auth.signature().ok()?;
                            Some(AuthorizationRecord {
                                chain_id: auth.chain_id().to::<u64>(),
                                address: (*auth.address()).into(),
                                nonce: auth.nonce(),
                                y_parity: sig.v(),
                                r: Hash32 {
                                    bytes: sig.r().to_be_bytes::<32>(),
                                },
                                s: Hash32 {
                                    bytes: sig.s().to_be_bytes::<32>(),
                                },
                            })
                        })
                        .collect(),
                ));
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
                    Some(inner.chain_id),
                    access_list,
                    None,
                    authorization_list,
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
            chain_id,
            access_list,
            blob_versioned_hashes,
            authorization_list,
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

/// Reverse conversions: Record → Alloy types
impl From<&AccessListItemRecord> for alloy_eip2930::AccessListItem {
    fn from(record: &AccessListItemRecord) -> Self {
        use alloy_primitives::{Address, FixedBytes};
        alloy_eip2930::AccessListItem {
            address: Address::from(record.address.bytes),
            storage_keys: record
                .storage_keys
                .values()
                .iter()
                .map(|k| FixedBytes::from(k.bytes))
                .collect(),
        }
    }
}

impl TryFrom<&TransactionRecord> for TxEnvelope {
    type Error = anyhow::Error;

    fn try_from(record: &TransactionRecord) -> Result<Self, Self::Error> {
        use alloy_consensus::*;
        use alloy_primitives::{Address, Bytes, FixedBytes, Signature, TxKind, U256};

        // Reconstruct signature
        let v = record
            .v
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Missing v"))?;
        let r_bytes: [u8; 32] = record.r.as_slice().try_into()?;
        let s_bytes: [u8; 32] = record.s.as_slice().try_into()?;
        let r = U256::from_be_bytes(r_bytes);
        let s = U256::from_be_bytes(s_bytes);
        let sig = Signature::new(r, s, v != 0);

        let tx_kind = record
            .to
            .as_ref()
            .map(|addr| TxKind::Call(Address::from(addr.bytes)))
            .unwrap_or(TxKind::Create);

        match record.tx_type {
            0 => {
                // Legacy
                let tx = TxLegacy {
                    chain_id: record.chain_id,
                    nonce: record.nonce,
                    gas_price: record
                        .gas_price
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Missing gas_price for legacy tx"))?
                        .into(),
                    gas_limit: record.gas_limit,
                    to: tx_kind,
                    value: (&record.value).into(),
                    input: Bytes::copy_from_slice(&record.input),
                };
                Ok(TxEnvelope::Legacy(Signed::new_unchecked(
                    tx,
                    sig,
                    record.tx_hash.bytes.into(),
                )))
            }
            1 => {
                // EIP-2930
                let access_list = record
                    .access_list
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Missing access_list for EIP-2930"))?
                    .values()
                    .iter()
                    .map(|item| item.into())
                    .collect();
                let tx = TxEip2930 {
                    chain_id: record
                        .chain_id
                        .ok_or_else(|| anyhow::anyhow!("Missing chain_id for EIP-2930"))?,
                    nonce: record.nonce,
                    gas_price: record
                        .gas_price
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Missing gas_price for EIP-2930"))?
                        .into(),
                    gas_limit: record.gas_limit,
                    to: tx_kind,
                    value: (&record.value).into(),
                    input: Bytes::copy_from_slice(&record.input),
                    access_list: alloy_eip2930::AccessList(access_list),
                };
                Ok(TxEnvelope::Eip2930(Signed::new_unchecked(
                    tx,
                    sig,
                    record.tx_hash.bytes.into(),
                )))
            }
            2 => {
                // EIP-1559
                let access_list = record
                    .access_list
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Missing access_list for EIP-1559"))?
                    .values()
                    .iter()
                    .map(|item| item.into())
                    .collect();
                let tx = TxEip1559 {
                    chain_id: record
                        .chain_id
                        .ok_or_else(|| anyhow::anyhow!("Missing chain_id for EIP-1559"))?,
                    nonce: record.nonce,
                    max_fee_per_gas: record
                        .max_fee_per_gas
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Missing max_fee_per_gas for EIP-1559"))?
                        .into(),
                    max_priority_fee_per_gas: record
                        .max_priority_fee_per_gas
                        .as_ref()
                        .ok_or_else(|| {
                            anyhow::anyhow!("Missing max_priority_fee_per_gas for EIP-1559")
                        })?
                        .into(),
                    gas_limit: record.gas_limit,
                    to: tx_kind,
                    value: (&record.value).into(),
                    input: Bytes::copy_from_slice(&record.input),
                    access_list: alloy_eip2930::AccessList(access_list),
                };
                Ok(TxEnvelope::Eip1559(Signed::new_unchecked(
                    tx,
                    sig,
                    record.tx_hash.bytes.into(),
                )))
            }
            3 => {
                // EIP-4844
                let access_list = record
                    .access_list
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Missing access_list for EIP-4844"))?
                    .values()
                    .iter()
                    .map(|item| item.into())
                    .collect();
                let blob_versioned_hashes = record
                    .blob_versioned_hashes
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Missing blob_versioned_hashes for EIP-4844"))?
                    .values()
                    .iter()
                    .map(|h| FixedBytes::from(h.bytes))
                    .collect();
                let to = record
                    .to
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("EIP-4844 must have recipient"))?;
                let tx = TxEip4844 {
                    chain_id: record
                        .chain_id
                        .ok_or_else(|| anyhow::anyhow!("Missing chain_id for EIP-4844"))?,
                    nonce: record.nonce,
                    max_fee_per_gas: record
                        .max_fee_per_gas
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Missing max_fee_per_gas for EIP-4844"))?
                        .into(),
                    max_priority_fee_per_gas: record
                        .max_priority_fee_per_gas
                        .as_ref()
                        .ok_or_else(|| {
                            anyhow::anyhow!("Missing max_priority_fee_per_gas for EIP-4844")
                        })?
                        .into(),
                    max_fee_per_blob_gas: record
                        .max_fee_per_blob_gas
                        .as_ref()
                        .ok_or_else(|| {
                            anyhow::anyhow!("Missing max_fee_per_blob_gas for EIP-4844")
                        })?
                        .into(),
                    gas_limit: record.gas_limit,
                    to: Address::from(to.bytes),
                    value: (&record.value).into(),
                    input: Bytes::copy_from_slice(&record.input),
                    access_list: alloy_eip2930::AccessList(access_list),
                    blob_versioned_hashes,
                };
                Ok(TxEnvelope::Eip4844(Signed::new_unchecked(
                    TxEip4844Variant::TxEip4844(tx),
                    sig,
                    record.tx_hash.bytes.into(),
                )))
            }
            4 => {
                // EIP-7702
                let access_list = record
                    .access_list
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Missing access_list for EIP-7702"))?
                    .values()
                    .iter()
                    .map(|item| item.into())
                    .collect();
                let authorization_list = record
                    .authorization_list
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Missing authorization_list for EIP-7702"))?
                    .values()
                    .iter()
                    .map(|auth| {
                        let inner = alloy_eip7702::Authorization {
                            chain_id: U256::from(auth.chain_id),
                            address: Address::from(auth.address.bytes),
                            nonce: auth.nonce,
                        };
                        alloy_eip7702::SignedAuthorization::new_unchecked(
                            inner,
                            if auth.y_parity { 1 } else { 0 },
                            U256::from_be_bytes(auth.r.bytes),
                            U256::from_be_bytes(auth.s.bytes),
                        )
                    })
                    .collect();
                let to = record
                    .to
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("EIP-7702 must have recipient"))?;
                let tx = TxEip7702 {
                    chain_id: record
                        .chain_id
                        .ok_or_else(|| anyhow::anyhow!("Missing chain_id for EIP-7702"))?,
                    nonce: record.nonce,
                    max_fee_per_gas: record
                        .max_fee_per_gas
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Missing max_fee_per_gas for EIP-7702"))?
                        .into(),
                    max_priority_fee_per_gas: record
                        .max_priority_fee_per_gas
                        .as_ref()
                        .ok_or_else(|| {
                            anyhow::anyhow!("Missing max_priority_fee_per_gas for EIP-7702")
                        })?
                        .into(),
                    gas_limit: record.gas_limit,
                    to: Address::from(to.bytes),
                    value: (&record.value).into(),
                    input: Bytes::copy_from_slice(&record.input),
                    access_list: alloy_eip2930::AccessList(access_list),
                    authorization_list,
                };
                Ok(TxEnvelope::Eip7702(Signed::new_unchecked(
                    tx,
                    sig,
                    record.tx_hash.bytes.into(),
                )))
            }
            _ => anyhow::bail!("Unknown transaction type: {}", record.tx_type),
        }
    }
}

impl TryFrom<TransactionRecord> for TxEnvelope {
    type Error = anyhow::Error;

    fn try_from(record: TransactionRecord) -> Result<Self, Self::Error> {
        Self::try_from(&record)
    }
}
