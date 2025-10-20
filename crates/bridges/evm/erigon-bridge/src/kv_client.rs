/// KV client for querying Erigon's key-value database
///
/// This module provides access to Erigon's internal KV database via gRPC.
/// Most importantly, it allows querying the TxSender table which contains
/// precomputed sender addresses for all transactions.
///
/// ## Why Query TxSender Instead of Signature Recovery?
///
/// Ethereum transactions don't store sender addresses on-chain - they must be
/// cryptographically derived from ECDSA signatures through elliptic curve point
/// multiplication (ecrecover). This is computationally expensive:
///
/// - Perf profiling showed 75% of CPU time on k256 elliptic curve operations
/// - Processing 23M blocks means ~3.5 billion signature recoveries
/// - Each recovery involves EC scalar multiplication, public key decompression,
///   and Keccak256 hashing
///
/// Erigon solves this by computing senders once during its "Senders stage" of
/// block import and storing them in a dedicated TxSender table:
///
/// ```text
/// Table: "TxSender"
/// Key:   block_number (u64 BE) + block_hash (32 bytes) = 40 bytes
/// Value: sender_0 | sender_1 | ... | sender_n (20 bytes each, concatenated)
/// ```
///
/// By querying this table, we eliminate the CPU bottleneck:
/// - **Before**: 150 transactions/block × expensive ecrecover = 75% CPU
/// - **After**: 1 KV lookup/block + trivial byte parsing = near-zero CPU
///
/// This is a 4x+ performance improvement for transaction processing.
///
/// ## Architecture Note
///
/// Per CLAUDE.md guidelines, bridges should be stateless protocol translators.
/// One could argue that signature recovery is data enrichment, not protocol
/// translation - the protocol gives us RLP transactions with signatures, deriving
/// the sender is computed state. However, since Erigon has already computed this
/// and made it available via their KV API, we treat this as source data to query.
use crate::error::ErigonBridgeError;
use crate::generated::remote::{kv_client::KvClient, RangeReq};
use evm_common::types::Address20;
use tonic::transport::Channel;
use tracing::{debug, warn};

const TX_SENDER_TABLE: &str = "TxSender";

/// Client for querying Erigon's KV database
pub struct ErigonKvClient {
    client: KvClient<Channel>,
}

impl ErigonKvClient {
    /// Connect to Erigon's KV gRPC endpoint
    ///
    /// Default endpoint is localhost:9090 (--private.api.addr in Erigon)
    pub async fn connect(endpoint: &str) -> Result<Self, ErigonBridgeError> {
        let client = KvClient::connect(format!("http://{}", endpoint)).await?;
        Ok(Self { client })
    }

    /// Query sender addresses for all transactions in a block
    ///
    /// Returns a vector of addresses in transaction index order.
    /// If the TxSender table doesn't have this block, returns None
    /// (caller should fall back to signature recovery).
    ///
    /// ## Key Format
    /// ```text
    /// block_number: u64 big-endian (8 bytes)
    /// block_hash: 32 bytes
    /// Total: 40 bytes
    /// ```
    ///
    /// ## Value Format
    /// ```text
    /// Raw concatenated addresses: addr0|addr1|addr2|...
    /// Each address is exactly 20 bytes
    /// Total length = num_transactions × 20
    /// ```
    pub async fn get_block_senders(
        &mut self,
        block_number: u64,
        block_hash: &[u8; 32],
    ) -> Result<Option<Vec<Address20>>, ErigonBridgeError> {
        // Build the key: block_number (BE) + block_hash
        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&block_number.to_be_bytes());
        key.extend_from_slice(block_hash);

        debug!(block_number, "Querying TxSender table for block senders");

        // Query the TxSender table using Range API
        // Note: tx_id = 0 means no explicit transaction context needed for simple range queries
        let request = RangeReq {
            tx_id: 0,
            table: TX_SENDER_TABLE.to_string(),
            from_prefix: key.clone(),
            to_prefix: key, // Same key for exact match
            order_ascend: true,
            limit: 1, // We only want this specific block
            page_size: 0,
            page_token: String::new(),
        };

        let response = self.client.range(request).await?;
        let pairs = response.into_inner();

        // Check if we got a result
        if pairs.values.is_empty() {
            debug!(
                block_number,
                "No TxSender entry found for block (table may not be populated yet)"
            );
            return Ok(None);
        }

        // Parse the sender addresses from the value
        let senders_bytes = &pairs.values[0];

        if senders_bytes.len() % 20 != 0 {
            warn!(
                block_number,
                value_len = senders_bytes.len(),
                "TxSender value length is not a multiple of 20 bytes"
            );
            return Ok(None);
        }

        let senders: Vec<Address20> = senders_bytes
            .chunks_exact(20)
            .map(|chunk| {
                let mut bytes = [0u8; 20];
                bytes.copy_from_slice(chunk);
                Address20 { bytes }
            })
            .collect();

        debug!(
            block_number,
            sender_count = senders.len(),
            "Successfully retrieved sender addresses from TxSender table"
        );

        Ok(Some(senders))
    }
}
