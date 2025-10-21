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
use crate::generated::remote::{kv_client::KvClient, Cursor, Op, Pair, RangeReq};
use evm_common::types::Address20;
use futures::StreamExt;
use tokio::sync::mpsc;
use tonic::{transport::Channel, Streaming};
use tracing::{debug, info, warn};

const TX_SENDER_TABLE: &str = "TxSender";

/// Client for querying Erigon's KV database
///
/// Holds a long-lived read-only transaction for the entire segment.
/// This minimizes database transaction overhead - instead of opening/closing
/// thousands of transactions, we open ONE transaction per worker and reuse it
/// for all queries within that segment (500K blocks).
pub struct ErigonKvClient {
    client: KvClient<Channel>,
    tx_id: u64,
    _tx_stream: Streaming<Pair>, // Keep response stream alive
    _tx_sender: mpsc::UnboundedSender<Cursor>, // Keep outbound stream alive
}

impl ErigonKvClient {
    /// Connect to Erigon's KV gRPC endpoint and open a read-only transaction
    ///
    /// The transaction remains open for the lifetime of this client.
    /// Default endpoint is localhost:9090 (--private.api.addr in Erigon)
    pub async fn connect(endpoint: &str) -> Result<Self, ErigonBridgeError> {
        let mut client = KvClient::connect(format!("http://{}", endpoint)).await?;

        // Create a channel to keep the outbound stream alive
        // The Tx RPC is bidirectional - if we close the outbound stream,
        // the transaction closes even if we're still reading responses
        let (tx_sender, mut tx_receiver) = mpsc::unbounded_channel();

        // Send initial message to open transaction
        tx_sender.send(Cursor {
            op: Op::First as i32,
            bucket_name: String::new(),
            cursor: 0,
            k: vec![],
            v: vec![],
        }).map_err(|e| ErigonBridgeError::Internal(anyhow::anyhow!("Failed to send initial cursor: {}", e)))?;

        // Create the outbound stream from the receiver
        let outbound = async_stream::stream! {
            while let Some(cursor) = tx_receiver.recv().await {
                yield cursor;
            }
        };

        let mut response_stream = client.tx(outbound).await?.into_inner();

        // Receive the tx_id from the first response
        let tx_id = if let Some(pair) = response_stream.next().await {
            let pair = pair?;
            if pair.tx_id == 0 {
                return Err(ErigonBridgeError::Internal(anyhow::anyhow!(
                    "Erigon returned invalid tx_id=0"
                )));
            }
            info!("Opened KV transaction with tx_id={}", pair.tx_id);
            pair.tx_id
        } else {
            return Err(ErigonBridgeError::Internal(anyhow::anyhow!(
                "Erigon Tx stream ended without returning tx_id"
            )));
        };

        // Keep both the response stream AND the sender alive
        // When this client is dropped, both close and the transaction ends
        Ok(Self {
            client,
            tx_id,
            _tx_stream: response_stream,
            _tx_sender: tx_sender,
        })
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

        debug!(block_number, tx_id = self.tx_id, "Querying TxSender table for block senders");

        // Query the TxSender table using our long-lived transaction
        let request = RangeReq {
            tx_id: self.tx_id,
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
