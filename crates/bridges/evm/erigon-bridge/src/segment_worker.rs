//! Segment-based transaction processing with parallel validation
//!
//! Aligns with Erigon's snapshot segment structure (500k blocks) for efficient
//! parallel processing. Each worker owns a segment with its own gRPC connection
//! and validates transactions in batches before converting to Arrow.

use crate::blockdata_client::BlockDataClient;
use crate::blockdata_converter::BlockDataConverter;
use crate::error::ErigonBridgeError;
use crate::proto::custom::TransactionData;
use alloy_consensus::Header;
use alloy_primitives::Bytes;
use alloy_rlp::Decodable;
use arrow_array::RecordBatch;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use validators_evm::ValidationExecutor;

/// Configuration for segment-based processing and validation
#[derive(Debug, Clone)]
pub struct SegmentConfig {
    /// Size of each segment in blocks (default: 500_000, aligned with Erigon snapshots)
    pub segment_size: u64,

    /// Maximum number of segments to process in parallel (default: num_cpus / 4)
    pub max_concurrent_segments: usize,

    /// Validation batch size within a segment (default: 100 blocks)
    /// How many blocks to collect before executing validations and converting to Arrow
    pub validation_batch_size: usize,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            segment_size: 500_000,
            max_concurrent_segments: (num_cpus::get() / 4).max(1),
            validation_batch_size: 100,
        }
    }
}

/// A single block's data ready for validation
#[derive(Debug)]
pub struct BlockData {
    pub block_num: u64,
    pub header: Header,
    pub transactions: Vec<TransactionData>,
}

/// Type alias for validation futures returned by ValidationExecutor
type ValidationFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<(), validators_evm::ValidationError>> + Send>,
>;

/// Type of data to process in a segment
#[derive(Debug, Clone, Copy)]
pub enum SegmentDataType {
    Transactions,
    Logs,
}

/// Worker that processes a segment of blocks with validation
pub struct SegmentWorker {
    segment_start: u64,
    segment_end: u64,
    blockdata_client: BlockDataClient,
    config: SegmentConfig,
    validator: Option<Arc<dyn ValidationExecutor>>,
    data_type: SegmentDataType,
}

impl SegmentWorker {
    /// Create a new segment worker for transactions
    pub fn new(
        segment_start: u64,
        segment_end: u64,
        blockdata_client: BlockDataClient,
        config: SegmentConfig,
        validator: Option<Arc<dyn ValidationExecutor>>,
    ) -> Self {
        Self {
            segment_start,
            segment_end,
            blockdata_client,
            config,
            validator,
            data_type: SegmentDataType::Transactions,
        }
    }

    /// Create a new segment worker for logs
    pub fn new_for_logs(
        segment_start: u64,
        segment_end: u64,
        blockdata_client: BlockDataClient,
        config: SegmentConfig,
        validator: Option<Arc<dyn ValidationExecutor>>,
    ) -> Self {
        Self {
            segment_start,
            segment_end,
            blockdata_client,
            config,
            validator,
            data_type: SegmentDataType::Logs,
        }
    }

    /// Process the segment: fetch blocks, validate, convert to Arrow
    ///
    /// Returns a stream of Arrow RecordBatches, yielding one per validation batch
    pub fn process(
        self,
    ) -> impl futures::Stream<Item = Result<RecordBatch, ErigonBridgeError>> + Send {
        match self.data_type {
            SegmentDataType::Transactions => self.process_transactions().boxed(),
            SegmentDataType::Logs => self.process_logs().boxed(),
        }
    }

    /// Process transactions for this segment with eager validation
    fn process_transactions(
        self,
    ) -> impl futures::Stream<Item = Result<RecordBatch, ErigonBridgeError>> + Send {
        use futures::stream::StreamExt;

        async_stream::stream! {
            info!(
                "Segment worker processing transactions for blocks {} to {} ({} blocks)",
                self.segment_start,
                self.segment_end,
                self.segment_end - self.segment_start + 1
            );

            // Clone the client at the start (shares underlying HTTP/2 connection)
            let mut client = self.blockdata_client.clone();

            // Process blocks in chunks, fetching headers only as needed
            let mut current_block = self.segment_start;
            while current_block <= self.segment_end {
            let chunk_end = (current_block + self.config.validation_batch_size as u64 - 1).min(self.segment_end);

            // Fetch headers only for this chunk
            let headers = match Self::fetch_headers(current_block, chunk_end, &mut client).await {
                Ok(h) => h,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            debug!(
                "Segment {}-{}: Fetched {} headers for chunk {}-{}",
                self.segment_start,
                self.segment_end,
                headers.len(),
                current_block,
                chunk_end
            );

            // Sort headers by block number for sequential processing
            let mut sorted_headers: Vec<_> = headers.into_iter().collect();
            sorted_headers.sort_by_key(|(block_num, _)| *block_num);

            // Process this chunk
            {
            let chunk = &sorted_headers[..];
            // Step A: Stream all transactions for this chunk at once
            let start_block = chunk.first().unwrap().0;
            let end_block = chunk.last().unwrap().0;

            debug!(
                "Segment {}-{}: Streaming transactions for blocks {}-{}",
                self.segment_start, self.segment_end, start_block, end_block
            );

            let mut tx_stream = match client
                .stream_transactions(start_block, end_block, 100)
                .await {
                    Ok(s) => s,
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                };

            // Collect all transactions from stream (in block order)
            let mut all_transactions = Vec::new();
            while let Some(batch_result) = tx_stream.message().await.transpose() {
                let tx_batch = match batch_result {
                    Ok(b) => b,
                    Err(e) => {
                        yield Err(ErigonBridgeError::ErigonClient(e));
                        return;
                    }
                };
                debug!("Received {} transactions", tx_batch.transactions.len());
                all_transactions.extend(tx_batch.transactions);
            }

            // Peekable iterator to pull transactions for each block
            let mut tx_iter = all_transactions.into_iter().peekable();

            // Create block data and validation futures
            let mut blocks = Vec::with_capacity(chunk.len());
            let mut validation_futures = Vec::new();

            for (block_num, (header, _timestamp)) in chunk {
                // Take all transactions for this block
                let mut transactions = Vec::new();
                while let Some(tx) = tx_iter.next_if(|tx| tx.block_number == *block_num) {
                    transactions.push(tx);
                }

                let block_data = BlockData {
                    block_num: *block_num,
                    header: header.clone(),
                    transactions,
                };

                // Eagerly spawn validation work (starts immediately on first poll)
                if let Some(ref validator) = self.validator {
                    // Extract RLP for validation (already in correct order)
                    let tx_rlps: Vec<Bytes> = block_data
                        .transactions
                        .iter()
                        .map(|tx| Bytes::from(tx.rlp_transaction.clone()))
                        .collect();

                    let validator_clone = validator.clone();
                    let expected_root = block_data.header.transactions_root;
                    let validation_future: ValidationFuture = Box::pin(async move {
                        validator_clone
                            .spawn_validate_rlp(expected_root, tx_rlps)
                            .await
                    });
                    validation_futures.push((*block_num, validation_future));
                }

                blocks.push(block_data);
            }

            // Step B: Collect validation results and convert to Arrow
            debug!(
                "Segment {}-{}: Awaiting {} validations and converting {} blocks",
                self.segment_start,
                self.segment_end,
                validation_futures.len(),
                blocks.len()
            );

            let arrow_batch = match Self::collect_validations_and_convert(
                self.segment_start,
                self.segment_end,
                blocks,
                validation_futures,
            )
            .await {
                Ok(batch) => batch,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            // Yield this batch immediately - no buffering!
            yield Ok(arrow_batch);
            }

            // Move to next chunk
            current_block = chunk_end + 1;
        }

        info!(
            "Segment {}-{}: Completed transaction processing",
            self.segment_start,
            self.segment_end
        );
        }
    }

    /// Process logs/receipts for this segment
    fn process_logs(
        self,
    ) -> impl futures::Stream<Item = Result<RecordBatch, ErigonBridgeError>> + Send {
        use futures::stream::StreamExt;

        async_stream::stream! {
        info!(
            "Segment worker processing logs for blocks {} to {} ({} blocks)",
            self.segment_start,
            self.segment_end,
            self.segment_end - self.segment_start + 1
        );

        // Clone the client at the start (shares underlying HTTP/2 connection)
        let mut client = self.blockdata_client.clone();

        // Process blocks in chunks, fetching headers and executing blocks concurrently
        let mut current_block = self.segment_start;

        while current_block <= self.segment_end {
            // Fetch a chunk of headers (batch_size blocks at a time)
            let chunk_end = (current_block + self.config.validation_batch_size as u64 - 1).min(self.segment_end);

            let chunk_headers = match Self::fetch_headers(current_block, chunk_end, &mut client).await {
                Ok(h) => h,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            debug!(
                "Segment {}-{}: Fetched {} headers for chunk {}-{}, spawning {} concurrent ExecuteBlocks calls",
                self.segment_start,
                self.segment_end,
                chunk_headers.len(),
                current_block,
                chunk_end,
                chunk_headers.len()
            );

            // Sort headers by block number
            let mut sorted_chunk: Vec<_> = chunk_headers.into_iter().collect();
            sorted_chunk.sort_by_key(|(block_num, _)| *block_num);

            // Fire all ExecuteBlocks calls concurrently for this chunk
            let receipt_futures: Vec<_> = sorted_chunk
                .iter()
                .map(|(block_num, (header, _timestamp))| {
                    let mut client_clone = client.clone();
                    let block_num = *block_num;
                    let header = header.clone();
                    async move {
                        let receipts = Self::collect_receipts_for_block(block_num, &mut client_clone).await?;
                        Ok::<_, ErigonBridgeError>((block_num, receipts, header))
                    }
                })
                .collect();

            // Await all ExecuteBlocks calls together
            let results = futures::future::join_all(receipt_futures).await;

            // Collect successful results
            let mut current_batch = Vec::new();
            for result in results {
                match result {
                    Ok(data) => current_batch.push(data),
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                }
            }

            // Validate and convert this batch
            if !current_batch.is_empty() {
                debug!(
                    "Segment {}-{}: Validating and converting batch of {} blocks",
                    self.segment_start,
                    self.segment_end,
                    current_batch.len()
                );

                let batches = match Self::validate_and_convert_receipts(
                    self.segment_start,
                    self.segment_end,
                    self.config.clone(),
                    self.validator.clone(),
                    current_batch,
                )
                .await {
                    Ok(b) => b,
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                };

                // Yield each batch immediately
                for batch in batches {
                    yield Ok(batch);
                }
            }

            // Move to next chunk
            current_block = chunk_end + 1;
        }

        info!(
            "Segment {}-{}: Completed log processing",
            self.segment_start,
            self.segment_end
        );
        }
    }

    /// Fetch all block headers for the segment
    async fn fetch_headers(
        segment_start: u64,
        segment_end: u64,
        client: &mut BlockDataClient,
    ) -> Result<HashMap<u64, (Header, i64)>, ErigonBridgeError> {
        let mut headers = HashMap::new();

        let mut block_stream = client
            .stream_blocks(segment_start, segment_end, 100)
            .await?;

        let mut batch_count = 0;
        while let Some(batch_result) = block_stream.message().await.transpose() {
            let block_batch = batch_result?;
            batch_count += 1;

            debug!(
                "Segment {}-{}: Received header batch {} with {} blocks",
                segment_start,
                segment_end,
                batch_count,
                block_batch.blocks.len()
            );

            for block in block_batch.blocks {
                match Header::decode(&mut block.rlp_header.as_slice()) {
                    Ok(header) => {
                        let timestamp = header.timestamp as i64;
                        headers.insert(block.block_number, (header, timestamp));
                    }
                    Err(e) => {
                        warn!(
                            "Failed to decode header for block {}: {}",
                            block.block_number, e
                        );
                    }
                }
            }
        }

        Ok(headers)
    }

    /// Collect validation results and convert blocks to Arrow
    ///
    /// Validation work has already been spawned to the executor thread pool.
    /// This function awaits the results and converts validated blocks to Arrow.
    async fn collect_validations_and_convert(
        segment_start: u64,
        segment_end: u64,
        blocks: Vec<BlockData>,
        validation_futures: Vec<(u64, ValidationFuture)>,
    ) -> Result<RecordBatch, ErigonBridgeError> {
        // Await validation results if any exist
        if !validation_futures.is_empty() {
            info!(
                "Segment {}-{}: Collecting {} validation results",
                segment_start,
                segment_end,
                validation_futures.len()
            );

            // Separate block numbers from futures for error reporting
            let (block_nums, futures): (Vec<_>, Vec<_>) = validation_futures.into_iter().unzip();

            // Collect all validation results (work already running in executor thread pool)
            let results = futures::future::join_all(futures).await;

            // Check for any validation errors
            for (block_num, result) in block_nums.into_iter().zip(results) {
                if let Err(e) = result {
                    error!(
                        "Validation failed for block {} in segment {}-{}: {}",
                        block_num, segment_start, segment_end, e
                    );
                    return Err(ErigonBridgeError::ValidationError(format!(
                        "Block {} validation failed: {}",
                        block_num, e
                    )));
                }
            }

            info!(
                "Segment {}-{}: Validated {} blocks successfully",
                segment_start,
                segment_end,
                blocks.len()
            );
        }

        // Convert validated blocks to Arrow (takes ownership to avoid cloning)
        let arrow_batch = Self::convert_batch_to_arrow(blocks)?;

        Ok(arrow_batch)
    }

    /// Convert a batch of validated blocks to Arrow RecordBatch
    ///
    /// Takes ownership of blocks to avoid cloning transaction data
    fn convert_batch_to_arrow(blocks: Vec<BlockData>) -> Result<RecordBatch, ErigonBridgeError> {
        // Use the optimized converter that takes ownership and avoids cloning
        BlockDataConverter::block_transactions_to_arrow(blocks)
    }

    /// Collect receipts for a single block
    async fn collect_receipts_for_block(
        block_num: u64,
        client: &mut BlockDataClient,
    ) -> Result<Vec<crate::proto::custom::ReceiptData>, ErigonBridgeError> {
        let mut receipts = Vec::new();

        // Request just this one block's receipts (start = end = block_num)
        let mut receipt_stream = client.execute_blocks(block_num, block_num, 100).await?;

        while let Some(batch_result) = receipt_stream.message().await.transpose() {
            let receipt_batch = batch_result?;

            debug!(
                "Block {}: Received {} receipts",
                block_num,
                receipt_batch.receipts.len()
            );

            receipts.extend(receipt_batch.receipts);
        }

        Ok(receipts)
    }

    /// Validate receipts in batches and convert to Arrow logs
    async fn validate_and_convert_receipts(
        segment_start: u64,
        segment_end: u64,
        _config: SegmentConfig,
        validator: Option<Arc<dyn ValidationExecutor>>,
        block_receipts: Vec<(u64, Vec<crate::proto::custom::ReceiptData>, Header)>,
    ) -> Result<Vec<RecordBatch>, ErigonBridgeError> {
        let mut record_batches = Vec::new();

        // Validate this batch if validator is configured
        if let Some(ref validator) = validator {
            Self::validate_receipt_batch(segment_start, segment_end, validator, &block_receipts)
                .await?;
        }

        // Convert validated receipts to Arrow logs
        let arrow_batch = Self::convert_receipts_to_logs(&block_receipts)?;
        record_batches.push(arrow_batch);

        Ok(record_batches)
    }

    /// Validate a batch of blocks' receipts using RLP validation
    ///
    /// Spawns N validation tasks and executes them concurrently using buffer_unordered
    async fn validate_receipt_batch(
        segment_start: u64,
        segment_end: u64,
        validator: &Arc<dyn ValidationExecutor>,
        blocks: &[(u64, Vec<crate::proto::custom::ReceiptData>, Header)],
    ) -> Result<(), ErigonBridgeError> {
        use futures::stream::{self, StreamExt};

        let num_cores = num_cpus::get();

        info!(
            "Segment {}-{}: Validating {} blocks' receipts with up to {} concurrent tasks",
            segment_start,
            segment_end,
            blocks.len(),
            num_cores
        );

        // Collect all validation data first to avoid lifetime issues
        // IMPORTANT: Receipts must be sorted by tx_index for merkle trie validation
        let validation_data: Vec<(u64, alloy_primitives::B256, Vec<Bytes>)> = blocks
            .iter()
            .map(|(block_num, receipts, header)| {
                // Sort receipts by tx_index before validation
                let mut sorted_receipts: Vec<_> = receipts.iter().collect();
                sorted_receipts.sort_by_key(|r| r.tx_index);

                let receipt_rlps: Vec<Bytes> = sorted_receipts
                    .into_iter()
                    .map(|r| Bytes::from(r.rlp_receipt.clone()))
                    .collect();
                (*block_num, header.receipts_root, receipt_rlps)
            })
            .collect();

        // Run all validations in parallel (underlying executor manages thread pool)
        let results = futures::future::join_all(validation_data.into_iter().map(
            |(block_num, expected_root, receipt_rlps)| {
                let validator = validator.clone();

                async move {
                    validator
                        .spawn_validate_receipts_rlp(expected_root, receipt_rlps)
                        .await
                        .map_err(|e| (block_num, e))
                }
            },
        ))
        .await;

        // Check for any validation errors
        for result in results {
            if let Err((block_num, e)) = result {
                error!(
                    "Receipt validation failed for block {} in segment {}-{}: {}",
                    block_num, segment_start, segment_end, e
                );
                return Err(ErigonBridgeError::ValidationError(format!(
                    "Block {} receipt validation failed: {}",
                    block_num, e
                )));
            }
        }

        info!(
            "Segment {}-{}: Validated {} blocks' receipts successfully",
            segment_start,
            segment_end,
            blocks.len()
        );

        Ok(())
    }

    /// Convert a batch of validated receipts to Arrow log RecordBatch
    fn convert_receipts_to_logs(
        blocks: &[(u64, Vec<crate::proto::custom::ReceiptData>, Header)],
    ) -> Result<RecordBatch, ErigonBridgeError> {
        // Collect all receipts
        let mut all_receipts = Vec::new();
        for (_, receipts, _) in blocks {
            all_receipts.extend(receipts.iter().cloned());
        }

        // Build timestamps map from the headers we already have
        let timestamps: HashMap<u64, i64> = blocks
            .iter()
            .map(|(block_num, _, header)| (*block_num, header.timestamp as i64 * 1_000_000_000))
            .collect();

        // Convert using existing converter
        let receipt_batch = crate::proto::custom::ReceiptBatch {
            receipts: all_receipts,
            first_block: blocks.first().map(|(b, _, _)| *b).unwrap_or(0),
            last_block: blocks.last().map(|(b, _, _)| *b).unwrap_or(0),
            is_last: false, // Not used in this context
        };

        BlockDataConverter::receipts_to_logs_arrow(receipt_batch, &timestamps)
    }
}

/// Split a block range into segments
pub fn split_into_segments(start: u64, end: u64, segment_size: u64) -> Vec<(u64, u64)> {
    let mut segments = Vec::new();
    let mut current = start;

    while current <= end {
        let segment_end = (current + segment_size - 1).min(end);
        segments.push((current, segment_end));
        current = segment_end + 1;
    }

    segments
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_into_segments() {
        let segments = split_into_segments(0, 1_000_000, 500_000);
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0], (0, 499_999));
        assert_eq!(segments[1], (500_000, 999_999));
        assert_eq!(segments[2], (1_000_000, 1_000_000));
    }

    #[test]
    fn test_split_partial_segment() {
        let segments = split_into_segments(0, 100_000, 500_000);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0], (0, 100_000));
    }
}
