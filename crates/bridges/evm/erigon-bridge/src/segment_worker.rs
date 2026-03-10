//! Segment-based transaction processing with parallel validation
//!
//! Aligns with Erigon's snapshot segment structure (500k blocks) for efficient
//! parallel processing. Each worker owns a segment with its own gRPC connection
//! and validates transactions in batches before converting to Arrow.

use crate::blockdata_client::BlockDataClient;
use crate::blockdata_converter::BlockDataConverter;
use crate::error::ErigonBridgeError;
use crate::metrics::{BridgeMetrics, SegmentMetrics, WorkerStage};
use crate::proto::custom::TransactionData;
use alloy_consensus::Header;
use alloy_primitives::Bytes;
use alloy_rlp::Decodable;
use arrow_array::RecordBatch;
use futures::stream::StreamExt;
use phaser_server::{BatchWithRange, StreamError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};
use validators_evm::ValidationExecutor;

/// Configuration for segment-based processing and validation
#[derive(Clone)]
pub struct SegmentConfig {
    /// Size of each segment in blocks (default: 500_000, aligned with Erigon snapshots)
    pub segment_size: u64,

    /// Maximum number of segments to process in parallel (default: num_cpus / 4)
    pub max_concurrent_segments: usize,

    /// Number of independent gRPC connections in the pool (default: 8)
    pub connection_pool_size: usize,

    /// Validation batch size within a segment (default: 100 blocks)
    /// How many blocks to collect before executing validations and converting to Arrow
    pub validation_batch_size: usize,

    /// Maximum number of concurrent ExecuteBlocks calls within a segment (default: num_cpus)
    /// Controls parallelism for block execution when fetching logs/receipts
    pub max_concurrent_executions: usize,

    /// Global maximum number of concurrent ExecuteBlocks calls across ALL segments (default: 64)
    /// This prevents overwhelming Erigon with too many concurrent gRPC streams
    pub global_max_execute_blocks: usize,

    /// Global semaphore for limiting ExecuteBlocks calls across all workers
    /// Not included in Default implementation - must be set by bridge
    pub execute_blocks_semaphore: Option<Arc<tokio::sync::Semaphore>>,

    /// Enable transaction traces (callTracer) during log/receipt execution (default: false)
    pub enable_traces: bool,
}

impl std::fmt::Debug for SegmentConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentConfig")
            .field("segment_size", &self.segment_size)
            .field("max_concurrent_segments", &self.max_concurrent_segments)
            .field("connection_pool_size", &self.connection_pool_size)
            .field("validation_batch_size", &self.validation_batch_size)
            .field("max_concurrent_executions", &self.max_concurrent_executions)
            .field("global_max_execute_blocks", &self.global_max_execute_blocks)
            .field(
                "execute_blocks_semaphore",
                &self.execute_blocks_semaphore.is_some(),
            )
            .field("enable_traces", &self.enable_traces)
            .finish()
    }
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            segment_size: 500_000,
            max_concurrent_segments: (num_cpus::get() / 4).max(1),
            connection_pool_size: 8,
            validation_batch_size: 100,
            max_concurrent_executions: num_cpus::get(),
            global_max_execute_blocks: 64,
            execute_blocks_semaphore: None,
            enable_traces: false,
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
    worker_id: usize,
    segment_start: u64,
    segment_end: u64,
    blockdata_client: BlockDataClient,
    config: SegmentConfig,
    validator: Option<Arc<dyn ValidationExecutor>>,
    data_type: SegmentDataType,
    metrics: crate::metrics::BridgeMetrics,
}

impl SegmentWorker {
    /// Create a new segment worker for transactions
    pub fn new(
        worker_id: usize,
        segment_start: u64,
        segment_end: u64,
        blockdata_client: BlockDataClient,
        config: SegmentConfig,
        validator: Option<Arc<dyn ValidationExecutor>>,
        metrics: crate::metrics::BridgeMetrics,
    ) -> Self {
        Self {
            worker_id,
            segment_start,
            segment_end,
            blockdata_client,
            config,
            validator,
            data_type: SegmentDataType::Transactions,
            metrics,
        }
    }

    /// Create a new segment worker for logs
    pub fn new_for_logs(
        worker_id: usize,
        segment_start: u64,
        segment_end: u64,
        blockdata_client: BlockDataClient,
        config: SegmentConfig,
        validator: Option<Arc<dyn ValidationExecutor>>,
        metrics: crate::metrics::BridgeMetrics,
    ) -> Self {
        Self {
            worker_id,
            segment_start,
            segment_end,
            blockdata_client,
            config,
            validator,
            data_type: SegmentDataType::Logs,
            metrics,
        }
    }

    /// Process the segment: fetch blocks, validate, convert to Arrow
    ///
    /// Returns a stream of BatchWithRange, yielding one per validation batch
    pub fn process(
        self,
    ) -> impl futures::Stream<Item = Result<BatchWithRange, ErigonBridgeError>> + Send {
        match self.data_type {
            SegmentDataType::Transactions => self.process_transactions().boxed(),
            SegmentDataType::Logs => self.process_logs().boxed(),
        }
    }

    /// Process transactions for this segment with eager validation
    fn process_transactions(
        self,
    ) -> impl futures::Stream<Item = Result<BatchWithRange, ErigonBridgeError>> + Send {
        async_stream::stream! {
            let worker_id = self.worker_id;
            let segment_id = self.segment_start / 500_000; // Calculate segment ID
            let total_blocks = self.segment_end - self.segment_start + 1;
            let phase_start = Instant::now();
            let blocks_phase_start = Instant::now();
            let mut yielded_batches = 0u64;

            info!(
                "Worker {} segment {} processing transactions for blocks {} to {} ({} blocks)",
                worker_id,
                segment_id,
                self.segment_start,
                self.segment_end,
                total_blocks
            );

            // Set initial worker stage
            self.metrics
                .set_worker_stage(worker_id, segment_id, WorkerStage::Blocks);
            self.metrics.active_workers_inc("blocks");

            // Clone the client at the start (shares underlying HTTP/2 connection)
            let mut client = self.blockdata_client.clone();

            // Process blocks in chunks, fetching headers only as needed
            let mut current_block = self.segment_start;
            let mut first_chunk = true;
            while current_block <= self.segment_end {
            let chunk_end = (current_block + self.config.validation_batch_size as u64 - 1).min(self.segment_end);

            // Fetch headers only for this chunk
            let headers = match Self::fetch_headers(current_block, chunk_end, &mut client, self.config.validation_batch_size as u32, &self.metrics).await {
                Ok(h) => {
                    if h.is_empty() {
                        warn!("Worker {} segment {}: Received EMPTY header response for blocks {}-{}",
                            worker_id, segment_id, current_block, chunk_end);
                    }
                    h
                },
                Err(e) => {
                    error!("Worker {} segment {}: Failed to fetch headers for blocks {}-{}: {}",
                        worker_id, segment_id, current_block, chunk_end, e);

                    // Track error type for monitoring
                    let error_type = Self::categorize_error(&e);
                    self.metrics.error(&error_type, "blocks");

                    self.metrics.active_workers_dec("blocks");
                    self.metrics.segment_attempt(false);
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
            let num_headers = headers.len();
            let mut sorted_headers: Vec<_> = headers.into_iter().collect();
            sorted_headers.sort_by_key(|(block_num, _)| *block_num);

            // Update progress - blocks phase
            let blocks_processed = current_block - self.segment_start;
            let progress_pct = (blocks_processed as f64 / total_blocks as f64) * 100.0;
            self.metrics
                .set_worker_progress(worker_id, segment_id, "blocks", progress_pct);
            self.metrics
                .items_processed_inc(worker_id, segment_id, "blocks", num_headers as u64);

            // Transition to transactions phase on first chunk only
            if first_chunk {
                // Record blocks phase duration
                let blocks_duration = blocks_phase_start.elapsed().as_secs_f64();
                self.metrics.segment_duration("blocks", blocks_duration);

                self.metrics.active_workers_dec("blocks");
                self.metrics
                    .set_worker_stage(worker_id, segment_id, WorkerStage::Transactions);
                self.metrics.active_workers_inc("transactions");
                first_chunk = false;
            }

            // Process this chunk
            {
            let chunk = &sorted_headers[..];
            // Step A: Stream all transactions for this chunk at once
            let start_block = chunk.first().unwrap().0;
            let end_block = chunk.last().unwrap().0;

            self.metrics.grpc_stream_inc("transactions");

            debug!(
                "Worker {} segment {}: Streaming transactions for blocks {}-{}",
                worker_id, segment_id, start_block, end_block
            );

            let request_start = Instant::now();
            let mut tx_stream = client.stream_transactions(start_block, end_block, self.config.validation_batch_size as u32).await.map_err(|e| {
                error!("Worker {} segment {}: Failed to create transaction stream: {}", worker_id, segment_id, e);
                e
            })?;
            let duration_ms = request_start.elapsed().as_millis() as f64;
            self.metrics.grpc_request_duration_transactions(segment_id, "stream_transactions", duration_ms);
            let mut all_transactions = Vec::new();

            let mut batch_count = 0u64;
            while let Some(batch_result) = tx_stream.message().await.transpose() {
                match batch_result {
                    Ok(tx_batch) => {
                        batch_count += 1;
                        debug!("Worker {} segment {}: Received transaction batch {} with {} transactions",
                            worker_id, segment_id, batch_count, tx_batch.transactions.len());
                        let tx_count = tx_batch.transactions.len() as u64;
                        all_transactions.extend(tx_batch.transactions);
                        self.metrics
                            .items_processed_inc(worker_id, segment_id, "transactions", tx_count);
                    }
                    Err(e) => {
                        error!("Worker {} segment {}: Transaction stream error after {} batches: {}",
                            worker_id, segment_id, batch_count, e);
                        self.metrics.grpc_stream_dec("transactions");
                        self.metrics.active_workers_dec("transactions");
                        yield Err(ErigonBridgeError::ErigonClient(Box::new(e)));
                        return;
                    }
                }
            }

            // Stream completed successfully
            info!("Worker {} segment {}: Transaction stream completed for blocks {}-{}, received {} batches with {} total transactions",
                worker_id, segment_id, start_block, end_block, batch_count, all_transactions.len());
            self.metrics.grpc_stream_dec("transactions");

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
                    // Track error type for monitoring
                    let error_type = Self::categorize_error(&e);
                    self.metrics.error(&error_type, "transactions");

                    // Clean up metrics before error return
                    self.metrics.active_workers_dec("transactions");
                    self.metrics.segment_attempt(false);
                    yield Err(e);
                    return;
                }
            };

            // Yield this batch immediately - no buffering!
            // Wrap with responsibility range metadata
            yielded_batches += 1;
            info!(
                "BRIDGE YIELD: Worker {} segment {}: Yielding transaction batch {}, blocks {}-{}",
                worker_id, segment_id, yielded_batches, start_block, end_block
            );
            yield Ok(BatchWithRange::new(arrow_batch, start_block, end_block));
            }

            // Move to next chunk
            current_block = chunk_end + 1;
        }

        // Record successful completion
        let duration = phase_start.elapsed().as_secs_f64();

        // Detect empty streams and ERROR OUT
        if yielded_batches == 0 {
            error!(
                "Worker {} segment {}: EMPTY STREAM - Processed {} blocks but yielded ZERO RecordBatches! \
                This indicates the client disconnected or dropped the stream before receiving data.",
                worker_id,
                segment_id,
                total_blocks
            );
            self.metrics.error("empty_stream", "transactions");
            self.metrics.active_workers_dec("transactions");
            self.metrics
                .set_worker_stage(worker_id, segment_id, WorkerStage::Idle);
            self.metrics.segment_attempt(false);
            yield Err(ErigonBridgeError::StreamProtocol(StreamError::ZeroBatchesConsumed {
                start: self.segment_start,
                end: self.segment_end,
                yielded: 0,
            }));
            return;
        }

        info!(
            "Worker {} segment {}: Completed transaction processing in {:.2}s, yielded {} RecordBatches",
            worker_id,
            segment_id,
            duration,
            yielded_batches
        );

        self.metrics
            .segment_duration("transactions", duration);
        self.metrics.active_workers_dec("transactions");
        self.metrics
            .set_worker_stage(worker_id, segment_id, WorkerStage::Idle);
        self.metrics.segment_attempt(true);
        }
    }

    /// Process logs/receipts for this segment
    fn process_logs(
        self,
    ) -> impl futures::Stream<Item = Result<BatchWithRange, ErigonBridgeError>> + Send {
        async_stream::stream! {
        let worker_id = self.worker_id;
        let segment_id = self.segment_start / 500_000;
        let total_blocks = self.segment_end - self.segment_start + 1;
        let phase_start = Instant::now();
        let mut yielded_batches = 0u64;
        let mut total_receipts_processed = 0u64;

        info!(
            "Worker {} segment {} processing logs for blocks {} to {} ({} blocks)",
            worker_id,
            segment_id,
            self.segment_start,
            self.segment_end,
            total_blocks
        );

        // Set initial worker stage
        self.metrics
            .set_worker_stage(worker_id, segment_id, WorkerStage::Logs);
        self.metrics.active_workers_inc("logs");

        // Split entire segment into work units upfront
        let mut work_units = Vec::new();
        let mut current_block = self.segment_start;

        while current_block <= self.segment_end {
            let chunk_end = (current_block + self.config.validation_batch_size as u64 - 1)
                .min(self.segment_end);
            work_units.push((current_block, chunk_end));
            current_block = chunk_end + 1;
        }

        info!(
            "Worker {} segment {}: Created {} work units for parallel execution",
            worker_id, segment_id, work_units.len()
        );

        // Execute work units with bounded concurrency, yielding results in order
        // Use buffered() to automatically handle concurrency and ordering
        use futures::stream::{self, StreamExt};

        let max_concurrent = self.config.max_concurrent_executions;

        // Extract needed fields to avoid borrowing self in closure
        let blockdata_client = self.blockdata_client.clone();
        let metrics = self.metrics.clone();
        let validation_batch_size = self.config.validation_batch_size;
        let execute_blocks_semaphore = self.config.execute_blocks_semaphore.clone();

        // Create a stream that processes work units in order with bounded concurrency
        let mut work_stream = stream::iter(work_units.into_iter().enumerate())
            .map(move |(work_idx, (start, end))| {
                let mut client = blockdata_client.clone();
                let metrics = metrics.clone();
                let semaphore = execute_blocks_semaphore.clone();

                async move {
                    // Fetch headers
                    let headers_result = Self::fetch_headers(
                        start,
                        end,
                        &mut client,
                        validation_batch_size as u32,
                        &metrics,
                    ).await;

                    let headers = match headers_result {
                        Ok(h) => {
                            if h.is_empty() {
                                warn!(
                                    "Work unit {}: Received EMPTY header response for blocks {}-{}",
                                    work_idx, start, end
                                );
                            }
                            h
                        }
                        Err(e) => {
                            error!(
                                "Work unit {}: Failed to fetch headers for blocks {}-{}: {}",
                                work_idx, start, end, e
                            );
                            return (work_idx, start, end, Err(e));
                        }
                    };

                    // Sort headers by block number
                    let mut sorted_headers: Vec<_> = headers.into_iter().collect();
                    sorted_headers.sort_by_key(|(block_num, _)| *block_num);

                    // Execute blocks for this range
                    let result = Self::collect_receipts_for_range(
                        start,
                        end,
                        sorted_headers,
                        &mut client,
                        &metrics,
                        semaphore,
                        self.config.enable_traces,
                    ).await;

                    (work_idx, start, end, result)
                }
            })
            .buffered(max_concurrent);

        // Process results as they arrive in order
        while let Some((work_idx, range_start, range_end, work_result)) = work_stream.next().await {
                match work_result {
                    Ok(receipts_data) => {
                        // Count receipts for metrics
                        for (_, receipts, _) in &receipts_data {
                            total_receipts_processed += receipts.len() as u64;
                        }

                        if !receipts_data.is_empty() {

                            debug!(
                                "Segment {}-{}: Validating and converting work unit {} ({} blocks)",
                                self.segment_start,
                                self.segment_end,
                                work_idx,
                                receipts_data.len()
                            );

                            // Validate and convert
                            let batches = match Self::validate_and_convert_receipts(
                                self.segment_start,
                                self.segment_end,
                                self.config.clone(),
                                self.validator.clone(),
                                receipts_data,
                            )
                            .await {
                                Ok(b) => b,
                                Err(e) => {
                                    let error_type = Self::categorize_error(&e);
                                    self.metrics.error(&error_type, "logs");
                                    self.metrics.active_workers_dec("logs");
                                    yield Err(e);
                                    return;
                                }
                            };

                            // Yield batches in order
                            for batch in batches {
                                yielded_batches += 1;
                                yield Ok(BatchWithRange::new(batch, range_start, range_end));
                            }
                        }

                        // Update progress
                        let blocks_processed = range_end - self.segment_start + 1;
                        let progress_pct = (blocks_processed as f64 / total_blocks as f64) * 100.0;
                        self.metrics
                            .set_worker_progress(worker_id, segment_id, "logs", progress_pct);
                    }
                    Err(e) => {
                        let error_type = Self::categorize_error(&e);
                        self.metrics.error(&error_type, "logs");
                        self.metrics.active_workers_dec("logs");
                        yield Err(e);
                        return;
                    }
                }
        }

        // Record total receipts processed
        self.metrics
            .items_processed_inc(worker_id, segment_id, "receipts", total_receipts_processed);

        // Detect empty streams and ERROR OUT
        if yielded_batches == 0 {
            error!(
                "Worker {} segment {}: EMPTY STREAM - Processed {} blocks but yielded ZERO RecordBatches! \
                This indicates the client disconnected or dropped the stream before receiving data.",
                worker_id,
                segment_id,
                total_blocks
            );
            self.metrics.error("empty_stream", "logs");
            self.metrics.active_workers_dec("logs");
            yield Err(ErigonBridgeError::StreamProtocol(StreamError::ZeroBatchesConsumed {
                start: self.segment_start,
                end: self.segment_end,
                yielded: 0,
            }));
            return;
        }

        // Record phase duration and metrics
        let duration = phase_start.elapsed().as_secs_f64();
        info!(
            "Worker {} segment {}: Completed log processing, yielded {} RecordBatches, processed {} receipts in {:.2}s",
            worker_id,
            segment_id,
            yielded_batches,
            total_receipts_processed,
            duration
        );

        self.metrics
            .segment_duration("logs", duration);
        self.metrics.active_workers_dec("logs");
        self.metrics
            .set_worker_stage(worker_id, segment_id, WorkerStage::Idle);
        }
    }

    /// Fetch all block headers for the segment
    async fn fetch_headers(
        segment_start: u64,
        segment_end: u64,
        client: &mut BlockDataClient,
        batch_size: u32,
        metrics: &BridgeMetrics,
    ) -> Result<HashMap<u64, (Header, i64)>, ErigonBridgeError> {
        info!(
            "ERIGON REQUEST: Requesting blocks {}-{} from Erigon (expecting {} blocks)",
            segment_start,
            segment_end,
            segment_end - segment_start + 1
        );

        let segment_id = segment_start / 500_000;
        let request_start = Instant::now();
        let mut block_stream = client
            .stream_blocks(segment_start, segment_end, batch_size)
            .await
            .map_err(|e| {
                error!(
                    "Blocks {}-{}: Failed to create header stream: {}",
                    segment_start, segment_end, e
                );
                e
            })?;
        let duration_ms = request_start.elapsed().as_millis() as f64;
        metrics.grpc_request_duration_blocks(segment_id, "stream_blocks", duration_ms);

        // Track active gRPC stream
        metrics.grpc_stream_inc("blocks");

        let mut headers = HashMap::new();
        let mut batch_count = 0u64;

        while let Some(batch_result) = block_stream.message().await.transpose() {
            match batch_result {
                Ok(block_batch) => {
                    batch_count += 1;

                    debug!(
                        "Blocks {}-{}: Received header batch {} with {} blocks",
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
                Err(e) => {
                    error!(
                        "Blocks {}-{}: Header stream error after {} batches: {}",
                        segment_start, segment_end, batch_count, e
                    );
                    metrics.grpc_stream_dec("blocks");
                    return Err(ErigonBridgeError::ErigonClient(Box::new(e)));
                }
            }
        }

        // Stream completed successfully
        metrics.grpc_stream_dec("blocks");

        if headers.is_empty() {
            warn!(
                "Blocks {}-{}: Header stream completed with ZERO headers after {} batches",
                segment_start, segment_end, batch_count
            );
        } else {
            // Calculate actual range received
            let block_numbers: Vec<u64> = headers.keys().copied().collect();
            let min_received = block_numbers.iter().min().copied().unwrap_or(0);
            let max_received = block_numbers.iter().max().copied().unwrap_or(0);
            let expected_count = segment_end - segment_start + 1;
            let missing_count = expected_count - headers.len() as u64;

            if missing_count > 0 {
                warn!(
                    "ERIGON RESPONSE: Blocks {}-{}: Received {} headers (expected {}), actual range {}-{}, MISSING {} blocks",
                    segment_start, segment_end, headers.len(), expected_count, min_received, max_received, missing_count
                );
            } else {
                info!(
                    "ERIGON RESPONSE: Blocks {}-{}: Received {} headers (complete), range {}-{}",
                    segment_start,
                    segment_end,
                    headers.len(),
                    min_received,
                    max_received
                );
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
            debug!(
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
                        "Block {block_num} validation failed: {e}"
                    )));
                }
            }

            debug!(
                "Segment {}-{}: Validated {} blocks successfully",
                segment_start,
                segment_end,
                blocks.len()
            );
        }

        // Convert validated blocks to Arrow (takes ownership to avoid cloning)
        let arrow_batch = Self::convert_batch_to_arrow(blocks).await?;

        Ok(arrow_batch)
    }

    /// Convert a batch of validated blocks to Arrow RecordBatch
    ///
    /// Takes ownership of blocks to avoid cloning transaction data
    async fn convert_batch_to_arrow(
        blocks: Vec<BlockData>,
    ) -> Result<RecordBatch, ErigonBridgeError> {
        // Use the optimized converter that takes ownership and avoids cloning
        BlockDataConverter::block_transactions_to_arrow(blocks).await
    }

    /// Collect receipts for a range of blocks
    ///
    /// Requests receipts for multiple blocks in a single gRPC stream to reduce overhead.
    /// Returns results for each block with its header.
    async fn collect_receipts_for_range(
        from_block: u64,
        to_block: u64,
        block_headers: Vec<(u64, (Header, i64))>,
        client: &mut BlockDataClient,
        metrics: &BridgeMetrics,
        execute_blocks_semaphore: Option<Arc<tokio::sync::Semaphore>>,
        enable_traces: bool,
    ) -> Result<Vec<(u64, Vec<crate::proto::custom::ReceiptData>, Header)>, ErigonBridgeError> {
        let call_start = std::time::Instant::now();
        let segment_id = from_block / 500_000;

        // Acquire semaphore permit before making ExecuteBlocks call (if configured)
        // This limits total concurrent ExecuteBlocks calls across ALL workers
        let _permit = if let Some(sem) = execute_blocks_semaphore.as_ref() {
            let permit = sem.acquire().await.map_err(|e| {
                ErigonBridgeError::Internal(anyhow::anyhow!(
                    "Failed to acquire ExecuteBlocks semaphore: {}",
                    e
                ))
            })?;

            debug!(
                "Blocks {}-{}: Acquired ExecuteBlocks permit (available: {})",
                from_block,
                to_block,
                sem.available_permits()
            );

            Some(permit)
        } else {
            None
        };

        // Spawn a monitoring task that warns if the call takes too long
        const SLOW_CALL_WARNING_THRESHOLD_SECS: u64 = 300; // 5 minutes
        let monitor_handle = {
            let from = from_block;
            let to = to_block;
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    SLOW_CALL_WARNING_THRESHOLD_SECS,
                ))
                .await;
                warn!(
                    "ExecuteBlocks for blocks {}-{} has been running for >{} seconds and is still in progress",
                    from, to, SLOW_CALL_WARNING_THRESHOLD_SECS
                );
            })
        };

        let request_start = Instant::now();
        let mut receipt_stream = client
            .execute_blocks(from_block, to_block, 100, enable_traces)
            .await
            .map_err(|e| {
                // Cancel the monitor task since the call failed
                monitor_handle.abort();
                error!(
                    "Blocks {}-{}: ExecuteBlocks failed after {:?}: {}",
                    from_block,
                    to_block,
                    call_start.elapsed(),
                    e
                );
                e
            })?;
        let duration_ms = request_start.elapsed().as_millis() as f64;
        metrics.grpc_request_duration_logs(segment_id, "execute_blocks", duration_ms);

        // Cancel the monitor task since the call succeeded
        monitor_handle.abort();

        // Track active gRPC stream
        metrics.grpc_stream_inc("receipts");

        // Collect all receipts by block number
        let mut receipts_by_block: HashMap<u64, Vec<crate::proto::custom::ReceiptData>> =
            HashMap::new();
        let mut batch_count = 0u64;
        let mut total_receipts = 0u64;

        while let Some(batch_result) = receipt_stream.message().await.transpose() {
            match batch_result {
                Ok(receipt_batch) => {
                    batch_count += 1;
                    for receipt in receipt_batch.receipts {
                        let block_num = receipt.block_number;
                        receipts_by_block
                            .entry(block_num)
                            .or_default()
                            .push(receipt);
                        total_receipts += 1;
                    }
                }
                Err(e) => {
                    error!(
                        "Blocks {}-{}: Receipt stream error after {} batches: {}",
                        from_block, to_block, batch_count, e
                    );
                    metrics.grpc_stream_dec("receipts");
                    return Err(ErigonBridgeError::ErigonClient(Box::new(e)));
                }
            }
        }

        // Stream completed successfully
        metrics.grpc_stream_dec("receipts");

        let elapsed = call_start.elapsed();
        if elapsed > std::time::Duration::from_secs(3) {
            warn!(
                "SLOW ExecuteBlocks for blocks {}-{} took {:?} ({} receipts from {} batches)",
                from_block, to_block, elapsed, total_receipts, batch_count
            );
        }

        // Build result vec matching block order
        let mut results = Vec::new();
        for (block_num, (header, _timestamp)) in block_headers {
            let receipts = receipts_by_block.remove(&block_num).unwrap_or_default();
            results.push((block_num, receipts, header));
        }

        Ok(results)
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

        // Convert validated receipts to Arrow logs (consumes block_receipts)
        let arrow_batch = Self::convert_receipts_to_logs(block_receipts)?;
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
        let num_cores = num_cpus::get();

        debug!(
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
                    "Block {block_num} receipt validation failed: {e}"
                )));
            }
        }

        debug!(
            "Segment {}-{}: Validated {} blocks' receipts successfully",
            segment_start,
            segment_end,
            blocks.len()
        );

        Ok(())
    }

    /// Convert a batch of validated receipts to Arrow log RecordBatch
    /// Takes ownership to avoid cloning receipt data
    fn convert_receipts_to_logs(
        blocks: Vec<(u64, Vec<crate::proto::custom::ReceiptData>, Header)>,
    ) -> Result<RecordBatch, ErigonBridgeError> {
        // Build timestamps map from the headers we have
        let timestamps: HashMap<u64, i64> = blocks
            .iter()
            .map(|(block_num, _, header)| (*block_num, header.timestamp as i64 * 1_000_000_000))
            .collect();

        // Collect all receipts by consuming the vec (no cloning!)
        let mut all_receipts = Vec::new();
        for (_, receipts, _) in blocks {
            all_receipts.extend(receipts);
        }

        // Convert directly without intermediate ReceiptBatch struct
        BlockDataConverter::receipts_to_logs_arrow(all_receipts, &timestamps)
    }

    /// Categorize error for metrics tracking
    pub(crate) fn categorize_error(error: &ErigonBridgeError) -> String {
        let err_str = error.to_string();
        let err_lower = err_str.to_lowercase();

        // Check for known error patterns first
        if err_lower.contains("timeout") || err_lower.contains("timed out") {
            return "timeout".to_string();
        } else if err_lower.contains("header not found") || err_lower.contains("block not found") {
            return "not_found".to_string();
        } else if err_lower.contains("connection") || err_lower.contains("connect") {
            return "connection".to_string();
        } else if err_lower.contains("unavailable") {
            return "unavailable".to_string();
        } else if err_lower.contains("rlp") || err_lower.contains("decoding") {
            return "decode_error".to_string();
        } else if err_lower.contains("validation") {
            return "validation".to_string();
        }

        // For unknown errors, include the actual error message (truncated to avoid extremely long labels)
        // Take the first part up to first colon, or first 80 chars
        let pattern = err_str
            .split(':')
            .next()
            .unwrap_or(&err_str)
            .trim()
            .chars()
            .take(80)
            .collect::<String>();

        format!("unknown:{pattern}")
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
