use crate::client::JsonRpcClient;
use crate::converter::JsonRpcConverter;
use crate::metrics::{BridgeMetrics, SegmentMetrics};
use crate::streaming::StreamingService;
use arrow_flight::{
    encode::FlightDataEncoderBuilder, flight_service_server::FlightService, Action, ActionType,
    Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};
use phaser_server::{
    BridgeCapabilities, BridgeInfo, DiscoveryCapabilities, FlightBridge, GenericQuery,
    GenericQueryMode, StreamType, TableDescriptor,
};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};
use validators_evm::ValidationExecutor;

/// Configuration for segment-based parallel fetching
///
/// ## Architecture: Segments and Batches
///
/// Data fetching is organized into two levels:
/// - **Segments**: Large chunks of blocks (e.g., 10K-500K) processed as units
/// - **Batches**: Smaller groups within a segment (e.g., 50-100 blocks) fetched together
///
/// ```text
/// Total Range: blocks 0 to 100,000
/// ├── Segment 0: blocks 0-9,999        (processed in parallel)
/// │   ├── Batch 0: blocks 0-49         (fetched concurrently)
/// │   ├── Batch 1: blocks 50-99
/// │   └── ... (200 batches per segment)
/// ├── Segment 1: blocks 10,000-19,999  (processed in parallel)
/// └── ... (10 segments total)
/// ```
///
/// ## Node-Specific Tuning
///
/// - **Generic JSON-RPC**: Default settings work well (10K segments, 50 block batches)
/// - **Erigon via JSON-RPC**: Consider `segment_size: 500_000` to align with Erigon snapshots
/// - **Rate-limited nodes**: Reduce `max_concurrent_requests` to avoid 429 errors
///
/// ## Logs vs Blocks/Transactions
///
/// Logs use range-based `eth_getLogs` calls (one call per batch range), while
/// blocks/transactions still fetch per-block (with concurrent requests within batch).
#[derive(Clone, Debug)]
pub struct SegmentConfig {
    /// Size of each segment in blocks
    ///
    /// Segments are the unit of parallel processing and progress tracking.
    /// Default: 10,000 blocks per segment.
    ///
    /// Tuning:
    /// - Larger segments = fewer segment boundaries, less overhead
    /// - Smaller segments = more granular progress updates, better error isolation
    /// - For Erigon, 500,000 matches the snapshot segment size
    pub segment_size: u64,

    /// Maximum segments to process in parallel
    ///
    /// Controls top-level concurrency. Each segment streams independently.
    /// Default: 4 concurrent segments.
    ///
    /// Tuning:
    /// - More segments = higher throughput but more memory/connections
    /// - For memory-constrained environments, reduce to 1-2
    pub max_concurrent_segments: usize,

    /// Blocks per batch within a segment
    ///
    /// Controls granularity of fetching within a segment.
    /// Default: 50 blocks per batch.
    ///
    /// For blocks/transactions: This many blocks are fetched concurrently.
    /// For logs: A single `eth_getLogs` call covers this many blocks.
    ///
    /// Tuning:
    /// - Larger batches = fewer RPC round-trips
    /// - Smaller batches = better progress granularity
    pub blocks_per_batch: usize,

    /// Maximum concurrent RPC requests within a batch
    ///
    /// For blocks/transactions, this limits how many `eth_getBlockByNumber`
    /// calls run simultaneously. For logs, this is ignored (single call per batch).
    /// Default: 50 (same as blocks_per_batch for maximum parallelism).
    ///
    /// Tuning:
    /// - Match to blocks_per_batch for full parallelism
    /// - Reduce for rate-limited or overloaded nodes
    pub max_concurrent_requests: usize,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            segment_size: 10_000,
            max_concurrent_segments: 4,
            blocks_per_batch: 50,
            max_concurrent_requests: 50,
        }
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

/// A bridge that connects to any JSON-RPC compatible node
pub struct JsonRpcFlightBridge {
    client: Arc<JsonRpcClient>,
    chain_id: u64,
    streaming_service: Arc<StreamingService>,
    segment_config: SegmentConfig,
    validator: Option<Arc<dyn ValidationExecutor>>,
    metrics: Option<BridgeMetrics>,
}

impl JsonRpcFlightBridge {
    /// Create a new JSON-RPC bridge
    pub async fn new(
        node_url: String,
        chain_id: Option<u64>,
        validator_config: Option<validators_evm::ExecutorConfig>,
    ) -> std::result::Result<Self, anyhow::Error> {
        let client = Arc::new(JsonRpcClient::connect(&node_url).await?);

        // Use provided chain ID or get from node
        let chain_id = chain_id.unwrap_or_else(|| client.chain_id());

        // Get node version for info
        let node_version = client
            .get_client_version()
            .await
            .unwrap_or_else(|_| "unknown".to_string());
        info!(
            "Connected to node: {} (chain ID: {})",
            node_version, chain_id
        );

        // Build validator if config is provided
        let validator = validator_config.map(|config| {
            let boxed_validator = config.build();
            Arc::from(boxed_validator) as Arc<dyn ValidationExecutor>
        });
        if validator.is_some() {
            info!("Validation enabled");
        }

        // Create and start the streaming service
        let streaming_service = Arc::new(StreamingService::new(client.clone()));

        let service_clone = streaming_service.clone();
        tokio::spawn(async move {
            if let Err(e) = service_clone.start_streaming().await {
                error!("Streaming service error: {}", e);
            }
        });

        // Initialize metrics
        let metrics = BridgeMetrics::new("jsonrpc_bridge", chain_id, "jsonrpc");

        Ok(Self {
            client,
            chain_id,
            streaming_service,
            segment_config: SegmentConfig::default(),
            validator,
            metrics: Some(metrics),
        })
    }

    /// Create a new JSON-RPC bridge with custom segment configuration
    pub async fn with_segment_config(
        node_url: String,
        chain_id: Option<u64>,
        validator_config: Option<validators_evm::ExecutorConfig>,
        segment_config: SegmentConfig,
    ) -> std::result::Result<Self, anyhow::Error> {
        let mut bridge = Self::new(node_url, chain_id, validator_config).await?;
        bridge.segment_config = segment_config;
        Ok(bridge)
    }

    /// Get bridge information
    pub fn bridge_info(&self) -> BridgeInfo {
        let mut capabilities = vec![
            "blocks".to_string(),
            "transactions".to_string(),
            "logs".to_string(),
        ];

        if self.client.supports_subscriptions() {
            capabilities.push("streaming".to_string());
            capabilities.push("subscriptions".to_string());
        } else {
            capabilities.push("polling".to_string());
        }

        BridgeInfo {
            name: "jsonrpc-bridge".to_string(),
            node_type: "json-rpc".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            chain_id: self.chain_id,
            capabilities,
            current_block: 0, // Would need to query this
            oldest_block: 0,  // Would need to query this
        }
    }

    /// Parse a GenericQuery from a Flight Ticket
    fn parse_ticket(ticket: &Ticket) -> Result<GenericQuery, Status> {
        GenericQuery::from_ticket(ticket)
            .map_err(|e| Status::invalid_argument(format!("Invalid query in ticket: {e}")))
    }

    /// Parse a GenericQuery from a FlightDescriptor
    fn parse_descriptor(descriptor: &FlightDescriptor) -> Result<GenericQuery, Status> {
        GenericQuery::from_flight_descriptor(descriptor)
            .map_err(|e| Status::invalid_argument(format!("Invalid query in descriptor: {e}")))
    }

    /// Map table name to StreamType
    fn table_to_stream_type(table: &str) -> Result<StreamType, Status> {
        match table {
            "blocks" => Ok(StreamType::Blocks),
            "transactions" => Ok(StreamType::Transactions),
            "logs" => Ok(StreamType::Logs),
            other => Err(Status::invalid_argument(format!(
                "Unknown table: {other}. Available: blocks, transactions, logs"
            ))),
        }
    }

    /// Get the Arrow schema for a given stream type
    fn get_schema_for_type(
        stream_type: StreamType,
    ) -> Result<Arc<arrow::datatypes::Schema>, Box<Status>> {
        match stream_type {
            StreamType::Blocks => Ok(JsonRpcConverter::block_schema()),
            StreamType::Transactions => Ok(JsonRpcConverter::transaction_schema()),
            StreamType::Logs => Ok(JsonRpcConverter::log_schema()),
            StreamType::Trie => Err(Box::new(Status::unimplemented(
                "Trie streaming not supported via JSON-RPC",
            ))),
        }
    }

    /// Create a stream for historical data (specific block range)
    /// Returns BatchWithRange for phaser-query progress tracking
    ///
    /// Uses segment-level parallelism: multiple segments are processed concurrently,
    /// with parallel block fetching within each segment.
    fn create_historical_stream(
        &self,
        stream_type: StreamType,
        start_block: u64,
        end_block: u64,
        validate: bool,
    ) -> impl Stream<Item = Result<phaser_server::BatchWithRange, Status>> + Send {
        // Split range into segments
        let segments =
            split_into_segments(start_block, end_block, self.segment_config.segment_size);
        let max_concurrent_segments = self.segment_config.max_concurrent_segments;

        info!(
            "Creating historical {:?} stream for blocks {} to {} ({} segments, {} concurrent)",
            stream_type,
            start_block,
            end_block,
            segments.len(),
            max_concurrent_segments
        );

        // Clone values needed for the async closure
        let client = self.client.clone();
        let config = self.segment_config.clone();
        let validator = self.validator.clone();
        let metrics = self.metrics.clone();

        // Process segments in parallel, yielding results in order
        futures::stream::iter(segments)
            .map(move |(seg_start, seg_end)| {
                let client = client.clone();
                let config = config.clone();
                let validator = validator.clone();
                let metrics = metrics.clone();

                async move {
                    // Process this segment and return a stream of batches
                    Self::process_segment(
                        client,
                        seg_start,
                        seg_end,
                        stream_type,
                        validate,
                        validator,
                        metrics,
                        config,
                    )
                }
            })
            .buffered(max_concurrent_segments)
            .flatten()
    }

    /// Process a single segment: fetch blocks in parallel batches and yield results
    ///
    /// For logs, uses optimized range-based fetching (single eth_getLogs call per batch)
    /// instead of per-block fetching, significantly reducing RPC round-trips.
    #[allow(clippy::too_many_arguments)]
    fn process_segment(
        client: Arc<JsonRpcClient>,
        seg_start: u64,
        seg_end: u64,
        stream_type: StreamType,
        validate: bool,
        validator: Option<Arc<dyn ValidationExecutor>>,
        metrics: Option<BridgeMetrics>,
        config: SegmentConfig,
    ) -> Pin<Box<dyn Stream<Item = Result<phaser_server::BatchWithRange, Status>> + Send>> {
        // For logs, use optimized range-based fetching
        if stream_type == StreamType::Logs {
            return Box::pin(Self::process_segment_logs(
                client, seg_start, seg_end, metrics, config,
            ));
        }

        // For blocks/transactions, use parallel per-block fetching
        Box::pin(Self::process_segment_blocks_txs(
            client,
            seg_start,
            seg_end,
            stream_type,
            validate,
            validator,
            metrics,
            config,
        ))
    }

    /// Process segment for logs using range-based eth_getLogs calls
    ///
    /// This is much more efficient than per-block fetching because:
    /// 1. Single RPC call per batch instead of one per block
    /// 2. Logs are returned already sorted by block
    /// 3. No need to fetch block headers for context (log includes block_num, block_hash)
    fn process_segment_logs(
        client: Arc<JsonRpcClient>,
        seg_start: u64,
        seg_end: u64,
        metrics: Option<BridgeMetrics>,
        config: SegmentConfig,
    ) -> impl Stream<Item = Result<phaser_server::BatchWithRange, Status>> + Send {
        let batch_size = config.blocks_per_batch as u64;
        let segment_num = seg_start / config.segment_size;

        async_stream::stream! {
            use alloy_rpc_types_eth::Filter;

            let total_blocks = seg_end - seg_start + 1;
            info!(
                "Segment {}: Processing logs for blocks {} to {} ({} blocks, batch size: {})",
                segment_num, seg_start, seg_end, total_blocks, batch_size
            );

            // Track active workers (mirrors erigon-bridge pattern)
            if let Some(ref m) = metrics {
                m.active_workers_inc("logs");
            }

            let segment_start_time = Instant::now();
            let mut current_block = seg_start;
            let mut total_logs_in_segment: u64 = 0;
            let mut batches_fetched: u64 = 0;

            while current_block <= seg_end {
                let batch_end = std::cmp::min(current_block + batch_size - 1, seg_end);
                let block_range_size = batch_end - current_block + 1;

                debug!(
                    "Segment {}: Fetching logs for block range {} to {} ({} blocks)",
                    segment_num, current_block, batch_end, block_range_size
                );

                // Track active RPC request (similar to grpc_stream_inc in erigon-bridge)
                if let Some(ref m) = metrics {
                    m.grpc_stream_inc("logs");
                }

                // Single eth_getLogs call for the entire batch range
                let filter = Filter::new()
                    .from_block(current_block)
                    .to_block(batch_end);

                let log_start = Instant::now();
                match client.get_logs(filter).await {
                    Ok(logs) => {
                        let fetch_duration_ms = log_start.elapsed().as_millis() as f64;
                        let log_count = logs.len();
                        batches_fetched += 1;

                        if let Some(ref m) = metrics {
                            m.grpc_stream_dec("logs");

                            // Record fetch duration with method indicating range-based fetching
                            m.grpc_request_duration_logs(
                                segment_num,
                                "eth_getLogs_range",
                                fetch_duration_ms,
                            );

                            // Track response size (estimate: ~200 bytes per log on average)
                            // This mirrors grpc_message_size in erigon-bridge
                            let estimated_size = log_count * 200;
                            m.grpc_message_size("logs", estimated_size);
                        }

                        if !logs.is_empty() {
                            total_logs_in_segment += log_count as u64;

                            // Use multi-block conversion - extracts block context from each log
                            match JsonRpcConverter::convert_logs_multi_block(&logs) {
                                Ok(batch) => {
                                    if let Some(ref m) = metrics {
                                        // Track logs processed (worker_id=0 for single-threaded)
                                        m.items_processed_inc(0, segment_num, "logs", log_count as u64);

                                        // Update progress
                                        let blocks_processed = batch_end - seg_start + 1;
                                        let progress_pct = (blocks_processed as f64 / total_blocks as f64) * 100.0;
                                        m.set_worker_progress(0, segment_num, "logs", progress_pct);
                                    }

                                    info!(
                                        "Segment {}: Yielding {} logs for blocks {} to {} ({:.0}ms, {:.1} logs/block)",
                                        segment_num, batch.num_rows(), current_block, batch_end,
                                        fetch_duration_ms,
                                        log_count as f64 / block_range_size as f64
                                    );
                                    yield Ok(phaser_server::BatchWithRange::new(batch, current_block, batch_end));
                                }
                                Err(e) => {
                                    error!("Segment {}: Failed to convert logs: {}", segment_num, e);
                                    if let Some(ref m) = metrics {
                                        m.error("conversion_error", "logs");
                                        m.active_workers_dec("logs");
                                        m.segment_attempt(false);
                                    }
                                    yield Err(Status::internal(format!("Failed to convert logs: {e}")));
                                    return;
                                }
                            }
                        } else {
                            // No logs in this range - still track it for progress
                            debug!(
                                "Segment {}: No logs in block range {} to {} ({:.0}ms)",
                                segment_num, current_block, batch_end, fetch_duration_ms
                            );
                            // Don't yield anything for empty ranges - the range will still
                            // be considered processed based on the next batch's start
                        }
                    }
                    Err(e) => {
                        if let Some(ref m) = metrics {
                            m.grpc_stream_dec("logs");
                        }

                        // Categorize error for better monitoring
                        let error_type = Self::categorize_rpc_error(&e);
                        error!(
                            "Segment {}: Failed to fetch logs for range {} to {}: {} (type: {})",
                            segment_num, current_block, batch_end, e, error_type
                        );
                        if let Some(ref m) = metrics {
                            m.error(&error_type, "logs");
                            m.active_workers_dec("logs");
                            m.segment_attempt(false);
                        }
                        yield Err(Status::internal(format!(
                            "Failed to fetch logs for range {current_block}-{batch_end}: {e}"
                        )));
                        return;
                    }
                }

                current_block = batch_end + 1;
            }

            // Record segment-level metrics (success path only - errors return early)
            let duration = segment_start_time.elapsed();
            if let Some(ref m) = metrics {
                m.segment_duration("logs", duration.as_secs_f64());
                m.active_workers_dec("logs");
                m.segment_attempt(true);
            }

            let logs_per_second = if duration.as_secs_f64() > 0.0 {
                total_logs_in_segment as f64 / duration.as_secs_f64()
            } else {
                0.0
            };

            info!(
                "Segment {}: Completed logs for blocks {} to {} in {:.2}s ({} logs in {} batches, {:.0} logs/sec)",
                segment_num, seg_start, seg_end, duration.as_secs_f64(),
                total_logs_in_segment, batches_fetched, logs_per_second
            );
        }
    }

    /// Categorize RPC error for metrics (mirrors erigon-bridge's categorize_error pattern)
    fn categorize_rpc_error(error: &anyhow::Error) -> String {
        let err_str = error.to_string();
        let err_lower = err_str.to_lowercase();

        if err_lower.contains("timeout") || err_lower.contains("timed out") {
            "timeout".to_string()
        } else if err_lower.contains("connection") || err_lower.contains("connect") {
            "connection".to_string()
        } else if err_lower.contains("rate limit") || err_lower.contains("429") {
            "rate_limit".to_string()
        } else if err_lower.contains("not found") || err_lower.contains("404") {
            "not_found".to_string()
        } else if err_lower.contains("server error") || err_lower.contains("500") {
            "server_error".to_string()
        } else if err_lower.contains("invalid") || err_lower.contains("parse") {
            "invalid_response".to_string()
        } else {
            // For unknown errors, include truncated message
            let pattern = err_str
                .split(':')
                .next()
                .unwrap_or(&err_str)
                .trim()
                .chars()
                .take(50)
                .collect::<String>();
            format!("unknown:{pattern}")
        }
    }

    /// Process segment for blocks/transactions using parallel per-block fetching
    ///
    /// Fetches blocks concurrently using `max_concurrent_requests` to limit parallelism.
    /// Each block requires a separate `eth_getBlockByNumber` RPC call.
    #[allow(clippy::too_many_arguments)]
    fn process_segment_blocks_txs(
        client: Arc<JsonRpcClient>,
        seg_start: u64,
        seg_end: u64,
        stream_type: StreamType,
        validate: bool,
        validator: Option<Arc<dyn ValidationExecutor>>,
        metrics: Option<BridgeMetrics>,
        config: SegmentConfig,
    ) -> impl Stream<Item = Result<phaser_server::BatchWithRange, Status>> + Send {
        let batch_size = config.blocks_per_batch as u64;
        let max_concurrent = config.max_concurrent_requests;
        let segment_num = seg_start / config.segment_size;

        let data_type = match stream_type {
            StreamType::Blocks => "blocks",
            StreamType::Transactions => "transactions",
            StreamType::Logs => "logs",
            StreamType::Trie => "trie",
        };

        async_stream::stream! {
            use arrow::compute::concat_batches;
            use futures::stream::{self as fstream, StreamExt as FuturesStreamExt};

            let total_blocks = seg_end - seg_start + 1;
            info!(
                "Segment {}: Processing {} for blocks {} to {} ({} blocks, batch size: {}, concurrency: {})",
                segment_num, data_type, seg_start, seg_end, total_blocks, batch_size, max_concurrent
            );

            // Track active workers (mirrors erigon-bridge pattern)
            if let Some(ref m) = metrics {
                m.active_workers_inc(data_type);
            }

            let segment_start_time = Instant::now();
            let mut current_block = seg_start;
            let mut total_items: u64 = 0;
            let mut batches_yielded: u64 = 0;

            while current_block <= seg_end {
                let batch_end = std::cmp::min(current_block + batch_size - 1, seg_end);
                let batch_count = (batch_end - current_block + 1) as usize;

                debug!(
                    "Segment {}: Fetching {} blocks {} to {} ({} blocks)",
                    segment_num, data_type, current_block, batch_end, batch_count
                );

                // Create block range iterator
                let block_range: Vec<u64> = (current_block..=batch_end).collect();

                // Track active RPC requests
                if let Some(ref m) = metrics {
                    m.grpc_stream_inc(data_type);
                }

                let batch_start = Instant::now();

                // Fetch blocks in parallel using buffered stream
                let client_clone = client.clone();
                let validator_clone = validator.clone();
                let stream_type_clone = stream_type;
                let metrics_clone = metrics.clone();

                let results: Vec<Result<Option<arrow::record_batch::RecordBatch>, Status>> = fstream::iter(block_range)
                    .map(move |block_num| {
                        let client = client_clone.clone();
                        let validator = validator_clone.clone();
                        let metrics = metrics_clone.clone();
                        async move {
                            Self::fetch_and_convert_block(
                                &client,
                                block_num,
                                stream_type_clone,
                                validate,
                                validator.as_ref(),
                                metrics.as_ref(),
                                segment_num,
                            ).await
                        }
                    })
                    .buffered(max_concurrent)
                    .collect()
                    .await;

                let batch_duration_ms = batch_start.elapsed().as_millis() as f64;

                if let Some(ref m) = metrics {
                    m.grpc_stream_dec(data_type);
                }

                // Process results - collect successful batches and report errors
                let mut record_batches = Vec::with_capacity(batch_count);
                let mut error_count = 0;

                for result in results {
                    match result {
                        Ok(Some(batch)) => {
                            total_items += batch.num_rows() as u64;
                            record_batches.push(batch);
                        }
                        Ok(None) => {
                            // Block had no data (e.g., empty transactions) - skip
                        }
                        Err(e) => {
                            // Log but continue - we want to process as many blocks as possible
                            let error_type = Self::categorize_rpc_error_from_status(&e);
                            error!("Segment {}: Error fetching block: {} (type: {})", segment_num, e, error_type);
                            if let Some(ref m) = metrics {
                                m.error(&error_type, data_type);
                            }
                            error_count += 1;
                        }
                    }
                }

                // If we collected any batches, concatenate and yield them wrapped with range
                if !record_batches.is_empty() {
                    let schema = record_batches[0].schema();
                    match concat_batches(&schema, &record_batches) {
                        Ok(combined_batch) => {
                            if let Some(ref m) = metrics {
                                // Track items processed
                                m.items_processed_inc(0, segment_num, data_type, combined_batch.num_rows() as u64);

                                // Update progress
                                let blocks_processed = batch_end - seg_start + 1;
                                let progress_pct = (blocks_processed as f64 / total_blocks as f64) * 100.0;
                                m.set_worker_progress(0, segment_num, data_type, progress_pct);
                            }

                            batches_yielded += 1;
                            info!(
                                "Segment {}: Yielding {} rows for blocks {} to {} ({:.0}ms, {} errors)",
                                segment_num, combined_batch.num_rows(), current_block, batch_end,
                                batch_duration_ms, error_count
                            );
                            yield Ok(phaser_server::BatchWithRange::new(combined_batch, current_block, batch_end));
                        }
                        Err(e) => {
                            error!("Segment {}: Failed to concatenate batches: {}", segment_num, e);
                            if let Some(ref m) = metrics {
                                m.error("concatenate_error", data_type);
                                m.active_workers_dec(data_type);
                                m.segment_attempt(false);
                            }
                            yield Err(Status::internal(format!("Failed to concatenate batches: {e}")));
                            return;
                        }
                    }
                } else if error_count > 0 {
                    // All blocks had errors - report the batch as failed
                    error!(
                        "Segment {}: All {} blocks failed in range {}-{}",
                        segment_num, batch_count, current_block, batch_end
                    );
                    if let Some(ref m) = metrics {
                        m.error("all_blocks_failed", data_type);
                        m.active_workers_dec(data_type);
                        m.segment_attempt(false);
                    }
                    yield Err(Status::internal(format!(
                        "Segment {segment_num}: Failed to fetch any blocks in range {current_block}-{batch_end}"
                    )));
                    return;
                }

                current_block = batch_end + 1;
            }

            // Record segment-level metrics (success path only - errors return early)
            let duration = segment_start_time.elapsed();
            if let Some(ref m) = metrics {
                m.segment_duration(data_type, duration.as_secs_f64());
                m.active_workers_dec(data_type);
                m.segment_attempt(true);
            }

            let items_per_second = if duration.as_secs_f64() > 0.0 {
                total_items as f64 / duration.as_secs_f64()
            } else {
                0.0
            };

            info!(
                "Segment {}: Completed {} for blocks {} to {} in {:.2}s ({} items in {} batches, {:.0} items/sec)",
                segment_num, data_type, seg_start, seg_end, duration.as_secs_f64(),
                total_items, batches_yielded, items_per_second
            );
        }
    }

    /// Categorize error from Status for metrics
    fn categorize_rpc_error_from_status(status: &Status) -> String {
        let msg = status.message().to_lowercase();
        if msg.contains("timeout") || msg.contains("timed out") {
            "timeout".to_string()
        } else if msg.contains("connection") || msg.contains("connect") {
            "connection".to_string()
        } else if msg.contains("rate limit") || msg.contains("429") {
            "rate_limit".to_string()
        } else if msg.contains("not found") || msg.contains("404") {
            "not_found".to_string()
        } else if msg.contains("server error") || msg.contains("500") {
            "server_error".to_string()
        } else {
            "rpc_error".to_string()
        }
    }

    /// Fetch a single block and convert it to RecordBatch based on stream type
    async fn fetch_and_convert_block(
        client: &JsonRpcClient,
        block_num: u64,
        stream_type: StreamType,
        validate: bool,
        validator: Option<&Arc<dyn ValidationExecutor>>,
        metrics: Option<&BridgeMetrics>,
        segment_num: u64,
    ) -> Result<Option<arrow::record_batch::RecordBatch>, Status> {
        use alloy::eips::BlockNumberOrTag;
        use alloy_rpc_types_eth::Filter;

        // Fetch block with transactions
        let start = Instant::now();
        let block = match client
            .get_block_with_txs(BlockNumberOrTag::Number(block_num))
            .await
        {
            Ok(Some(block)) => {
                if let Some(m) = metrics {
                    m.grpc_request_duration_blocks(
                        segment_num,
                        "eth_getBlockByNumber",
                        start.elapsed().as_millis() as f64,
                    );
                }
                block
            }
            Ok(None) => {
                error!("Block #{} not found", block_num);
                if let Some(m) = metrics {
                    m.error("not_found", "blocks");
                }
                return Err(Status::not_found(format!("Block {block_num} not found")));
            }
            Err(e) => {
                error!("Failed to fetch block #{}: {}", block_num, e);
                if let Some(m) = metrics {
                    m.error("fetch_error", "blocks");
                }
                return Err(Status::internal(format!(
                    "Failed to fetch block {block_num}: {e}"
                )));
            }
        };

        match stream_type {
            StreamType::Blocks => {
                // Convert block header to RecordBatch
                match evm_common::rpc_conversions::convert_any_header(&block.header) {
                    Ok(batch) => Ok(Some(batch)),
                    Err(e) => {
                        error!("Failed to convert block header #{}: {}", block_num, e);
                        Err(Status::internal(format!("Conversion error: {e}")))
                    }
                }
            }
            StreamType::Transactions => {
                // Convert transactions (if any)
                if block.transactions.is_empty() {
                    return Ok(None);
                }

                // Validate transactions if requested and validator is available
                if validate {
                    if let Some(val) = validator {
                        let block_record =
                            evm_common::rpc_conversions::convert_any_header_to_record(
                                &block.header,
                            );

                        match evm_common::rpc_conversions::extract_transaction_records(&block) {
                            Ok(tx_records) => {
                                match val.spawn_validate_records(block_record, tx_records).await {
                                    Ok(()) => {
                                        debug!(
                                            "Validated {} transactions for block #{}",
                                            block.transactions.len(),
                                            block_num
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "Transaction validation failed for block #{}: {}",
                                            block_num, e
                                        );
                                        return Err(Status::internal(format!(
                                            "Validation error for block {block_num}: {e}"
                                        )));
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to extract transaction records for validation: {}",
                                    e
                                );
                                return Err(Status::internal(format!(
                                    "Failed to extract records for validation: {e}"
                                )));
                            }
                        }
                    } else {
                        error!("Validation requested but validator not configured");
                        return Err(Status::failed_precondition(
                            "Validation requested but validator not configured",
                        ));
                    }
                }

                // Convert to RecordBatch
                match JsonRpcConverter::convert_transactions(&block) {
                    Ok(batch) => Ok(Some(batch)),
                    Err(e) => {
                        error!(
                            "Failed to convert transactions for block #{}: {}",
                            block_num, e
                        );
                        Err(Status::internal(format!("Conversion error: {e}")))
                    }
                }
            }
            StreamType::Logs => {
                // Fetch and convert logs
                let filter = Filter::new().from_block(block_num).to_block(block_num);

                let log_start = Instant::now();
                match client.get_logs(filter).await {
                    Ok(logs) if !logs.is_empty() => {
                        if let Some(m) = metrics {
                            m.grpc_request_duration_logs(
                                segment_num,
                                "eth_getLogs",
                                log_start.elapsed().as_millis() as f64,
                            );
                        }
                        let block_hash = block.header.hash;
                        match JsonRpcConverter::convert_logs(
                            &logs,
                            block_num,
                            block_hash,
                            block.header.timestamp,
                        ) {
                            Ok(batch) => Ok(Some(batch)),
                            Err(e) => {
                                error!("Failed to convert logs for block #{}: {}", block_num, e);
                                Err(Status::internal(format!("Conversion error: {e}")))
                            }
                        }
                    }
                    Ok(_) => {
                        // No logs in this block
                        if let Some(m) = metrics {
                            m.grpc_request_duration_logs(
                                segment_num,
                                "eth_getLogs",
                                log_start.elapsed().as_millis() as f64,
                            );
                        }
                        Ok(None)
                    }
                    Err(e) => {
                        error!("Failed to fetch logs for block #{}: {}", block_num, e);
                        if let Some(m) = metrics {
                            m.error("fetch_error", "logs");
                        }
                        Err(Status::internal(format!("Failed to fetch logs: {e}")))
                    }
                }
            }
            StreamType::Trie => Err(Status::unimplemented(
                "Trie streaming not supported via JSON-RPC",
            )),
        }
    }
}

#[async_trait]
impl FlightBridge for JsonRpcFlightBridge {
    async fn get_info(&self) -> std::result::Result<BridgeInfo, Status> {
        Ok(self.bridge_info())
    }

    async fn get_capabilities(&self) -> std::result::Result<BridgeCapabilities, Status> {
        Ok(BridgeCapabilities {
            supports_historical: true, // Can fetch historical blocks via JSON-RPC
            supports_streaming: true, // Always support streaming (HTTP uses polling, WS/IPC use subscriptions)
            supports_reorg_notifications: false,
            supports_filters: true,
            supports_validation: self.validator.is_some(),
            max_batch_size: self.segment_config.blocks_per_batch,
        })
    }

    async fn get_discovery_capabilities(&self) -> Result<DiscoveryCapabilities, Status> {
        // Query current block from node
        let current_block = self.client.get_block_number().await.unwrap_or(0);

        // Get node capabilities (probed at connection time)
        let caps = self.client.capabilities();

        // Define available tables (JSON-RPC doesn't support trie)
        // If eth_getBlockReceipts is supported, we could add a "receipts" table
        let mut tables = vec![
            TableDescriptor::new("blocks", "_block_num")
                .with_modes(vec!["historical", "live"])
                .with_sorted_by(vec!["_block_num"]),
            TableDescriptor::new("transactions", "_block_num")
                .with_modes(vec!["historical", "live"])
                .with_sorted_by(vec!["_block_num", "_tx_idx"]),
            TableDescriptor::new("logs", "_block_num")
                .with_modes(vec!["historical", "live"])
                .with_sorted_by(vec!["_block_num", "_tx_idx", "_log_idx"]),
        ];

        // Add receipts table if eth_getBlockReceipts is supported
        if caps.supports_block_receipts {
            tables.push(
                TableDescriptor::new("receipts", "_block_num")
                    .with_modes(vec!["historical"])
                    .with_sorted_by(vec!["_block_num", "_tx_idx"]),
            );
        }

        // Build metadata from node capabilities
        let mut metadata = std::collections::HashMap::new();

        // Chain info
        metadata.insert(
            "chain_id".to_string(),
            serde_json::Value::Number(self.chain_id.into()),
        );

        // Transport capabilities
        metadata.insert(
            "supports_subscriptions".to_string(),
            serde_json::Value::Bool(self.client.supports_subscriptions()),
        );

        // Node capabilities (from probing at connection time)
        metadata.insert(
            "client_version".to_string(),
            serde_json::Value::String(caps.client_version.clone()),
        );
        metadata.insert(
            "supports_block_receipts".to_string(),
            serde_json::Value::Bool(caps.supports_block_receipts),
        );
        metadata.insert(
            "supports_debug_namespace".to_string(),
            serde_json::Value::Bool(caps.supports_debug_namespace),
        );

        // Query hints for clients
        if let Some(max_range) = caps.max_logs_block_range {
            metadata.insert(
                "max_logs_block_range".to_string(),
                serde_json::Value::Number(max_range.into()),
            );
        }
        metadata.insert(
            "recommended_logs_batch_size".to_string(),
            serde_json::Value::Number(self.client.recommended_logs_batch_size().into()),
        );

        // Node type hints
        if caps.is_erigon {
            metadata.insert(
                "node_type".to_string(),
                serde_json::Value::String("erigon".to_string()),
            );
        } else if caps.is_geth {
            metadata.insert(
                "node_type".to_string(),
                serde_json::Value::String("geth".to_string()),
            );
        }
        metadata.insert(
            "is_managed_provider".to_string(),
            serde_json::Value::Bool(caps.is_managed_provider),
        );

        Ok(DiscoveryCapabilities {
            name: "jsonrpc-bridge".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            protocol: "evm".to_string(),
            position_label: "block_number".to_string(),
            current_position: current_block,
            oldest_position: 0,
            tables,
            metadata,
        })
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<
        Response<
            Pin<Box<dyn Stream<Item = std::result::Result<HandshakeResponse, Status>> + Send>>,
        >,
        Status,
    > {
        // Simple handshake - return bridge info
        let response = HandshakeResponse {
            protocol_version: 1,
            payload: serde_json::to_vec(&self.bridge_info())
                .map_err(|e| Status::internal(format!("Failed to serialize bridge info: {e}")))?
                .into(),
        };

        let stream = stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<
        Response<Pin<Box<dyn Stream<Item = std::result::Result<FlightInfo, Status>> + Send>>>,
        Status,
    > {
        // List available stream types
        // Only include stream types that JSON-RPC supports
        let mut info_streams = vec![];
        if let Ok(info) = create_flight_info(StreamType::Blocks) {
            info_streams.push(info);
        }
        if let Ok(info) = create_flight_info(StreamType::Transactions) {
            info_streams.push(info);
        }
        if let Ok(info) = create_flight_info(StreamType::Logs) {
            info_streams.push(info);
        }

        let stream = stream::iter(info_streams.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let query = Self::parse_descriptor(&descriptor)?;
        let stream_type = Self::table_to_stream_type(&query.table)?;

        let info = create_flight_info(stream_type).map_err(|e| *e)?;

        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        use arrow_flight::SchemaAsIpc;
        use arrow_ipc::writer::IpcWriteOptions;

        let descriptor = request.into_inner();
        let query = Self::parse_descriptor(&descriptor)?;
        let stream_type = Self::table_to_stream_type(&query.table)?;
        let schema = Self::get_schema_for_type(stream_type).map_err(|e| *e)?;

        // Convert Arrow schema to IPC format for Flight SchemaResult
        let options = IpcWriteOptions::default();
        let schema_result: SchemaResult = SchemaAsIpc::new(&schema, &options).try_into().map_err(
            |e: arrow::error::ArrowError| {
                Status::internal(format!("Failed to encode schema: {}", e))
            },
        )?;

        Ok(Response::new(schema_result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<
        Response<Pin<Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        let ticket = request.into_inner();

        // Parse the GenericQuery from the ticket
        let query = Self::parse_ticket(&ticket)?;
        let stream_type = Self::table_to_stream_type(&query.table)?;

        info!(
            "Processing do_get for table '{}' ({:?}) with mode {:?}",
            query.table, stream_type, query.mode
        );

        // Get schema for the stream type
        let schema = Self::get_schema_for_type(stream_type).map_err(|e| *e)?;

        // Build the batch stream based on query mode
        // Historical streams return BatchWithRange, live streams return plain RecordBatch
        let batch_stream: Pin<
            Box<dyn Stream<Item = Result<phaser_server::BatchWithRange, Status>> + Send>,
        > = match query.mode {
            GenericQueryMode::Range { start, end } => {
                info!(
                    "Creating historical stream for {:?} positions {}-{}",
                    stream_type, start, end
                );
                Box::pin(self.create_historical_stream(
                    stream_type,
                    start,
                    end,
                    false, // No validation via generic query
                ))
            }
            GenericQueryMode::Snapshot { at } => {
                info!("Creating snapshot at position {}", at);
                Box::pin(self.create_historical_stream(stream_type, at, at, false))
            }
            GenericQueryMode::Live => {
                // Live streaming - subscribe to broadcast channels
                let receiver = match stream_type {
                    StreamType::Blocks => self.streaming_service.subscribe_blocks(),
                    StreamType::Transactions => self.streaming_service.subscribe_transactions(),
                    StreamType::Logs => self.streaming_service.subscribe_logs(),
                    StreamType::Trie => {
                        return Err(Status::unimplemented(
                            "Trie streaming not supported via JSON-RPC",
                        ))
                    }
                };

                // Wrap live batches with placeholder range (0,0) since they're real-time
                Box::pin(async_stream::stream! {
                    let mut rx = receiver;
                    while let Ok(batch) = rx.recv().await {
                        yield Ok(phaser_server::BatchWithRange::new(batch, 0, 0));
                    }
                })
            }
        };

        // Manually construct FlightData to include app_metadata with responsibility ranges
        // This mirrors the erigon-bridge approach for phaser-query compatibility
        use arrow::ipc::writer::IpcWriteOptions;
        use arrow_flight::utils::batches_to_flight_data;

        let flight_stream = async_stream::stream! {
            // First, send the schema
            let schema_flight_data: FlightData = arrow_flight::SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
                .into();
            yield Ok(schema_flight_data);

            // Then stream batches with app_metadata containing responsibility ranges
            let mut batch_stream = batch_stream;
            while let Some(batch_result) = batch_stream.next().await {
                match batch_result {
                    Ok(batch_with_range) => {
                        // Encode the batch metadata (responsibility range)
                        let metadata = match batch_with_range.encode_metadata() {
                            Ok(m) => m,
                            Err(e) => {
                                error!("Failed to encode batch metadata: {}", e);
                                yield Err(Status::internal(format!("Metadata encoding error: {e}")));
                                continue;
                            }
                        };

                        // Convert RecordBatch to FlightData using arrow-flight utilities
                        let batches = vec![batch_with_range.batch];
                        match batches_to_flight_data(&schema, batches) {
                            Ok(flight_data_vec) => {
                                // batches_to_flight_data includes a schema message as the first element
                                // Skip it since we already sent the schema
                                let data_messages: Vec<_> = flight_data_vec
                                    .into_iter()
                                    .skip(1) // Skip the schema message
                                    .collect();

                                // Attach app_metadata to each FlightData
                                for mut flight_data in data_messages {
                                    flight_data.app_metadata = metadata.clone().into();
                                    yield Ok(flight_data);
                                }
                            }
                            Err(e) => {
                                error!("Error encoding batch to flight data: {}", e);
                                yield Err(Status::internal(format!("Batch encoding error: {e}")));
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error in batch stream: {}", e);
                        yield Err(Status::internal(format!("Stream error: {e}")));
                    }
                }
            }
        };

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<
        Response<Pin<Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        // Parse the initial descriptor from the stream
        let mut stream = request.into_inner();

        // Get the first message which should contain the descriptor
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty stream"))?
            .map_err(|e| Status::internal(format!("Stream error: {e}")))?;

        let stream_type = if let Some(desc) = first.flight_descriptor {
            Self::parse_descriptor(&desc)
                .and_then(|q| Self::table_to_stream_type(&q.table))
                .unwrap_or(StreamType::Blocks) // Safe fallback for failed descriptor parsing
        } else {
            StreamType::Blocks
        };

        info!("Starting data stream for {:?}", stream_type);

        // Subscribe to the appropriate stream
        let receiver = match stream_type {
            StreamType::Blocks => self.streaming_service.subscribe_blocks(),
            StreamType::Transactions => self.streaming_service.subscribe_transactions(),
            StreamType::Logs => self.streaming_service.subscribe_logs(),
            StreamType::Trie => {
                return Err(Status::unimplemented(
                    "Trie streaming not supported via JSON-RPC",
                ))
            }
        };

        let schema = Self::get_schema_for_type(stream_type).map_err(|e| *e)?;

        // Create a stream of RecordBatches from the receiver
        let batch_stream = async_stream::stream! {
            let mut rx = receiver;
            while let Ok(batch) = rx.recv().await {
                yield Ok(batch);
            }
        };

        // Create encoder for the entire stream
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);

        // Convert the encoder stream to the response format
        let flight_stream = encoder.map(|result| {
            result.map_err(|e| {
                error!("Error encoding flight data: {}", e);
                Status::internal(format!("Encoding error: {e}"))
            })
        });

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn health_check(&self) -> std::result::Result<bool, Status> {
        // Simple health check - try to get block number
        match self.client.get_block_number().await {
            Ok(_) => Ok(true),
            Err(e) => {
                error!("Health check failed: {}", e);
                Ok(false)
            }
        }
    }
}

#[async_trait]
impl FlightService for JsonRpcFlightBridge {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = std::result::Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = std::result::Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = std::result::Result<PutResult, Status>> + Send>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = std::result::Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = std::result::Result<ActionType, Status>> + Send>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        <Self as FlightBridge>::handshake(self, request).await
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        <Self as FlightBridge>::list_flights(self, request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        <Self as FlightBridge>::get_flight_info(self, request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        <Self as FlightBridge>::do_get(self, request).await
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported"))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        <Self as FlightBridge>::do_exchange(self, request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        <Self as FlightBridge>::do_action(self, request).await
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        let actions = <Self as FlightBridge>::list_actions(self).await?;
        let stream = stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let query = Self::parse_descriptor(&descriptor)?;
        let stream_type = Self::table_to_stream_type(&query.table)?;
        let schema = Self::get_schema_for_type(stream_type).map_err(|e| *e)?;

        // Convert Schema to IPC format
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_as_ipc = arrow_flight::SchemaAsIpc {
            pair: (&*schema, &options),
        };

        let schema_result = schema_as_ipc
            .try_into()
            .map_err(|e: arrow::error::ArrowError| {
                Status::internal(format!("Failed to encode schema: {e}"))
            })?;

        Ok(Response::new(schema_result))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }
}

fn create_flight_info(stream_type: StreamType) -> Result<FlightInfo, Box<Status>> {
    let schema = JsonRpcFlightBridge::get_schema_for_type(stream_type)?;

    // For discovery, use the stream type as a simple string descriptor
    let stream_type_str =
        serde_json::to_string(&stream_type).expect("StreamType should always serialize to JSON");
    let descriptor = FlightDescriptor::new_path(vec![stream_type_str]);

    Ok(FlightInfo::new()
        .with_descriptor(descriptor)
        .try_with_schema(&schema)
        .expect("Schema should always be valid for FlightInfo")
        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(vec![])))
        .with_total_records(0)
        .with_total_bytes(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_into_segments() {
        // Test full segments
        let segments = split_into_segments(0, 29_999, 10_000);
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0], (0, 9_999));
        assert_eq!(segments[1], (10_000, 19_999));
        assert_eq!(segments[2], (20_000, 29_999));
    }

    #[test]
    fn test_split_partial_segment() {
        // Test partial last segment
        let segments = split_into_segments(0, 15_000, 10_000);
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0], (0, 9_999));
        assert_eq!(segments[1], (10_000, 15_000));
    }

    #[test]
    fn test_split_single_segment() {
        // Test range smaller than segment size
        let segments = split_into_segments(0, 5_000, 10_000);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0], (0, 5_000));
    }

    #[test]
    fn test_split_unaligned_start() {
        // Test starting from an unaligned block
        let segments = split_into_segments(5_000, 25_000, 10_000);
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0], (5_000, 14_999));
        assert_eq!(segments[1], (15_000, 24_999));
        assert_eq!(segments[2], (25_000, 25_000));
    }

    #[test]
    fn test_split_single_block() {
        // Test single block range
        let segments = split_into_segments(100, 100, 10_000);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0], (100, 100));
    }
}
