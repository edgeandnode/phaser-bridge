use arrow_flight::{
    encode::FlightDataEncoderBuilder, Criteria, FlightData, FlightDescriptor, FlightEndpoint,
    FlightInfo, HandshakeRequest, HandshakeResponse, SchemaResult, Ticket,
};
use async_trait::async_trait;
use futures::{stream, Stream, StreamExt as FuturesStreamExt};
use phaser_bridge::{
    bridge::{BridgeCapabilities, FlightBridge},
    descriptors::{BridgeInfo, StreamType},
};
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info};
use validators_evm::ValidationExecutor;

use crate::blockdata_client::BlockDataClient;
use crate::blockdata_converter::BlockDataConverter;
use crate::client::ErigonClient;
use crate::converter::ErigonDataConverter;
use crate::error::ErigonBridgeError;
use crate::segment_worker::{split_into_segments, SegmentConfig, SegmentWorker};
use crate::streaming_service::StreamingService;
use crate::trie_client::TrieClient;
use crate::trie_converter;
use std::sync::Arc;
use tonic::Status as TonicStatus;

/// A stateless bridge that translates between Erigon gRPC and Arrow Flight
pub struct ErigonFlightBridge {
    client: Arc<tokio::sync::Mutex<ErigonClient>>,
    blockdata_client: Arc<tokio::sync::Mutex<BlockDataClient>>,
    trie_client: Option<Arc<tokio::sync::Mutex<TrieClient>>>,
    chain_id: u64,
    streaming_service: Arc<StreamingService>,
    validator: Option<Arc<dyn ValidationExecutor>>,
    segment_config: SegmentConfig,
    endpoint: String,
}

impl ErigonFlightBridge {
    pub async fn new(
        endpoint: String,
        chain_id: u64,
        validator_config: Option<validators_evm::ExecutorConfig>,
        segment_config: Option<SegmentConfig>,
    ) -> Result<Self, anyhow::Error> {
        let client = ErigonClient::connect(endpoint.clone()).await?;

        // Try to connect to the TrieBackend service (custom Erigon only)
        // This is optional - if not available, trie streaming won't work
        let trie_client = match TrieClient::connect(endpoint.clone()).await {
            Ok(mut client) => {
                // Test the connection to verify TrieBackend is available
                match client.test_connection().await {
                    Ok(()) => {
                        info!("TrieBackend service available at {}", endpoint);
                        Some(Arc::new(tokio::sync::Mutex::new(client)))
                    }
                    Err(e) => {
                        info!("TrieBackend service not available: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                info!("Could not connect to TrieBackend service: {}", e);
                None
            }
        };

        // Create BlockDataClient for historical queries
        let blockdata_client = BlockDataClient::connect(endpoint.clone()).await?;

        // Build validator if config is provided
        let validator = validator_config.map(|config| {
            let boxed_validator = config.build();
            Arc::from(boxed_validator) as Arc<dyn ValidationExecutor>
        });
        if validator.is_some() {
            info!("Validation enabled");
        }

        // Create the streaming service for live subscriptions
        let streaming_service = Arc::new(StreamingService::new(client.clone()));

        // Start the streaming service (handles blocks, transactions, and logs)
        let service_clone = streaming_service.clone();
        tokio::spawn(async move {
            if let Err(e) = service_clone.start_streaming().await {
                error!("Streaming service error: {}", e);
            }
        });

        Ok(Self {
            client: Arc::new(tokio::sync::Mutex::new(client)),
            blockdata_client: Arc::new(tokio::sync::Mutex::new(blockdata_client)),
            trie_client,
            chain_id,
            streaming_service,
            validator,
            segment_config: segment_config.unwrap_or_default(),
            endpoint,
        })
    }

    pub fn bridge_info(&self) -> BridgeInfo {
        BridgeInfo {
            name: "erigon-bridge".to_string(),
            node_type: "erigon".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            chain_id: self.chain_id,
            capabilities: vec!["streaming".to_string()],
            current_block: 0, // Would need to query this from Erigon
            oldest_block: 0,  // Would need to query this from Erigon
        }
    }

    /// Parse a FlightDescriptor to extract the BlockchainDescriptor
    fn parse_descriptor(
        descriptor: &FlightDescriptor,
    ) -> Result<phaser_bridge::descriptors::BlockchainDescriptor, TonicStatus> {
        if let Some(first) = descriptor.path.first() {
            serde_json::from_str::<phaser_bridge::descriptors::BlockchainDescriptor>(first)
                .map_err(|e| TonicStatus::invalid_argument(format!("Invalid descriptor: {}", e)))
        } else {
            Err(TonicStatus::invalid_argument("Empty descriptor path"))
        }
    }

    /// Create a stream of trie node batches
    async fn create_trie_stream(
        &self,
    ) -> Result<
        impl Stream<Item = Result<arrow_array::RecordBatch, arrow_flight::error::FlightError>> + Send,
        Status,
    > {
        info!("Starting trie stream creation");
        use futures::StreamExt;

        let trie_client = self
            .trie_client
            .as_ref()
            .ok_or_else(|| Status::unavailable("TrieBackend service not available"))?
            .clone();

        let stream = async_stream::stream! {
            let mut client = trie_client.lock().await;

            info!("Streaming commitment nodes from Erigon TrieBackend");
            // Stream all commitment nodes without a specific state root
            // This tests the CommitmentIterator path
            let mut node_stream = match client.stream_commitment_nodes(None, 0, 0, 1000).await {
                Ok(stream) => stream,
                Err(e) => {
                    error!("Failed to start trie streaming: {}", e);
                    return;
                }
            };

            while let Some(result) = node_stream.next().await {
                let batch = match result {
                    Ok(batch) => batch,
                    Err(e) => {
                        error!("Stream error: {}", e);
                        break;
                    }
                };

                // Convert protobuf batch to Arrow RecordBatch
                match trie_converter::convert_trie_batch(batch) {
                    Ok(record_batch) => yield Ok(record_batch),
                    Err(e) => {
                        error!("Failed to convert trie batch: {}", e);
                        break;
                    }
                }
            }
        };

        Ok(stream)
    }

    /// Check if an error is transient and should be retried
    fn is_transient_error(err: &ErigonBridgeError) -> bool {
        match err {
            ErigonBridgeError::ErigonClient(e) => {
                let err_str = e.to_string();
                err_str.contains("txn 0 already rollback")
                    || err_str.contains("Timeout expired")
                    || err_str.contains("connection")
                    || err_str.contains("Cancelled")
            }
            _ => false,
        }
    }

    /// Process transactions using segment-based workers
    ///
    /// Extracted as a separate function to avoid lifetime capture issues
    fn process_transactions_with_segments(
        blockdata_client: BlockDataClient,
        config: SegmentConfig,
        validator: Option<Arc<dyn ValidationExecutor>>,
        start: u64,
        end: u64,
        validate: bool,
    ) -> impl Stream<Item = Result<arrow_array::RecordBatch, arrow_flight::error::FlightError>> + Send
    {
        let max_concurrent = config.max_concurrent_segments;
        let should_validate = validate && validator.is_some();

        // Split range into segments
        let segments = split_into_segments(start, end, config.segment_size);

        // Process segments in parallel but emit results in order
        futures::stream::iter(segments)
            .map(move |(seg_start, seg_end): (u64, u64)| {
                // Derive worker_id from segment to make it unique across all requests
                let worker_id = (seg_start / config.segment_size) as usize;
                let client = blockdata_client.clone(); // Clone shares the underlying HTTP/2 connection
                let config = config.clone();
                let validator = if should_validate {
                    validator.clone()
                } else {
                    None
                };

                async move {
                    let worker = SegmentWorker::new(
                        worker_id,
                        seg_start,
                        seg_end,
                        client,
                        config,
                        validator,
                    );

                    // Convert the stream to handle FlightError
                    worker.process().map(move |result| {
                        result.map_err(|e| {
                            error!(
                                "Segment {}-{}: Processing failed: {}",
                                seg_start, seg_end, e
                            );
                            arrow_flight::error::FlightError::ExternalError(Box::new(
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("Segment {}-{} failed: {}", seg_start, seg_end, e),
                                ),
                            ))
                        })
                    })
                }
            })
            .buffered(max_concurrent)
            .flatten()
    }

    /// Process logs using segment-based workers
    ///
    /// Extracted as a separate function to avoid lifetime capture issues
    fn process_logs_with_segments(
        blockdata_client: BlockDataClient,
        config: SegmentConfig,
        validator: Option<Arc<dyn ValidationExecutor>>,
        start: u64,
        end: u64,
        validate: bool,
    ) -> impl Stream<Item = Result<arrow_array::RecordBatch, arrow_flight::error::FlightError>> + Send
    {
        let max_concurrent = config.max_concurrent_segments;
        let should_validate = validate && validator.is_some();

        // Split range into segments
        let segments = split_into_segments(start, end, config.segment_size);

        // Process segments in parallel but emit results in order
        futures::stream::iter(segments)
            .map(move |(seg_start, seg_end): (u64, u64)| {
                // Derive worker_id from segment to make it unique across all requests
                let worker_id = (seg_start / config.segment_size) as usize;
                let client = blockdata_client.clone(); // Clone shares the underlying HTTP/2 connection
                let config = config.clone();
                let validator = if should_validate {
                    validator.clone()
                } else {
                    None
                };

                async move {
                    let worker = SegmentWorker::new_for_logs(
                        worker_id,
                        seg_start,
                        seg_end,
                        client,
                        config,
                        validator,
                    );

                    // Convert the stream to handle FlightError
                    worker.process().map(move |result| {
                        result.map_err(|e| {
                            error!(
                                "Segment {}-{}: Log processing failed: {}",
                                seg_start, seg_end, e
                            );
                            arrow_flight::error::FlightError::ExternalError(Box::new(
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("Segment {}-{} failed: {}", seg_start, seg_end, e),
                                ),
                            ))
                        })
                    })
                }
            })
            .buffered(max_concurrent)
            .flatten()
    }

    /// Create a historical stream that fetches data from Erigon for a block range
    async fn create_historical_stream(
        &self,
        stream_type: StreamType,
        start: u64,
        end: u64,
        validate: bool,
    ) -> Result<
        Pin<
            Box<
                dyn Stream<
                        Item = Result<arrow_array::RecordBatch, arrow_flight::error::FlightError>,
                    > + Send
                    + 'static,
            >,
        >,
        Status,
    > {
        info!(
            "Creating historical stream for {:?} from block {} to {}",
            stream_type, start, end
        );

        // Handle transactions and logs separately since they use segment workers
        if stream_type == StreamType::Transactions {
            let client = self.blockdata_client.lock().await.clone();
            let stream = Self::process_transactions_with_segments(
                client,
                self.segment_config.clone(),
                self.validator.clone(),
                start,
                end,
                validate,
            );
            return Ok(Box::pin(stream));
        }

        if stream_type == StreamType::Logs {
            let client = self.blockdata_client.lock().await.clone();
            let stream = Self::process_logs_with_segments(
                client,
                self.segment_config.clone(),
                self.validator.clone(),
                start,
                end,
                validate,
            );
            return Ok(Box::pin(stream));
        }

        let blockdata_client = self.blockdata_client.clone();

        let stream = async_stream::stream! {
            match stream_type {
                StreamType::Blocks => {
                    // Stream blocks using BlockDataClient
                    let mut client_guard = blockdata_client.lock().await;
                    let mut block_stream = match client_guard.stream_blocks(start, end, 100).await {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to start block stream: {}", e);
                            yield Err(arrow_flight::error::FlightError::ExternalError(
                                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                            ));
                            return;
                        }
                    };
                    drop(client_guard);

                    let mut batch_count = 0;
                    while let Some(batch_result) = block_stream.message().await.transpose() {
                        match batch_result {
                            Ok(block_batch) => {
                                batch_count += 1;
                                info!("Received batch {} from BlockDataBackend with {} blocks", batch_count, block_batch.blocks.len());
                                match BlockDataConverter::blocks_to_arrow(block_batch) {
                                    Ok(record_batch) => {
                                        info!("Converted block batch {} with {} rows", batch_count, record_batch.num_rows());
                                        yield Ok(record_batch);
                                    }
                                    Err(e) => {
                                        error!("Failed to convert block batch: {}", e);
                                        yield Err(arrow_flight::error::FlightError::ExternalError(
                                            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                                        ));
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to receive block batch: {}", e);
                                yield Err(arrow_flight::error::FlightError::ExternalError(
                                    Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                                ));
                                break;
                            }
                        }
                    }
                    info!("Block stream ended after receiving {} batches", batch_count);
                }
                StreamType::Transactions => {
                    // Unreachable - handled before async_stream::stream! block
                    unreachable!("Transactions handled separately")
                }
                StreamType::Logs => {
                    // Unreachable - handled before async_stream::stream! block
                    unreachable!("Logs handled separately")
                }
                StreamType::Trie => {
                    error!("Historical trie streaming not supported");
                    yield Err(arrow_flight::error::FlightError::ExternalError(
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::Unsupported,
                            "Historical trie streaming not supported"
                        ))
                    ));
                }
            }

            info!("Completed historical stream for {:?} blocks {}-{}", stream_type, start, end);
        };

        Ok(Box::pin(stream))
    }

    /// Get the Arrow schema for a given stream type
    fn get_schema_for_type(stream_type: StreamType) -> Arc<arrow::datatypes::Schema> {
        match stream_type {
            StreamType::Blocks => ErigonDataConverter::block_schema(),
            StreamType::Transactions => ErigonDataConverter::transaction_schema(),
            StreamType::Logs => ErigonDataConverter::log_schema(),
            StreamType::Trie => {
                // Use the TrieNodeRecord schema
                use evm_common::trie::TrieNodeRecord;
                use typed_arrow::schema::SchemaMeta;
                TrieNodeRecord::schema()
            }
        }
    }
}

#[async_trait]
impl FlightBridge for ErigonFlightBridge {
    async fn get_info(&self) -> Result<BridgeInfo, Status> {
        Ok(self.bridge_info())
    }

    async fn get_capabilities(&self) -> Result<BridgeCapabilities, Status> {
        Ok(BridgeCapabilities {
            supports_historical: true, // Now supports historical queries via do_get
            supports_streaming: true,
            supports_reorg_notifications: false,
            supports_filters: false,
            supports_validation: self.validator.is_some(),
            max_batch_size: 1000,
        })
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        // Simple handshake - return bridge info
        let response = HandshakeResponse {
            protocol_version: 1,
            payload: serde_json::to_vec(&self.bridge_info())
                .map_err(|e| Status::internal(format!("Failed to serialize bridge info: {}", e)))?
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
        let mut info_streams = vec![
            create_flight_info(StreamType::Blocks)?,
            create_flight_info(StreamType::Transactions)?,
            create_flight_info(StreamType::Logs)?,
        ];

        // Only advertise trie streaming if we have the TrieBackend service
        if self.trie_client.is_some() {
            info_streams.push(create_flight_info(StreamType::Trie)?);
        }

        let stream = stream::iter(info_streams.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let blockchain_desc = Self::parse_descriptor(&descriptor)?;

        let info = create_flight_info(blockchain_desc.stream_type)?;

        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let blockchain_desc = Self::parse_descriptor(&descriptor)?;
        let schema = Self::get_schema_for_type(blockchain_desc.stream_type);

        // Convert Arrow schema to IPC format for Flight
        // SchemaResult expects raw bytes - we need to encode the schema properly
        let ipc_message = {
            use arrow::ipc::writer::IpcWriteOptions;
            let options = IpcWriteOptions::default();

            // Use FlightDataEncoderBuilder to get the schema as bytes
            let encoder = FlightDataEncoderBuilder::new()
                .with_schema(schema.clone())
                .with_options(options)
                .build(stream::empty());

            // Get just the schema message (first item from encoder)
            let mut encoded_schema = vec![];
            futures::pin_mut!(encoder);
            if let Some(Ok(flight_data)) = encoder.next().await {
                encoded_schema = flight_data.data_header.to_vec();
            }
            encoded_schema
        };

        Ok(Response::new(SchemaResult {
            schema: ipc_message.into(),
        }))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        let ticket = request.into_inner();

        debug!(
            "Received do_get request with ticket: {:?}",
            String::from_utf8_lossy(&ticket.ticket)
        );

        // Parse ticket to determine what data to stream
        // The ticket contains a JSON-serialized BlockchainDescriptor
        let blockchain_desc = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("Ticket is not valid UTF-8"))
            .and_then(|s| {
                serde_json::from_str::<phaser_bridge::descriptors::BlockchainDescriptor>(&s)
                    .map_err(|e| {
                        Status::invalid_argument(format!("Invalid descriptor in ticket: {}", e))
                    })
            })?;
        let stream_type = blockchain_desc.stream_type;
        let query_mode = blockchain_desc.query_mode.clone();

        info!(
            "Processing do_get for {:?} with mode {:?}",
            stream_type, query_mode
        );

        // Handle trie streaming separately
        if stream_type == StreamType::Trie {
            let batch_stream = self.create_trie_stream().await?;
            let schema = Self::get_schema_for_type(stream_type);
            let encoder = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batch_stream);

            let flight_stream = encoder.map(|result| {
                result.map_err(|e| {
                    error!("Error encoding flight data: {}", e);
                    Status::internal(format!("Encoding error: {}", e))
                })
            });

            return Ok(Response::new(Box::pin(flight_stream)));
        }

        // Handle based on query mode
        use phaser_bridge::subscription::QueryMode;
        use phaser_bridge::ValidationStage;

        // Determine if we should do ingestion validation
        let should_validate_ingestion = matches!(
            blockchain_desc.validation,
            ValidationStage::Ingestion | ValidationStage::Both
        );

        let batch_stream: Pin<
            Box<
                dyn Stream<
                        Item = Result<arrow_array::RecordBatch, arrow_flight::error::FlightError>,
                    > + Send,
            >,
        > = match query_mode {
            QueryMode::Historical { start, end } => {
                info!(
                    "Creating historical stream for blocks {}-{} (validation: {:?})",
                    start, end, blockchain_desc.validation
                );
                Box::pin(
                    self.create_historical_stream(
                        stream_type,
                        start,
                        end,
                        should_validate_ingestion,
                    )
                    .await?,
                )
            }
            QueryMode::Live => {
                info!("Creating live stream from current head");
                let receiver = match stream_type {
                    StreamType::Blocks => self.streaming_service.subscribe_blocks(),
                    StreamType::Transactions => self.streaming_service.subscribe_transactions(),
                    StreamType::Logs => self.streaming_service.subscribe_logs(),
                    StreamType::Trie => unreachable!("Trie handled above"),
                };

                Box::pin(async_stream::stream! {
                    let mut rx = receiver;
                    while let Ok(batch) = rx.recv().await {
                        yield Ok(batch);
                    }
                })
            }
        };

        let schema = Self::get_schema_for_type(stream_type);
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);

        let flight_stream = encoder.map(|result| {
            result.map_err(|e| {
                error!("Error encoding flight data: {}", e);
                Status::internal(format!("Encoding error: {}", e))
            })
        });

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>>, Status>
    {
        // Parse the initial descriptor from the stream
        let mut stream = request.into_inner();

        // Get the first message which should contain the descriptor
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("Empty stream"))?
            .map_err(|e| Status::internal(format!("Stream error: {}", e)))?;

        let stream_type = if let Some(desc) = first.flight_descriptor {
            Self::parse_descriptor(&desc)
                .map(|bd| bd.stream_type)
                .unwrap_or(StreamType::Blocks)
        } else {
            StreamType::Blocks
        };

        info!("Starting data stream for {:?}", stream_type);

        // Handle trie streaming separately
        if stream_type == StreamType::Trie {
            let batch_stream = self.create_trie_stream().await?;
            let schema = Self::get_schema_for_type(stream_type);
            let encoder = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(batch_stream);

            let flight_stream = encoder.map(|result| {
                result.map_err(|e| {
                    error!("Error encoding flight data: {}", e);
                    Status::internal(format!("Encoding error: {}", e))
                })
            });

            return Ok(Response::new(Box::pin(flight_stream)));
        }

        // Subscribe to the appropriate stream for regular data
        let receiver = match stream_type {
            StreamType::Blocks => self.streaming_service.subscribe_blocks(),
            StreamType::Transactions => self.streaming_service.subscribe_transactions(),
            StreamType::Logs => self.streaming_service.subscribe_logs(),
            StreamType::Trie => unreachable!("Trie handled above"),
        };
        let schema = Self::get_schema_for_type(stream_type);

        // Create a stream of RecordBatches from the receiver
        let batch_stream = async_stream::stream! {
            let mut rx = receiver;
            while let Ok(batch) = rx.recv().await {
                yield Ok(batch);
            }
        };

        // Create ONE encoder for the entire stream with schema
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);

        // Convert the encoder stream to the response format
        let flight_stream = encoder.map(|result| {
            result.map_err(|e| {
                error!("Error encoding flight data: {}", e);
                Status::internal(format!("Encoding error: {}", e))
            })
        });

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn health_check(&self) -> Result<bool, Status> {
        // Simple health check - just verify we can lOck the client
        let _client = self.client.lock().await;
        Ok(true)
    }
}

fn create_flight_info(stream_type: StreamType) -> Result<FlightInfo, ErigonBridgeError> {
    let schema = ErigonFlightBridge::get_schema_for_type(stream_type);

    // For discovery, just use the stream type as a simple string descriptor
    let stream_type_str = serde_json::to_string(&stream_type)?;
    let descriptor = FlightDescriptor::new_path(vec![stream_type_str]);

    let info = FlightInfo::new()
        .with_descriptor(descriptor)
        .try_with_schema(&schema)?
        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(vec![])))
        .with_total_records(0)
        .with_total_bytes(0);

    Ok(info)
}
