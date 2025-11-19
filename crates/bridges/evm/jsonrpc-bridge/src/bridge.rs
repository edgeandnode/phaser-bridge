use crate::client::JsonRpcClient;
use crate::converter::JsonRpcConverter;
use crate::streaming::StreamingService;
use arrow_flight::{
    encode::FlightDataEncoderBuilder, flight_service_server::FlightService, Action, ActionType,
    Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};
use phaser_bridge::{
    bridge::{BridgeCapabilities, FlightBridge},
    descriptors::{BridgeInfo, StreamType},
    subscription::QueryMode,
};
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};
use validators_evm::ValidationExecutor;

/// A bridge that connects to any JSON-RPC compatible node
pub struct JsonRpcFlightBridge {
    client: Arc<JsonRpcClient>,
    chain_id: u64,
    streaming_service: Arc<StreamingService>,
    max_batch_size: usize,
    validator: Option<Arc<dyn ValidationExecutor>>,
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

        Ok(Self {
            client,
            chain_id,
            streaming_service,
            max_batch_size: 1000, // Default batch size, matches BridgeCapabilities
            validator,
        })
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

    /// Parse a FlightDescriptor to extract the BlockchainDescriptor
    fn parse_descriptor(
        descriptor: &FlightDescriptor,
    ) -> std::result::Result<phaser_bridge::descriptors::BlockchainDescriptor, Box<Status>> {
        if let Some(first) = descriptor.path.first() {
            serde_json::from_str::<phaser_bridge::descriptors::BlockchainDescriptor>(first)
                .map_err(|e| Box::new(Status::invalid_argument(format!("Invalid descriptor: {e}"))))
        } else {
            Err(Box::new(Status::invalid_argument("Empty descriptor path")))
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
    fn create_historical_stream(
        &self,
        stream_type: StreamType,
        start_block: u64,
        end_block: u64,
        validate: bool,
    ) -> impl Stream<Item = Result<arrow::record_batch::RecordBatch, Status>> + Send {
        let client = self.client.clone();
        let batch_size = self.max_batch_size as u64;
        let validator = self.validator.clone();

        async_stream::stream! {
            use alloy::eips::BlockNumberOrTag;
            use alloy_rpc_types_eth::Filter;
            use arrow::compute::concat_batches;
            use tracing::debug;

            info!("Fetching historical {:?} from block {} to {} (batch size: {})",
                  stream_type, start_block, end_block, batch_size);

            let mut current_block = start_block;

            while current_block <= end_block {
                let batch_end = std::cmp::min(current_block + batch_size - 1, end_block);
                let batch_count = (batch_end - current_block + 1) as usize;

                debug!("Fetching batch: blocks {} to {} ({} blocks)", current_block, batch_end, batch_count);

                // Collect RecordBatches for this batch
                let mut record_batches = Vec::new();

                for block_num in current_block..=batch_end {
                    // Fetch block with transactions
                    let block = match client.get_block_with_txs(BlockNumberOrTag::Number(block_num)).await {
                        Ok(Some(block)) => block,
                        Ok(None) => {
                            error!("Block #{} not found", block_num);
                            continue;
                        }
                        Err(e) => {
                            error!("Failed to fetch block #{}: {}", block_num, e);
                            yield Err(Status::internal(format!("Failed to fetch block {block_num}: {e}")));
                            continue;
                        }
                    };

                    match stream_type {
                        StreamType::Blocks => {
                            // Convert block header to RecordBatch
                            match evm_common::rpc_conversions::convert_any_header(&block.header) {
                                Ok(batch) => {
                                    record_batches.push(batch);
                                },
                                Err(e) => {
                                    error!("Failed to convert block header #{}: {}", block_num, e);
                                    yield Err(Status::internal(format!("Conversion error: {e}")));
                                }
                            }
                        }
                        StreamType::Transactions => {
                            // Convert transactions (if any)
                            if !block.transactions.is_empty() {
                                // Validate transactions if requested and validator is available
                                if validate {
                                    if let Some(ref val) = validator {
                                        // Extract block record and transaction records for validation
                                        // This validates our conversion: TransactionRecord → TxEnvelope → RLP → merkle root
                                        let block_record = evm_common::rpc_conversions::convert_any_header_to_record(&block.header);

                                        match evm_common::rpc_conversions::extract_transaction_records(&block) {
                                            Ok(tx_records) => {
                                                // Validate the transactions against the block's transactions_root
                                                // Using spawn_validate_records() for post-conversion validation
                                                match val.spawn_validate_records(block_record, tx_records).await {
                                                    Ok(()) => {
                                                        debug!("Validated {} transactions for block #{}", block.transactions.len(), block_num);
                                                    },
                                                    Err(e) => {
                                                        error!("Transaction validation failed for block #{}: {}", block_num, e);
                                                        yield Err(Status::internal(format!("Validation error for block {block_num}: {e}")));
                                                        continue;
                                                    }
                                                }
                                            },
                                            Err(e) => {
                                                error!("Failed to extract transaction records for validation: {}", e);
                                                yield Err(Status::internal(format!("Failed to extract records for validation: {e}")));
                                                continue;
                                            }
                                        }
                                    } else {
                                        // Validation requested but no validator available
                                        error!("Validation requested but validator not configured");
                                        yield Err(Status::failed_precondition("Validation requested but validator not configured"));
                                        return;
                                    }
                                }

                                // Convert to RecordBatch after validation
                                match JsonRpcConverter::convert_transactions(&block) {
                                    Ok(batch) => {
                                        record_batches.push(batch);
                                    },
                                    Err(e) => {
                                        error!("Failed to convert transactions for block #{}: {}", block_num, e);
                                        yield Err(Status::internal(format!("Conversion error: {e}")));
                                    }
                                }
                            }
                        }
                        StreamType::Logs => {
                            // Fetch and convert logs
                            let filter = Filter::new().from_block(block_num).to_block(block_num);

                            match client.get_logs(filter).await {
                                Ok(logs) if !logs.is_empty() => {
                                    let block_hash = block.header.hash;
                                    match JsonRpcConverter::convert_logs(&logs, block_num, block_hash, block.header.timestamp) {
                                        Ok(batch) => record_batches.push(batch),
                                        Err(e) => {
                                            error!("Failed to convert logs for block #{}: {}", block_num, e);
                                            yield Err(Status::internal(format!("Conversion error: {e}")));
                                        }
                                    }
                                }
                                Ok(_) => {
                                    // No logs in this block, skip
                                }
                                Err(e) => {
                                    error!("Failed to fetch logs for block #{}: {}", block_num, e);
                                    yield Err(Status::internal(format!("Failed to fetch logs: {e}")));
                                }
                            }
                        }
                        StreamType::Trie => {
                            yield Err(Status::unimplemented("Trie streaming not supported via JSON-RPC"));
                            return;
                        }
                    }
                }

                // If we collected any batches, concatenate and yield them
                if !record_batches.is_empty() {
                    let schema = record_batches[0].schema();
                    match concat_batches(&schema, &record_batches) {
                        Ok(combined_batch) => {
                            debug!("Yielding combined batch with {} rows for blocks {} to {}",
                                   combined_batch.num_rows(), current_block, batch_end);
                            yield Ok(combined_batch);
                        }
                        Err(e) => {
                            error!("Failed to concatenate batches: {}", e);
                            yield Err(Status::internal(format!("Failed to concatenate batches: {e}")));
                        }
                    }
                }

                current_block = batch_end + 1;
            }

            info!("Completed historical {:?} query for blocks {} to {}", stream_type, start_block, end_block);
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
            max_batch_size: self.max_batch_size,
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
        let blockchain_desc = Self::parse_descriptor(&descriptor).map_err(|e| *e)?;

        let info = create_flight_info(blockchain_desc.stream_type).map_err(|e| *e)?;

        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let blockchain_desc = Self::parse_descriptor(&descriptor).map_err(|e| *e)?;
        let schema = Self::get_schema_for_type(blockchain_desc.stream_type).map_err(|e| *e)?;

        // Convert Arrow schema to IPC format for Flight
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
    ) -> std::result::Result<
        Response<Pin<Box<dyn Stream<Item = std::result::Result<FlightData, Status>> + Send>>>,
        Status,
    > {
        let ticket = request.into_inner();

        // Parse ticket to determine what data to stream
        let blockchain_desc = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("Ticket is not valid UTF-8"))
            .and_then(|s| {
                serde_json::from_str::<phaser_bridge::descriptors::BlockchainDescriptor>(&s)
                    .map_err(|e| {
                        Status::invalid_argument(format!("Invalid descriptor in ticket: {e}"))
                    })
            })?;

        let stream_type = blockchain_desc.stream_type;
        let query_mode = blockchain_desc.query_mode.clone();
        info!(
            "Processing do_get for {:?} in {:?} mode",
            stream_type, query_mode
        );

        // Get schema for the stream type
        let schema = Self::get_schema_for_type(stream_type).map_err(|e| *e)?;

        // Determine if we should do conversion validation
        // JSON-RPC doesn't give us raw RLP, so we can only do conversion validation
        use phaser_bridge::ValidationStage;
        let should_validate_conversion = matches!(
            blockchain_desc.validation,
            ValidationStage::Conversion | ValidationStage::Both
        );

        // Branch based on query mode
        let batch_stream: Pin<
            Box<dyn Stream<Item = Result<arrow::record_batch::RecordBatch, Status>> + Send>,
        > = match query_mode {
            QueryMode::Historical { start, end } => {
                // Historical query - fetch specific block range
                info!(
                    "Creating historical stream for {:?} blocks {}-{} (validation: {:?})",
                    stream_type, start, end, blockchain_desc.validation
                );
                Box::pin(self.create_historical_stream(
                    stream_type,
                    start,
                    end,
                    should_validate_conversion,
                ))
            }
            QueryMode::Live => {
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

                Box::pin(async_stream::stream! {
                    let mut rx = receiver;
                    while let Ok(batch) = rx.recv().await {
                        yield Ok(batch);
                    }
                })
            }
        };

        // Convert Status errors to FlightError for the encoder
        let flight_batch_stream = batch_stream.map(|result| {
            result.map_err(|status| arrow_flight::error::FlightError::Tonic(Box::new(status)))
        });

        // Encode as Flight data
        let encoder = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(flight_batch_stream);

        let flight_stream = encoder.map(|result| {
            result.map_err(|e| {
                error!("Error encoding flight data: {}", e);
                Status::internal(format!("Encoding error: {e}"))
            })
        });

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
                .map(|bd| bd.stream_type)
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
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not supported"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let stream_type = Self::parse_descriptor(&descriptor).map_err(|e| *e)?;
        let schema = Self::get_schema_for_type(stream_type.stream_type).map_err(|e| *e)?;

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
