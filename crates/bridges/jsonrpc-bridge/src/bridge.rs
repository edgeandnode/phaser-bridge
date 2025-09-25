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
};
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};

/// A bridge that connects to any JSON-RPC compatible node
pub struct JsonRpcFlightBridge {
    client: Arc<JsonRpcClient>,
    chain_id: u64,
    streaming_service: Arc<StreamingService>,
    node_url: String,
}

impl JsonRpcFlightBridge {
    /// Create a new JSON-RPC bridge
    pub async fn new(
        node_url: String,
        chain_id: Option<u64>,
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
            node_url,
        })
    }

    /// Get bridge information
    pub fn bridge_info(&self) -> BridgeInfo {
        BridgeInfo {
            name: "jsonrpc-bridge".to_string(),
            node_type: "json-rpc".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            chain_id: self.chain_id,
            capabilities: if self.client.supports_subscriptions() {
                vec!["streaming".to_string(), "subscriptions".to_string()]
            } else {
                vec!["polling".to_string()]
            },
            current_block: 0, // Would need to query this
            oldest_block: 0,  // Would need to query this
        }
    }

    /// Parse a FlightDescriptor to extract the BlockchainDescriptor
    fn parse_descriptor(
        descriptor: &FlightDescriptor,
    ) -> std::result::Result<phaser_bridge::descriptors::BlockchainDescriptor, Status> {
        if let Some(first) = descriptor.path.first() {
            serde_json::from_str::<phaser_bridge::descriptors::BlockchainDescriptor>(first)
                .map_err(|e| Status::invalid_argument(format!("Invalid descriptor: {}", e)))
        } else {
            Err(Status::invalid_argument("Empty descriptor path"))
        }
    }

    /// Get the Arrow schema for a given stream type
    fn get_schema_for_type(stream_type: StreamType) -> Arc<arrow::datatypes::Schema> {
        match stream_type {
            StreamType::Blocks => JsonRpcConverter::block_schema(),
            StreamType::Transactions => JsonRpcConverter::transaction_schema(),
            StreamType::Logs => JsonRpcConverter::log_schema(),
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
            supports_streaming: self.client.supports_subscriptions(),
            supports_reorg_notifications: false,
            supports_filters: true,
            max_batch_size: 1000,
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
        let info_streams = vec![
            create_flight_info(StreamType::Blocks),
            create_flight_info(StreamType::Transactions),
            create_flight_info(StreamType::Logs),
        ];

        let stream = stream::iter(info_streams.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let blockchain_desc = Self::parse_descriptor(&descriptor)?;

        let info = create_flight_info(blockchain_desc.stream_type);

        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let blockchain_desc = Self::parse_descriptor(&descriptor)?;
        let schema = Self::get_schema_for_type(blockchain_desc.stream_type);

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
                        Status::invalid_argument(format!("Invalid descriptor in ticket: {}", e))
                    })
            })?;

        let stream_type = blockchain_desc.stream_type;
        info!("Processing do_get for {:?}", stream_type);

        // Get the appropriate receiver
        let receiver = match stream_type {
            StreamType::Blocks => self.streaming_service.subscribe_blocks(),
            StreamType::Transactions => self.streaming_service.subscribe_transactions(),
            StreamType::Logs => self.streaming_service.subscribe_logs(),
        };

        // Get schema for the stream type
        let schema = Self::get_schema_for_type(stream_type);

        // Create stream from the broadcast receiver
        let batch_stream = async_stream::stream! {
            let mut rx = receiver;
            while let Ok(batch) = rx.recv().await {
                yield Ok(batch);
            }
        };

        // Encode as Flight data
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
            .map_err(|e| Status::internal(format!("Stream error: {}", e)))?;

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
        };

        let schema = Self::get_schema_for_type(stream_type);

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
                Status::internal(format!("Encoding error: {}", e))
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
        let stream_type = Self::parse_descriptor(&descriptor)?;
        let schema = Self::get_schema_for_type(stream_type.stream_type);

        // Convert Schema to IPC format
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_as_ipc = arrow_flight::SchemaAsIpc {
            pair: (&*schema, &options),
        };

        let schema_result = schema_as_ipc
            .try_into()
            .map_err(|e: arrow::error::ArrowError| {
                Status::internal(format!("Failed to encode schema: {}", e))
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

fn create_flight_info(stream_type: StreamType) -> FlightInfo {
    let schema = JsonRpcFlightBridge::get_schema_for_type(stream_type);

    // For discovery, use the stream type as a simple string descriptor
    let stream_type_str =
        serde_json::to_string(&stream_type).expect("StreamType should always serialize to JSON");
    let descriptor = FlightDescriptor::new_path(vec![stream_type_str]);

    FlightInfo::new()
        .with_descriptor(descriptor)
        .try_with_schema(&schema)
        .expect("Schema should always be valid for FlightInfo")
        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(vec![])))
        .with_total_records(0)
        .with_total_bytes(0)
}
