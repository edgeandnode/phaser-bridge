use arrow_array::RecordBatch;
use arrow_flight::{
    encode::FlightDataEncoderBuilder, Criteria, FlightData, FlightDescriptor, FlightEndpoint,
    FlightInfo, HandshakeRequest, HandshakeResponse, SchemaResult, Ticket,
};
use async_trait::async_trait;
use futures::{stream, Stream, StreamExt};
use phaser_bridge::{
    bridge::{BridgeCapabilities, FlightBridge},
    descriptors::{BridgeInfo, StreamType},
};
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::client::ErigonClient;
use crate::converter::ErigonDataConverter;
use crate::error::ErigonBridgeError;
use crate::streaming_service::StreamingService;
use std::sync::Arc;
use tonic::Status as TonicStatus;

/// A stateless bridge that translates between Erigon gRPC and Arrow Flight
pub struct ErigonFlightBridge {
    client: Arc<tokio::sync::Mutex<ErigonClient>>,
    chain_id: u64,
    streaming_service: Arc<StreamingService>,
}

impl ErigonFlightBridge {
    pub async fn new(endpoint: String, chain_id: u64) -> Result<Self, anyhow::Error> {
        let client = ErigonClient::connect(endpoint).await?;

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
            chain_id,
            streaming_service,
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

    /// Get the Arrow schema for a given stream type
    fn get_schema_for_type(stream_type: StreamType) -> Arc<arrow::datatypes::Schema> {
        match stream_type {
            StreamType::Blocks => ErigonDataConverter::block_schema(),
            StreamType::Transactions => ErigonDataConverter::transaction_schema(),
            StreamType::Logs => ErigonDataConverter::log_schema(),
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
            supports_historical: false, // We're stateless, no historical data
            supports_streaming: true,
            supports_reorg_notifications: false,
            supports_filters: false,
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
        let info_streams = vec![
            create_flight_info(StreamType::Blocks)?,
            create_flight_info(StreamType::Transactions)?,
            create_flight_info(StreamType::Logs)?,
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

        info!("Processing do_get for {:?}", stream_type);

        let receiver = match stream_type {
            StreamType::Blocks => self.streaming_service.subscribe_blocks(),
            StreamType::Transactions => self.streaming_service.subscribe_transactions(),
            StreamType::Logs => self.streaming_service.subscribe_logs(),
        };

        let schema = Self::get_schema_for_type(stream_type);
        let batch_stream = async_stream::stream! {
            let mut rx = receiver;
            while let Ok(batch) = rx.recv().await {
                yield Ok(batch);
            }
        };
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
