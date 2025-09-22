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
use crate::streaming_service::StreamingService;
use std::sync::Arc;

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

        // Start the streaming service
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
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>>, Status>
    {
        // List available data streams
        let info_streams = vec![
            create_flight_info("blocks", &ErigonDataConverter::block_schema()),
            create_flight_info("transactions", &ErigonDataConverter::transaction_schema()),
            create_flight_info("logs", &ErigonDataConverter::log_schema()),
        ];

        let stream = stream::iter(info_streams.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();

        // Parse the path to determine which stream type
        // Handle both simple path (["blocks"]) and JSON descriptor in path
        let stream_type = if let Some(first) = descriptor.path.first() {
            // Check if it's JSON (starts with '{')
            if first.starts_with('{') {
                // Try to parse as BlockchainDescriptor
                use phaser_bridge::descriptors::BlockchainDescriptor;
                match serde_json::from_str::<BlockchainDescriptor>(first) {
                    Ok(desc) => match desc.stream_type {
                        phaser_bridge::descriptors::StreamType::Blocks => "blocks",
                        phaser_bridge::descriptors::StreamType::Transactions => "transactions",
                        phaser_bridge::descriptors::StreamType::Logs => "logs",
                    },
                    Err(e) => {
                        return Err(Status::invalid_argument(format!(
                            "Invalid descriptor: {}",
                            e
                        )));
                    }
                }
            } else {
                first.as_str()
            }
        } else {
            return Err(Status::invalid_argument("Empty descriptor path"));
        };

        let schema = match stream_type {
            "blocks" => ErigonDataConverter::block_schema(),
            "transactions" => ErigonDataConverter::transaction_schema(),
            "logs" => ErigonDataConverter::log_schema(),
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Unknown stream type: {}",
                    stream_type
                )))
            }
        };

        let info = FlightInfo::new()
            .with_descriptor(descriptor)
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema error: {}", e)))?
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(vec![])))
            .with_total_records(0)
            .with_total_bytes(0);

        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();

        // Parse the path to determine which stream type
        // Handle both simple path (["blocks"]) and JSON descriptor in path
        let stream_type = if let Some(first) = descriptor.path.first() {
            // Check if it's JSON (starts with '{')
            if first.starts_with('{') {
                // Try to parse as BlockchainDescriptor
                use phaser_bridge::descriptors::BlockchainDescriptor;
                match serde_json::from_str::<BlockchainDescriptor>(first) {
                    Ok(desc) => match desc.stream_type {
                        phaser_bridge::descriptors::StreamType::Blocks => "blocks",
                        phaser_bridge::descriptors::StreamType::Transactions => "transactions",
                        phaser_bridge::descriptors::StreamType::Logs => "logs",
                    },
                    Err(e) => {
                        return Err(Status::invalid_argument(format!(
                            "Invalid descriptor: {}",
                            e
                        )));
                    }
                }
            } else {
                first.as_str()
            }
        } else {
            return Err(Status::invalid_argument("Empty descriptor path"));
        };

        let schema = match stream_type {
            "blocks" => ErigonDataConverter::block_schema(),
            "transactions" => ErigonDataConverter::transaction_schema(),
            "logs" => ErigonDataConverter::log_schema(),
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Unknown stream type: {}",
                    stream_type
                )))
            }
        };

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
        let stream_type = if let Ok(descriptor_json) = String::from_utf8(ticket.ticket.to_vec()) {
            use phaser_bridge::descriptors::BlockchainDescriptor;
            match serde_json::from_str::<BlockchainDescriptor>(&descriptor_json) {
                Ok(desc) => match desc.stream_type {
                    phaser_bridge::descriptors::StreamType::Blocks => "blocks",
                    phaser_bridge::descriptors::StreamType::Transactions => "transactions",
                    phaser_bridge::descriptors::StreamType::Logs => "logs",
                },
                Err(e) => {
                    error!("Failed to parse ticket as BlockchainDescriptor: {}", e);
                    return Err(Status::invalid_argument(format!("Invalid ticket: {}", e)));
                }
            }
        } else {
            return Err(Status::invalid_argument("Ticket is not valid UTF-8"));
        };

        info!("Processing do_get for {}", stream_type);

        // Subscribe to live updates for do_get as well
        let receiver = self.streaming_service.subscribe();

        let schema = match stream_type {
            "blocks" => ErigonDataConverter::block_schema(),
            "transactions" => ErigonDataConverter::transaction_schema(),
            "logs" => ErigonDataConverter::log_schema(),
            _ => unreachable!(),
        };

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
            desc.path
                .first()
                .map(|s| s.clone())
                .unwrap_or_else(|| "blocks".to_string())
        } else {
            "blocks".to_string()
        };

        info!("Starting data stream for {}", stream_type);

        // Subscribe to live updates
        let receiver = self.streaming_service.subscribe();

        // Get the schema
        let schema = match stream_type.as_str() {
            "blocks" => ErigonDataConverter::block_schema(),
            "transactions" => ErigonDataConverter::transaction_schema(),
            "logs" => ErigonDataConverter::log_schema(),
            _ => ErigonDataConverter::block_schema(),
        };

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
        // Simple health check - just verify we can lock the client
        let _client = self.client.lock().await;
        Ok(true)
    }
}

fn create_flight_info(name: &str, schema: &Arc<arrow::datatypes::Schema>) -> FlightInfo {
    let descriptor = FlightDescriptor::new_path(vec![name.to_string()]);

    FlightInfo::new()
        .with_descriptor(descriptor)
        .try_with_schema(schema)
        .unwrap()
        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(name.as_bytes().to_vec())))
        .with_total_records(0)
        .with_total_bytes(0)
}
