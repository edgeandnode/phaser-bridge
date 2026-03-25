//! Flight client for connecting to blockchain data bridges

use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use arrow_flight::{flight_service_client::FlightServiceClient, Action, FlightClient, FlightInfo};
use futures::stream::StreamExt;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use phaser_types::{
    BatchMetadata, BlockchainDescriptor, BridgeInfo, DiscoveryCapabilities, GenericQuery,
    ACTION_DESCRIBE,
};

use crate::error::BridgeError;

/// Result type for bridge client operations
pub type Result<T> = std::result::Result<T, BridgeError>;

/// Client for connecting to blockchain data bridges
pub struct PhaserClient {
    client: FlightClient,
    info: Option<BridgeInfo>,
}

impl PhaserClient {
    /// Connect to a bridge endpoint (TCP or Unix domain socket)
    pub async fn connect(endpoint: String) -> Result<Self> {
        info!("Connecting to bridge at {}", endpoint);

        let channel = if endpoint.starts_with("unix:")
            || endpoint.starts_with("/")
            || endpoint.starts_with("./")
        {
            // Unix domain socket
            let path = if endpoint.starts_with("unix:") {
                endpoint.strip_prefix("unix:").unwrap().to_string()
            } else {
                endpoint.clone()
            };

            info!("Connecting via Unix domain socket: {}", path);

            // For Unix domain sockets, we need a special URI format
            let uri = "http://[::]:50051".to_string(); // dummy URI for unix socket

            // Use tonic's built-in Unix socket support
            use tonic::transport::Uri;

            // Parse as endpoint
            let channel_endpoint = Channel::from_shared(uri)?;

            // Connect with Unix domain socket
            #[cfg(unix)]
            {
                use tokio::net::UnixStream;
                use tower::service_fn;

                channel_endpoint
                    .connect_with_connector(service_fn(move |_: Uri| {
                        let path = path.clone();
                        async move {
                            // Use hyper_util to wrap the UnixStream properly
                            UnixStream::connect(path).await.map(|stream| {
                                use hyper_util::rt::tokio::TokioIo;
                                TokioIo::new(stream)
                            })
                        }
                    }))
                    .await?
            }

            #[cfg(not(unix))]
            {
                return Err(BridgeError::platform_not_supported(
                    "Unix domain sockets are not supported on this platform",
                ));
            }
        } else {
            // TCP connection
            let uri = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
                endpoint.clone()
            } else {
                format!("http://{endpoint}")
            };
            Channel::from_shared(uri)?.connect().await?
        };

        // Configure message size limits (512MB global max for large batches)
        // This allows the client to receive large messages from the bridge
        const MAX_MESSAGE_SIZE: usize = 512 * 1024 * 1024;

        // Client accepts compression if server sends it
        // Compression is controlled by the bridge's --compression flag
        let flight_service_client = FlightServiceClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip);

        let client = FlightClient::new_from_inner(flight_service_client);

        Ok(Self { client, info: None })
    }

    /// Get bridge information
    pub async fn get_info(&mut self) -> Result<BridgeInfo> {
        // This would typically be implemented as a custom action
        // For now, return cached info or a placeholder
        if let Some(ref info) = self.info {
            Ok(info.clone())
        } else {
            Ok(BridgeInfo {
                name: "Unknown".to_string(),
                node_type: "Unknown".to_string(),
                version: "0.0.0".to_string(),
                chain_id: 0,
                capabilities: vec![],
                current_block: 0,
                oldest_block: 0,
            })
        }
    }

    /// Get flight info for a blockchain data request
    pub async fn get_flight_info(
        &mut self,
        descriptor: &BlockchainDescriptor,
    ) -> Result<FlightInfo> {
        let flight_desc = descriptor.to_flight_descriptor();
        let response = self.client.get_flight_info(flight_desc).await?;
        Ok(response)
    }

    /// Stream blockchain data
    pub async fn stream_data(
        &mut self,
        descriptor: &BlockchainDescriptor,
    ) -> Result<Vec<RecordBatch>> {
        let ticket = descriptor.to_ticket();

        info!("Requesting data stream from bridge");
        let mut decoder = self.client.do_get(ticket).await?;

        let mut batches = Vec::new();
        while let Some(batch) = decoder.next().await {
            match batch {
                Ok(batch) => {
                    debug!("Received batch with {} rows", batch.num_rows());
                    batches.push(batch);
                }
                Err(e) => {
                    error!("Error decoding batch: {}", e);
                    return Err(e.into());
                }
            }
        }

        info!("Received {} batches from bridge", batches.len());
        Ok(batches)
    }

    /// Subscribe with access to batch metadata from FlightData.app_metadata
    ///
    /// Returns a stream of (RecordBatch, BatchMetadata) tuples.
    ///
    /// This method accesses raw FlightData messages to preserve app_metadata that contains
    /// batch metadata including responsibility range information (which blocks were processed,
    /// even if the batch contains 0 rows).
    ///
    /// All batches are required to have metadata - if metadata is missing or invalid,
    /// an error is returned.
    pub async fn subscribe_with_metadata(
        &mut self,
        descriptor: &BlockchainDescriptor,
    ) -> Result<
        impl futures::Stream<
            Item = std::result::Result<
                (RecordBatch, BatchMetadata),
                arrow_flight::error::FlightError,
            >,
        >,
    > {
        let ticket = descriptor.to_ticket();

        info!(
            "Subscribing to data with metadata from bridge ({})",
            descriptor.stream_type.to_string()
        );

        // Access the inner FlightServiceClient to get raw FlightData stream
        let mut inner_client = self.client.inner().clone();
        let response = inner_client.do_get(ticket).await?;
        let flight_data_stream = response.into_inner();

        // We need to decode FlightData messages while preserving app_metadata
        // FlightRecordBatchStream would handle decoding correctly, but we'd lose app_metadata
        // So we'll manually process FlightData using arrow_flight::utils for proper decoding
        let stream = async_stream::try_stream! {
            use arrow_ipc::{root_as_message, convert::fb_to_schema};
            use std::sync::Arc;

            let mut flight_data_stream = flight_data_stream;

            let mut schema: Option<arrow::datatypes::SchemaRef> = None;
            let dictionaries_by_id = std::collections::HashMap::new();

            while let Some(flight_data_result) = flight_data_stream.next().await {
                let flight_data = flight_data_result
                    .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e)))?;

                // First message should be the schema
                if schema.is_none() {
                    // Decode schema from FlightData header (based on flight_data_to_batches implementation)
                    let message = root_as_message(&flight_data.data_header[..])
                        .map_err(|err| arrow_flight::error::FlightError::DecodeError(
                            format!("Cannot get root as message: {err:?}")
                        ))?;

                    let ipc_schema = message
                        .header_as_schema()
                        .ok_or_else(|| arrow_flight::error::FlightError::DecodeError(
                            "First message should be schema".to_string()
                        ))?;

                    schema = Some(Arc::new(fb_to_schema(ipc_schema)));
                    continue;
                }

                // Extract and decode app_metadata (required)
                let metadata = BatchMetadata::decode(&flight_data.app_metadata)
                    .map_err(|e| arrow_flight::error::FlightError::DecodeError(
                        format!("Failed to decode batch metadata: {e}")
                    ))?;

                // Decode the RecordBatch from FlightData using the schema
                if !flight_data.data_body.is_empty() {
                    if let Some(ref schema_ref) = schema {
                        let batch = arrow_flight::utils::flight_data_to_arrow_batch(
                            &flight_data,
                            schema_ref.clone(),
                            &dictionaries_by_id
                        )?;

                        yield (batch, metadata);
                    }
                }
            }
        };

        Ok(stream)
    }

    /// Get schema for a stream type
    pub async fn get_schema(&mut self, descriptor: &BlockchainDescriptor) -> Result<Schema> {
        let flight_desc = descriptor.to_flight_descriptor();
        let schema = self.client.get_schema(flight_desc).await?;
        Ok(schema)
    }

    /// List available data streams
    pub async fn list_available_streams(&mut self) -> Result<Vec<FlightInfo>> {
        use prost::bytes::Bytes;

        let mut stream = self.client.list_flights(Bytes::new()).await?;
        let mut flights = Vec::new();

        while let Some(flight_result) = stream.next().await {
            match flight_result {
                Ok(flight_info) => flights.push(flight_info),
                Err(e) => {
                    error!("Error listing flight: {}", e);
                }
            }
        }

        Ok(flights)
    }

    /// Check if the bridge is healthy
    pub async fn health_check(&mut self) -> Result<bool> {
        // Implement a simple health check by trying to get flight info for blocks table
        let query = GenericQuery::historical("blocks", 0, 0);
        let flight_desc = query.to_flight_descriptor();

        match self.client.get_flight_info(flight_desc).await {
            Ok(_) => Ok(true),
            Err(e) => {
                error!("Health check failed: {}", e);
                Ok(false)
            }
        }
    }

    // ==================== Protocol-Agnostic Discovery API ====================

    /// Discover bridge capabilities using the "describe" action
    ///
    /// Returns protocol-agnostic information about available tables,
    /// position semantics, and supported filters.
    pub async fn discover(&mut self) -> Result<DiscoveryCapabilities> {
        info!("Discovering bridge capabilities");

        let action = Action {
            r#type: ACTION_DESCRIBE.to_string(),
            body: Default::default(),
        };

        let mut inner_client = self.client.inner().clone();
        let mut stream = inner_client.do_action(action).await?.into_inner();

        // The describe action returns a single result with JSON body
        let result = stream
            .next()
            .await
            .ok_or_else(|| BridgeError::discovery("No response from describe action"))??;

        let capabilities: DiscoveryCapabilities = serde_json::from_slice(&result.body)?;

        info!(
            "Discovered bridge: {} v{} (protocol: {}, {} tables)",
            capabilities.name,
            capabilities.version,
            capabilities.protocol,
            capabilities.tables.len()
        );

        Ok(capabilities)
    }

    /// Get schema for a table using generic query format
    ///
    /// Uses Flight's native GetSchema with the generic query descriptor.
    pub async fn get_table_schema(&mut self, table: &str) -> Result<Schema> {
        let query = GenericQuery::historical(table, 0, 0);
        let flight_desc = query.to_flight_descriptor();
        let schema = self.client.get_schema(flight_desc).await?;
        Ok(schema)
    }

    /// Stream data using generic query format
    ///
    /// Protocol-agnostic streaming that works with any bridge.
    /// Returns raw RecordBatches - clients can import schema crates
    /// for typed deserialization if needed.
    pub async fn query(
        &mut self,
        query: GenericQuery,
    ) -> Result<
        impl futures::Stream<Item = std::result::Result<RecordBatch, arrow_flight::error::FlightError>>,
    > {
        let ticket = query.to_ticket();

        info!(
            "Querying table '{}' with mode {:?}",
            query.table, query.mode
        );
        let decoder = self.client.do_get(ticket).await?;

        Ok(decoder)
    }

    /// Stream data with metadata using generic query format
    ///
    /// Like `query`, but also returns batch metadata (responsibility ranges).
    pub async fn query_with_metadata(
        &mut self,
        query: GenericQuery,
    ) -> Result<
        impl futures::Stream<
            Item = std::result::Result<
                (RecordBatch, BatchMetadata),
                arrow_flight::error::FlightError,
            >,
        >,
    > {
        let ticket = query.to_ticket();

        info!(
            "Querying table '{}' with metadata, mode {:?}",
            query.table, query.mode
        );

        // Access the inner FlightServiceClient to get raw FlightData stream
        let mut inner_client = self.client.inner().clone();
        let response = inner_client.do_get(ticket).await?;
        let flight_data_stream = response.into_inner();

        // Process FlightData to preserve app_metadata
        let stream = async_stream::try_stream! {
            use arrow_ipc::{root_as_message, convert::fb_to_schema};
            use std::sync::Arc;

            let mut flight_data_stream = flight_data_stream;

            let mut schema: Option<arrow::datatypes::SchemaRef> = None;
            let dictionaries_by_id = std::collections::HashMap::new();

            while let Some(flight_data_result) = flight_data_stream.next().await {
                let flight_data = flight_data_result
                    .map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e)))?;

                // First message should be the schema
                if schema.is_none() {
                    let message = root_as_message(&flight_data.data_header[..])
                        .map_err(|err| arrow_flight::error::FlightError::DecodeError(
                            format!("Cannot get root as message: {err:?}")
                        ))?;

                    let ipc_schema = message
                        .header_as_schema()
                        .ok_or_else(|| arrow_flight::error::FlightError::DecodeError(
                            "First message should be schema".to_string()
                        ))?;

                    schema = Some(Arc::new(fb_to_schema(ipc_schema)));
                    continue;
                }

                // Extract and decode app_metadata (required)
                let metadata = BatchMetadata::decode(&flight_data.app_metadata)
                    .map_err(|e| arrow_flight::error::FlightError::DecodeError(
                        format!("Failed to decode batch metadata: {e}")
                    ))?;

                // Decode the RecordBatch from FlightData using the schema
                if !flight_data.data_body.is_empty() {
                    if let Some(ref schema_ref) = schema {
                        let batch = arrow_flight::utils::flight_data_to_arrow_batch(
                            &flight_data,
                            schema_ref.clone(),
                            &dictionaries_by_id
                        )?;

                        yield (batch, metadata);
                    }
                }
            }
        };

        Ok(stream)
    }
}
