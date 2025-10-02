use arrow::datatypes::Schema;
use arrow_array::RecordBatch;
use arrow_flight::{decode::FlightRecordBatchStream, FlightClient, FlightInfo};
use futures::stream::StreamExt;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::descriptors::{BlockchainDescriptor, BridgeInfo};

/// Client for connecting to blockchain data bridges
pub struct FlightBridgeClient {
    client: FlightClient,
    info: Option<BridgeInfo>,
}

impl FlightBridgeClient {
    /// Connect to a bridge endpoint (TCP or Unix domain socket)
    pub async fn connect(endpoint: String) -> Result<Self, anyhow::Error> {
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
                return Err(anyhow::anyhow!(
                    "Unix domain sockets are not supported on this platform"
                ));
            }
        } else {
            // TCP connection
            let uri = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
                endpoint.clone()
            } else {
                format!("http://{}", endpoint)
            };
            Channel::from_shared(uri)?.connect().await?
        };

        let client = FlightClient::new(channel);

        Ok(Self { client, info: None })
    }

    /// Get bridge information
    pub async fn get_info(&mut self) -> Result<BridgeInfo, anyhow::Error> {
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
    ) -> Result<FlightInfo, anyhow::Error> {
        let flight_desc = descriptor.to_flight_descriptor();
        let response = self.client.get_flight_info(flight_desc).await?;
        Ok(response)
    }

    /// Stream blockchain data
    pub async fn stream_data(
        &mut self,
        descriptor: &BlockchainDescriptor,
    ) -> Result<Vec<RecordBatch>, anyhow::Error> {
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

    /// Subscribe to real-time blockchain data
    pub async fn subscribe(
        &mut self,
        descriptor: &BlockchainDescriptor,
    ) -> Result<FlightRecordBatchStream, anyhow::Error> {
        let ticket = descriptor.to_ticket();

        info!(
            "Subscribing to real-time data from bridge ({})",
            descriptor.stream_type.to_string()
        );
        let stream = self.client.do_get(ticket).await?;

        // Return the stream for the caller to consume
        Ok(stream)
    }

    /// Get schema for a stream type
    pub async fn get_schema(
        &mut self,
        descriptor: &BlockchainDescriptor,
    ) -> Result<Schema, anyhow::Error> {
        let flight_desc = descriptor.to_flight_descriptor();
        let schema = self.client.get_schema(flight_desc).await?;
        Ok(schema)
    }

    /// List available data streams
    pub async fn list_available_streams(&mut self) -> Result<Vec<FlightInfo>, anyhow::Error> {
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
    pub async fn health_check(&mut self) -> Result<bool, anyhow::Error> {
        // Implement a simple health check by trying to get flight info
        let descriptor =
            BlockchainDescriptor::historical(crate::descriptors::StreamType::Blocks, 0, 0);

        match self.get_flight_info(&descriptor).await {
            Ok(_) => Ok(true),
            Err(e) => {
                error!("Health check failed: {}", e);
                Ok(false)
            }
        }
    }
}
