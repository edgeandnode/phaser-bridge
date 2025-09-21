use arrow_array::RecordBatch;
use arrow::datatypes::Schema;
use arrow_flight::{
    decode::FlightRecordBatchStream,
    FlightClient, FlightInfo,
};
use futures::stream::StreamExt;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::descriptors::{BlockchainDescriptor, BridgeInfo};

/// Client for connecting to blockchain data bridges
pub struct FlightBridgeClient {
    client: FlightClient,
    endpoint: String,
    info: Option<BridgeInfo>,
}

impl FlightBridgeClient {
    /// Connect to a bridge endpoint
    pub async fn connect(endpoint: String) -> Result<Self, anyhow::Error> {
        info!("Connecting to bridge at {}", endpoint);

        let channel = Channel::from_shared(endpoint.clone())?
            .connect()
            .await?;

        let client = FlightClient::new(channel);

        Ok(Self {
            client,
            endpoint,
            info: None,
        })
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

        info!("Subscribing to real-time data from bridge");
        let stream = self.client.do_get(ticket).await?;

        // Return the stream for the caller to consume
        Ok(stream)
    }

    /// Get schema for a stream type
    pub async fn get_schema(&mut self, descriptor: &BlockchainDescriptor) -> Result<Schema, anyhow::Error> {
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
        let descriptor = BlockchainDescriptor::historical(
            crate::descriptors::StreamType::Blocks,
            0,
            0
        );

        match self.get_flight_info(&descriptor).await {
            Ok(_) => Ok(true),
            Err(e) => {
                error!("Health check failed: {}", e);
                Ok(false)
            }
        }
    }
}