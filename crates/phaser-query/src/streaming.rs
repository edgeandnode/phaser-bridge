use anyhow::Result;
use phaser_bridge::{FlightBridgeClient, descriptors::{BlockchainDescriptor, StreamType}};
use futures::StreamExt;
use tracing::{info, error};

/// Service that connects to bridge(s) and streams blockchain data
pub struct StreamingService {
    bridges: Vec<FlightBridgeClient>,
}

impl StreamingService {
    pub async fn new(bridge_endpoints: Vec<String>) -> Result<Self> {
        let mut bridges = Vec::new();

        for endpoint in bridge_endpoints {
            info!("Connecting to bridge at {}", endpoint);
            let client = FlightBridgeClient::connect(endpoint).await?;
            bridges.push(client);
        }

        Ok(Self { bridges })
    }

    /// Start streaming blocks from all bridges
    pub async fn start_streaming(&mut self) -> Result<()> {
        for bridge in &mut self.bridges {
            // Check bridge health
            if !bridge.health_check().await? {
                error!("Bridge health check failed");
                continue;
            }

            // Subscribe to blocks
            let descriptor = BlockchainDescriptor::live(StreamType::Blocks, None);

            info!("Subscribing to blocks from bridge");
            let mut stream = bridge.subscribe(&descriptor).await?;

            // Process blocks in background
            tokio::spawn(async move {
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(batch) => {
                            info!("Received batch with {} rows", batch.num_rows());
                            // TODO: Write batch to parquet files
                            // TODO: Update RocksDB indexes
                        }
                        Err(e) => {
                            error!("Error receiving batch: {}", e);
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Fetch historical data from bridges
    pub async fn fetch_historical(&mut self, start_block: u64, end_block: u64) -> Result<()> {
        for bridge in &mut self.bridges {
            let descriptor = BlockchainDescriptor::historical(
                StreamType::Blocks,
                start_block,
                end_block
            );

            info!("Fetching historical blocks {} to {}", start_block, end_block);
            let batches = bridge.stream_data(&descriptor).await?;

            for batch in batches {
                info!("Processing historical batch with {} rows", batch.num_rows());
                // TODO: Write to parquet files
            }
        }

        Ok(())
    }
}