use anyhow::Result;
use phaser_bridge::{FlightBridgeClient, descriptors::{BlockchainDescriptor, StreamType}};
use futures::StreamExt;
use tracing::{info, error};
use crate::parquet_writer::ParquetWriter;
use std::path::PathBuf;
use tokio::sync::mpsc;
use arrow::array::{RecordBatch, UInt64Array};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Enhanced streaming service with parquet writing capabilities
pub struct StreamingServiceWithWriter {
    bridges: Vec<FlightBridgeClient>,
    data_dir: PathBuf,
    max_file_size_mb: u64,
    segment_size: u64,
}

impl StreamingServiceWithWriter {
    pub async fn new(
        bridge_endpoints: Vec<String>,
        data_dir: PathBuf,
        max_file_size_mb: u64,
        segment_size: u64,
    ) -> Result<Self> {
        let mut bridges = Vec::new();

        for endpoint in bridge_endpoints {
            info!("Connecting to bridge at {}", endpoint);
            let client = FlightBridgeClient::connect(endpoint).await?;
            bridges.push(client);
        }

        // Create data directory
        std::fs::create_dir_all(&data_dir)?;

        Ok(Self {
            bridges,
            data_dir,
            max_file_size_mb,
            segment_size,
        })
    }

    /// Start streaming blocks from all bridges with parquet persistence
    pub async fn start_streaming(&mut self) -> Result<()> {
        // Create channel for passing batches to writer
        let (tx, mut rx) = mpsc::channel::<RecordBatch>(100);

        // Spawn writer task
        let data_dir = self.data_dir.clone();
        let max_file_size_mb = self.max_file_size_mb;
        let segment_size = self.segment_size;

        let writer_handle = tokio::spawn(async move {
            let mut writer = match ParquetWriter::new(data_dir, max_file_size_mb, segment_size) {
                Ok(w) => w,
                Err(e) => {
                    error!("Failed to create parquet writer: {}", e);
                    return;
                }
            };

            while let Some(batch) = rx.recv().await {
                if let Err(e) = writer.write_batch(batch).await {
                    error!("Failed to write batch to parquet: {}", e);
                }
            }

            // Finalize any pending file
            if let Err(e) = writer.finalize_current_file() {
                error!("Failed to finalize parquet file: {}", e);
            }

            info!("Parquet writer task completed");
        });

        // Start streaming from each bridge
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

            let tx_clone = tx.clone();

            // Process blocks in background
            tokio::spawn(async move {
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(batch) => {
                            info!("Received batch with {} rows, block #{}",
                                batch.num_rows(),
                                batch.column(0)
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .and_then(|a| if a.len() > 0 { Some(a.value(0)) } else { None })
                                    .unwrap_or(0)
                            );

                            // Send to writer
                            if let Err(e) = tx_clone.send(batch).await {
                                error!("Failed to send batch to writer: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Error receiving batch: {}", e);
                        }
                    }
                }
                info!("Stream ended");
            });
        }

        Ok(())
    }

    /// Fetch and write historical data from bridges
    pub async fn fetch_historical(&mut self, start_block: u64, end_block: u64) -> Result<()> {
        let mut writer = ParquetWriter::new(
            self.data_dir.clone(),
            self.max_file_size_mb,
            self.segment_size,
        )?;

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
                writer.write_batch(batch).await?;
            }
        }

        writer.finalize_current_file()?;
        info!("Historical data fetch completed");

        Ok(())
    }
}