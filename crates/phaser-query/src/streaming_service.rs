use anyhow::Result;
use arrow::datatypes::Schema;
use phaser_bridge::{FlightBridgeClient, descriptors::{BlockchainDescriptor, StreamType}};
use futures::StreamExt;
use rocksdb::DB;
use std::sync::Arc;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, error, warn};

use crate::streaming_writer::{DualWriteStreamingWriter, FlushPolicy};

/// Streaming service with dual-write strategy
pub struct StreamingServiceV2 {
    bridges: Vec<FlightBridgeClient>,
    writer: Arc<tokio::sync::Mutex<DualWriteStreamingWriter>>,
    shutdown: tokio::sync::watch::Receiver<bool>,
}

impl StreamingServiceV2 {
    pub async fn new(
        bridge_endpoints: Vec<String>,
        db: Arc<DB>,
        temp_cf_name: &str,
        index_cf_name: &str,
        streaming_dir: PathBuf,
        flush_policy: FlushPolicy,
        schema: Arc<Schema>,
    ) -> Result<Self> {
        let mut bridges = Vec::new();

        for endpoint in bridge_endpoints {
            info!("Connecting to bridge at {}", endpoint);
            let client = FlightBridgeClient::connect(endpoint).await?;
            bridges.push(client);
        }

        let writer = DualWriteStreamingWriter::new(
            db,
            temp_cf_name,
            index_cf_name,
            streaming_dir,
            flush_policy,
            schema,
        )?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        // Store the sender for later shutdown
        std::mem::forget(shutdown_tx); // We'll manage this externally

        Ok(Self {
            bridges,
            writer: Arc::new(tokio::sync::Mutex::new(writer)),
            shutdown: shutdown_rx,
        })
    }

    /// Start streaming blocks from all bridges
    pub async fn start_streaming(&mut self) -> Result<()> {
        for (idx, bridge) in self.bridges.iter_mut().enumerate() {
            // Check bridge health
            if !bridge.health_check().await? {
                error!("Bridge {} health check failed", idx);
                continue;
            }

            // Subscribe to blocks
            let descriptor = BlockchainDescriptor::live(StreamType::Blocks, None);

            info!("Subscribing to blocks from bridge {}", idx);
            let mut stream = bridge.subscribe(&descriptor).await?;

            let writer = Arc::clone(&self.writer);
            let mut shutdown = self.shutdown.clone();

            // Process blocks in background
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        // Check for shutdown signal
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                info!("Shutdown signal received for bridge {}", idx);
                                break;
                            }
                        }

                        // Process next batch
                        batch_result = stream.next() => {
                            match batch_result {
                                Some(Ok(batch)) => {
                                    info!("Bridge {}: received batch with {} rows", idx, batch.num_rows());

                                    // Debug: Print schema on first batch
                                    let schema = batch.schema();
                                    let column_names: Vec<_> = schema.fields().iter()
                                        .map(|f| f.name().as_str())
                                        .collect();

                                    // Try different possible column names for block number
                                    let block_num = if let Some(col) = batch.column_by_name("block_num") {
                                        col.as_any().downcast_ref::<arrow::array::UInt64Array>()
                                            .and_then(|arr| if arr.len() > 0 { Some(arr.value(0)) } else { None })
                                    } else if let Some(col) = batch.column_by_name("block_number") {
                                        col.as_any().downcast_ref::<arrow::array::UInt64Array>()
                                            .and_then(|arr| if arr.len() > 0 { Some(arr.value(0)) } else { None })
                                    } else if let Some(col) = batch.column_by_name("number") {
                                        col.as_any().downcast_ref::<arrow::array::UInt64Array>()
                                            .and_then(|arr| if arr.len() > 0 { Some(arr.value(0)) } else { None })
                                    } else {
                                        warn!("Bridge {}: No block number column found. Schema: {:?}", idx, column_names);
                                        None
                                    };

                                    if let Some(block_num) = block_num {
                                        // Write using dual-write strategy
                                        let mut writer_guard = writer.lock().await;
                                        match writer_guard.write_block(block_num, batch).await {
                                            Ok(_) => {
                                                info!("Bridge {}: wrote block {}", idx, block_num);
                                            }
                                            Err(e) => {
                                                error!("Bridge {}: failed to write block {}: {}", idx, block_num, e);
                                            }
                                        }
                                    } else {
                                        warn!("Bridge {}: Could not extract block number from batch", idx);
                                    }
                                }
                                Some(Err(e)) => {
                                    error!("Bridge {}: error receiving batch: {}", idx, e);
                                }
                                None => {
                                    warn!("Bridge {}: stream ended", idx);
                                    break;
                                }
                            }
                        }
                    }
                }

                info!("Bridge {} streaming task exiting", idx);
            });
        }

        // Start periodic flush task
        self.start_periodic_flush().await;

        Ok(())
    }

    /// Start periodic flush task to ensure data is written even during quiet periods
    async fn start_periodic_flush(&self) {
        let writer = Arc::clone(&self.writer);
        let mut shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));

            loop {
                tokio::select! {
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            info!("Periodic flush shutting down");
                            break;
                        }
                    }

                    _ = interval.tick() => {
                        let mut writer_guard = writer.lock().await;
                        if let Err(e) = writer_guard.force_flush().await {
                            error!("Periodic flush failed: {}", e);
                        }
                    }
                }
            }
        });
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down streaming service");

        // Finalize any active files
        let mut writer = self.writer.lock().await;
        writer.finalize().await?;

        info!("Streaming service shutdown complete");
        Ok(())
    }

    /// Get writer statistics
    pub async fn get_stats(&self) -> crate::streaming_writer::WriterStats {
        let writer = self.writer.lock().await;
        writer.get_stats()
    }
}

/// Builder for configuring the streaming service
pub struct StreamingServiceBuilder {
    bridge_endpoints: Vec<String>,
    db: Option<Arc<DB>>,
    temp_cf_name: String,
    index_cf_name: String,
    streaming_dir: Option<PathBuf>,
    flush_policy: FlushPolicy,
    schema: Option<Arc<Schema>>,
}

impl StreamingServiceBuilder {
    pub fn new() -> Self {
        Self {
            bridge_endpoints: Vec::new(),
            db: None,
            temp_cf_name: "streaming_temp".to_string(),
            index_cf_name: "streaming_index".to_string(),
            streaming_dir: None,
            flush_policy: FlushPolicy::default(),
            schema: None,
        }
    }

    pub fn with_bridges(mut self, endpoints: Vec<String>) -> Self {
        self.bridge_endpoints = endpoints;
        self
    }

    pub fn with_db(mut self, db: Arc<DB>) -> Self {
        self.db = Some(db);
        self
    }

    pub fn with_column_families(mut self, temp_cf: &str, index_cf: &str) -> Self {
        self.temp_cf_name = temp_cf.to_string();
        self.index_cf_name = index_cf.to_string();
        self
    }

    pub fn with_streaming_dir(mut self, dir: PathBuf) -> Self {
        self.streaming_dir = Some(dir);
        self
    }

    pub fn with_flush_policy(mut self, policy: FlushPolicy) -> Self {
        self.flush_policy = policy;
        self
    }

    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = Some(schema);
        self
    }

    pub async fn build(self) -> Result<StreamingServiceV2> {
        let db = self.db.ok_or_else(|| anyhow::anyhow!("DB is required"))?;
        let streaming_dir = self.streaming_dir
            .ok_or_else(|| anyhow::anyhow!("Streaming directory is required"))?;
        let schema = self.schema
            .ok_or_else(|| anyhow::anyhow!("Schema is required"))?;

        StreamingServiceV2::new(
            self.bridge_endpoints,
            db,
            &self.temp_cf_name,
            &self.index_cf_name,
            streaming_dir,
            self.flush_policy,
            schema,
        ).await
    }
}