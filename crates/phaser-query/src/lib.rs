pub mod buffer_manager;
pub mod catalog;
pub mod erigon_client;
pub mod index;
pub mod indexer;
pub mod parquet_writer;
pub mod proto;
pub mod rpc;
pub mod sql;
pub mod streaming_with_writer;

use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;

/// Main phaser-query service that combines RPC and SQL interfaces
pub struct PhaserQuery {
    pub catalog: Arc<catalog::RocksDbCatalog>,
    pub config: PhaserConfig,
}

impl PhaserQuery {
    pub async fn new(config: PhaserConfig) -> Result<Self> {
        // Create data directories if they don't exist
        std::fs::create_dir_all(config.historical_dir())?;
        std::fs::create_dir_all(config.streaming_dir())?;
        std::fs::create_dir_all(config.pending_dir())?;

        // Initialize RocksDB catalog
        let catalog = Arc::new(catalog::RocksDbCatalog::new(&config.rocksdb_path)?);

        // Scan and index parquet files from both historical and streaming directories
        indexer::build_indexes(&catalog, &config).await?;

        Ok(Self {
            catalog,
            config: config.clone(),
        })
    }

    pub async fn start_rpc_server(&self, port: u16) -> Result<()> {
        let server = rpc::RpcServer::new(self.catalog.clone(), port).await?;
        server.start().await
    }

    pub async fn start_sql_server(&self, port: u16) -> Result<()> {
        let server = sql::SqlServer::new(self.catalog.clone(), port).await?;
        server.start().await
    }
}

#[derive(Debug, Clone)]
pub struct PhaserConfig {
    pub rocksdb_path: PathBuf,
    pub data_root: PathBuf,
    pub erigon_grpc_endpoint: String,
    pub segment_size: u64,        // Blocks per segment (500_000)
    pub max_file_size_mb: u64,    // Max file size before rotation (1024 MB)
    pub buffer_timeout_secs: u64, // Timeout for flushing buffers (60 seconds)
    pub rpc_port: u16,
    pub sql_port: u16,
}

impl PhaserConfig {
    pub fn historical_dir(&self) -> PathBuf {
        self.data_root.join("historical")
    }

    pub fn streaming_dir(&self) -> PathBuf {
        self.data_root.join("streaming")
    }

    pub fn pending_dir(&self) -> PathBuf {
        self.data_root.join("pending")
    }
}
