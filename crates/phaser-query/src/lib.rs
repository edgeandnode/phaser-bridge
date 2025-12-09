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
pub mod sync;
pub mod trie_writer;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Type alias for blockchain chain ID
pub type ChainId = u64;

/// Identifier for a specific chain/bridge combination
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ChainBridgeId {
    pub chain_id: ChainId,
    pub bridge_name: String,
}

impl ChainBridgeId {
    pub fn new(chain_id: ChainId, bridge_name: impl Into<String>) -> Self {
        Self {
            chain_id,
            bridge_name: bridge_name.into(),
        }
    }
}

/// Shared state tracking live streaming boundaries per chain/bridge
/// This allows the sync service to know where live streaming has started
#[derive(Debug, Clone)]
pub struct LiveStreamingState {
    /// Map from chain/bridge to the current block number being streamed
    /// Set when the first block is received by the streaming service
    boundaries: Arc<RwLock<HashMap<ChainBridgeId, u64>>>,
    /// Track which chain/bridge combinations have live streaming enabled
    enabled: Arc<RwLock<HashSet<ChainBridgeId>>>,
}

impl LiveStreamingState {
    pub fn new() -> Self {
        Self {
            boundaries: Arc::new(RwLock::new(HashMap::new())),
            enabled: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Mark live streaming as enabled for a chain/bridge
    /// This should be called when starting a live streaming service
    pub async fn mark_enabled(&self, id: &ChainBridgeId) {
        let mut enabled = self.enabled.write().await;
        enabled.insert(id.clone());
    }

    /// Check if live streaming is enabled for a chain/bridge
    pub async fn is_enabled(&self, id: &ChainBridgeId) -> bool {
        let enabled = self.enabled.read().await;
        enabled.contains(id)
    }

    /// Set the live streaming boundary for a chain/bridge
    pub async fn set_boundary(&self, id: &ChainBridgeId, block_number: u64) {
        let mut boundaries = self.boundaries.write().await;
        boundaries.insert(id.clone(), block_number);
    }

    /// Get the live streaming boundary for a chain/bridge
    /// Returns None if live streaming hasn't started yet for this chain/bridge
    pub async fn get_boundary(&self, id: &ChainBridgeId) -> Option<u64> {
        let boundaries = self.boundaries.read().await;
        boundaries.get(id).copied()
    }

    /// Wait for live streaming to initialize (with timeout)
    /// Returns the boundary block number if initialized within timeout
    /// Only waits if live streaming is enabled for this chain/bridge
    pub async fn wait_for_boundary(&self, id: &ChainBridgeId, timeout_secs: u64) -> Option<u64> {
        use tokio::time::{sleep, Duration};

        // Only wait if live streaming is enabled
        if !self.is_enabled(id).await {
            return None;
        }

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(timeout_secs) {
            if let Some(boundary) = self.get_boundary(id).await {
                return Some(boundary);
            }
            sleep(Duration::from_millis(100)).await;
        }
        None
    }
}

impl Default for LiveStreamingState {
    fn default() -> Self {
        Self::new()
    }
}

/// Main phaser-query service that combines RPC and SQL interfaces
pub struct PhaserQuery {
    pub catalog: Arc<catalog::RocksDbCatalog>,
    pub config: PhaserConfig,
}

impl PhaserQuery {
    pub async fn new(config: PhaserConfig) -> Result<Self> {
        // Create root data directory if it doesn't exist
        std::fs::create_dir_all(&config.data_root)?;

        // Initialize RocksDB catalog
        let catalog = Arc::new(catalog::RocksDbCatalog::new(&config.rocksdb_path)?);

        // Scan and index parquet files from all bridge directories
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeConfig {
    pub chain_id: u64,
    pub endpoint: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetConfig {
    #[serde(default = "default_compression")]
    pub default_compression: String, // "zstd", "lz4", "snappy", "gzip", "brotli", "none"
    #[serde(default)]
    pub default_compression_level: Option<u32>,
    #[serde(default = "default_row_group_size_mb")]
    pub row_group_size_mb: usize,
    #[serde(default)]
    pub column_options: HashMap<String, ColumnOptions>,
    #[serde(default)]
    pub generate_proofs: bool, // Generate merkle proofs for transactions
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnOptions {
    #[serde(default)]
    pub compression: Option<String>,
    #[serde(default)]
    pub compression_level: Option<u32>,
    #[serde(default)]
    pub encoding: Option<String>, // "plain", "rle", "delta_binary_packed", "dictionary"
    #[serde(default)]
    pub bloom_filter: Option<bool>,
    #[serde(default)]
    pub statistics: Option<String>, // "none", "chunk", "page"
    #[serde(default)]
    pub dictionary: Option<bool>,
}

fn default_compression() -> String {
    "zstd".to_string()
}

fn default_row_group_size_mb() -> usize {
    128
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaserConfig {
    pub rocksdb_path: PathBuf,
    pub data_root: PathBuf,
    pub bridges: Vec<BridgeConfig>,
    #[serde(default = "default_segment_size")]
    pub segment_size: u64, // Blocks per segment (500_000)
    #[serde(default = "default_max_file_size_mb")]
    pub max_file_size_mb: u64, // Max file size before rotation (1024 MB)
    #[serde(default = "default_buffer_timeout_secs")]
    pub buffer_timeout_secs: u64, // Timeout for flushing buffers (60 seconds)
    #[serde(default = "default_rpc_port")]
    pub rpc_port: u16,
    #[serde(default)]
    pub sql_port: u16,
    #[serde(default = "default_sync_admin_port")]
    pub sync_admin_port: u16, // Port for sync admin gRPC (9090)
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16, // Port for Prometheus metrics HTTP server (9091)
    #[serde(default = "default_sync_parallelism")]
    pub sync_parallelism: u32, // Number of parallel workers for historical sync (4)
    #[serde(default = "default_max_concurrent_log_segments")]
    pub max_concurrent_log_segments: u32, // Max segments syncing logs concurrently (16)
    #[serde(default)]
    pub parquet: Option<ParquetConfig>,
    #[serde(default)]
    pub validation_stage: phaser_bridge::ValidationStage, // Validation stage for sync (none, ingestion, conversion, both)
}

fn default_segment_size() -> u64 {
    500_000
}

fn default_max_file_size_mb() -> u64 {
    1024
}

fn default_buffer_timeout_secs() -> u64 {
    60
}

fn default_rpc_port() -> u16 {
    8545
}

fn default_sync_admin_port() -> u16 {
    9090
}

fn default_metrics_port() -> u16 {
    9092
}

fn default_sync_parallelism() -> u32 {
    4
}

fn default_max_concurrent_log_segments() -> u32 {
    16
}

impl PhaserConfig {
    pub fn bridge_data_dir(&self, chain_id: u64, bridge_name: &str) -> PathBuf {
        self.data_root.join(format!("{chain_id}")).join(bridge_name)
    }

    pub fn from_yaml_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {path:?}"))?;
        let config: PhaserConfig = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML config: {path:?}"))?;
        Ok(config)
    }

    pub fn get_bridge(&self, chain_id: u64, bridge_name: &str) -> Option<&BridgeConfig> {
        self.bridges
            .iter()
            .find(|b| b.chain_id == chain_id && b.name == bridge_name)
    }
}
