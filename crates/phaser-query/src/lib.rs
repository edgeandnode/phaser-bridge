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
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shared state tracking live streaming boundaries per chain/bridge
/// This allows the sync service to know where live streaming has started
#[derive(Debug, Clone)]
pub struct LiveStreamingState {
    /// Map from (chain_id, bridge_name) to the current block number being streamed
    /// Set when the first block is received by the streaming service
    boundaries: Arc<RwLock<HashMap<(u64, String), u64>>>,
}

impl LiveStreamingState {
    pub fn new() -> Self {
        Self {
            boundaries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set the live streaming boundary for a chain/bridge
    pub async fn set_boundary(&self, chain_id: u64, bridge_name: &str, block_number: u64) {
        let mut boundaries = self.boundaries.write().await;
        boundaries.insert((chain_id, bridge_name.to_string()), block_number);
    }

    /// Get the live streaming boundary for a chain/bridge
    /// Returns None if live streaming hasn't started yet for this chain/bridge
    pub async fn get_boundary(&self, chain_id: u64, bridge_name: &str) -> Option<u64> {
        let boundaries = self.boundaries.read().await;
        boundaries
            .get(&(chain_id, bridge_name.to_string()))
            .copied()
    }

    /// Wait for live streaming to initialize (with timeout)
    /// Returns the boundary block number if initialized within timeout
    pub async fn wait_for_boundary(
        &self,
        chain_id: u64,
        bridge_name: &str,
        timeout_secs: u64,
    ) -> Option<u64> {
        use tokio::time::{sleep, Duration};

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(timeout_secs) {
            if let Some(boundary) = self.get_boundary(chain_id, bridge_name).await {
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
    #[serde(default = "default_sync_parallelism")]
    pub sync_parallelism: u32, // Number of parallel workers for historical sync (4)
    #[serde(default)]
    pub parquet: Option<ParquetConfig>,
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

fn default_sync_parallelism() -> u32 {
    4
}

impl PhaserConfig {
    pub fn bridge_data_dir(&self, chain_id: u64, bridge_name: &str) -> PathBuf {
        self.data_root
            .join(format!("{}", chain_id))
            .join(bridge_name)
    }

    pub fn from_yaml_file(path: &PathBuf) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {:?}", path))?;
        let config: PhaserConfig = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML config: {:?}", path))?;
        Ok(config)
    }

    pub fn get_bridge(&self, chain_id: u64, bridge_name: &str) -> Option<&BridgeConfig> {
        self.bridges
            .iter()
            .find(|b| b.chain_id == chain_id && b.name == bridge_name)
    }
}
