pub mod catalog;
pub mod index;
pub mod indexer;
pub mod rpc;
pub mod sql;

use std::sync::Arc;
use anyhow::Result;

/// Main phaser-query service that combines RPC and SQL interfaces
pub struct PhaserQuery {
    pub catalog: Arc<catalog::RocksDbCatalog>,
}

impl PhaserQuery {
    pub async fn new(config: PhaserConfig) -> Result<Self> {
        // Initialize RocksDB catalog
        let catalog = Arc::new(catalog::RocksDbCatalog::new(&config.rocksdb_path)?);

        // Scan and index parquet files
        indexer::build_indexes(&catalog, &config.data_dir).await?;

        Ok(Self {
            catalog,
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
    pub rocksdb_path: String,
    pub data_dir: String,
    pub rpc_port: u16,
    pub sql_port: u16,
}