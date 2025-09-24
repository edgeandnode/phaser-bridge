use crate::catalog::RocksDbCatalog;
use anyhow::Result;
use jsonrpsee::server::{RpcModule, Server};
use jsonrpsee::types::ErrorObjectOwned;
use std::sync::Arc;
use tracing::info;

pub struct RpcServer {
    catalog: Arc<RocksDbCatalog>,
    port: u16,
}

impl RpcServer {
    pub async fn new(catalog: Arc<RocksDbCatalog>, port: u16) -> Result<Self> {
        Ok(Self { catalog, port })
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting RPC server on port {}", self.port);

        let server = Server::builder()
            .build(format!("127.0.0.1:{}", self.port))
            .await?;

        let mut module = RpcModule::new(self.catalog.clone());

        // Register RPC methods
        module.register_async_method("eth_blockNumber", |_, _catalog, _| async move {
            // TODO: Get latest block number from RocksDB
            Ok::<String, ErrorObjectOwned>("0x0".to_string())
        })?;

        module.register_async_method(
            "eth_getBlockByNumber",
            |_params, _catalog, _| async move {
                // TODO: Get block by number from RocksDB index
                Ok::<serde_json::Value, ErrorObjectOwned>(serde_json::json!(null))
            },
        )?;

        module.register_async_method(
            "eth_getTransactionByHash",
            |_params, _catalog, _| async move {
                // TODO: Get transaction by hash from RocksDB index
                Ok::<serde_json::Value, ErrorObjectOwned>(serde_json::json!(null))
            },
        )?;

        module.register_async_method("eth_getLogs", |_params, _catalog, _| async move {
            // TODO: Get logs from RocksDB index
            Ok::<Vec<serde_json::Value>, ErrorObjectOwned>(Vec::new())
        })?;

        let handle = server.start(module);
        handle.stopped().await;

        Ok(())
    }
}
