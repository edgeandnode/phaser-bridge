use alloy::network::{AnyHeader, AnyNetwork, AnyRpcBlock};
use alloy::providers::{Provider, ProviderBuilder};
use alloy_pubsub::Subscription;
use alloy_rpc_types_eth::{BlockNumberOrTag, Filter, Header, Log};
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tracing::{debug, info};

/// Client for connecting to JSON-RPC nodes
#[derive(Clone)]
pub struct JsonRpcClient {
    provider: Arc<dyn Provider<AnyNetwork>>,
    chain_id: u64,
    supports_subscriptions: bool,
}

impl JsonRpcClient {
    /// Connect to a JSON-RPC endpoint (HTTP, WebSocket, or IPC)
    pub async fn connect(url: &str) -> Result<Self> {
        info!("Connecting to JSON-RPC endpoint: {}", url);

        // Determine transport type and subscription support
        let supports_subscriptions = url.starts_with("ws://")
            || url.starts_with("wss://")
            || url.ends_with(".ipc")
            || url.starts_with("/");

        // Build provider based on transport type
        let provider: Arc<dyn Provider<AnyNetwork>> =
            if url.starts_with("ws://") || url.starts_with("wss://") {
                info!("Using WebSocket transport with subscription support");
                Arc::new(
                    ProviderBuilder::new()
                        .network::<AnyNetwork>()
                        .on_ws(alloy::transports::ws::WsConnect::new(url))
                        .await?,
                )
            } else if url.ends_with(".ipc") || url.starts_with("/") {
                info!("Using IPC transport with subscription support");
                Arc::new(
                    ProviderBuilder::new()
                        .network::<AnyNetwork>()
                        .on_ipc(alloy::transports::ipc::IpcConnect::new(url.to_string()))
                        .await?,
                )
            } else {
                info!("Using HTTP transport (no subscription support)");
                Arc::new(
                    ProviderBuilder::new()
                        .network::<AnyNetwork>()
                        .on_http(url.parse()?),
                )
            };

        // Get chain ID
        let chain_id = provider.get_chain_id().await?;
        info!("Connected to chain ID: {}", chain_id);

        // Test connection with block number
        let block_num = provider.get_block_number().await?;
        info!("Current block number: {}", block_num);

        Ok(Self {
            provider,
            chain_id,
            supports_subscriptions,
        })
    }

    /// Get the chain ID
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Check if the transport supports subscriptions
    pub fn supports_subscriptions(&self) -> bool {
        self.supports_subscriptions
    }

    /// Get the current block number
    pub async fn get_block_number(&self) -> Result<u64> {
        Ok(self.provider.get_block_number().await?)
    }

    /// Get a block by number with full transaction details
    pub async fn get_block_with_txs(
        &self,
        number: BlockNumberOrTag,
    ) -> Result<Option<AnyRpcBlock>> {
        debug!("Fetching block {:?} with transactions", number);
        let block = self.provider.get_block(number.into()).full().await?;

        if let Some(ref b) = block {
            debug!(
                "Got block #{} with {} transactions",
                b.header.number,
                b.transactions.len()
            );
        }

        Ok(block)
    }

    /// Get logs matching a filter
    pub async fn get_logs(&self, filter: Filter) -> Result<Vec<Log>> {
        debug!("Fetching logs with filter");
        let logs = self.provider.get_logs(&filter).await?;
        debug!("Got {} logs", logs.len());
        Ok(logs)
    }

    /// Subscribe to new block headers (WebSocket/IPC only)
    pub async fn subscribe_new_heads(&self) -> Result<Subscription<Header<AnyHeader>>> {
        if !self.supports_subscriptions {
            return Err(anyhow!(
                "Transport doesn't support subscriptions (HTTP). Use WebSocket or IPC."
            ));
        }

        info!("Subscribing to new block headers");
        let sub = self.provider.subscribe_blocks().await?;
        Ok(sub)
    }

    /// Subscribe to logs (WebSocket/IPC only)
    pub async fn subscribe_logs(&self, filter: Filter) -> Result<Subscription<Log>> {
        if !self.supports_subscriptions {
            return Err(anyhow!(
                "Transport doesn't support subscriptions (HTTP). Use WebSocket or IPC."
            ));
        }

        info!("Subscribing to logs");
        let sub = self.provider.subscribe_logs(&filter).await?;
        Ok(sub)
    }

    /// Get node client version
    pub async fn get_client_version(&self) -> Result<String> {
        let version = self.provider.get_client_version().await?;
        Ok(version)
    }
}
