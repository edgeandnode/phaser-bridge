use alloy::network::{AnyHeader, AnyNetwork, AnyRpcBlock, ReceiptResponse};
use alloy::providers::{Provider, ProviderBuilder};
use alloy_pubsub::Subscription;
use alloy_rpc_types_eth::{BlockNumberOrTag, Filter, Header, Log};
use anyhow::{anyhow, Result};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Detected capabilities of the connected node
///
/// Probed at connection time to enable optimal fetching strategies:
/// - `eth_getBlockReceipts` is much faster than individual receipt fetches
/// - `debug_*` methods enable trace data collection
/// - Log range limits affect batch sizing
#[derive(Clone, Debug, Default)]
pub struct NodeCapabilities {
    /// Node supports `eth_getBlockReceipts` (EIP-1474)
    /// Returns all receipts for a block in one call
    pub supports_block_receipts: bool,

    /// Node supports `debug_*` namespace methods
    pub supports_debug_namespace: bool,

    /// Maximum block range for eth_getLogs (None = unlimited)
    /// Some providers limit: Infura=10000, Alchemy=2000, QuickNode=10000
    pub max_logs_block_range: Option<u64>,

    /// Node client version string (from web3_clientVersion)
    pub client_version: String,

    /// Is this an Erigon node? (enables Erigon-specific optimizations)
    pub is_erigon: bool,

    /// Is this a Geth node?
    pub is_geth: bool,

    /// Is this a managed provider (Infura, Alchemy, etc.)?
    pub is_managed_provider: bool,
}

impl NodeCapabilities {
    /// Log a summary of detected capabilities
    pub fn log_summary(&self) {
        info!("Node capabilities detected:");
        info!("  Client: {}", self.client_version);
        info!("  eth_getBlockReceipts: {}", self.supports_block_receipts);
        info!("  debug namespace: {}", self.supports_debug_namespace);
        if let Some(limit) = self.max_logs_block_range {
            info!("  eth_getLogs max range: {} blocks", limit);
        } else {
            info!("  eth_getLogs max range: unlimited");
        }
        if self.is_erigon {
            info!("  Node type: Erigon");
        } else if self.is_geth {
            info!("  Node type: Geth");
        }
    }
}

/// Client for connecting to JSON-RPC nodes
#[derive(Clone)]
pub struct JsonRpcClient {
    provider: Arc<dyn Provider<AnyNetwork>>,
    chain_id: u64,
    supports_subscriptions: bool,
    capabilities: NodeCapabilities,
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
                        .connect_ws(alloy::transports::ws::WsConnect::new(url))
                        .await?,
                )
            } else if url.ends_with(".ipc") || url.starts_with("/") {
                info!("Using IPC transport with subscription support");
                Arc::new(
                    ProviderBuilder::new()
                        .network::<AnyNetwork>()
                        .connect_ipc(alloy::transports::ipc::IpcConnect::new(url.to_string()))
                        .await?,
                )
            } else {
                info!("Using HTTP transport (no subscription support)");
                Arc::new(
                    ProviderBuilder::new()
                        .network::<AnyNetwork>()
                        .connect_http(url.parse()?),
                )
            };

        // Get chain ID
        let chain_id = provider.get_chain_id().await?;
        info!("Connected to chain ID: {}", chain_id);

        // Test connection with block number
        let block_num = provider.get_block_number().await?;
        info!("Current block number: {}", block_num);

        // Detect node capabilities
        let capabilities = Self::detect_capabilities(&provider).await;
        capabilities.log_summary();

        Ok(Self {
            provider,
            chain_id,
            supports_subscriptions,
            capabilities,
        })
    }

    /// Detect node capabilities by probing supported methods
    async fn detect_capabilities(provider: &Arc<dyn Provider<AnyNetwork>>) -> NodeCapabilities {
        // Get client version first
        let client_version = provider
            .get_client_version()
            .await
            .unwrap_or_else(|_| "unknown".to_string());

        // Detect node type from version string
        let version_lower = client_version.to_lowercase();
        let is_erigon = version_lower.contains("erigon");
        let is_geth = version_lower.contains("geth");
        let is_managed_provider = version_lower.contains("infura")
            || version_lower.contains("alchemy")
            || version_lower.contains("quicknode")
            || version_lower.contains("ankr");

        // Probe supported methods
        let supports_block_receipts = Self::probe_block_receipts_support(provider).await;
        let supports_debug_namespace = Self::probe_debug_support(provider).await;

        // Set known limits for managed providers
        let max_logs_block_range = if is_managed_provider {
            Some(2000) // Conservative default - most providers limit to 2000-10000
        } else {
            None
        };

        NodeCapabilities {
            supports_block_receipts,
            supports_debug_namespace,
            max_logs_block_range,
            client_version,
            is_erigon,
            is_geth,
            is_managed_provider,
        }
    }

    /// Probe if eth_getBlockReceipts is supported
    async fn probe_block_receipts_support(provider: &Arc<dyn Provider<AnyNetwork>>) -> bool {
        // Try to get receipts for block 1 (should exist on any chain)
        match provider
            .get_block_receipts(BlockNumberOrTag::Number(1).into())
            .await
        {
            Ok(Some(_)) => {
                debug!("eth_getBlockReceipts is supported");
                true
            }
            Ok(None) => {
                // Block exists but no receipts - method still works
                debug!("eth_getBlockReceipts is supported (no receipts in block 1)");
                true
            }
            Err(e) => {
                let err_str = e.to_string().to_lowercase();
                // Method not supported vs other errors
                if err_str.contains("method not found")
                    || err_str.contains("not supported")
                    || err_str.contains("unknown method")
                {
                    debug!("eth_getBlockReceipts not supported: {}", e);
                    false
                } else {
                    // Other error (rate limit, etc) - assume supported
                    warn!(
                        "eth_getBlockReceipts probe error (assuming supported): {}",
                        e
                    );
                    true
                }
            }
        }
    }

    /// Probe if debug namespace is supported
    async fn probe_debug_support(_provider: &Arc<dyn Provider<AnyNetwork>>) -> bool {
        // We'd need raw RPC calls here since Provider doesn't expose debug_*
        // For now, infer from node type
        // Erigon and Geth both support debug namespace by default
        // Managed providers typically don't expose it
        false // TODO: Implement raw RPC probe
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

    /// Get detected node capabilities
    pub fn capabilities(&self) -> &NodeCapabilities {
        &self.capabilities
    }

    /// Get all receipts for a block (requires eth_getBlockReceipts support)
    ///
    /// This is much more efficient than fetching individual receipts.
    /// Check `capabilities().supports_block_receipts` before calling.
    ///
    /// Returns receipts implementing `ReceiptResponse` trait for accessing
    /// common fields like `status()`, `gas_used()`, `logs()`, etc.
    pub async fn get_block_receipts(
        &self,
        block: BlockNumberOrTag,
    ) -> Result<Option<Vec<impl ReceiptResponse>>> {
        if !self.capabilities.supports_block_receipts {
            return Err(anyhow!("eth_getBlockReceipts not supported by this node"));
        }

        debug!("Fetching all receipts for block {:?}", block);
        let receipts = self.provider.get_block_receipts(block.into()).await?;

        if let Some(ref r) = receipts {
            debug!("Got {} receipts", r.len());
        }

        Ok(receipts)
    }

    /// Get recommended batch size for eth_getLogs based on node capabilities
    ///
    /// Returns the maximum block range that should be used per eth_getLogs call.
    pub fn recommended_logs_batch_size(&self) -> u64 {
        self.capabilities.max_logs_block_range.unwrap_or(10_000) // Default for self-hosted nodes
    }
}
