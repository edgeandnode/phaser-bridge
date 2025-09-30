use anyhow::Result;
use clap::Parser;
use phaser_query::{streaming_with_writer::StreamingServiceWithWriter, PhaserConfig, PhaserQuery};
use std::path::PathBuf;
use tracing::{error, info};
use tracing_subscriber;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to YAML configuration file
    #[clap(short, long)]
    config: PathBuf,

    /// Path to RocksDB database for indexes (overrides config)
    #[clap(short, long)]
    rocksdb_path: Option<PathBuf>,

    /// Root directory for data files (overrides config)
    #[clap(short, long)]
    data_root: Option<PathBuf>,

    /// Disable real-time streaming from bridge (enabled by default if bridges configured)
    #[clap(long)]
    disable_streaming: bool,

    /// Disable RPC server (enabled by default if rpc_port > 0)
    #[clap(long)]
    disable_rpc: bool,

    /// Enable trie streaming (state data) from bridge
    #[clap(long)]
    enable_trie: bool,

    /// Disable sync admin gRPC server (enabled by default if sync_admin_port > 0)
    #[clap(long)]
    disable_sync_admin: bool,

    /// Bridge name to use for streaming (must be defined in config)
    #[clap(long)]
    bridge_name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("phaser_query=info".parse()?)
                .add_directive("phaser_bridge=info".parse()?)
                .add_directive("erigon_bridge=info".parse()?),
        )
        .init();

    let args = Args::parse();

    info!("Starting phaser-query");
    info!("Loading config from: {:?}", args.config);

    // Load configuration from YAML
    let mut config = PhaserConfig::from_yaml_file(&args.config)?;

    // Apply CLI overrides
    if let Some(rocksdb_path) = args.rocksdb_path {
        info!("Overriding RocksDB path: {:?}", rocksdb_path);
        config.rocksdb_path = rocksdb_path;
    }
    if let Some(data_root) = args.data_root {
        info!("Overriding data root: {:?}", data_root);
        config.data_root = data_root;
    }

    info!("RocksDB path: {:?}", config.rocksdb_path);
    info!("Data root: {:?}", config.data_root);
    info!("Bridges configured: {}", config.bridges.len());
    for bridge in &config.bridges {
        info!(
            "  - Chain {}: {} at {}",
            bridge.chain_id, bridge.name, bridge.endpoint
        );
    }

    // Initialize phaser-query
    let phaser = PhaserQuery::new(config.clone()).await?;
    info!("Initialized phaser-query with catalog");

    // Start services based on flags
    let mut handles = vec![];

    // Determine which bridge to use for streaming
    let bridge_name = args.bridge_name.as_deref().unwrap_or("default");

    // Start streaming service if enabled (default: enabled if bridges configured)
    let enable_streaming = !args.disable_streaming && !config.bridges.is_empty();
    if enable_streaming {
        if config.bridges.is_empty() {
            error!("No bridges configured. Please add bridges to config file.");
            return Ok(());
        }

        // Use first bridge if specific bridge not found
        let bridge = config
            .bridges
            .iter()
            .find(|b| b.name == bridge_name)
            .or_else(|| config.bridges.first())
            .ok_or_else(|| anyhow::anyhow!("No bridge configuration found"))?;

        info!(
            "Starting streaming service for chain {} bridge '{}' at {}",
            bridge.chain_id, bridge.name, bridge.endpoint
        );

        let config_clone = config.clone();
        let catalog = phaser.catalog.clone();
        let bridge_clone = bridge.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = start_streaming_service(config_clone, catalog, bridge_clone).await {
                error!("Streaming service error: {}", e);
            }
        });
        handles.push(handle);
    }

    // Start trie streaming service if enabled
    if args.enable_trie {
        if config.bridges.is_empty() {
            error!("No bridges configured. Please add bridges to config file.");
            return Ok(());
        }

        // Use first bridge if specific bridge not found
        let bridge = config
            .bridges
            .iter()
            .find(|b| b.name == bridge_name)
            .or_else(|| config.bridges.first())
            .ok_or_else(|| anyhow::anyhow!("No bridge configuration found"))?;

        info!(
            "Starting trie streaming service for chain {} bridge '{}' at {}",
            bridge.chain_id, bridge.name, bridge.endpoint
        );

        let config_clone = config.clone();
        let catalog = phaser.catalog.clone();
        let bridge_clone = bridge.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) =
                start_trie_streaming_service(config_clone, catalog, bridge_clone).await
            {
                error!("Trie streaming service error: {}", e);
            }
        });
        handles.push(handle);
    }

    // Start RPC server if enabled (default: enabled if rpc_port > 0)
    let enable_rpc = !args.disable_rpc && config.rpc_port > 0;
    if enable_rpc {
        info!("Starting RPC server on port {}", config.rpc_port);

        let catalog = phaser.catalog.clone();
        let port = config.rpc_port;

        let handle = tokio::spawn(async move {
            if let Err(e) = start_rpc_server(catalog, port).await {
                error!("RPC server error: {}", e);
            }
        });
        handles.push(handle);
    }

    // Start sync admin server if enabled (default: enabled if sync_admin_port > 0)
    let enable_sync_admin = !args.disable_sync_admin && config.sync_admin_port > 0;
    if enable_sync_admin {
        info!(
            "Starting sync admin gRPC server on port {}",
            config.sync_admin_port
        );

        let config_clone = config.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = start_sync_admin_server(config_clone).await {
                error!("Sync admin server error: {}", e);
            }
        });
        handles.push(handle);
    }

    if handles.is_empty() {
        info!("No services enabled.");
        info!("Services are enabled by default based on config:");
        info!("  - Streaming: enabled if bridges configured (disable with --disable-streaming)");
        info!("  - RPC: enabled if rpc_port > 0 (disable with --disable-rpc)");
        info!("  - Sync Admin: enabled if sync_admin_port > 0 (disable with --disable-sync-admin)");
        info!("  - Trie: disabled by default (enable with --enable-trie)");
        return Ok(());
    }

    // Wait for Ctrl+C
    info!("Services started. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    Ok(())
}

async fn start_streaming_service(
    config: PhaserConfig,
    catalog: std::sync::Arc<phaser_query::catalog::RocksDbCatalog>,
    bridge: phaser_query::BridgeConfig,
) -> Result<()> {
    let data_dir = config.bridge_data_dir(bridge.chain_id, &bridge.name);

    let mut service = StreamingServiceWithWriter::new(
        vec![bridge.endpoint.clone()],
        data_dir.clone(),
        config.max_file_size_mb,
        config.segment_size,
    )
    .await?;

    info!(
        "Connected to bridge, starting streaming to {:?}",
        data_dir
    );

    // Start streaming with periodic index updates
    let catalog_clone = catalog.clone();
    let config_clone = config.clone();

    tokio::spawn(async move {
        loop {
            // Re-index every 60 seconds
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

            info!("Updating indexes...");
            if let Err(e) = phaser_query::indexer::build_indexes(
                &catalog_clone,
                &config_clone,
            )
            .await
            {
                error!("Failed to update indexes: {}", e);
            }
        }
    });

    service.start_streaming().await?;

    Ok(())
}

async fn start_trie_streaming_service(
    config: PhaserConfig,
    catalog: std::sync::Arc<phaser_query::catalog::RocksDbCatalog>,
    bridge: phaser_query::BridgeConfig,
) -> Result<()> {
    let data_dir = config.bridge_data_dir(bridge.chain_id, &bridge.name);

    // Create streaming service with writer
    let mut service = StreamingServiceWithWriter::new(
        vec![bridge.endpoint.clone()],
        data_dir,
        config.max_file_size_mb,
        config.segment_size,
    )
    .await?;

    // Set the RocksDB instance for trie storage
    service.set_db(catalog.db.clone());

    info!("Connected to bridge for trie streaming, will store in RocksDB");

    // Start trie streaming
    service.start_trie_streaming().await?;

    Ok(())
}

async fn start_rpc_server(
    catalog: std::sync::Arc<phaser_query::catalog::RocksDbCatalog>,
    port: u16,
) -> Result<()> {
    use phaser_query::rpc::RpcServer;

    let server = RpcServer::new(catalog, port).await?;
    server.start().await?;

    Ok(())
}

async fn start_sync_admin_server(config: PhaserConfig) -> Result<()> {
    use phaser_query::sync::SyncServer;

    let server = SyncServer::new(std::sync::Arc::new(config.clone()));
    server.start(config.sync_admin_port).await?;

    Ok(())
}
