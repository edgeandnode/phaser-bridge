use anyhow::Result;
use clap::Parser;
use phaser_query::{PhaserConfig, PhaserQuery};
use std::path::PathBuf;
use tracing_subscriber;
use tracing::{info, error};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to RocksDB database for indexes
    #[clap(short, long, default_value = "./rocksdb")]
    rocksdb_path: PathBuf,

    /// Root directory for data files
    #[clap(short, long, default_value = "./data")]
    data_root: PathBuf,

    /// Erigon gRPC endpoint for streaming (via bridge)
    #[clap(short = 'e', long, default_value = "http://localhost:8090")]
    bridge_endpoint: String,

    /// Port for JSON-RPC interface
    #[clap(long, default_value = "8545")]
    rpc_port: u16,

    /// Number of blocks per segment file
    #[clap(long, default_value = "500000")]
    segment_size: u64,

    /// Maximum file size in MB before rotation
    #[clap(long, default_value = "1024")]
    max_file_size_mb: u64,

    /// Buffer timeout in seconds before flushing
    #[clap(long, default_value = "60")]
    buffer_timeout_secs: u64,

    /// Import historical parquet files from directory
    #[clap(long)]
    import_from: Option<PathBuf>,

    /// Enable real-time streaming from bridge
    #[clap(long)]
    enable_streaming: bool,

    /// Enable RPC server
    #[clap(long)]
    enable_rpc: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("phaser_query=info".parse()?)
                .add_directive("phaser_bridge=info".parse()?)
                .add_directive("erigon_bridge=info".parse()?)
        )
        .init();

    let args = Args::parse();

    info!("Starting phaser-query");
    info!("RocksDB path: {:?}", args.rocksdb_path);
    info!("Data root: {:?}", args.data_root);

    // Create configuration
    let config = PhaserConfig {
        rocksdb_path: args.rocksdb_path.clone(),
        data_root: args.data_root.clone(),
        erigon_grpc_endpoint: args.bridge_endpoint.clone(),
        segment_size: args.segment_size,
        max_file_size_mb: args.max_file_size_mb,
        buffer_timeout_secs: args.buffer_timeout_secs,
        rpc_port: args.rpc_port,
        sql_port: 0, // SQL disabled for now
    };

    // Initialize phaser-query
    let phaser = PhaserQuery::new(config.clone()).await?;
    info!("Initialized phaser-query with catalog");

    // Import historical data if requested
    if let Some(import_path) = args.import_from {
        info!("Importing historical parquet files from {:?}", import_path);
        import_historical_data(&phaser, &import_path).await?;
    }

    // Start services based on flags
    let mut handles = vec![];

    // Start streaming service if enabled
    if args.enable_streaming {
        info!("Starting streaming service from bridge at {}", args.bridge_endpoint);

        let config = config.clone();
        let catalog = phaser.catalog.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = start_streaming_service(config, catalog).await {
                error!("Streaming service error: {}", e);
            }
        });
        handles.push(handle);
    }

    // Start RPC server if enabled
    if args.enable_rpc {
        info!("Starting RPC server on port {}", args.rpc_port);

        let catalog = phaser.catalog.clone();
        let port = args.rpc_port;

        let handle = tokio::spawn(async move {
            if let Err(e) = start_rpc_server(catalog, port).await {
                error!("RPC server error: {}", e);
            }
        });
        handles.push(handle);
    }

    if handles.is_empty() {
        info!("No services enabled. Use --enable-streaming or --enable-rpc");
        return Ok(());
    }

    // Wait for Ctrl+C
    info!("Services started. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    Ok(())
}

async fn import_historical_data(
    phaser: &PhaserQuery,
    import_path: &PathBuf
) -> Result<()> {
    use std::fs;

    let historical_dir = phaser.config.historical_dir();

    // Create historical directory if it doesn't exist
    fs::create_dir_all(&historical_dir)?;

    // Find all parquet files in import directory
    let entries = fs::read_dir(import_path)?;
    let mut count = 0;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            let filename = path.file_name().unwrap();
            let dest = historical_dir.join(filename);

            if !dest.exists() {
                info!("Importing {:?}", filename);
                fs::copy(&path, &dest)?;
                count += 1;
            } else {
                info!("Skipping {:?} (already exists)", filename);
            }
        }
    }

    info!("Imported {} parquet files to historical directory", count);

    // Re-index the imported files
    info!("Building indexes for imported files...");
    phaser_query::indexer::build_indexes(&phaser.catalog, &phaser.config).await?;

    Ok(())
}

async fn start_streaming_service(
    config: PhaserConfig,
    catalog: std::sync::Arc<phaser_query::catalog::RocksDbCatalog>,
) -> Result<()> {
    use phaser_query::streaming_with_writer::StreamingServiceWithWriter;

    let streaming_dir = config.streaming_dir();

    let mut service = StreamingServiceWithWriter::new(
        vec![config.erigon_grpc_endpoint.clone()],
        streaming_dir,
        config.max_file_size_mb,
        config.segment_size,
    ).await?;

    info!("Connected to bridge, starting streaming to {:?}", config.streaming_dir());

    // Start streaming with periodic index updates
    let catalog_clone = catalog.clone();
    let config_clone = config.clone();

    tokio::spawn(async move {
        loop {
            // Re-index every 60 seconds to pick up new streaming files
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

            info!("Updating indexes for streaming data...");
            if let Err(e) = phaser_query::indexer::build_indexes(&catalog_clone, &config_clone).await {
                error!("Failed to update indexes: {}", e);
            }
        }
    });

    service.start_streaming().await?;

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