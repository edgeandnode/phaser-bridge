use anyhow::Result;
use clap::Parser;
use phaser_query::{PhaserQuery, PhaserConfig};
use std::sync::Arc;
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "phaser-query")]
#[command(about = "Lightweight query engine for blockchain data", long_about = None)]
struct Args {
    #[arg(long, env = "PHASER_DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,

    #[arg(long, env = "PHASER_ROCKSDB_PATH", default_value = "./rocksdb")]
    rocksdb_path: PathBuf,

    #[arg(long, env = "ERIGON_GRPC_ENDPOINT", default_value = "localhost:9090")]
    erigon_grpc: String,

    #[arg(long, env = "PHASER_RPC_PORT", default_value_t = 8545)]
    rpc_port: u16,

    #[arg(long, env = "PHASER_SQL_PORT", default_value_t = 8080)]
    sql_port: u16,

    #[arg(long, default_value_t = false)]
    skip_indexing: bool,

    /// Test connection to Erigon (don't start servers)
    #[arg(long, default_value_t = false)]
    test_erigon: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("phaser_query=debug"))
        )
        .init();

    let args = Args::parse();

    // If testing Erigon connection, do that and exit
    if args.test_erigon {
        info!("Testing Erigon connection at {}...", args.erigon_grpc);

        let mut client = phaser_query::erigon_client::ErigonClient::connect(args.erigon_grpc.clone()).await?;

        // Test basic connection
        client.test_connection().await?;

        // Get syncing status
        client.syncing_status().await?;

        // Get latest block
        let latest_block = client.get_latest_block().await?;
        info!("Latest block: {}", latest_block);

        // Skip block fetching due to Erigon nil pointer issue
        // TODO: Debug why block fetching causes nil pointer in Erigon 3.0.15

        // Test subscription
        info!("Testing block header subscription...");
        client.subscribe_headers().await?;

        info!("Erigon connection test completed successfully!");
        return Ok(());
    }

    info!("Starting phaser-query...");
    info!("Configuration:");
    info!("  Data directory: {:?}", args.data_dir);
    info!("  RocksDB path: {:?}", args.rocksdb_path);
    info!("  Erigon gRPC: {}", args.erigon_grpc);
    info!("  RPC port: {}", args.rpc_port);
    info!("  SQL port: {}", args.sql_port);

    let config = PhaserConfig {
        rocksdb_path: args.rocksdb_path,
        data_root: args.data_dir,
        erigon_grpc_endpoint: args.erigon_grpc,
        segment_size: 500_000,        // 500k blocks per segment
        max_file_size_mb: 1024,       // 1GB max file size
        buffer_timeout_secs: 60,      // 60 second buffer timeout
        rpc_port: args.rpc_port,
        sql_port: args.sql_port,
    };

    // Initialize the phaser-query service
    let service = Arc::new(PhaserQuery::new(config.clone()).await?);

    // Start servers in separate tasks
    let rpc_service = service.clone();
    let sql_service = service.clone();
    let rpc_port = config.rpc_port;
    let sql_port = config.sql_port;

    let rpc_handle = tokio::spawn(async move {
        rpc_service.start_rpc_server(rpc_port).await
    });

    let sql_handle = tokio::spawn(async move {
        sql_service.start_sql_server(sql_port).await
    });

    info!("phaser-query initialized successfully");
    info!("RPC server listening on port {}", config.rpc_port);
    info!("SQL server listening on port {}", config.sql_port);

    // Keep running until shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Shutting down phaser-query...");

    // Cancel server tasks
    rpc_handle.abort();
    sql_handle.abort();

    Ok(())
}