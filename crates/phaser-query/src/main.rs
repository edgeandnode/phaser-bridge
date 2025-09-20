use anyhow::Result;
use clap::Parser;
use phaser_query::{PhaserQuery, PhaserConfig};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "phaser-query")]
#[command(about = "Lightweight query engine for blockchain data", long_about = None)]
struct Args {
    #[arg(long, env = "PHASER_DATA_DIR", default_value = "./data")]
    data_dir: String,

    #[arg(long, env = "PHASER_ROCKSDB_PATH", default_value = "./rocksdb")]
    rocksdb_path: String,

    #[arg(long, env = "PHASER_RPC_PORT", default_value_t = 8545)]
    rpc_port: u16,

    #[arg(long, env = "PHASER_SQL_PORT", default_value_t = 8080)]
    sql_port: u16,

    #[arg(long, default_value_t = false)]
    skip_indexing: bool,
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

    info!("Starting phaser-query...");
    info!("Configuration:");
    info!("  Data directory: {}", args.data_dir);
    info!("  RocksDB path: {}", args.rocksdb_path);
    info!("  RPC port: {}", args.rpc_port);
    info!("  SQL port: {}", args.sql_port);

    let config = PhaserConfig {
        rocksdb_path: args.rocksdb_path,
        data_dir: args.data_dir,
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