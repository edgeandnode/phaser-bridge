use anyhow::Result;
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use jsonrpc_bridge::JsonRpcFlightBridge;
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::{error, info};
use validators_evm::ExecutorType;

#[derive(Parser, Debug)]
#[command(name = "jsonrpc-bridge")]
#[command(about = "Arrow Flight bridge for JSON-RPC nodes", long_about = None)]
struct Args {
    /// JSON-RPC endpoint (HTTP, WebSocket, or IPC)
    #[arg(long, env = "JSONRPC_URL", default_value = "http://localhost:8545")]
    jsonrpc_url: String,

    /// Chain ID (optional, will be fetched from node if not provided)
    #[arg(long, env = "CHAIN_ID")]
    chain_id: Option<u64>,

    /// Arrow Flight server address
    #[arg(
        long,
        env = "FLIGHT_ADDR",
        default_value = "0.0.0.0:8090",
        conflicts_with = "ipc_path"
    )]
    flight_addr: Option<String>,

    /// Unix socket path for IPC mode
    #[arg(long, env = "FLIGHT_IPC_PATH", conflicts_with = "flight_addr")]
    ipc_path: Option<String>,

    /// Validation executor type (tokio or core)
    #[arg(long, env = "EXECUTOR", value_parser = clap::value_parser!(ExecutorType))]
    executor: Option<ExecutorType>,

    /// Number of threads for validation executor (defaults to core count)
    #[arg(long, env = "EXECUTOR_THREADS")]
    threads: Option<usize>,

    /// Enable debug logging
    #[arg(long, short, env = "DEBUG")]
    debug: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();

    info!("Starting JSON-RPC Flight Bridge");
    info!("Connecting to JSON-RPC endpoint: {}", args.jsonrpc_url);

    // Build validator config if executor is specified
    let validator_config = args
        .executor
        .map(|executor_type| executor_type.build_config(args.threads));

    if let Some(ref config) = validator_config {
        info!("Validation enabled with executor: {:?}", config);
    }

    // Create the bridge
    let bridge =
        JsonRpcFlightBridge::new(args.jsonrpc_url.clone(), args.chain_id, validator_config)
            .await
            .map_err(|e| {
                error!("Failed to create JSON-RPC bridge: {}", e);
                e
            })?;

    let bridge_info = bridge.bridge_info();
    info!(
        "Bridge initialized: {}",
        serde_json::to_string_pretty(&bridge_info)?
    );

    // Create the Flight service
    let flight_service = FlightServiceServer::new(bridge);

    // Start the server
    if let Some(ipc_path) = args.ipc_path {
        info!("Starting Arrow Flight server on IPC: {}", ipc_path);

        // Remove existing socket file if it exists
        if std::path::Path::new(&ipc_path).exists() {
            std::fs::remove_file(&ipc_path)?;
        }

        // Create parent directory if needed
        if let Some(parent) = std::path::Path::new(&ipc_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        #[cfg(unix)]
        {
            use tokio::net::UnixListener;
            use tokio_stream::wrappers::UnixListenerStream;

            let listener = UnixListener::bind(&ipc_path)?;
            let stream = UnixListenerStream::new(listener);

            Server::builder()
                .add_service(flight_service)
                .serve_with_incoming(stream)
                .await?;
        }

        #[cfg(not(unix))]
        {
            return Err(anyhow::anyhow!(
                "IPC mode is only supported on Unix systems"
            ));
        }
    } else if let Some(flight_addr) = args.flight_addr {
        let addr: SocketAddr = flight_addr.parse()?;
        info!("Starting Arrow Flight server on TCP: {}", addr);

        Server::builder()
            .add_service(flight_service)
            .serve(addr)
            .await?;
    } else {
        return Err(anyhow::anyhow!(
            "Either --flight-addr or --ipc-path must be provided"
        ));
    }

    Ok(())
}
