use anyhow::Result;
use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use jsonrpc_bridge::{gather_metrics, JsonRpcFlightBridge, MetricsLayer, SegmentConfig};
use std::net::SocketAddr;
use tonic::transport::Server;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
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

    /// Segment size in blocks for parallel fetching (default: 10000)
    #[arg(long, env = "SEGMENT_SIZE", default_value_t = 10_000)]
    segment_size: u64,

    /// Maximum number of segments to process in parallel (default: 4)
    #[arg(long, env = "MAX_CONCURRENT_SEGMENTS", default_value_t = 4)]
    max_concurrent_segments: usize,

    /// Number of blocks to fetch in parallel within a segment (default: 50)
    ///
    /// For logs: One eth_getLogs call covers this many blocks.
    /// For blocks/txs: This many blocks are fetched concurrently.
    #[arg(long, env = "BLOCKS_PER_BATCH", default_value_t = 50)]
    blocks_per_batch: usize,

    /// Maximum concurrent RPC requests (default: same as blocks_per_batch)
    ///
    /// Limits how many eth_getBlockByNumber calls run simultaneously.
    /// Reduce for rate-limited nodes (e.g., 10-20 for Infura/Alchemy).
    #[arg(long, env = "MAX_CONCURRENT_REQUESTS")]
    max_concurrent_requests: Option<usize>,

    /// Enable debug logging
    #[arg(long, short, env = "DEBUG")]
    debug: bool,

    /// Prometheus metrics port (default: 9091)
    #[arg(long, env = "METRICS_PORT", default_value_t = 9091)]
    metrics_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize tracing with metrics layer
    let log_level = if args.debug {
        "jsonrpc_bridge=debug"
    } else {
        "jsonrpc_bridge=info"
    };
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .with(tracing_subscriber::fmt::layer().with_ansi(false))
        .with(MetricsLayer::new("jsonrpc-bridge"))
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

    // Build segment config from CLI args
    let segment_config = SegmentConfig {
        segment_size: args.segment_size,
        max_concurrent_segments: args.max_concurrent_segments,
        blocks_per_batch: args.blocks_per_batch,
        max_concurrent_requests: args
            .max_concurrent_requests
            .unwrap_or(args.blocks_per_batch),
    };

    info!("Segment configuration:");
    info!("  Segment size: {} blocks", segment_config.segment_size);
    info!(
        "  Max concurrent segments: {}",
        segment_config.max_concurrent_segments
    );
    info!("  Blocks per batch: {}", segment_config.blocks_per_batch);
    info!(
        "  Max concurrent RPC requests: {}",
        segment_config.max_concurrent_requests
    );

    // Create the bridge with segment config
    let bridge = JsonRpcFlightBridge::with_segment_config(
        args.jsonrpc_url.clone(),
        args.chain_id,
        validator_config,
        segment_config,
    )
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

    // Start Prometheus metrics server
    let metrics_port = args.metrics_port;
    tokio::spawn(async move {
        use axum::{response::IntoResponse, routing::get, Router};

        async fn metrics_handler() -> impl IntoResponse {
            match gather_metrics() {
                Ok(metrics) => (
                    axum::http::StatusCode::OK,
                    [("content-type", "text/plain; version=0.0.4")],
                    metrics,
                ),
                Err(e) => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    [("content-type", "text/plain")],
                    format!("Error gathering metrics: {e}"),
                ),
            }
        }

        let app = Router::new().route("/metrics", get(metrics_handler));

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{metrics_port}"))
            .await
            .unwrap();
        info!(
            "Prometheus metrics server listening on {}",
            listener.local_addr().unwrap()
        );

        if let Err(e) = axum::serve(listener, app).await {
            error!("Metrics server error: {}", e);
        }
    });

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
