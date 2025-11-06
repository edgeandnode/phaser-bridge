mod blockdata_client;
mod blockdata_converter;
mod bridge;
mod client;
mod client_pool;
mod converter;
mod error;
mod generated;
mod metrics;
mod proto;
mod segment_worker;
mod streaming_service;
mod trie_client;
mod trie_converter;

use anyhow::Result;
use clap::Parser;
use segment_worker::SegmentConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use validators_evm::ExecutorType;

use bridge::ErigonFlightBridge;
use phaser_bridge::FlightBridgeServer;

#[derive(Parser, Debug)]
#[command(name = "erigon-bridge")]
#[command(about = "Arrow Flight bridge for Erigon blockchain node")]
struct Args {
    /// Erigon gRPC endpoint (TCP: localhost:9090 or IPC: /path/to/erigon.ipc)
    /// This endpoint is used for both BlockData streaming and KV table access (TxSender)
    #[arg(long, env = "ERIGON_GRPC_ENDPOINT", default_value = "localhost:9090")]
    erigon_grpc: String,

    /// Flight server address (TCP)
    #[arg(long, env = "FLIGHT_SERVER_ADDR", conflicts_with = "ipc_path")]
    flight_addr: Option<String>,

    /// Unix socket path for IPC mode
    #[arg(long, env = "FLIGHT_IPC_PATH", conflicts_with = "flight_addr")]
    ipc_path: Option<String>,

    /// Chain ID
    #[arg(long, env = "CHAIN_ID", default_value_t = 1)]
    chain_id: u64,

    /// Validation executor type (tokio or core)
    #[arg(long, env = "EXECUTOR", value_parser = clap::value_parser!(ExecutorType))]
    executor: Option<ExecutorType>,

    /// Number of threads for validation executor (defaults to core count)
    #[arg(long, env = "EXECUTOR_THREADS")]
    threads: Option<usize>,

    /// Segment size in blocks (aligned with Erigon snapshots, default: 500_000)
    #[arg(long, env = "SEGMENT_SIZE", default_value_t = 500_000)]
    segment_size: u64,

    /// Maximum number of segments to process in parallel (default: num_cpus / 4)
    #[arg(long, env = "MAX_CONCURRENT_SEGMENTS")]
    max_concurrent_segments: Option<usize>,

    /// Number of independent gRPC connections in the connection pool (default: 8)
    /// Each connection has its own HTTP/2 session with independent flow control.
    /// Increase this if you have many concurrent segments and see connection bottlenecks.
    #[arg(long, env = "CONNECTION_POOL_SIZE", default_value_t = 8)]
    connection_pool_size: usize,

    /// Validation batch size within a segment (default: 100 blocks)
    #[arg(long, env = "VALIDATION_BATCH_SIZE", default_value_t = 100)]
    validation_batch_size: usize,

    /// Prometheus metrics port (default: 9091)
    #[arg(long, env = "METRICS_PORT", default_value_t = 9091)]
    metrics_port: u16,

    /// gRPC compression for Arrow Flight streams (none, gzip, zstd)
    #[arg(long, env = "COMPRESSION", default_value = "none")]
    compression: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("erigon_bridge=info")),
        )
        .init();

    let args = Args::parse();

    // Determine transport mode and set defaults
    let (transport_mode, endpoint) = if let Some(ipc_path) = args.ipc_path {
        ("IPC", ipc_path)
    } else {
        (
            "TCP",
            args.flight_addr
                .unwrap_or_else(|| "0.0.0.0:8090".to_string()),
        )
    };

    info!("Starting Erigon Flight Bridge");
    info!("Erigon endpoint: {}", args.erigon_grpc);
    info!("Transport mode: {}", transport_mode);
    info!("Endpoint: {}", endpoint);
    info!("Chain ID: {}", args.chain_id);

    // Build validator config if executor is specified
    let validator_config = args
        .executor
        .map(|executor_type| executor_type.build_config(args.threads));

    if let Some(ref config) = validator_config {
        info!("Validation enabled with executor: {:?}", config);
    }

    // Build segment config
    let segment_config = SegmentConfig {
        segment_size: args.segment_size,
        max_concurrent_segments: args
            .max_concurrent_segments
            .unwrap_or_else(|| (num_cpus::get() / 4).max(1)),
        connection_pool_size: args.connection_pool_size,
        validation_batch_size: args.validation_batch_size,
    };

    info!("Segment configuration:");
    info!("  Segment size: {} blocks", segment_config.segment_size);
    info!(
        "  Max concurrent segments: {}",
        segment_config.max_concurrent_segments
    );
    info!(
        "  Connection pool size: {} connections",
        segment_config.connection_pool_size
    );
    info!(
        "  Validation batch size: {} blocks",
        segment_config.validation_batch_size
    );

    // Start Prometheus metrics server
    let metrics_port = args.metrics_port;
    tokio::spawn(async move {
        use axum::{response::IntoResponse, routing::get, Router};

        async fn metrics_handler() -> impl IntoResponse {
            match metrics::gather_metrics() {
                Ok(metrics) => (
                    axum::http::StatusCode::OK,
                    [("content-type", "text/plain; version=0.0.4")],
                    metrics,
                ),
                Err(e) => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    [("content-type", "text/plain")],
                    format!("Error gathering metrics: {}", e),
                ),
            }
        }

        let app = Router::new().route("/metrics", get(metrics_handler));

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", metrics_port))
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

    // Create the bridge
    let bridge = Arc::new(
        ErigonFlightBridge::new(
            args.erigon_grpc.clone(),
            args.chain_id,
            validator_config,
            Some(segment_config),
        )
        .await
        .map_err(|e| {
            error!("Failed to create bridge: {}", e);
            e
        })?,
    );

    // Create the Flight server
    let flight_server = FlightBridgeServer::new(bridge);

    // Configure global maximum message size (256MB)
    // Per-stream limits are negotiated via StreamPreferences
    const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

    // Configure compression based on CLI flag
    let mut flight_service = flight_server
        .into_service()
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_MESSAGE_SIZE);

    match args.compression.to_lowercase().as_str() {
        "gzip" => {
            info!("Enabling Gzip compression for Arrow Flight streams");
            flight_service = flight_service
                .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
                .send_compressed(tonic::codec::CompressionEncoding::Gzip);
        }
        "zstd" => {
            info!("Enabling Zstd compression for Arrow Flight streams");
            flight_service = flight_service
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                .send_compressed(tonic::codec::CompressionEncoding::Zstd);
        }
        "none" => {
            info!("Compression disabled for Arrow Flight streams");
        }
        other => {
            error!(
                "Invalid compression option '{}', valid options: none, gzip, zstd",
                other
            );
            return Err(anyhow::anyhow!("Invalid compression option: {}", other));
        }
    }

    // Start server based on transport mode
    if transport_mode == "IPC" {
        // Unix domain socket mode
        use std::fs;
        use tokio::net::UnixListener;
        use tokio_stream::wrappers::UnixListenerStream;

        // Remove existing socket file if it exists
        let _ = fs::remove_file(&endpoint);

        // Create parent directory if needed
        if let Some(parent) = std::path::Path::new(&endpoint).parent() {
            fs::create_dir_all(parent)?;
        }

        let uds_listener = UnixListener::bind(&endpoint)?;
        let uds_stream = UnixListenerStream::new(uds_listener);

        info!("Starting Arrow Flight server on Unix socket: {}", endpoint);

        tonic::transport::Server::builder()
            .add_service(flight_service)
            .serve_with_incoming(uds_stream)
            .await?;
    } else {
        // TCP mode
        let addr: SocketAddr = endpoint.parse()?;

        info!("Starting Arrow Flight server on TCP: {}", addr);

        tonic::transport::Server::builder()
            .add_service(flight_service)
            .serve(addr)
            .await?;
    }

    Ok(())
}
