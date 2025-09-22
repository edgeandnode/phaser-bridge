mod bridge;
mod client;
mod converter;
mod proto;
mod streaming_service;

use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use bridge::ErigonFlightBridge;
use phaser_bridge::FlightBridgeServer;

#[derive(Parser, Debug)]
#[command(name = "erigon-bridge")]
#[command(about = "Arrow Flight bridge for Erigon blockchain node")]
struct Args {
    /// Erigon gRPC endpoint
    #[arg(long, env = "ERIGON_GRPC_ENDPOINT", default_value = "localhost:9090")]
    erigon_grpc: String,

    /// Flight server address
    #[arg(long, env = "FLIGHT_SERVER_ADDR", default_value = "0.0.0.0:8090")]
    flight_addr: String,

    /// Chain ID
    #[arg(long, env = "CHAIN_ID", default_value_t = 1)]
    chain_id: u64,
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

    info!("Starting Erigon Flight Bridge");
    info!("Erigon endpoint: {}", args.erigon_grpc);
    info!("Flight server: {}", args.flight_addr);
    info!("Chain ID: {}", args.chain_id);

    // Create the bridge
    let bridge = Arc::new(
        ErigonFlightBridge::new(args.erigon_grpc, args.chain_id)
            .await
            .map_err(|e| {
                error!("Failed to create bridge: {}", e);
                e
            })?,
    );

    // Create the Flight server
    let flight_server = FlightBridgeServer::new(bridge);
    let flight_service = flight_server.into_service();

    // Parse the address
    let addr: SocketAddr = args.flight_addr.parse()?;

    info!("Starting Arrow Flight server on {}", addr);

    // Start the server
    tonic::transport::Server::builder()
        .add_service(flight_service)
        .serve(addr)
        .await?;

    Ok(())
}
