use anyhow::Result;
use clap::{Parser, Subcommand};

// Use the generated admin proto from phaser_query
use phaser_query::proto::admin::sync_service_client::SyncServiceClient;
use phaser_query::proto::admin::*;

#[derive(Parser, Debug)]
#[clap(author, version, about = "CLI for phaser-query admin operations", long_about = None)]
struct Args {
    /// Admin gRPC endpoint
    #[clap(short, long, default_value = "http://localhost:9090")]
    endpoint: String,

    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start a historical sync job
    Sync {
        /// Chain ID
        #[clap(short, long)]
        chain_id: u64,

        /// Bridge name
        #[clap(short, long)]
        bridge: String,

        /// Starting block number (inclusive)
        #[clap(short, long)]
        from: u64,

        /// Ending block number (inclusive)
        #[clap(short, long)]
        to: u64,
    },
    /// Get status of a sync job
    Status {
        /// Job ID to query
        job_id: String,
    },
    /// Cancel a running sync job
    Cancel {
        /// Job ID to cancel
        job_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut client = SyncServiceClient::connect(args.endpoint).await?;

    match args.command {
        Commands::Sync {
            chain_id,
            bridge,
            from,
            to,
        } => {
            let request = tonic::Request::new(SyncRequest {
                chain_id,
                bridge_name: bridge,
                from_block: from,
                to_block: to,
            });

            let response = client.start_sync(request).await?;
            let resp = response.into_inner();

            if resp.accepted {
                println!("✓ Sync job started");
                println!("  Job ID: {}", resp.job_id);
                println!("  {}", resp.message);
            } else {
                println!("✗ Sync job rejected");
                println!("  {}", resp.message);
            }
        }
        Commands::Status { job_id } => {
            let request = tonic::Request::new(SyncStatusRequest {
                job_id: job_id.clone(),
            });

            let response = client.get_sync_status(request).await?;
            let status = response.into_inner();

            let status_str = match SyncStatus::try_from(status.status) {
                Ok(SyncStatus::Pending) => "PENDING",
                Ok(SyncStatus::Running) => "RUNNING",
                Ok(SyncStatus::Completed) => "COMPLETED",
                Ok(SyncStatus::Failed) => "FAILED",
                Ok(SyncStatus::Cancelled) => "CANCELLED",
                _ => "UNKNOWN",
            };

            println!("Job ID: {}", job_id);
            println!("Status: {}", status_str);
            println!("Progress: {}/{} blocks", status.blocks_synced, status.total_blocks);
            println!("Current block: {}", status.current_block);
            println!("Active workers: {}", status.active_workers);

            if !status.error.is_empty() {
                println!("Error: {}", status.error);
            }
        }
        Commands::Cancel { job_id } => {
            let request = tonic::Request::new(CancelSyncRequest {
                job_id: job_id.clone(),
            });

            let response = client.cancel_sync(request).await?;
            let resp = response.into_inner();

            if resp.success {
                println!("✓ {}", resp.message);
            } else {
                println!("✗ {}", resp.message);
            }
        }
    }

    Ok(())
}
