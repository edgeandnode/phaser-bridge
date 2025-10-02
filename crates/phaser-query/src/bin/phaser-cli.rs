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
    /// Get detailed status of all sync jobs
    StatusAll {
        /// Optional: filter by status (PENDING, RUNNING, COMPLETED, FAILED, CANCELLED)
        #[clap(short, long)]
        status: Option<String>,
    },
    /// List all sync jobs
    List {
        /// Optional: filter by status (PENDING, RUNNING, COMPLETED, FAILED, CANCELLED)
        #[clap(short, long)]
        status: Option<String>,
    },
    /// Cancel a running sync job
    Cancel {
        /// Job ID to cancel
        job_id: String,
    },
    /// Analyze gaps in existing data without starting a sync
    Analyze {
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
    /// Stream live progress updates for a sync job
    Progress {
        /// Job ID to monitor
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

                // Show gap analysis if available
                if let Some(gap) = resp.gap_analysis {
                    println!("\nGap Analysis:");
                    if gap.cleaned_temp_files > 0 {
                        println!("  Cleaned {} stale temp files", gap.cleaned_temp_files);
                    }
                    println!("  Total segments: {}", gap.total_segments);
                    println!(
                        "  Complete: {} ({:.1}%)",
                        gap.complete_segments, gap.completion_percentage
                    );
                    println!("  Missing: {}", gap.missing_segments);

                    if !gap.incomplete_details.is_empty() && gap.incomplete_details.len() <= 10 {
                        println!("\n  Incomplete segments:");
                        for detail in &gap.incomplete_details {
                            println!(
                                "    Segment {} (blocks {}-{}): missing {}",
                                detail.segment_num,
                                detail.from_block,
                                detail.to_block,
                                detail.missing_data_types.join(", ")
                            );
                        }
                    } else if gap.incomplete_details.len() > 10 {
                        println!(
                            "\n  {} incomplete segments (showing first 5):",
                            gap.incomplete_details.len()
                        );
                        for detail in gap.incomplete_details.iter().take(5) {
                            println!(
                                "    Segment {} (blocks {}-{}): missing {}",
                                detail.segment_num,
                                detail.from_block,
                                detail.to_block,
                                detail.missing_data_types.join(", ")
                            );
                        }
                    }
                }
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
            println!(
                "Chain: {} / Bridge: {}",
                status.chain_id, status.bridge_name
            );
            println!("Blocks: {}-{}", status.from_block, status.to_block);

            // Progress includes blocks + transactions + logs (3 phases per segment)
            let percent = if status.total_blocks > 0 {
                (status.blocks_synced as f64 / status.total_blocks as f64) * 100.0
            } else {
                0.0
            };
            println!(
                "Progress: {}/{} blocks ({:.1}% - includes blocks+txs+logs)",
                status.blocks_synced, status.total_blocks, percent
            );

            // Only show highest completed block if it's meaningful (> from_block)
            if status.current_block > status.from_block {
                println!("Highest completed: block {}", status.current_block);
            }
            println!("Active workers: {}", status.active_workers);

            if !status.error.is_empty() {
                println!("Error: {}", status.error);
            }
        }
        Commands::StatusAll { status } => {
            let status_filter = if let Some(status_str) = status {
                let filter = match status_str.to_uppercase().as_str() {
                    "PENDING" => SyncStatus::Pending,
                    "RUNNING" => SyncStatus::Running,
                    "COMPLETED" => SyncStatus::Completed,
                    "FAILED" => SyncStatus::Failed,
                    "CANCELLED" => SyncStatus::Cancelled,
                    _ => {
                        println!("Invalid status filter: {}", status_str);
                        println!("Valid values: PENDING, RUNNING, COMPLETED, FAILED, CANCELLED");
                        return Ok(());
                    }
                };
                Some(filter as i32)
            } else {
                None
            };

            let request = tonic::Request::new(ListSyncJobsRequest { status_filter });

            let response = client.list_sync_jobs(request).await?;
            let jobs = response.into_inner().jobs;

            if jobs.is_empty() {
                println!("No sync jobs found");
            } else {
                println!("Found {} sync job(s):\n", jobs.len());
                for job in jobs {
                    let status_str = match SyncStatus::try_from(job.status) {
                        Ok(SyncStatus::Pending) => "PENDING",
                        Ok(SyncStatus::Running) => "RUNNING",
                        Ok(SyncStatus::Completed) => "COMPLETED",
                        Ok(SyncStatus::Failed) => "FAILED",
                        Ok(SyncStatus::Cancelled) => "CANCELLED",
                        _ => "UNKNOWN",
                    };

                    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                    println!("Job ID: {}", job.job_id);
                    println!("Status: {}", status_str);
                    println!("Chain: {} / Bridge: {}", job.chain_id, job.bridge_name);
                    println!("Blocks: {}-{}", job.from_block, job.to_block);

                    let percent = if job.total_blocks > 0 {
                        (job.blocks_synced as f64 / job.total_blocks as f64) * 100.0
                    } else {
                        0.0
                    };
                    println!(
                        "Progress: {}/{} blocks ({:.1}% - includes blocks+txs+logs)",
                        job.blocks_synced, job.total_blocks, percent
                    );

                    if job.current_block > job.from_block {
                        println!("Highest completed: block {}", job.current_block);
                    }
                    println!("Active workers: {}", job.active_workers);
                    if !job.error.is_empty() {
                        println!("Error: {}", job.error);
                    }
                }
                println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            }
        }
        Commands::List { status } => {
            let status_filter = if let Some(status_str) = status {
                let filter = match status_str.to_uppercase().as_str() {
                    "PENDING" => SyncStatus::Pending,
                    "RUNNING" => SyncStatus::Running,
                    "COMPLETED" => SyncStatus::Completed,
                    "FAILED" => SyncStatus::Failed,
                    "CANCELLED" => SyncStatus::Cancelled,
                    _ => {
                        println!("Invalid status filter: {}", status_str);
                        println!("Valid values: PENDING, RUNNING, COMPLETED, FAILED, CANCELLED");
                        return Ok(());
                    }
                };
                Some(filter as i32)
            } else {
                None
            };

            let request = tonic::Request::new(ListSyncJobsRequest { status_filter });

            let response = client.list_sync_jobs(request).await?;
            let jobs = response.into_inner().jobs;

            if jobs.is_empty() {
                println!("No sync jobs found");
            } else {
                println!("Found {} sync job(s):\n", jobs.len());
                for job in jobs {
                    let status_str = match SyncStatus::try_from(job.status) {
                        Ok(SyncStatus::Pending) => "PENDING",
                        Ok(SyncStatus::Running) => "RUNNING",
                        Ok(SyncStatus::Completed) => "COMPLETED",
                        Ok(SyncStatus::Failed) => "FAILED",
                        Ok(SyncStatus::Cancelled) => "CANCELLED",
                        _ => "UNKNOWN",
                    };

                    println!("Job: {}", job.job_id);
                    println!("  Status: {}", status_str);
                    println!("  Chain: {} / Bridge: {}", job.chain_id, job.bridge_name);
                    println!("  Blocks: {}-{}", job.from_block, job.to_block);

                    let percent = if job.total_blocks > 0 {
                        (job.blocks_synced as f64 / job.total_blocks as f64) * 100.0
                    } else {
                        0.0
                    };
                    println!(
                        "  Progress: {}/{} blocks ({:.1}%)",
                        job.blocks_synced, job.total_blocks, percent
                    );
                    println!("  Workers: {}", job.active_workers);
                    if !job.error.is_empty() {
                        println!("  Error: {}", job.error);
                    }
                    println!();
                }
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
        Commands::Analyze {
            chain_id,
            bridge,
            from,
            to,
        } => {
            let request = tonic::Request::new(AnalyzeGapsRequest {
                chain_id,
                bridge_name: bridge,
                from_block: from,
                to_block: to,
            });

            let response = client.analyze_gaps(request).await?;
            let resp = response.into_inner();

            println!("{}", resp.message);

            if let Some(gap) = resp.gap_analysis {
                println!("\nGap Analysis:");
                if gap.cleaned_temp_files > 0 {
                    println!("  Cleaned {} stale temp files", gap.cleaned_temp_files);
                }
                println!("  Total segments: {}", gap.total_segments);
                println!(
                    "  Complete: {} ({:.1}%)",
                    gap.complete_segments, gap.completion_percentage
                );
                println!("  Missing: {}", gap.missing_segments);

                if !gap.incomplete_details.is_empty() {
                    println!("\n  Incomplete segments:");
                    for detail in &gap.incomplete_details {
                        println!(
                            "    Segment {} (blocks {}-{}): missing {}",
                            detail.segment_num,
                            detail.from_block,
                            detail.to_block,
                            detail.missing_data_types.join(", ")
                        );
                    }
                }

                if gap.missing_segments > 0 && gap.segments_to_sync.len() <= 20 {
                    println!("\n  Segments to sync: {:?}", gap.segments_to_sync);
                }
            }
        }
        Commands::Progress { job_id } => {
            use futures::StreamExt;

            let request = tonic::Request::new(SyncProgressRequest {
                job_id: job_id.clone(),
            });

            let mut stream = client.stream_sync_progress(request).await?.into_inner();

            println!("Streaming progress for job {}...\n", job_id);

            while let Some(update) = stream.next().await {
                let update = update?;

                let status_str = match SyncStatus::try_from(update.status) {
                    Ok(SyncStatus::Pending) => "PENDING",
                    Ok(SyncStatus::Running) => "RUNNING",
                    Ok(SyncStatus::Completed) => "COMPLETED",
                    Ok(SyncStatus::Failed) => "FAILED",
                    Ok(SyncStatus::Cancelled) => "CANCELLED",
                    _ => "UNKNOWN",
                };

                let percent = if update.total_blocks > 0 {
                    (update.total_blocks_synced as f64 / update.total_blocks as f64) * 100.0
                } else {
                    0.0
                };

                println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                println!("Status: {} | Progress: {}/{} blocks ({:.1}%)",
                    status_str, update.total_blocks_synced, update.total_blocks, percent);
                println!("Rate: {:.1} blocks/sec | Bytes: {:.2} GB",
                    update.overall_rate, update.total_bytes_written as f64 / 1_000_000_000.0);

                if !update.workers.is_empty() {
                    println!("\nActive Workers: {}", update.workers.len());
                    for worker in &update.workers {
                        let elapsed = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64 - worker.started_at;

                        println!("  Worker {}: {} | blocks {}-{} | {:.1} blocks/sec | {} elapsed",
                            worker.worker_id,
                            worker.stage,
                            worker.from_block,
                            worker.to_block,
                            worker.rate,
                            format_duration(elapsed as u64));
                    }
                }

                println!();

                // Check if job is finished
                if status_str == "COMPLETED" || status_str == "FAILED" || status_str == "CANCELLED" {
                    println!("Job finished with status: {}", status_str);
                    break;
                }
            }
        }
    }

    Ok(())
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if hours > 0 {
        format!("{}h{}m{}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m{}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}
