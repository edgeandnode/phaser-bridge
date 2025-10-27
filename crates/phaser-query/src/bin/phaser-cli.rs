use anyhow::Result;
use clap::{Parser, Subcommand};

// Use the generated admin proto from phaser_query
use phaser_query::proto::admin::sync_service_client::SyncServiceClient;
use phaser_query::proto::admin::*;

#[derive(Parser, Debug)]
#[clap(author, version, about = "CLI for phaser-query admin operations", long_about = None)]
struct Args {
    /// Admin gRPC endpoint
    #[clap(short, long)]
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
    /// Get status of sync job(s). Without job-id, shows all jobs with detailed view.
    Status {
        /// Optional: Job ID to query. If omitted, shows all jobs.
        job_id: Option<String>,

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
        Commands::Status {
            job_id,
            status: status_filter,
        } => {
            if let Some(job_id) = job_id {
                // Show single job with detailed view
                let request = tonic::Request::new(SyncStatusRequest {
                    job_id: job_id.clone(),
                });

                let response = client.get_sync_status(request).await?;
                let job = response.into_inner();

                // Reuse the detailed display code from status-all
                let status_str = match SyncStatus::try_from(job.status) {
                    Ok(SyncStatus::Pending) => "PENDING",
                    Ok(SyncStatus::Running) => "RUNNING",
                    Ok(SyncStatus::Completed) => "COMPLETED",
                    Ok(SyncStatus::Failed) => "FAILED",
                    Ok(SyncStatus::Cancelled) => "CANCELLED",
                    _ => "UNKNOWN",
                };

                println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                println!("Job ID: {}", job_id);
                println!("Status: {}", status_str);
                println!("Chain: {} / Bridge: {}", job.chain_id, job.bridge_name);
                println!("Blocks: {}-{}", job.from_block, job.to_block);

                // Show detailed progress
                if let Some(ref progress) = job.data_progress {
                    let format_size = |bytes: u64| -> String {
                        if bytes >= 1_000_000_000_000 {
                            format!("{:.2} TB", bytes as f64 / 1_000_000_000_000.0)
                        } else if bytes >= 1_000_000_000 {
                            format!("{:.1} GB", bytes as f64 / 1_000_000_000.0)
                        } else if bytes >= 1_000_000 {
                            format!("{:.1} MB", bytes as f64 / 1_000_000.0)
                        } else if bytes >= 1_000 {
                            format!("{:.1} KB", bytes as f64 / 1_000.0)
                        } else {
                            format!("{} bytes", bytes)
                        }
                    };

                    if let Some(ref gap) = job.gap_analysis {
                        println!("\nData Progress (by segment):");
                        let total_segments = gap.total_segments;
                        let mut blocks_incomplete = 0;
                        let mut txs_incomplete = 0;
                        let mut logs_incomplete = 0;

                        for detail in &gap.incomplete_details {
                            if !detail.missing_blocks_ranges.is_empty() {
                                blocks_incomplete += 1;
                            }
                            if !detail.missing_transactions_ranges.is_empty() {
                                txs_incomplete += 1;
                            }
                            if !detail.missing_logs_ranges.is_empty() {
                                logs_incomplete += 1;
                            }
                        }

                        if let Some(ref blocks) = progress.blocks {
                            let blocks_complete = total_segments - blocks_incomplete;
                            println!(
                                "  Blocks:        {}/{} segments - {} files, {}{}",
                                blocks_complete,
                                total_segments,
                                progress
                                    .file_stats
                                    .as_ref()
                                    .map(|s| s.blocks_files)
                                    .unwrap_or(0),
                                format_size(
                                    progress
                                        .file_stats
                                        .as_ref()
                                        .map(|s| s.blocks_disk_bytes)
                                        .unwrap_or(0)
                                ),
                                if blocks.gap_count > 0 {
                                    format!(" ({} gaps)", blocks.gap_count)
                                } else {
                                    String::new()
                                }
                            );
                        }

                        if let Some(ref txs) = progress.transactions {
                            let txs_complete = total_segments - txs_incomplete;
                            println!(
                                "  Transactions:  {}/{} segments - {} files, {}{}",
                                txs_complete,
                                total_segments,
                                progress
                                    .file_stats
                                    .as_ref()
                                    .map(|s| s.transactions_files)
                                    .unwrap_or(0),
                                format_size(
                                    progress
                                        .file_stats
                                        .as_ref()
                                        .map(|s| s.transactions_disk_bytes)
                                        .unwrap_or(0)
                                ),
                                if txs.gap_count > 0 {
                                    format!(" ({} gaps)", txs.gap_count)
                                } else {
                                    String::new()
                                }
                            );
                        }

                        if let Some(ref logs) = progress.logs {
                            let logs_complete = total_segments - logs_incomplete;
                            println!(
                                "  Logs:          {}/{} segments - {} files, {}{}",
                                logs_complete,
                                total_segments,
                                progress
                                    .file_stats
                                    .as_ref()
                                    .map(|s| s.logs_files)
                                    .unwrap_or(0),
                                format_size(
                                    progress
                                        .file_stats
                                        .as_ref()
                                        .map(|s| s.logs_disk_bytes)
                                        .unwrap_or(0)
                                ),
                                if logs.gap_count > 0 {
                                    format!(" ({} gaps)", logs.gap_count)
                                } else {
                                    String::new()
                                }
                            );
                        }

                        if let Some(ref stats) = progress.file_stats {
                            println!(
                                "\nTotal Files: {} ({})",
                                stats.total_files,
                                format_size(stats.total_disk_bytes)
                            );
                        }
                        println!();
                    }
                }

                // Show segment completeness
                if let Some(ref gap) = job.gap_analysis {
                    println!(
                        "Complete Segments: {}/{} ({:.1}% of segments)",
                        gap.complete_segments, gap.total_segments, gap.completion_percentage
                    );

                    if gap.missing_segments > 0 {
                        let mut missing_blocks_count = 0;
                        let mut missing_txs_count = 0;
                        let mut missing_logs_count = 0;

                        for detail in &gap.incomplete_details {
                            if !detail.missing_blocks_ranges.is_empty() {
                                missing_blocks_count += 1;
                            }
                            if !detail.missing_transactions_ranges.is_empty() {
                                missing_txs_count += 1;
                            }
                            if !detail.missing_logs_ranges.is_empty() {
                                missing_logs_count += 1;
                            }
                        }

                        println!("Incomplete Segments: {}", gap.missing_segments);
                        if missing_blocks_count > 0 {
                            println!("  - {} segments missing blocks", missing_blocks_count);
                        }
                        if missing_txs_count > 0 {
                            println!("  - {} segments missing transactions", missing_txs_count);
                        }
                        if missing_logs_count > 0 {
                            println!("  - {} segments missing logs", missing_logs_count);
                        }
                    }

                    if job.current_block > job.from_block {
                        println!("Highest completed: block {}", job.current_block);
                    }
                } else {
                    let percent = if job.total_blocks > 0 {
                        (job.blocks_synced as f64 / job.total_blocks as f64) * 100.0
                    } else {
                        0.0
                    };
                    println!("Progress: {:.1}%", percent);
                    if job.current_block > job.from_block {
                        println!("Highest completed: block {}", job.current_block);
                    }
                }
                println!("Active workers: {}", job.active_workers);

                if job.download_rate_bytes_per_sec > 0.0 {
                    let rate = if job.download_rate_bytes_per_sec >= 1_000_000_000.0 {
                        format!(
                            "{:.2} GB/s",
                            job.download_rate_bytes_per_sec / 1_000_000_000.0
                        )
                    } else if job.download_rate_bytes_per_sec >= 1_000_000.0 {
                        format!("{:.1} MB/s", job.download_rate_bytes_per_sec / 1_000_000.0)
                    } else if job.download_rate_bytes_per_sec >= 1_000.0 {
                        format!("{:.1} KB/s", job.download_rate_bytes_per_sec / 1_000.0)
                    } else {
                        format!("{:.0} B/s", job.download_rate_bytes_per_sec)
                    };
                    println!("Download rate: {}", rate);
                }

                if !job.error.is_empty() {
                    println!("Error: {}", job.error);
                }
                println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            } else {
                // Show all jobs (what status-all did)
                let status_filter = if let Some(status_str) = status_filter {
                    let filter = match status_str.to_uppercase().as_str() {
                        "PENDING" => SyncStatus::Pending,
                        "RUNNING" => SyncStatus::Running,
                        "COMPLETED" => SyncStatus::Completed,
                        "FAILED" => SyncStatus::Failed,
                        "CANCELLED" => SyncStatus::Cancelled,
                        _ => {
                            println!("Invalid status filter: {}", status_str);
                            println!(
                                "Valid values: PENDING, RUNNING, COMPLETED, FAILED, CANCELLED"
                            );
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

                        // Show detailed progress if available
                        if let Some(ref progress) = job.data_progress {
                            // Format helper for file size
                            let format_size = |bytes: u64| -> String {
                                if bytes >= 1_000_000_000_000 {
                                    format!("{:.2} TB", bytes as f64 / 1_000_000_000_000.0)
                                } else if bytes >= 1_000_000_000 {
                                    format!("{:.1} GB", bytes as f64 / 1_000_000_000.0)
                                } else if bytes >= 1_000_000 {
                                    format!("{:.1} MB", bytes as f64 / 1_000_000.0)
                                } else if bytes >= 1_000 {
                                    format!("{:.1} KB", bytes as f64 / 1_000.0)
                                } else {
                                    format!("{} bytes", bytes)
                                }
                            };

                            // Calculate segments complete per data type from gap analysis
                            if let Some(ref gap) = job.gap_analysis {
                                println!("\nData Progress (by segment):");

                                let total_segments = gap.total_segments;
                                let mut blocks_incomplete = 0;
                                let mut txs_incomplete = 0;
                                let mut logs_incomplete = 0;

                                for detail in &gap.incomplete_details {
                                    if !detail.missing_blocks_ranges.is_empty() {
                                        blocks_incomplete += 1;
                                    }
                                    if !detail.missing_transactions_ranges.is_empty() {
                                        txs_incomplete += 1;
                                    }
                                    if !detail.missing_logs_ranges.is_empty() {
                                        logs_incomplete += 1;
                                    }
                                }

                                // Blocks
                                if let Some(ref blocks) = progress.blocks {
                                    let blocks_complete = total_segments - blocks_incomplete;
                                    println!(
                                        "  Blocks:        {}/{} segments - {} files, {}{}",
                                        blocks_complete,
                                        total_segments,
                                        progress
                                            .file_stats
                                            .as_ref()
                                            .map(|s| s.blocks_files)
                                            .unwrap_or(0),
                                        format_size(
                                            progress
                                                .file_stats
                                                .as_ref()
                                                .map(|s| s.blocks_disk_bytes)
                                                .unwrap_or(0)
                                        ),
                                        if blocks.gap_count > 0 {
                                            format!(" ({} gaps)", blocks.gap_count)
                                        } else {
                                            String::new()
                                        }
                                    );
                                }

                                // Transactions
                                if let Some(ref txs) = progress.transactions {
                                    let txs_complete = total_segments - txs_incomplete;
                                    println!(
                                        "  Transactions:  {}/{} segments - {} files, {}{}",
                                        txs_complete,
                                        total_segments,
                                        progress
                                            .file_stats
                                            .as_ref()
                                            .map(|s| s.transactions_files)
                                            .unwrap_or(0),
                                        format_size(
                                            progress
                                                .file_stats
                                                .as_ref()
                                                .map(|s| s.transactions_disk_bytes)
                                                .unwrap_or(0)
                                        ),
                                        if txs.gap_count > 0 {
                                            format!(" ({} gaps)", txs.gap_count)
                                        } else {
                                            String::new()
                                        }
                                    );
                                }

                                // Logs
                                if let Some(ref logs) = progress.logs {
                                    let logs_complete = total_segments - logs_incomplete;
                                    println!(
                                        "  Logs:          {}/{} segments - {} files, {}{}",
                                        logs_complete,
                                        total_segments,
                                        progress
                                            .file_stats
                                            .as_ref()
                                            .map(|s| s.logs_files)
                                            .unwrap_or(0),
                                        format_size(
                                            progress
                                                .file_stats
                                                .as_ref()
                                                .map(|s| s.logs_disk_bytes)
                                                .unwrap_or(0)
                                        ),
                                        if logs.gap_count > 0 {
                                            format!(" ({} gaps)", logs.gap_count)
                                        } else {
                                            String::new()
                                        }
                                    );
                                }

                                // Total files and disk usage
                                if let Some(ref stats) = progress.file_stats {
                                    println!(
                                        "\nTotal Files: {} ({})",
                                        stats.total_files,
                                        format_size(stats.total_disk_bytes)
                                    );
                                }
                                println!();
                            }
                        }

                        // Show segment-level completeness
                        if let Some(ref gap) = job.gap_analysis {
                            println!(
                                "Complete Segments: {}/{} ({:.1}% of segments)",
                                gap.complete_segments,
                                gap.total_segments,
                                gap.completion_percentage
                            );

                            // Show breakdown of incomplete segments by data type
                            if gap.missing_segments > 0 {
                                let mut missing_blocks_count = 0;
                                let mut missing_txs_count = 0;
                                let mut missing_logs_count = 0;

                                for detail in &gap.incomplete_details {
                                    if !detail.missing_blocks_ranges.is_empty() {
                                        missing_blocks_count += 1;
                                    }
                                    if !detail.missing_transactions_ranges.is_empty() {
                                        missing_txs_count += 1;
                                    }
                                    if !detail.missing_logs_ranges.is_empty() {
                                        missing_logs_count += 1;
                                    }
                                }

                                println!("Incomplete Segments: {}", gap.missing_segments);
                                if missing_blocks_count > 0 {
                                    println!(
                                        "  - {} segments missing blocks",
                                        missing_blocks_count
                                    );
                                }
                                if missing_txs_count > 0 {
                                    println!(
                                        "  - {} segments missing transactions",
                                        missing_txs_count
                                    );
                                }
                                if missing_logs_count > 0 {
                                    println!("  - {} segments missing logs", missing_logs_count);
                                }
                            }

                            if job.current_block > job.from_block {
                                println!("Highest completed: block {}", job.current_block);
                            }
                        } else {
                            // Fallback if gap_analysis not available
                            let percent = if job.total_blocks > 0 {
                                (job.blocks_synced as f64 / job.total_blocks as f64) * 100.0
                            } else {
                                0.0
                            };
                            println!("Progress: {:.1}%", percent);
                            if job.current_block > job.from_block {
                                println!("Highest completed: block {}", job.current_block);
                            }
                        }
                        println!("Active workers: {}", job.active_workers);

                        // Display download rate
                        if job.download_rate_bytes_per_sec > 0.0 {
                            let rate = if job.download_rate_bytes_per_sec >= 1_000_000_000.0 {
                                format!(
                                    "{:.2} GB/s",
                                    job.download_rate_bytes_per_sec / 1_000_000_000.0
                                )
                            } else if job.download_rate_bytes_per_sec >= 1_000_000.0 {
                                format!("{:.1} MB/s", job.download_rate_bytes_per_sec / 1_000_000.0)
                            } else if job.download_rate_bytes_per_sec >= 1_000.0 {
                                format!("{:.1} KB/s", job.download_rate_bytes_per_sec / 1_000.0)
                            } else {
                                format!("{:.0} B/s", job.download_rate_bytes_per_sec)
                            };
                            println!("Download rate: {}", rate);
                        }

                        if !job.error.is_empty() {
                            println!("Error: {}", job.error);
                        }
                    }
                    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
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
                            "    Segment {} (blocks {}-{}):",
                            detail.segment_num, detail.from_block, detail.to_block
                        );

                        // Show detailed ranges for each data type
                        if !detail.missing_blocks_ranges.is_empty() {
                            println!("      - blocks:");
                            for range in &detail.missing_blocks_ranges {
                                let count = range.end - range.start + 1;
                                println!(
                                    "        {}-{} ({} blocks)",
                                    range.start, range.end, count
                                );
                            }
                        }

                        if !detail.missing_transactions_ranges.is_empty() {
                            println!("      - transactions:");
                            for range in &detail.missing_transactions_ranges {
                                let count = range.end - range.start + 1;
                                println!(
                                    "        {}-{} ({} blocks)",
                                    range.start, range.end, count
                                );
                            }
                        }

                        if !detail.missing_logs_ranges.is_empty() {
                            println!("      - logs:");
                            for range in &detail.missing_logs_ranges {
                                let count = range.end - range.start + 1;
                                println!(
                                    "        {}-{} ({} blocks)",
                                    range.start, range.end, count
                                );
                            }
                        }
                    }
                }

                if gap.missing_segments > 0 && gap.segments_to_sync.len() <= 20 {
                    println!("\n  Segments to sync: {:?}", gap.segments_to_sync);
                }
            }
        }
    }

    Ok(())
}
