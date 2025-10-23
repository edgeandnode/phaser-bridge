use anyhow::Result;
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

// Use the generated admin proto from phaser_query
use phaser_query::proto::admin::sync_service_client::SyncServiceClient;
use phaser_query::proto::admin::*;

#[derive(Debug, Serialize, Deserialize)]
struct ProgressOutput {
    job_id: String,
    status: String,
    timestamp: String,
    total_blocks_synced: u64,
    total_blocks: u64,
    overall_rate: f64,
    total_bytes_written: u64,
    workers: Vec<WorkerOutput>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkerOutput {
    worker_id: u32,
    stage: String,
    from_block: u64,
    to_block: u64,
    current_block: u64,
    blocks_processed: u64,
    rate: f64,
    bytes_written: u64,
    files_created: u32,
    started_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed: Option<String>,
}

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

        /// Output as JSON (one line per update)
        #[clap(long, conflicts_with = "tui")]
        json: bool,

        /// Output as TUI (default, overwrites same lines)
        #[clap(long, conflicts_with = "json")]
        tui: bool,
    },
}

async fn run_progress_json(
    mut client: SyncServiceClient<tonic::transport::Channel>,
    job_id: &str,
) -> Result<()> {
    use futures::StreamExt;
    use std::collections::HashMap;

    let request = tonic::Request::new(SyncProgressRequest {
        job_id: job_id.to_string(),
    });

    let mut stream = client.stream_sync_progress(request).await?.into_inner();
    let mut completed_workers: HashMap<u32, u64> = HashMap::new();

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

        let timestamp = chrono::DateTime::from_timestamp(update.timestamp, 0)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| update.timestamp.to_string());

        let workers = update
            .workers
            .iter()
            .map(|w| {
                let started_at = chrono::DateTime::from_timestamp(w.started_at, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| w.started_at.to_string());

                let elapsed = if w.stage == "completed" {
                    if !completed_workers.contains_key(&w.worker_id) {
                        let elapsed_secs = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs() as i64
                            - w.started_at;
                        completed_workers.insert(w.worker_id, elapsed_secs as u64);
                    }
                    Some(format_duration(completed_workers[&w.worker_id]))
                } else {
                    let elapsed_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64
                        - w.started_at;
                    Some(format_duration(elapsed_secs as u64))
                };

                WorkerOutput {
                    worker_id: w.worker_id,
                    stage: w.stage.clone(),
                    from_block: w.from_block,
                    to_block: w.to_block,
                    current_block: w.current_block,
                    blocks_processed: w.blocks_processed,
                    rate: w.rate,
                    bytes_written: w.bytes_written,
                    files_created: w.files_created,
                    started_at,
                    elapsed,
                }
            })
            .collect();

        let output = ProgressOutput {
            job_id: update.job_id.clone(),
            status: status_str.to_string(),
            timestamp,
            total_blocks_synced: update.total_blocks_synced,
            total_blocks: update.total_blocks,
            overall_rate: update.overall_rate,
            total_bytes_written: update.total_bytes_written,
            workers,
        };

        println!("{}", serde_json::to_string(&output)?);
    }

    Ok(())
}

async fn run_progress_tui(
    mut client: SyncServiceClient<tonic::transport::Channel>,
    job_id: &str,
) -> Result<()> {
    use crossterm::{
        event::{self, Event, KeyCode},
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
        ExecutableCommand,
    };
    use futures::StreamExt;
    use ratatui::prelude::*;
    use ratatui::widgets::{Block, Borders, Gauge, Paragraph, Row, Table};
    use std::collections::HashMap;
    use std::io::stdout;
    use std::time::Duration;

    // Setup terminal
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;

    let request = tonic::Request::new(SyncProgressRequest {
        job_id: job_id.to_string(),
    });

    let mut stream = client.stream_sync_progress(request).await?.into_inner();
    let mut completed_workers: HashMap<u32, u64> = HashMap::new();
    let mut last_update: Option<SyncProgressUpdate> = None;
    let mut should_quit = false;

    while !should_quit {
        // Check for keyboard input
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    should_quit = true;
                }
            }
        }

        // Try to get new update
        if let Ok(Some(Ok(update))) =
            tokio::time::timeout(Duration::from_millis(100), stream.next()).await
        {
            last_update = Some(update);
        }

        // Render UI
        if let Some(ref update) = last_update {
            let status_str = match SyncStatus::try_from(update.status) {
                Ok(SyncStatus::Pending) => "PENDING",
                Ok(SyncStatus::Running) => "RUNNING",
                Ok(SyncStatus::Completed) => "COMPLETED",
                Ok(SyncStatus::Failed) => "FAILED",
                Ok(SyncStatus::Cancelled) => "CANCELLED",
                _ => "UNKNOWN",
            };

            // Update completed workers tracking
            for worker in &update.workers {
                if worker.stage == "completed" && !completed_workers.contains_key(&worker.worker_id)
                {
                    let elapsed = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64
                        - worker.started_at;
                    completed_workers.insert(worker.worker_id, elapsed as u64);
                }
            }

            terminal.draw(|f| {
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .margin(1)
                    .constraints([
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(5),
                        Constraint::Length(1),
                    ])
                    .split(f.area());

                // Job status
                let status_text = format!(
                    "Job: {} | Status: {} | Total: {}/{} blocks",
                    job_id, status_str, update.total_blocks_synced, update.total_blocks
                );
                let status = Paragraph::new(status_text)
                    .block(Block::default().borders(Borders::ALL).title("Status"));
                f.render_widget(status, chunks[0]);

                // Overall progress bar
                let percent = if update.total_blocks > 0 {
                    (update.total_blocks_synced as f64 / update.total_blocks as f64) * 100.0
                } else {
                    0.0
                };
                let gauge = Gauge::default()
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Overall Progress"),
                    )
                    .gauge_style(Style::default().fg(Color::Green))
                    .percent(percent as u16)
                    .label(format!(
                        "{:.1}% | {:.1} blocks/sec | {:.2} GB",
                        percent,
                        update.overall_rate,
                        update.total_bytes_written as f64 / 1_000_000_000.0
                    ));
                f.render_widget(gauge, chunks[1]);

                // Workers table
                let headers = Row::new(vec!["ID", "Stage", "Blocks", "Current", "Rate", "Elapsed"])
                    .style(Style::default().fg(Color::Yellow));

                let rows: Vec<Row> = update
                    .workers
                    .iter()
                    .map(|w| {
                        let blocks_range = format!("{}-{}", w.from_block, w.to_block);
                        let current = if w.stage == "completed" {
                            "✓".to_string()
                        } else {
                            w.current_block.to_string()
                        };

                        let elapsed = if w.stage == "completed" {
                            format_duration(completed_workers[&w.worker_id])
                        } else {
                            let e = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as i64
                                - w.started_at;
                            format_duration(e as u64)
                        };

                        Row::new(vec![
                            w.worker_id.to_string(),
                            w.stage.clone(),
                            blocks_range,
                            current,
                            format!("{:.1}/s", w.rate),
                            elapsed,
                        ])
                    })
                    .collect();

                let table = Table::new(
                    rows,
                    [
                        Constraint::Length(4),
                        Constraint::Length(15),
                        Constraint::Length(25),
                        Constraint::Length(12),
                        Constraint::Length(10),
                        Constraint::Length(10),
                    ],
                )
                .header(headers)
                .block(Block::default().borders(Borders::ALL).title("Workers"));

                f.render_widget(table, chunks[2]);

                // Footer
                let footer = Paragraph::new("Press 'q' to quit");
                f.render_widget(footer, chunks[3]);
            })?;

            // Check if job finished
            if matches!(status_str, "COMPLETED" | "FAILED" | "CANCELLED") {
                // Wait a bit so user can see final state
                std::thread::sleep(Duration::from_secs(2));
                should_quit = true;
            }
        }
    }

    // Cleanup terminal
    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;

    Ok(())
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

                    // Show detailed progress if available
                    if let Some(ref progress) = job.data_progress {
                        println!("\nData Progress:");

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

                        // Blocks progress
                        if let Some(ref blocks) = progress.blocks {
                            println!(
                                "  Blocks:        {:>12} / {} ({:>5.1}%) - {} files, {}",
                                blocks.blocks_on_disk,
                                job.total_blocks,
                                blocks.coverage_percentage,
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
                                )
                            );
                            if blocks.gap_count > 0 {
                                println!(
                                    "                 {} gaps, highest continuous: {}",
                                    blocks.gap_count, blocks.highest_continuous
                                );
                            }
                        }

                        // Transactions progress
                        if let Some(ref txs) = progress.transactions {
                            println!(
                                "  Transactions:  {:>12} / {} ({:>5.1}%) - {} files, {}",
                                txs.blocks_on_disk,
                                job.total_blocks,
                                txs.coverage_percentage,
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
                                )
                            );
                            if txs.gap_count > 0 {
                                println!(
                                    "                 {} gaps, highest continuous: {}",
                                    txs.gap_count, txs.highest_continuous
                                );
                            }
                        }

                        // Logs progress
                        if let Some(ref logs) = progress.logs {
                            println!(
                                "  Logs:          {:>12} / {} ({:>5.1}%) - {} files, {}",
                                logs.blocks_on_disk,
                                job.total_blocks,
                                logs.coverage_percentage,
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
                                )
                            );
                            if logs.gap_count > 0 {
                                println!(
                                    "                 {} gaps, highest continuous: {}",
                                    logs.gap_count, logs.highest_continuous
                                );
                            }
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

                    let percent = if job.total_blocks > 0 {
                        (job.blocks_synced as f64 / job.total_blocks as f64) * 100.0
                    } else {
                        0.0
                    };
                    println!(
                        "Complete Segments: {}/{} blocks ({:.1}%)",
                        job.blocks_synced, job.total_blocks, percent
                    );

                    if job.current_block > job.from_block {
                        println!("Highest completed: block {}", job.current_block);
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
        Commands::Progress { job_id, json, tui } => {
            if json {
                // JSON mode
                run_progress_json(client, &job_id).await?;
            } else {
                // TUI mode (default)
                run_progress_tui(client, &job_id).await?;
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
