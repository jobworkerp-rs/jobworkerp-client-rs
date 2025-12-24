// as job-status: valid commands are find, list, search, cleanup
// -i, --id <id> id of the job (for find)
// --status <status> status filter (for search)
// --worker-id <id> worker id filter (for search)
// --channel <channel> channel filter (for search)
// --min-elapsed-time <ms> minimum elapsed time filter (for search)
// --limit <n> limit (for search)
// --offset <n> offset (for search)
// --descending sort order (for search)
// --retention-hours <n> retention hours override (for cleanup)

use std::collections::HashMap;

use crate::{
    client::JobworkerpClient,
    command::to_request,
    display::{
        utils::supports_color, CardVisualizer, DisplayOptions, JsonPrettyVisualizer,
        JsonVisualizer, TableVisualizer,
    },
    jobworkerp::{
        data::{Empty, JobId, JobProcessingStatus},
        service::{CleanupRequest, FindJobProcessingStatusRequest},
    },
};
use clap::{Parser, ValueEnum};

pub mod display;
use display::{job_processing_status_detail_to_json, job_processing_status_to_json};

#[derive(Parser, Debug)]
pub struct JobStatusArg {
    #[clap(subcommand)]
    pub cmd: JobStatusCommand,
}

#[derive(ValueEnum, Debug, Clone)]
pub enum JobProcessingStatusArg {
    Unknown,
    Pending,
    Running,
    WaitResult,
    Cancelling,
}

impl JobProcessingStatusArg {
    pub fn to_grpc(&self) -> JobProcessingStatus {
        match self {
            JobProcessingStatusArg::Unknown => JobProcessingStatus::Unknown,
            JobProcessingStatusArg::Pending => JobProcessingStatus::Pending,
            JobProcessingStatusArg::Running => JobProcessingStatus::Running,
            JobProcessingStatusArg::WaitResult => JobProcessingStatus::WaitResult,
            JobProcessingStatusArg::Cancelling => JobProcessingStatus::Cancelling,
        }
    }
}

#[derive(Parser, Debug)]
pub enum JobStatusCommand {
    /// Find job processing status by job ID
    Find {
        #[clap(short, long)]
        id: i64,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
    },
    /// List all job processing statuses
    List {
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    /// Search job processing statuses with conditions (requires JOB_STATUS_RDB_INDEXING=true)
    Search {
        #[clap(
            short,
            long,
            help = "Status filter: Pending, Running, WaitResult, Cancelling"
        )]
        status: Option<JobProcessingStatusArg>,
        #[clap(short, long, help = "Worker ID filter")]
        worker_id: Option<i64>,
        #[clap(short, long, help = "Channel filter")]
        channel: Option<String>,
        #[clap(
            short,
            long,
            help = "Minimum elapsed time in milliseconds (e.g., 600000 for 10 minutes)"
        )]
        min_elapsed_time: Option<i64>,
        #[clap(
            short,
            long,
            help = "Maximum number of results (default: 100, max: 1000)"
        )]
        limit: Option<i32>,
        #[clap(short, long, help = "Offset for pagination")]
        offset: Option<i32>,
        #[clap(short, long, help = "Sort in descending order (default: true)")]
        descending: Option<bool>,
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    /// Cleanup old deleted records (requires JOB_STATUS_RDB_INDEXING=true)
    Cleanup {
        #[clap(
            short,
            long,
            help = "Override retention hours (default: JOB_STATUS_RETENTION_HOURS env or 24)"
        )]
        retention_hours: Option<u64>,
    },
}

impl JobStatusCommand {
    pub async fn execute(&self, client: &JobworkerpClient, metadata: &HashMap<String, String>) {
        match self {
            JobStatusCommand::Find { id, format } => {
                let job_id = JobId { value: *id };
                let response = client
                    .job_processing_status_client()
                    .await
                    .find(to_request(metadata, job_id).unwrap())
                    .await;

                match response {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        if let Some(status) = inner.status {
                            let status_enum = JobProcessingStatus::try_from(status)
                                .unwrap_or(JobProcessingStatus::Unknown);
                            let status_formatter = display::JobProcessingStatusFormatter;
                            use crate::display::format::EnumFormatter;
                            println!(
                                "Job ID: {}, Status: {}",
                                id,
                                status_formatter.format(status_enum, format)
                            );
                        } else {
                            println!("Job ID {} not found or no status available", id);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e.message());
                    }
                }
            }
            JobStatusCommand::List {
                format,
                no_truncate,
            } => {
                let response = client
                    .job_processing_status_client()
                    .await
                    .find_all(to_request(metadata, Empty {}).unwrap())
                    .await;

                match response {
                    Ok(resp) => {
                        let mut stream = resp.into_inner();
                        let mut statuses = Vec::new();

                        while let Ok(Some(status_response)) = stream.message().await {
                            let status_json =
                                job_processing_status_to_json(&status_response, format);
                            statuses.push(status_json);
                        }

                        if statuses.is_empty() {
                            println!("No job processing statuses found");
                            return;
                        }

                        let options = DisplayOptions::new(format.clone())
                            .with_color(supports_color())
                            .with_no_truncate(*no_truncate);

                        let output = match format {
                            crate::display::DisplayFormat::Table => {
                                let visualizer = TableVisualizer;
                                visualizer.visualize(&statuses, &options)
                            }
                            crate::display::DisplayFormat::Card => {
                                let visualizer = CardVisualizer;
                                visualizer.visualize(&statuses, &options)
                            }
                            crate::display::DisplayFormat::Json => {
                                let visualizer = JsonPrettyVisualizer;
                                visualizer.visualize(&statuses, &options)
                            }
                        };

                        println!("{output}");
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e.message());
                    }
                }
            }
            JobStatusCommand::Search {
                status,
                worker_id,
                channel,
                min_elapsed_time,
                limit,
                offset,
                descending,
                format,
                no_truncate,
            } => {
                let request = FindJobProcessingStatusRequest {
                    status: status.as_ref().map(|s| s.to_grpc() as i32),
                    worker_id: *worker_id,
                    channel: channel.clone(),
                    min_elapsed_time_ms: *min_elapsed_time,
                    limit: *limit,
                    offset: *offset,
                    descending: *descending,
                };

                let response = client
                    .job_processing_status_client()
                    .await
                    .find_by_condition(to_request(metadata, request).unwrap())
                    .await;

                match response {
                    Ok(resp) => {
                        let mut stream = resp.into_inner();
                        let mut statuses = Vec::new();

                        while let Ok(Some(detail_response)) = stream.message().await {
                            let status_json =
                                job_processing_status_detail_to_json(&detail_response, format);
                            statuses.push(status_json);
                        }

                        if statuses.is_empty() {
                            println!("No job processing statuses found matching criteria");
                            return;
                        }

                        let options = DisplayOptions::new(format.clone())
                            .with_color(supports_color())
                            .with_no_truncate(*no_truncate);

                        let output = match format {
                            crate::display::DisplayFormat::Table => {
                                let visualizer = TableVisualizer;
                                visualizer.visualize(&statuses, &options)
                            }
                            crate::display::DisplayFormat::Card => {
                                let visualizer = CardVisualizer;
                                visualizer.visualize(&statuses, &options)
                            }
                            crate::display::DisplayFormat::Json => {
                                let visualizer = JsonPrettyVisualizer;
                                visualizer.visualize(&statuses, &options)
                            }
                        };

                        println!("{output}");
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e.message());
                        if e.code() == tonic::Code::Unimplemented {
                            eprintln!(
                                "Hint: This feature requires JOB_STATUS_RDB_INDEXING=true on the server"
                            );
                        }
                    }
                }
            }
            JobStatusCommand::Cleanup { retention_hours } => {
                let request = CleanupRequest {
                    retention_hours_override: *retention_hours,
                };

                let response = client
                    .job_processing_status_client()
                    .await
                    .cleanup(to_request(metadata, request).unwrap())
                    .await;

                match response {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        println!("Cleanup completed:");
                        println!("  Deleted count: {}", inner.deleted_count);
                        println!("  Cutoff time: {}", inner.cutoff_time);
                        println!("  Message: {}", inner.message);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e.message());
                        match e.code() {
                            tonic::Code::FailedPrecondition => {
                                eprintln!(
                                    "Hint: This feature requires JOB_STATUS_RDB_INDEXING=true on the server"
                                );
                            }
                            tonic::Code::Unauthenticated => {
                                eprintln!(
                                    "Hint: Authentication required. Set AUTH_TOKEN header if server has AUTH_TOKEN configured"
                                );
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}
