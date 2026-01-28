use std::collections::HashMap;

use crate::{
    client::JobworkerpClient,
    command::to_request,
    display::{
        utils::supports_color, CardVisualizer, DisplayOptions, JsonPrettyVisualizer,
        JsonVisualizer, TableVisualizer,
    },
    jobworkerp::{
        data::{Empty, WorkerInstanceId},
        service::FindInstanceListRequest,
    },
};
use clap::Parser;

pub mod display;
use display::{instance_channel_info_to_json, worker_instance_to_json};

#[derive(Parser, Debug)]
pub struct WorkerInstanceArg {
    #[clap(subcommand)]
    pub cmd: WorkerInstanceCommand,
}

#[derive(Parser, Debug)]
pub enum WorkerInstanceCommand {
    /// Find a worker instance by ID
    Find {
        #[clap(short, long)]
        id: i64,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    /// List worker instances (streaming)
    List {
        #[clap(short, long, help = "Filter by channel name")]
        channel: Option<String>,
        #[clap(short, long, help = "Only return active instances")]
        active_only: Option<bool>,
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    /// Count worker instances (total and active)
    Count {},
    /// List channel information aggregated from worker instances
    Channels {
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
}

impl WorkerInstanceCommand {
    pub async fn execute(&self, client: &JobworkerpClient, metadata: &HashMap<String, String>) {
        match self {
            WorkerInstanceCommand::Find {
                id,
                format,
                no_truncate,
            } => {
                let instance_id = WorkerInstanceId { value: *id };
                let response = client
                    .worker_instance_client()
                    .await
                    .find(to_request(metadata, instance_id).unwrap())
                    .await;

                match response {
                    Ok(resp) => {
                        let instance = resp.into_inner();
                        if instance.id.is_some() {
                            let json = worker_instance_to_json(&instance, format);
                            let items = vec![json];

                            let options = DisplayOptions::new(format.clone())
                                .with_color(supports_color())
                                .with_no_truncate(*no_truncate);

                            let output = match format {
                                crate::display::DisplayFormat::Table => {
                                    let visualizer = TableVisualizer;
                                    visualizer.visualize(&items, &options)
                                }
                                crate::display::DisplayFormat::Card => {
                                    let visualizer = CardVisualizer;
                                    visualizer.visualize(&items, &options)
                                }
                                crate::display::DisplayFormat::Json => {
                                    let visualizer = JsonPrettyVisualizer;
                                    visualizer.visualize(&items, &options)
                                }
                            };

                            println!("{output}");
                        } else {
                            println!("Worker instance not found: id = {}", id);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e.message());
                    }
                }
            }
            WorkerInstanceCommand::List {
                channel,
                active_only,
                format,
                no_truncate,
            } => {
                let request = FindInstanceListRequest {
                    channel: channel.clone(),
                    active_only: *active_only,
                };
                let response = client
                    .worker_instance_client()
                    .await
                    .find_list(to_request(metadata, request).unwrap())
                    .await;

                match response {
                    Ok(resp) => {
                        let mut stream = resp.into_inner();
                        let mut instances = Vec::new();

                        while let Ok(Some(instance)) = stream.message().await {
                            let json = worker_instance_to_json(&instance, format);
                            instances.push(json);
                        }

                        if instances.is_empty() {
                            println!("No worker instances found");
                            return;
                        }

                        let options = DisplayOptions::new(format.clone())
                            .with_color(supports_color())
                            .with_no_truncate(*no_truncate);

                        let output = match format {
                            crate::display::DisplayFormat::Table => {
                                let visualizer = TableVisualizer;
                                visualizer.visualize(&instances, &options)
                            }
                            crate::display::DisplayFormat::Card => {
                                let visualizer = CardVisualizer;
                                visualizer.visualize(&instances, &options)
                            }
                            crate::display::DisplayFormat::Json => {
                                let visualizer = JsonPrettyVisualizer;
                                visualizer.visualize(&instances, &options)
                            }
                        };

                        println!("{output}");
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e.message());
                    }
                }
            }
            WorkerInstanceCommand::Count {} => {
                let response = client
                    .worker_instance_client()
                    .await
                    .count(to_request(metadata, Empty {}).unwrap())
                    .await;

                match response {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        println!("Total instances: {}", inner.total);
                        println!("Active instances: {}", inner.active);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e.message());
                    }
                }
            }
            WorkerInstanceCommand::Channels {
                format,
                no_truncate,
            } => {
                let response = client
                    .worker_instance_client()
                    .await
                    .find_channel_list(to_request(metadata, Empty {}).unwrap())
                    .await;

                match response {
                    Ok(resp) => {
                        let inner = resp.into_inner();
                        let channels: Vec<_> = inner
                            .channels
                            .iter()
                            .map(|ch| instance_channel_info_to_json(ch, format))
                            .collect();

                        if channels.is_empty() {
                            println!("No channels found");
                            return;
                        }

                        let options = DisplayOptions::new(format.clone())
                            .with_color(supports_color())
                            .with_no_truncate(*no_truncate);

                        let output = match format {
                            crate::display::DisplayFormat::Table => {
                                let visualizer = TableVisualizer;
                                visualizer.visualize(&channels, &options)
                            }
                            crate::display::DisplayFormat::Card => {
                                let visualizer = CardVisualizer;
                                visualizer.visualize(&channels, &options)
                            }
                            crate::display::DisplayFormat::Json => {
                                let visualizer = JsonPrettyVisualizer;
                                visualizer.visualize(&channels, &options)
                            }
                        };

                        println!("{output}");
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e.message());
                    }
                }
            }
        }
    }
}
