// as runner: valid commands are find, list, delete, count
// -i, --id <id> id of the job (for find, delete)
// --offset <offset> offset of the list (for list)
// --limit <limit> limit of the list (for list)

use std::collections::HashMap;

use crate::{
    client::JobworkerpClient,
    command::to_request,
    display::{
        CardVisualizer, DisplayOptions, JsonPrettyVisualizer, JsonVisualizer, TableVisualizer,
        utils::supports_color,
    },
    jobworkerp::data::{Runner, RunnerId},
};
use clap::Parser;

pub mod display;
use display::runner_to_json;
#[derive(Parser, Debug)]
pub struct RunnerArg {
    #[clap(subcommand)]
    pub cmd: RunnerCommand,
}

#[derive(Parser, Debug)]
pub enum RunnerCommand {
    Create {
        #[clap(short, long)]
        name: String,
        #[clap(short, long)]
        description: String,
        #[clap(short, long, help = "runner type (MCP_SERVER or PLUGIN)")]
        runner_type: String,
        #[clap(long)]
        definition: String,
    },
    Find {
        #[clap(short, long)]
        id: i64,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    FindByName {
        #[clap(short, long)]
        name: String,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    List {
        #[clap(short, long)]
        offset: Option<i64>,
        #[clap(short, long)]
        limit: Option<i32>,
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    Delete {
        #[clap(short, long)]
        id: i64,
    },
    Count {},
}

impl RunnerCommand {
    pub async fn execute(&self, client: &JobworkerpClient, metadata: &HashMap<String, String>) {
        match self {
            RunnerCommand::Create {
                name,
                description,
                runner_type,
                definition,
            } => {
                let definition = if definition.starts_with("@") {
                    let path = definition.trim_start_matches('@');
                    std::fs::read_to_string(path).unwrap_or_else(|_| {
                        panic!("Failed to read file: {path}");
                    })
                } else {
                    definition.clone()
                };
                let request = crate::jobworkerp::service::CreateRunnerRequest {
                    name: name.clone(),
                    description: description.clone(),
                    runner_type: crate::jobworkerp::data::RunnerType::from_str_name(
                        runner_type.as_str(),
                    )
                    .ok_or("Invalid runner type (MCP_SERVER or PLUGIN)".to_string())
                    .unwrap() as i32,
                    definition: definition.clone(),
                };
                let response = client
                    .runner_client()
                    .await
                    .create(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
            RunnerCommand::Find {
                id,
                format,
                no_truncate,
            } => {
                let id = RunnerId { value: *id };
                let response = client
                    .runner_client()
                    .await
                    .find(to_request(metadata, id).unwrap())
                    .await
                    .unwrap()
                    .into_inner()
                    .data;
                if let Some(data) = response {
                    Self::print_runner_formatted(&data, format, *no_truncate);
                } else {
                    println!("runner not found");
                }
            }
            RunnerCommand::FindByName {
                name,
                format,
                no_truncate,
            } => {
                let request = crate::jobworkerp::service::RunnerNameRequest { name: name.clone() };
                let response = client
                    .runner_client()
                    .await
                    .find_by_name(to_request(metadata, request).unwrap())
                    .await
                    .unwrap()
                    .into_inner()
                    .data;
                if let Some(data) = response {
                    Self::print_runner_formatted(&data, format, *no_truncate);
                } else {
                    println!("runner not found");
                }
            }
            RunnerCommand::List {
                offset,
                limit,
                format,
                no_truncate,
            } => {
                let request = crate::jobworkerp::service::FindRunnerListRequest {
                    runner_types: vec![], // Empty = all types
                    name_filter: None,
                    limit: *limit,
                    offset: *offset,
                    sort_by: None,
                    ascending: None,
                };
                let response = client
                    .runner_client()
                    .await
                    .find_list_by(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();

                let mut data = response.into_inner();

                // Collect all runners into a vector for batch processing
                let mut runners = Vec::new();
                while let Some(runner) = data.message().await.unwrap() {
                    let runner_json = runner_to_json(&runner, format, *no_truncate);
                    runners.push(runner_json);
                }

                // Display using the appropriate visualizer
                let options = DisplayOptions::new(format.clone())
                    .with_color(supports_color())
                    .with_no_truncate(*no_truncate);

                let output = match format {
                    crate::display::DisplayFormat::Table => {
                        let visualizer = TableVisualizer;
                        visualizer.visualize(&runners, &options)
                    }
                    crate::display::DisplayFormat::Card => {
                        let visualizer = CardVisualizer;
                        visualizer.visualize(&runners, &options)
                    }
                    crate::display::DisplayFormat::Json => {
                        let visualizer = JsonPrettyVisualizer;
                        visualizer.visualize(&runners, &options)
                    }
                };

                println!("{output}");
            }
            RunnerCommand::Delete { id } => {
                let id = RunnerId { value: *id };
                let response = client.runner_client().await.delete(id).await.unwrap();
                println!("{response:#?}");
            }
            RunnerCommand::Count {} => {
                let request = crate::jobworkerp::service::CountRunnerRequest {
                    runner_types: vec![],
                    name_filter: None,
                };
                let response = client
                    .runner_client()
                    .await
                    .count_by(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();
                println!("{:#?}", response.into_inner().total);
            }
        }
    }
    pub fn print_runner_formatted(
        runner: &Runner,
        format: &crate::display::DisplayFormat,
        no_truncate: bool,
    ) {
        let runner_json = runner_to_json(runner, format, no_truncate);
        let runners = vec![runner_json];

        // Display using the appropriate visualizer
        let options = DisplayOptions::new(format.clone())
            .with_color(supports_color())
            .with_no_truncate(no_truncate);

        let output = match format {
            crate::display::DisplayFormat::Table => {
                let visualizer = TableVisualizer;
                visualizer.visualize(&runners, &options)
            }
            crate::display::DisplayFormat::Card => {
                let visualizer = CardVisualizer;
                visualizer.visualize(&runners, &options)
            }
            crate::display::DisplayFormat::Json => {
                let visualizer = JsonPrettyVisualizer;
                visualizer.visualize(&runners, &options)
            }
        };

        println!("{output}");
    }

    pub fn print_runner(runner: &Runner) {
        if let Runner {
            id: Some(_id),
            data: Some(data),
        } = runner
        {
            println!("[runner]:\n\t[id] {}", &_id.value);
            println!("\t[name] {}", &data.name);
            println!("\t[description] {}", &data.description);
            println!("\t[runner_type] {}", &data.runner_type().as_str_name());
            println!(
                "\t[runner_settings_proto] |\n---\n{}",
                &data.runner_settings_proto
            );
            if let Some(method_map) = &data.method_proto_map {
                println!("\t[methods] {} available", method_map.schemas.len());
                for (name, schema) in &method_map.schemas {
                    println!("\t\t[method: {}]", name);
                    if let Some(desc) = &schema.description {
                        println!("\t\t\t[description] {}", desc);
                    }
                    println!("\t\t\t[args_proto] |\n{}", &schema.args_proto);
                    println!("\t\t\t[result_proto] |\n{}", &schema.result_proto);
                    println!("\t\t\t[output_type] {}", schema.output_type().as_str_name());
                }
            } else {
                println!("\t[methods] (None)");
            }
        } else {
            println!("[runner]:\n\tdata is empty");
        }
    }
}
