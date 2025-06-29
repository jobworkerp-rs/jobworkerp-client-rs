// as runner: valid commands are find, list, delete, count
// -i, --id <id> id of the job (for find, delete)
// --offset <offset> offset of the list (for list)
// --limit <limit> limit of the list (for list)

use std::collections::HashMap;

use crate::{
    client::JobworkerpClient,
    command::to_request,
    jobworkerp::{
        data::{Runner, RunnerId},
        service::{CountCondition, FindListRequest},
    },
};
use clap::Parser;
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
    },
    FindByName {
        #[clap(short, long)]
        name: String,
    },
    List {
        #[clap(short, long)]
        offset: Option<i64>,
        #[clap(short, long)]
        limit: Option<i32>,
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
            RunnerCommand::Find { id } => {
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
                    Self::print_runner(&data);
                } else {
                    println!("runner not found");
                }
            }
            RunnerCommand::FindByName { name } => {
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
                    Self::print_runner(&data);
                } else {
                    println!("runner not found");
                }
            }
            RunnerCommand::List { offset, limit } => {
                let request = FindListRequest {
                    offset: *offset,
                    limit: *limit,
                };
                let response = client
                    .runner_client()
                    .await
                    .find_list(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();
                println!("meta: {:#?}", response.metadata());
                let mut data = response.into_inner();
                while let Some(data) = data.message().await.unwrap() {
                    Self::print_runner(&data);
                }
                println!(
                    "trailer: {:#?}",
                    data.trailers().await.unwrap().unwrap_or_default()
                );
            }
            RunnerCommand::Delete { id } => {
                let id = RunnerId { value: *id };
                let response = client.runner_client().await.delete(id).await.unwrap();
                println!("{response:#?}");
            }
            RunnerCommand::Count {} => {
                let response = client
                    .runner_client()
                    .await
                    .count(to_request(metadata, CountCondition {}).unwrap())
                    .await
                    .unwrap();
                println!("{:#?}", response.into_inner().total);
            }
        }
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
            println!("\t[job_args_proto] |\n---\n{}", &data.job_args_proto);
            println!(
                "\t[result_output_proto] |\n---\n{}",
                &data
                    .result_output_proto
                    .clone()
                    .unwrap_or("(None)".to_string())
            );
            println!(
                "\t[output_type] |\n---\n{}",
                crate::jobworkerp::data::StreamingOutputType::try_from(data.output_type)
                    .unwrap_or(crate::jobworkerp::data::StreamingOutputType::NonStreaming)
                    .as_str_name()
            );
        } else {
            println!("[runner]:\n\tdata is empty");
        }
    }
}
