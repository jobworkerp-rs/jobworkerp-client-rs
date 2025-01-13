// as runner: valid commands are find, list, delete, count
// -i, --id <id> id of the job (for find, delete)
// --offset <offset> offset of the list (for list)
// --limit <limit> limit of the list (for list)

use crate::{
    client::JobworkerpClient,
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
    Find {
        #[clap(short, long)]
        id: i64,
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
    pub async fn execute(&self, client: &JobworkerpClient) {
        match self {
            RunnerCommand::Find { id } => {
                let id = RunnerId { value: *id };
                let response = client
                    .runner_client()
                    .await
                    .find(id)
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
                    .find_list(request)
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
                println!("{:#?}", response);
            }
            RunnerCommand::Count {} => {
                let response = client
                    .runner_client()
                    .await
                    .count(CountCondition {})
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
        } else {
            println!("[runner]:\n\tdata is empty");
        }
    }
}
