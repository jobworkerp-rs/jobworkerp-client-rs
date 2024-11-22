// as worker_schema: valid commands are find, list, delete, count
// -i, --id <id> id of the job (for find, delete)
// --offset <offset> offset of the list (for list)
// --limit <limit> limit of the list (for list)

use crate::{
    client::JobworkerpClient,
    jobworkerp::{
        data::{WorkerSchema, WorkerSchemaId},
        service::{CountCondition, FindListRequest},
    },
};
use clap::Parser;
#[derive(Parser, Debug)]
pub struct WorkerSchemaArg {
    #[clap(subcommand)]
    pub cmd: WorkerSchemaCommand,
}

#[derive(Parser, Debug)]
pub enum WorkerSchemaCommand {
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

impl WorkerSchemaCommand {
    pub async fn execute(&self, client: &JobworkerpClient) {
        match self {
            WorkerSchemaCommand::Find { id } => {
                let id = WorkerSchemaId { value: *id };
                let response = client
                    .worker_schema_client()
                    .await
                    .find(id)
                    .await
                    .unwrap()
                    .into_inner()
                    .data;
                if let Some(data) = response {
                    Self::print_worker_schema(&data);
                } else {
                    println!("schema not found");
                }
            }
            WorkerSchemaCommand::List { offset, limit } => {
                let request = FindListRequest {
                    offset: *offset,
                    limit: *limit,
                };
                let response = client
                    .worker_schema_client()
                    .await
                    .find_list(request)
                    .await
                    .unwrap();
                println!("meta: {:#?}", response.metadata());
                let mut data = response.into_inner();
                while let Some(data) = data.message().await.unwrap() {
                    Self::print_worker_schema(&data);
                }
                println!(
                    "trailer: {:#?}",
                    data.trailers().await.unwrap().unwrap_or_default()
                );
            }
            WorkerSchemaCommand::Delete { id } => {
                let id = WorkerSchemaId { value: *id };
                let response = client
                    .worker_schema_client()
                    .await
                    .delete(id)
                    .await
                    .unwrap();
                println!("{:#?}", response);
            }
            WorkerSchemaCommand::Count {} => {
                let response = client
                    .worker_schema_client()
                    .await
                    .count(CountCondition {})
                    .await
                    .unwrap();
                println!("{:#?}", response.into_inner().total);
            }
        }
    }
    pub fn print_worker_schema(schema: &WorkerSchema) {
        if let WorkerSchema {
            id: Some(_id),
            data: Some(data),
        } = schema
        {
            println!("[worker_schema]:\n\t[id] {}", &_id.value);
            println!("\t[name] {}", &data.name);
            println!("\t[runner_type] {}", &data.runner_type().as_str_name());
            println!("\t[operation_proto] |\n---\n{}", &data.operation_proto);
            println!("\t[job_arg_proto] |\n---\n{}", &data.job_arg_proto);
            println!(
                "\t[result_output_proto] |\n---\n{}",
                &data
                    .result_output_proto
                    .clone()
                    .unwrap_or("(None)".to_string())
            );
        } else {
            println!("[worker_schema]:\n\tdata is empty");
        }
    }
}
