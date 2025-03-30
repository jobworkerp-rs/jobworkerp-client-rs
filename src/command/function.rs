use crate::{
    client::JobworkerpClient,
    jobworkerp::{data::FunctionSpecs, service::FindFunctionRequest},
};
use clap::Parser;

#[derive(Parser, Debug)]
pub struct FunctionArg {
    #[clap(subcommand)]
    pub cmd: FunctionCommand,
}

#[derive(Parser, Debug)]
pub enum FunctionCommand {
    List {
        #[clap(long)]
        exclude_runner: bool,
        #[clap(long)]
        exclude_worker: bool,
    },
}

impl FunctionCommand {
    pub async fn execute(&self, client: &JobworkerpClient) {
        match self {
            FunctionCommand::List {
                exclude_runner,
                exclude_worker,
            } => {
                let request = FindFunctionRequest {
                    exclude_runner: *exclude_runner,
                    exclude_worker: *exclude_worker,
                };
                let response = client
                    .function_client()
                    .await
                    .find_list(request)
                    .await
                    .unwrap();
                println!("meta: {:#?}", response.metadata());
                let mut data = response.into_inner();
                while let Some(function) = data.message().await.unwrap() {
                    Self::print_function(&function);
                }
                println!(
                    "trailer: {:#?}",
                    data.trailers().await.unwrap().unwrap_or_default()
                );
            }
        }
    }

    pub fn print_function(function: &FunctionSpecs) {
        println!("[function]:");

        // Print function ID (either runner or worker)
        match &function.function_id {
            Some(crate::jobworkerp::data::function_specs::FunctionId::RunnerId(runner_id)) => {
                println!("\t[runner_id] {}", runner_id.value);
            }
            Some(crate::jobworkerp::data::function_specs::FunctionId::WorkerId(worker_id)) => {
                println!("\t[worker_id] {}", worker_id.value);
            }
            None => {
                println!("\t[id] None");
            }
        }

        println!("\t[name] {}", &function.name);
        println!("\t[description] {}", &function.description);

        // Print input schema
        if let Some(input_schema) = &function.input_schema {
            println!("\t[input_schema]:");
            if let Some(settings) = &input_schema.settings {
                println!("\t\t[settings] |\n---\n{}", settings);
            } else {
                println!("\t\t[settings] (None)");
            }
            println!("\t\t[arguments] |\n---\n{}", &input_schema.arguments);
        }

        // Print output schema if available
        if let Some(result_output_schema) = &function.result_output_schema {
            println!("\t[result_output_schema] |\n---\n{}", result_output_schema);
        } else {
            println!("\t[result_output_schema] (None)");
        }

        // Print output type
        println!(
            "\t[output_type] {}",
            crate::jobworkerp::data::StreamingOutputType::try_from(function.output_type)
                .unwrap_or(crate::jobworkerp::data::StreamingOutputType::NonStreaming)
                .as_str_name()
        );
    }
}
