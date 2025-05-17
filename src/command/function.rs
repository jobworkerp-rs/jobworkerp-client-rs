use crate::{
    client::JobworkerpClient,
    jobworkerp::function::{
        data::{function_specs, FunctionSchema, FunctionSpecs, McpToolList},
        service::FindFunctionRequest,
        service::FindFunctionSetRequest,
    },
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
    ListBySet {
        #[clap(short, long)]
        name: String,
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
            FunctionCommand::ListBySet { name } => {
                let request = FindFunctionSetRequest { name: name.clone() };
                let response = client
                    .function_client()
                    .await
                    .find_list_by_set(request)
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
        function.runner_id.as_ref().inspect(|runner_id| {
            println!("\t[runner_id] {}", runner_id.value);
        });
        function.worker_id.as_ref().inspect(|worker_id| {
            println!("\t[worker_id] {}", worker_id.value);
        });
        println!("\t[name] {}", &function.name);
        println!("\t[description] {}", &function.description);

        // Print input schema
        match &function.schema {
            Some(function_specs::Schema::SingleSchema(FunctionSchema {
                settings,
                arguments,
                result_output_schema,
            })) => {
                println!("\t[input_schema]:");
                if let Some(settings) = &settings {
                    println!("\t\t[settings] |\n---\n{}", settings);
                } else {
                    println!("\t\t[settings] (None)");
                }
                println!("\t\t[arguments] |\n---\n{}", &arguments);
                // Print output schema if available
                if let Some(result_output_schema) = &result_output_schema {
                    println!("\t[result_output_schema] |\n---\n{}", result_output_schema);
                } else {
                    println!("\t[result_output_schema] (None)");
                }
            }
            Some(function_specs::Schema::McpTools(McpToolList { list })) => {
                println!("\t[input_schema]:");
                for tool in list {
                    println!("\t\t[tool] {}", tool.name);
                    println!("\t\t[description] {:?}", tool.description);
                    println!("\t\t[input schema] {:?}", tool.input_schema);
                    println!("\t\t[annotations] {:?}", tool.annotations);
                }
            }
            None => {
                println!("\t[input_schema] (None)");
            }
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
