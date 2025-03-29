use crate::{
    client::JobworkerpClient,
    jobworkerp::{
        data::ToolSpecs,
        service::FindToolRequest,
    },
};
use clap::Parser;

#[derive(Parser, Debug)]
pub struct ToolArg {
    #[clap(subcommand)]
    pub cmd: ToolCommand,
}

#[derive(Parser, Debug)]
pub enum ToolCommand {
    List {
        #[clap(long)]
        exclude_runner: bool,
        #[clap(long)]
        exclude_worker: bool,
    },
}

impl ToolCommand {
    pub async fn execute(&self, client: &JobworkerpClient) {
        match self {
            ToolCommand::List { exclude_runner, exclude_worker } => {
                let request = FindToolRequest {
                    exclude_runner: *exclude_runner,
                    exclude_worker: *exclude_worker,
                };
                let response = client
                    .tool_client()
                    .await
                    .find_list(request)
                    .await
                    .unwrap();
                println!("meta: {:#?}", response.metadata());
                let mut data = response.into_inner();
                while let Some(tool) = data.message().await.unwrap() {
                    Self::print_tool(&tool);
                }
                println!(
                    "trailer: {:#?}",
                    data.trailers().await.unwrap().unwrap_or_default()
                );
            }
        }
    }

    pub fn print_tool(tool: &ToolSpecs) {
        println!("[tool]:");
        
        // Print tool ID (either runner or worker)
        match &tool.tool_id {
            Some(crate::jobworkerp::data::tool_specs::ToolId::RunnerId(runner_id)) => {
                println!("\t[runner_id] {}", runner_id.value);
            }
            Some(crate::jobworkerp::data::tool_specs::ToolId::WorkerId(worker_id)) => {
                println!("\t[worker_id] {}", worker_id.value);
            }
            None => {
                println!("\t[id] None");
            }
        }
        
        println!("\t[name] {}", &tool.name);
        println!("\t[description] {}", &tool.description);
        
        // Print input schema
        if let Some(input_schema) = &tool.input_schema {
            println!("\t[input_schema]:");
            if let Some(settings) = &input_schema.settings {
                println!("\t\t[settings] |\n---\n{}", settings);
            } else {
                println!("\t\t[settings] (None)");
            }
            println!("\t\t[arguments] |\n---\n{}", &input_schema.arguments);
        }
        
        // Print output schema if available
        if let Some(result_output_schema) = &tool.result_output_schema {
            println!("\t[result_output_schema] |\n---\n{}", result_output_schema);
        } else {
            println!("\t[result_output_schema] (None)");
        }
        
        // Print output type
        println!(
            "\t[output_type] {}",
            crate::jobworkerp::data::StreamingOutputType::try_from(tool.output_type)
                .unwrap_or(crate::jobworkerp::data::StreamingOutputType::NonStreaming)
                .as_str_name()
        );
    }
}
