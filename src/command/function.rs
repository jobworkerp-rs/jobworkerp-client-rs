use std::collections::HashMap;

use crate::{
    client::JobworkerpClient,
    command::to_request,
    jobworkerp::data::RetryPolicy,
    jobworkerp::function::{
        data::{
            function_specs, FunctionCallOptions, FunctionResult, FunctionSchema, FunctionSpecs,
            McpToolList, WorkerOptions,
        },
        service::{
            function_call_request, FindFunctionRequest, FindFunctionSetRequest,
            FunctionCallRequest, RunnerParameters,
        },
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
    Call {
        // Function name - either runner or worker
        #[clap(short = 'f', long)]
        function_name: Option<String>,
        // Runner name (alternative to function_name)
        #[clap(short = 'r', long)]
        runner_name: Option<String>,
        // Worker name (alternative to function_name)
        #[clap(short = 'w', long)]
        worker_name: Option<String>,
        // JSON arguments for the function call
        #[clap(short, long)]
        args: String,
        // Optional unique key to prevent duplicate jobs
        #[clap(short, long)]
        unique_key: Option<String>,
        // Optional timeout in milliseconds
        #[clap(short, long)]
        timeout: Option<i64>,
        // Enable streaming output
        #[clap(long)]
        streaming: bool,
        // Runner settings in JSON format (only for runner calls)
        #[clap(long)]
        settings: Option<String>,
        // Worker options
        #[clap(long)]
        channel: Option<String>,
        #[clap(long)]
        with_backup: bool,
        #[clap(long)]
        store_success: bool,
        #[clap(long)]
        store_failure: bool,
        #[clap(long)]
        use_static: bool,
        #[clap(long)]
        broadcast_results: bool,
    },
}

impl FunctionCommand {
    pub async fn execute(&self, client: &JobworkerpClient, metadata: &HashMap<String, String>) {
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
                    .find_list(to_request(metadata, request).unwrap())
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
                    .find_list_by_set(to_request(metadata, request).unwrap())
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
            FunctionCommand::Call {
                function_name,
                runner_name,
                worker_name,
                args,
                unique_key,
                timeout,
                streaming,
                settings,
                channel,
                with_backup,
                store_success,
                store_failure,
                use_static,
                broadcast_results,
            } => {
                // Determine which name to use (priority: function_name, runner_name, worker_name)
                let name_type = if let Some(fname) = function_name {
                    // For function_name, we default to treating it as a worker name
                    // TODO: We could enhance this by first checking if it's a runner or worker
                    Some(function_call_request::Name::WorkerName(fname.clone()))
                } else if let Some(rname) = runner_name {
                    Some(function_call_request::Name::RunnerName(rname.clone()))
                } else if let Some(wname) = worker_name {
                    Some(function_call_request::Name::WorkerName(wname.clone()))
                } else {
                    panic!("One of function_name, runner_name, or worker_name must be specified");
                };

                // Prepare runner parameters if calling a runner
                let runner_parameters = if runner_name.is_some() {
                    // Create WorkerOptions from CLI parameters
                    let worker_options = WorkerOptions {
                        retry_policy: Some(RetryPolicy {
                            r#type: crate::jobworkerp::data::RetryType::Exponential as i32,
                            interval: 1000,      // 1 second
                            max_interval: 60000, // 60 seconds
                            max_retry: 3,
                            basis: 2.0,
                        }),
                        channel: channel.clone(),
                        with_backup: *with_backup,
                        store_success: *store_success,
                        store_failure: *store_failure,
                        use_static: *use_static,
                        broadcast_results: *broadcast_results,
                    };

                    Some(RunnerParameters {
                        settings_json: settings.clone().unwrap_or_default(),
                        worker_options: Some(worker_options),
                    })
                } else {
                    None
                };

                // Prepare function call options
                let call_options = Some(FunctionCallOptions {
                    timeout_ms: *timeout,
                    streaming: Some(*streaming),
                    metadata: metadata.clone(),
                });

                // Create the request
                let request = FunctionCallRequest {
                    name: name_type,
                    runner_parameters,
                    args_json: args.clone(),
                    uniq_key: unique_key.clone(),
                    options: call_options,
                };

                // Make the call
                let response = client
                    .function_client()
                    .await
                    .call(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();

                println!("meta: {:#?}", response.metadata());
                let mut result_stream = response.into_inner();

                // Process streaming results
                while let Some(function_result) = result_stream.message().await.unwrap() {
                    Self::print_function_result(&function_result);
                }

                println!(
                    "trailer: {:#?}",
                    result_stream.trailers().await.unwrap().unwrap_or_default()
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
                    println!("\t\t[settings] |\n---\n{settings}");
                } else {
                    println!("\t\t[settings] (None)");
                }
                println!("\t\t[arguments] |\n---\n{}", &arguments);
                // Print output schema if available
                if let Some(result_output_schema) = &result_output_schema {
                    println!("\t[result_output_schema] |\n---\n{result_output_schema}");
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

    pub fn print_function_result(result: &FunctionResult) {
        println!("[function_result]:");
        println!("\t[output] {}", &result.output);

        if let Some(status) = &result.status {
            println!("\t[status] {status:?}");
        }

        if let Some(error_message) = &result.error_message {
            println!("\t[error_message] {error_message}");
        }

        if let Some(error_code) = &result.error_code {
            println!("\t[error_code] {error_code}");
        }

        if let Some(last_info) = &result.last_info {
            println!("\t[execution_info]:");
            println!("\t\t[job_id] {}", &last_info.job_id);
            println!("\t\t[started_at] {}", &last_info.started_at);
            if let Some(completed_at) = &last_info.completed_at {
                println!("\t\t[completed_at] {completed_at}");
            }
            if let Some(execution_time_ms) = &last_info.execution_time_ms {
                println!("\t\t[execution_time_ms] {execution_time_ms}");
            }
            if !last_info.metadata.is_empty() {
                println!("\t\t[metadata] {:#?}", &last_info.metadata);
            }
        }
    }
}
