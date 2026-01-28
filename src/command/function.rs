use std::collections::HashMap;

use crate::{
    client::JobworkerpClient,
    command::to_request,
    jobworkerp::data::{QueueType, ResponseType, RetryPolicy, RunnerId, WorkerId},
    jobworkerp::function::{
        data::{FunctionCallOptions, FunctionId, FunctionResult, FunctionSpecs, WorkerOptions},
        service::{
            FindFunctionByNameRequest, FindFunctionRequest, FindFunctionSetRequest,
            FunctionCallRequest, RunnerParameters, function_call_request,
        },
    },
};
use clap::Parser;

pub mod display;

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
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    ListBySet {
        #[clap(short, long)]
        name: String,
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
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
        queue_type: Option<String>,
        #[clap(long)]
        response_type: Option<String>,
        #[clap(long)]
        store_success: bool,
        #[clap(long)]
        store_failure: bool,
        #[clap(long)]
        use_static: bool,
        #[clap(long)]
        broadcast_results: bool,
    },
    Find {
        #[clap(short = 'r', long)]
        runner_id: Option<i64>,
        #[clap(short = 'w', long)]
        worker_id: Option<i64>,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    FindByName {
        #[clap(short = 'r', long)]
        runner_name: Option<String>,
        #[clap(short = 'w', long)]
        worker_name: Option<String>,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
}

impl FunctionCommand {
    pub async fn execute(&self, client: &JobworkerpClient, metadata: &HashMap<String, String>) {
        match self {
            FunctionCommand::List {
                exclude_runner,
                exclude_worker,
                format,
                no_truncate,
            } => {
                let request = FindFunctionRequest {
                    exclude_runner: *exclude_runner,
                    exclude_worker: *exclude_worker,
                };
                use self::display::function_to_json;
                use crate::display::{
                    CardVisualizer, DisplayOptions, JsonPrettyVisualizer, JsonVisualizer,
                    TableVisualizer, utils::supports_color,
                };

                let response = client
                    .function_client()
                    .await
                    .find_list(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();

                println!("meta: {:#?}", response.metadata());
                let mut data = response.into_inner();

                // Collect all functions into a vector for batch processing
                let mut functions_json = Vec::new();
                while let Some(function) = data.message().await.unwrap() {
                    let function_json = function_to_json(&function, format);
                    functions_json.push(function_json);
                }

                // Display using the appropriate visualizer
                let options = DisplayOptions::new(format.clone())
                    .with_color(supports_color())
                    .with_no_truncate(*no_truncate);

                let output = match format {
                    crate::display::DisplayFormat::Table => {
                        let visualizer = TableVisualizer;
                        visualizer.visualize(&functions_json, &options)
                    }
                    crate::display::DisplayFormat::Card => {
                        let visualizer = CardVisualizer;
                        visualizer.visualize(&functions_json, &options)
                    }
                    crate::display::DisplayFormat::Json => {
                        let visualizer = JsonPrettyVisualizer;
                        visualizer.visualize(&functions_json, &options)
                    }
                };

                println!("{output}");

                println!(
                    "trailer: {:#?}",
                    data.trailers().await.unwrap().unwrap_or_default()
                );
            }
            FunctionCommand::ListBySet {
                name,
                format,
                no_truncate,
            } => {
                use self::display::function_to_json;
                use crate::display::{
                    CardVisualizer, DisplayOptions, JsonPrettyVisualizer, JsonVisualizer,
                    TableVisualizer, utils::supports_color,
                };

                let request = FindFunctionSetRequest { name: name.clone() };
                let response = client
                    .function_client()
                    .await
                    .find_list_by_set(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();

                println!("meta: {:#?}", response.metadata());
                let mut data = response.into_inner();

                // Collect all functions into a vector for batch processing
                let mut functions_json = Vec::new();
                while let Some(function) = data.message().await.unwrap() {
                    let function_json = function_to_json(&function, format);
                    functions_json.push(function_json);
                }

                // Display using the appropriate visualizer
                let options = DisplayOptions::new(format.clone())
                    .with_color(supports_color())
                    .with_no_truncate(*no_truncate);

                let output = match format {
                    crate::display::DisplayFormat::Table => {
                        let visualizer = TableVisualizer;
                        visualizer.visualize(&functions_json, &options)
                    }
                    crate::display::DisplayFormat::Card => {
                        let visualizer = CardVisualizer;
                        visualizer.visualize(&functions_json, &options)
                    }
                    crate::display::DisplayFormat::Json => {
                        let visualizer = JsonPrettyVisualizer;
                        visualizer.visualize(&functions_json, &options)
                    }
                };

                println!("{output}");

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
                queue_type,
                response_type,
                store_success,
                store_failure,
                use_static,
                broadcast_results,
            } => {
                // Determine which name to use (priority: function_name, runner_name, worker_name)
                let name_type = if let Some(fname) = function_name {
                    // For function_name, we default to treating it as a worker name
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
                    // Parse queue_type and response_type
                    let queue_type_value = queue_type.as_ref().map(|qt| match qt.as_str() {
                        "NORMAL" => QueueType::Normal as i32,
                        "DB_ONLY" => QueueType::DbOnly as i32,
                        "WITH_BACKUP" => QueueType::WithBackup as i32,
                        _ => QueueType::Normal as i32,
                    });

                    let response_type_value = response_type.as_ref().map(|rt| match rt.as_str() {
                        "NO_RESULT" => ResponseType::NoResult as i32,
                        "DIRECT" => ResponseType::Direct as i32,
                        _ => ResponseType::Direct as i32,
                    });

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
                        queue_type: queue_type_value,
                        response_type: response_type_value,
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

                // Process streaming results with improved formatting
                while let Some(function_result) = result_stream.message().await.unwrap() {
                    Self::print_function_result(&function_result);
                }

                println!(
                    "trailer: {:#?}",
                    result_stream.trailers().await.unwrap().unwrap_or_default()
                );
            }
            FunctionCommand::Find {
                runner_id,
                worker_id,
                format,
                no_truncate,
            } => {
                // Validation: exactly one of runner_id or worker_id must be specified
                if runner_id.is_some() && worker_id.is_some() {
                    eprintln!("Error: Only one of --runner-id or --worker-id can be specified");
                    return;
                }
                if runner_id.is_none() && worker_id.is_none() {
                    eprintln!("Error: Either --runner-id or --worker-id must be specified");
                    return;
                }

                use crate::jobworkerp::function::data::{FunctionUsing, function_id};

                // Build request based on which ID is provided
                let function_id_inner = if let Some(runner_id) = runner_id {
                    FunctionId {
                        id: Some(function_id::Id::RunnerId(RunnerId { value: *runner_id })),
                    }
                } else if let Some(worker_id) = worker_id {
                    FunctionId {
                        id: Some(function_id::Id::WorkerId(WorkerId { value: *worker_id })),
                    }
                } else {
                    unreachable!("Validation should have caught this case")
                };

                let request = FunctionUsing {
                    function_id: Some(function_id_inner),
                    using: None,
                };

                // Make the gRPC call
                let response = client
                    .function_client()
                    .await
                    .find(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();

                let function_specs = response.into_inner().data;

                match function_specs {
                    Some(specs) => {
                        use self::display::function_to_json;
                        use crate::display::{
                            CardVisualizer, DisplayOptions, JsonPrettyVisualizer, TableVisualizer,
                            utils::supports_color, visualizer::JsonVisualizer,
                        };

                        // Convert to JSON and display
                        let function_json = function_to_json(&specs, format);
                        let functions_vec = vec![function_json];

                        let options = DisplayOptions::new(format.clone())
                            .with_color(supports_color())
                            .with_no_truncate(*no_truncate);

                        let output = match format {
                            crate::display::DisplayFormat::Table => {
                                let visualizer = TableVisualizer;
                                visualizer.visualize(&functions_vec, &options)
                            }
                            crate::display::DisplayFormat::Card => {
                                let visualizer = CardVisualizer;
                                visualizer.visualize(&functions_vec, &options)
                            }
                            crate::display::DisplayFormat::Json => {
                                let visualizer = JsonPrettyVisualizer;
                                visualizer.visualize(&functions_vec, &options)
                            }
                        };

                        println!("{output}");
                    }
                    None => {
                        println!("Function not found");
                    }
                }
            }
            FunctionCommand::FindByName {
                runner_name,
                worker_name,
                format,
                no_truncate,
            } => {
                // Validation: exactly one of runner_name or worker_name must be specified
                if runner_name.is_some() && worker_name.is_some() {
                    eprintln!("Error: Only one of --runner-name or --worker-name can be specified");
                    return;
                }
                if runner_name.is_none() && worker_name.is_none() {
                    eprintln!("Error: Either --runner-name or --worker-name must be specified");
                    return;
                }

                use crate::jobworkerp::function::service::find_function_by_name_request;

                // Build request based on which name is provided
                let request = if let Some(runner_name) = runner_name {
                    FindFunctionByNameRequest {
                        name: Some(find_function_by_name_request::Name::RunnerName(
                            runner_name.clone(),
                        )),
                    }
                } else if let Some(worker_name) = worker_name {
                    FindFunctionByNameRequest {
                        name: Some(find_function_by_name_request::Name::WorkerName(
                            worker_name.clone(),
                        )),
                    }
                } else {
                    unreachable!("Validation should have caught this case")
                };

                // Make the gRPC call
                let response = client
                    .function_client()
                    .await
                    .find_by_name(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();

                let function_specs = response.into_inner().data;

                match function_specs {
                    Some(specs) => {
                        use self::display::function_to_json;
                        use crate::display::{
                            CardVisualizer, DisplayOptions, JsonPrettyVisualizer, TableVisualizer,
                            utils::supports_color, visualizer::JsonVisualizer,
                        };

                        // Convert to JSON and display
                        let function_json = function_to_json(&specs, format);
                        let functions_vec = vec![function_json];

                        let options = DisplayOptions::new(format.clone())
                            .with_color(supports_color())
                            .with_no_truncate(*no_truncate);

                        let output = match format {
                            crate::display::DisplayFormat::Table => {
                                let visualizer = TableVisualizer;
                                visualizer.visualize(&functions_vec, &options)
                            }
                            crate::display::DisplayFormat::Card => {
                                let visualizer = CardVisualizer;
                                visualizer.visualize(&functions_vec, &options)
                            }
                            crate::display::DisplayFormat::Json => {
                                let visualizer = JsonPrettyVisualizer;
                                visualizer.visualize(&functions_vec, &options)
                            }
                        };

                        println!("{output}");
                    }
                    None => {
                        println!("Function not found");
                    }
                }
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

        // Print settings schema
        if !function.settings_schema.is_empty() {
            println!("\t[settings_schema] |\n---\n{}", &function.settings_schema);
        } else {
            println!("\t[settings_schema] (None)");
        }

        // Print methods
        if let Some(method_map) = &function.methods {
            if method_map.schemas.is_empty() {
                println!("\t[methods] (None)");
            } else if method_map.schemas.len() == 1 {
                // Single method - detailed display
                let (method_name, method_schema) = method_map.schemas.iter().next().unwrap();
                println!("\t[method] {}", method_name);

                if let Some(desc) = &method_schema.description {
                    println!("\t\t[description] {}", desc);
                }

                println!(
                    "\t\t[arguments_schema] |\n---\n{}",
                    &method_schema.arguments_schema
                );

                if let Some(result_schema) = &method_schema.result_schema {
                    println!("\t\t[result_schema] |\n---\n{}", result_schema);
                } else {
                    println!("\t\t[result_schema] (None)");
                }

                println!(
                    "\t\t[output_type] {}",
                    crate::jobworkerp::data::StreamingOutputType::try_from(
                        method_schema.output_type
                    )
                    .unwrap_or(crate::jobworkerp::data::StreamingOutputType::NonStreaming)
                    .as_str_name()
                );

                if let Some(annotations) = &method_schema.annotations {
                    println!("\t\t[annotations]:");
                    if let Some(title) = &annotations.title {
                        println!("\t\t\t[title] {}", title);
                    }
                    if let Some(read_only) = annotations.read_only_hint {
                        println!("\t\t\t[read_only_hint] {}", read_only);
                    }
                    if let Some(destructive) = annotations.destructive_hint {
                        println!("\t\t\t[destructive_hint] {}", destructive);
                    }
                    if let Some(idempotent) = annotations.idempotent_hint {
                        println!("\t\t\t[idempotent_hint] {}", idempotent);
                    }
                    if let Some(open_world) = annotations.open_world_hint {
                        println!("\t\t\t[open_world_hint] {}", open_world);
                    }
                }
            } else {
                // Multiple methods - list view (sorted alphabetically)
                println!("\t[methods] ({} methods):", method_map.schemas.len());
                let mut method_names: Vec<_> = method_map.schemas.keys().collect();
                method_names.sort();
                for method_name in method_names {
                    let method_schema = &method_map.schemas[method_name];
                    println!("\t\t[method] {}", method_name);
                    if let Some(desc) = &method_schema.description {
                        println!("\t\t\t[description] {}", desc);
                    }
                    println!(
                        "\t\t\t[output_type] {}",
                        crate::jobworkerp::data::StreamingOutputType::try_from(
                            method_schema.output_type
                        )
                        .unwrap_or(crate::jobworkerp::data::StreamingOutputType::NonStreaming)
                        .as_str_name()
                    );
                }
            }
        } else {
            println!("\t[methods] (None)");
        }
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
