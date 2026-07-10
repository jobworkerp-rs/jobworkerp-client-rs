#![allow(
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    clippy::module_name_repetitions,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::option_if_let_else,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::unused_self
)]

// as job: valid commands are enqueue, find, list, delete, count
// -i, --id <id> id of the job (for find, delete)
// -w, --worker <id or name> worker id or name of the job (if string, treat as name, if number, treat as id)(for enqueue)
// --args <args json string> arguments of the worker runner (json string (transform to grpc message internally))
// -u, --unique-key <string> unique key of the job (for enqueue)
// -r, --run-after-time <number> execute unix time (milli-seconds) of the job (for enqueue)
// -p, --priority <priority> priority of the job (HIGH, MIDDLE, LOW)(for enqueue)
// -t, --timeout <timeout> timeout of the job (milli-seconds) (for enqueue)

use std::{collections::HashMap, str::FromStr, sync::Arc};

use super::WorkerIdOrName;
use crate::{
    client::{UseJobworkerpClient, helper::UseJobworkerpClientHelper},
    command::{job_result::JobResultCommand, to_request},
    display::{
        CardVisualizer, DisplayOptions, JsonPrettyVisualizer, JsonVisualizer, TableVisualizer,
        utils::supports_color,
    },
    jobworkerp::{
        self,
        data::{
            JobId, JobProcessingStatus, Priority, QueueType, ResponseType, Runner, RunnerType,
            WorkerData,
        },
        service::{
            CountCondition, FindListRequest, FindListWithProcessingStatusRequest, JobRequest,
            job_request,
        },
    },
    proto::JobworkerpProto,
};
use anyhow::Result;
use chrono::DateTime;
use clap::{Parser, ValueEnum};
use command_utils::protobuf::ProtobufDescriptor;
use command_utils::trace::Tracing;
use opentelemetry::{Context, global, trace::Span};
use prost::Message;

pub const JOB_RESULT_HEADER_NAME: &str = "x-job-result-bin";
pub const JOB_ID_HEADER_NAME: &str = "x-job-id-bin";

pub mod display;
use display::job_to_json;

#[derive(Parser, Debug)]
pub struct JobArg {
    #[clap(subcommand)]
    pub cmd: JobCommand,
}

#[derive(Parser, Debug, Clone)]
pub enum JobCommand {
    Enqueue {
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
        #[clap(short, long)]
        args: String,
        #[clap(short, long)]
        unique_key: Option<String>,
        #[clap(short, long)]
        run_after_time: Option<i64>,
        #[clap(short, long)]
        priority: Option<PriorityArg>,
        #[clap(short, long)]
        timeout: Option<u64>,
        #[clap(
            long,
            help = "Method name (required for multi-tool MCP/Plugin runners)"
        )]
        using: Option<String>,
    },
    EnqueueForStream {
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
        #[clap(short, long)]
        args: String,
        #[clap(short, long)]
        unique_key: Option<String>,
        #[clap(short, long)]
        run_after_time: Option<i64>,
        #[clap(short, long)]
        priority: Option<PriorityArg>,
        #[clap(short, long)]
        timeout: Option<u64>,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
        #[clap(
            long,
            help = "Method name (required for multi-tool MCP/Plugin runners)"
        )]
        using: Option<String>,
    },
    // `workflow_file` (URL/path) and `workflow_data` (inline DSL) map to the
    // server's `WorkflowRunArgs.workflow_source` oneof, so exactly one must be set.
    #[clap(group = clap::ArgGroup::new("enqueue_workflow_source").required(true).multiple(false).args(["workflow_file", "workflow_data"]))]
    EnqueueWorkflow {
        #[clap(short, long)]
        channel: Option<String>,
        #[clap(long)]
        context: Option<String>,
        #[clap(short, long)]
        input: String,
        #[clap(short, long)]
        priority: Option<PriorityArg>,
        #[clap(short, long)]
        run_after_time: Option<i64>,
        #[clap(short, long)]
        timeout: Option<u64>,
        #[clap(short, long)]
        workflow_file: Option<String>,
        #[clap(long, help = "Inline workflow definition (JSON or YAML string)")]
        workflow_data: Option<String>,
        #[clap(long, help = "Unique execution id for the workflow run")]
        execution_id: Option<String>,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    // `workflow_file` / `workflow_data` map to `CreateWorkflowArgs.workflow_source` oneof.
    #[clap(group = clap::ArgGroup::new("create_workflow_source").required(true).multiple(false).args(["workflow_file", "workflow_data"]))]
    CreateWorkflow {
        #[clap(short, long, help = "Name of the worker to create from the workflow")]
        name: String,
        #[clap(short, long)]
        workflow_file: Option<String>,
        #[clap(long, help = "Inline workflow definition (JSON or YAML string)")]
        workflow_data: Option<String>,
        #[clap(long)]
        context: Option<String>,
        #[clap(short, long)]
        channel: Option<String>,
        #[clap(long, value_parser = crate::command::worker::QueueTypeArg::parse)]
        queue_type: Option<crate::command::worker::QueueTypeArg>,
        #[clap(long, value_parser = crate::command::worker::ResponseTypeArg::parse)]
        response_type: Option<crate::command::worker::ResponseTypeArg>,
        #[clap(long)]
        store_success: Option<bool>,
        #[clap(long)]
        store_failure: Option<bool>,
        #[clap(long)]
        use_static: Option<bool>,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    Find {
        #[clap(short, long)]
        id: i64,
    },
    List {
        #[clap(short, long)]
        offset: Option<i64>,
        #[clap(short, long)]
        limit: Option<u32>,
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
    ListWithProcessingStatus {
        #[clap(short, long)]
        status: JobProcessingStatusArg,
        #[clap(short, long)]
        limit: Option<i32>,
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
}

#[derive(ValueEnum, Debug, Clone)]
pub enum PriorityArg {
    High,
    Middle,
    Low,
}

#[derive(ValueEnum, Debug, Clone)]
pub enum JobProcessingStatusArg {
    Unknown,
    Pending,
    Running,
    WaitResult,
    Cancelling,
}
impl PriorityArg {
    #[must_use]
    pub const fn to_grpc(&self) -> Priority {
        match self {
            Self::High => Priority::High,
            Self::Middle => Priority::Medium,
            Self::Low => Priority::Low,
        }
    }
}

impl JobProcessingStatusArg {
    #[must_use]
    pub const fn to_grpc(&self) -> JobProcessingStatus {
        match self {
            Self::Unknown => JobProcessingStatus::Unknown,
            Self::Pending => JobProcessingStatus::Pending,
            Self::Running => JobProcessingStatus::Running,
            Self::WaitResult => JobProcessingStatus::WaitResult,
            Self::Cancelling => JobProcessingStatus::Cancelling,
        }
    }
}

/// Build the WORKFLOW runner `run` job_args JSON from CLI inputs.
///
/// Why pure: keeps descriptor/gRPC plumbing out so the JSON shaping
/// (the `workflow_source` oneof and optional fields) is unit-testable
/// without a running server.
fn build_workflow_run_args_json(
    workflow_file: Option<&str>,
    workflow_data: Option<&str>,
    input: &str,
    context: Option<&str>,
    execution_id: Option<&str>,
) -> Result<serde_json::Value> {
    let mut job_args = serde_json::json!({ "input": input });
    set_workflow_source(&mut job_args, workflow_file, workflow_data)?;
    if let Some(ctx) = context.filter(|c| !c.is_empty()) {
        job_args["workflow_context"] = serde_json::json!(ctx);
    }
    if let Some(eid) = execution_id.filter(|e| !e.is_empty()) {
        job_args["execution_id"] = serde_json::json!(eid);
    }
    Ok(job_args)
}

/// Build the WORKFLOW runner `create` job_args JSON (CreateWorkflowArgs).
fn build_create_workflow_args_json(
    name: &str,
    workflow_file: Option<&str>,
    workflow_data: Option<&str>,
    context: Option<&str>,
    worker_options: serde_json::Value,
) -> Result<serde_json::Value> {
    let mut args = serde_json::json!({ "name": name });
    set_workflow_source(&mut args, workflow_file, workflow_data)?;
    if let Some(ctx) = context.filter(|c| !c.is_empty()) {
        args["workflow_context"] = serde_json::json!(ctx);
    }
    if worker_options.as_object().is_some_and(|m| !m.is_empty()) {
        args["worker_options"] = worker_options;
    }
    Ok(args)
}

/// Set the `workflow_source` oneof; exactly one of file/data must be present.
fn set_workflow_source(
    target: &mut serde_json::Value,
    workflow_file: Option<&str>,
    workflow_data: Option<&str>,
) -> Result<()> {
    match (workflow_file, workflow_data) {
        (Some(f), None) => target["workflow_url"] = serde_json::json!(f),
        // workflow_data carries the inline DSL (JSON/YAML) string.
        (None, Some(d)) => target["workflow_data"] = serde_json::json!(d),
        _ => anyhow::bail!("exactly one of workflow_file / workflow_data must be set"),
    }
    Ok(())
}

/// Build the `worker_options` object for CreateWorkflowArgs.
///
/// Only set keys are emitted so the server applies its own defaults for the
/// rest. Enum values use their proto names for `normalize_enum` conversion.
fn build_worker_options_json(
    channel: Option<&str>,
    queue_type: Option<&crate::command::worker::QueueTypeArg>,
    response_type: Option<&crate::command::worker::ResponseTypeArg>,
    store_success: Option<bool>,
    store_failure: Option<bool>,
    use_static: Option<bool>,
) -> serde_json::Value {
    let mut opts = serde_json::Map::new();
    if let Some(c) = channel.filter(|c| !c.is_empty()) {
        opts.insert("channel".into(), serde_json::json!(c));
    }
    if let Some(q) = queue_type {
        opts.insert("queue_type".into(), serde_json::json!(q.as_proto_name()));
    }
    if let Some(r) = response_type {
        opts.insert("response_type".into(), serde_json::json!(r.as_proto_name()));
    }
    if let Some(v) = store_success {
        opts.insert("store_success".into(), serde_json::json!(v));
    }
    if let Some(v) = store_failure {
        opts.insert("store_failure".into(), serde_json::json!(v));
    }
    if let Some(v) = use_static {
        opts.insert("use_static".into(), serde_json::json!(v));
    }
    serde_json::Value::Object(opts)
}

/// Extract the created worker id from a decoded `CreateWorkflowResult` JSON.
///
/// The result message has no static Rust type on the client (proto is fetched
/// dynamically), and the field name has varied across server versions, so we
/// probe the known candidate shapes rather than depend on one layout.
fn extract_worker_id_from_create_result(json: &serde_json::Value) -> Option<i64> {
    fn as_i64(v: &serde_json::Value) -> Option<i64> {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
    }
    for key in ["worker_id", "workerId", "id"] {
        if let Some(field) = json.get(key) {
            // Wrapper form `{ "worker_id": { "value": 42 } }` or scalar `{ "worker_id": 42 }`.
            if let Some(id) = field
                .get("value")
                .and_then(as_i64)
                .or_else(|| as_i64(field))
            {
                return Some(id);
            }
        }
    }
    None
}

impl JobCommand {
    fn create_parent_span(
        &self,
        tracer_name: &'static str,
        span_name: &'static str,
    ) -> (opentelemetry::global::BoxedSpan, Context) {
        use opentelemetry::trace::{TraceContextExt, Tracer};

        let context = Context::current();
        let tracer = global::tracer(tracer_name);
        let builder = tracer.span_builder(span_name);
        let span = tracer.build_with_context(builder, &context);
        let new_context_with_span_active =
            context.with_remote_span_context(span.span_context().clone());

        (span, new_context_with_span_active)
    }
    pub async fn execute(
        &self,
        client: &crate::client::JobworkerpClient,
        metadata: Arc<HashMap<String, String>>,
    ) {
        match self {
            Self::Enqueue {
                worker,
                args,
                unique_key,
                run_after_time,
                priority,
                timeout,
                using,
            } => {
                let req = worker.to_job_worker();
                let using_ref = using.as_deref();
                let (_, args_desc, result_desc) =
                    JobworkerpProto::find_runner_descriptors_by_worker(client, req, using_ref)
                        .await
                        .unwrap();
                let request = JobRequest {
                    worker: Some(worker.to_job_worker()),
                    args: if let Some(args_d) = args_desc {
                        JobworkerpProto::json_to_message(args_d, args.as_str(), true).unwrap()
                    } else {
                        args.as_bytes().to_vec()
                    },
                    uniq_key: unique_key.clone(),
                    run_after_time: *run_after_time,
                    priority: priority.clone().map(|p| p.to_grpc() as i32),
                    timeout: timeout.and_then(|t| if t > 0 { Some(t) } else { None }),
                    using: using.clone(),
                    overrides: None,
                };
                let response = client
                    .job_client()
                    .await
                    .enqueue(to_request(&metadata, request).unwrap())
                    .await
                    .unwrap()
                    .into_inner();
                if let Some(result) = response.result {
                    // Use simplified output for job result
                    if let Some(rid) = &result.id {
                        println!("Job enqueued successfully - Result ID: {}", rid.value);
                    }
                    if let Some(data) = &result.data {
                        println!(
                            "Worker: {} | Status: {}",
                            data.worker_name,
                            data.status().as_str_name()
                        );
                        if let Some(output) = &data.output
                            && !output.items.is_empty()
                        {
                            JobResultCommand::print_job_result_output(&output.items, result_desc);
                        }
                    }
                } else {
                    println!("{response:#?}");
                }
            }
            Self::EnqueueForStream {
                worker,
                args,
                unique_key,
                run_after_time,
                priority,
                timeout,
                format,
                no_truncate,
                using,
            } => {
                let req = worker.to_job_worker();
                let using_ref = using.as_deref();
                let (_, args_desc, result_desc) =
                    JobworkerpProto::find_runner_descriptors_by_worker(client, req, using_ref)
                        .await
                        .unwrap();
                let request = JobRequest {
                    worker: Some(worker.to_job_worker()),
                    args: if let Some(args_d) = args_desc {
                        JobworkerpProto::json_to_message(args_d, args.as_str(), true).unwrap()
                    } else {
                        args.as_bytes().to_vec()
                    },
                    uniq_key: unique_key.clone(),
                    run_after_time: *run_after_time,
                    priority: priority.clone().map(|p| p.to_grpc() as i32),
                    timeout: timeout.and_then(|t| if t > 0 { Some(t) } else { None }),
                    using: using.clone(),
                    overrides: None,
                };
                let response = client
                    .job_client()
                    .await
                    .enqueue_for_stream(to_request(&metadata, request).unwrap())
                    .await
                    .unwrap();

                let meta = response.metadata().clone();
                let mut response = response.into_inner();

                // Check for job result header
                if let Some(result_bin) = meta.get(JOB_RESULT_HEADER_NAME) {
                    println!("Job result header found: {result_bin:#?}");
                }

                // Create display options
                let display_options = crate::display::DisplayOptions {
                    format: *format,
                    color_enabled: true,
                    max_field_length: None,
                    use_unicode: true,
                    no_truncate: *no_truncate,
                };

                // Start streaming session
                JobResultCommand::start_streaming_session(
                    "job execution",
                    format,
                    &display_options,
                );

                // result meta header
                JobResultCommand::print_job_result_metadata(&meta, result_desc.clone());

                // print streaming response with improved formatting
                let mut item_count = 0;
                while let Ok(Some(item)) = response.message().await {
                    match &item.item {
                        Some(jobworkerp::data::result_output_item::Item::Data(v)) => {
                            JobResultCommand::print_streaming_output(
                                v.as_slice(),
                                result_desc.clone(),
                                format,
                                &display_options,
                                item_count,
                            );
                            item_count += 1;
                        }
                        Some(jobworkerp::data::result_output_item::Item::FinalCollected(v))
                            // Only display if no Data chunks were output yet
                            if item_count == 0 =>
                        {
                            JobResultCommand::print_streaming_output(
                                v.as_slice(),
                                result_desc.clone(),
                                format,
                                &display_options,
                                item_count,
                            );
                            item_count += 1;
                        }
                        Some(jobworkerp::data::result_output_item::Item::End(_)) => {
                            break;
                        }
                        Some(jobworkerp::data::result_output_item::Item::FinalCollected(_))
                        | None => {
                            // Skip: FinalCollected after Data chunks already output, or empty items
                        }
                    }
                }

                // End streaming session
                JobResultCommand::end_streaming_session(item_count, format, &display_options);
            }
            Self::EnqueueWorkflow {
                channel,
                context,
                input,
                priority,
                run_after_time,
                timeout,
                workflow_file,
                workflow_data,
                execution_id,
                format,
                no_truncate,
            } => {
                let helper = JobCommandHelper::new(client.clone());
                let (_span, cx) =
                    self.create_parent_span("jobworkerp-client", "JobCommand::EnqueueWorkflow");
                let cx = Some(cx);

                let using = Some("run");
                let runner = helper
                    .find_runner_by_name(
                        cx.as_ref(),
                        metadata.clone(),
                        RunnerType::Workflow.as_str_name(),
                    )
                    .await
                    .unwrap();
                if let Some(Runner {
                    id: Some(rid),
                    data: Some(rdata),
                }) = runner
                {
                    let wname =
                        crate::client::helper::ephemeral_worker_name("JobworkerpClientWorkflow");

                    let worker_data = WorkerData {
                        name: wname.clone(),
                        runner_id: Some(rid),
                        runner_settings: vec![],
                        retry_policy: None,
                        channel: channel.clone(),
                        queue_type: QueueType::Normal as i32,
                        response_type: ResponseType::Direct as i32,
                        store_success: false,
                        store_failure: false,
                        broadcast_results: true,
                        use_static: false,
                        ..Default::default()
                    };
                    let args = if let Some(args_descriptor) =
                        JobworkerpProto::parse_job_args_schema_descriptor(&rdata, using)
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to parse job_args schema descriptor: {e:#?}"
                                )
                            })
                            .unwrap()
                    {
                        let job_args = match build_workflow_run_args_json(
                            workflow_file.as_deref(),
                            workflow_data.as_deref(),
                            input,
                            context.as_deref(),
                            execution_id.as_deref(),
                        ) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("Invalid workflow arguments: {e:#}");
                                return;
                            }
                        };
                        JobworkerpProto::json_value_to_message(
                            args_descriptor,
                            &job_args,
                            true,
                            true,
                        )
                        .map_err(|e| {
                            println!("Failed to parse job_args schema: {:#?}", e);
                            anyhow::anyhow!("Failed to parse job_args schema: {e:#?}")
                        })
                        .unwrap()
                    } else {
                        println!("args_descriptor not found");
                        return;
                    };
                    let result_desc =
                        JobworkerpProto::parse_result_schema_descriptor(&rdata, using)
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to parse job_result schema descriptor: {e:#?}"
                                )
                            })
                            .unwrap();
                    let enqueue_result = helper
                        .enqueue_stream_worker_job(
                            cx.as_ref(),
                            metadata.clone(),
                            &worker_data,
                            args,
                            timeout.map(|t| (t / 1000) as u32).unwrap_or(3600),
                            *run_after_time,
                            priority.clone().map(|p| p.to_grpc()),
                            using,
                        )
                        .await;
                    // Clean up temp worker regardless of enqueue result
                    let _ = helper
                        .delete_worker_by_name(cx.as_ref(), metadata, wname.as_str())
                        .await;
                    let (meta, mut response) = match enqueue_result {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("Workflow execution failed: {e:#}");
                            return;
                        }
                    };
                    // Check for job result header in initial response metadata
                    if let Some(id_bin) = meta.get_bin(JOB_ID_HEADER_NAME) {
                        match JobId::decode(id_bin.to_bytes().unwrap().as_ref()) {
                            Ok(job_id) => println!("Job ID header found: {job_id:?}"),
                            Err(e) => println!("Failed to decode job ID: {e}"),
                        }
                    }
                    // Create display options for workflow
                    let display_options = crate::display::DisplayOptions {
                        format: *format,
                        color_enabled: true,
                        max_field_length: None,
                        use_unicode: true,
                        no_truncate: *no_truncate,
                    };

                    // Check for job result header in initial response metadata
                    if let Some(_result_bin) = meta.get_bin(JOB_RESULT_HEADER_NAME) {
                        println!("Job result initial response header found");
                        JobResultCommand::print_job_result_metadata(&meta, result_desc.clone());
                    }

                    // Start streaming session for workflow
                    JobResultCommand::start_streaming_session(
                        "workflow execution",
                        format,
                        &display_options,
                    );

                    let mut item_count = 0;
                    while let Ok(Some(item)) = response.message().await {
                        match &item.item {
                            Some(jobworkerp::data::result_output_item::Item::Data(v)) => {
                                JobResultCommand::print_streaming_output(
                                    v.as_slice(),
                                    result_desc.clone(),
                                    format,
                                    &display_options,
                                    item_count,
                                );
                                item_count += 1;
                            }
                            Some(jobworkerp::data::result_output_item::Item::FinalCollected(v))
                                // Only display if no Data chunks were output yet
                                if item_count == 0 =>
                            {
                                JobResultCommand::print_streaming_output(
                                    v.as_slice(),
                                    result_desc.clone(),
                                    format,
                                    &display_options,
                                    item_count,
                                );
                                item_count += 1;
                            }
                            Some(jobworkerp::data::result_output_item::Item::End(_)) => {
                                break;
                            }
                            Some(jobworkerp::data::result_output_item::Item::FinalCollected(_))
                            | None => {
                                // Skip: FinalCollected after Data chunks already output, or empty items
                            }
                        }
                    }

                    // End streaming session for workflow
                    JobResultCommand::end_streaming_session(item_count, format, &display_options);
                    //  // Check for job result header in last response metadata
                    // if let Some(_result_bin) = meta.get_bin(JOB_RESULT_HEADER_NAME) {
                    //     println!("Job result header found in last response metadata");
                    //     JobResultCommand::print_job_result_metadata(&meta, result_desc.clone());
                    // }

                    // Also check trailers for completeness
                    match response.trailers().await {
                        Ok(Some(trailers)) => {
                            if !trailers.is_empty()
                                && let Some(_trailer_result) =
                                    trailers.get_bin(JOB_RESULT_HEADER_NAME)
                            {
                                println!("Trailer job result header found");
                                JobResultCommand::print_job_result_metadata(
                                    &trailers,
                                    result_desc.clone(),
                                );
                            }
                        }
                        Ok(None) => {
                            println!("No trailers found");
                        }
                        Err(e) => {
                            println!("Error reading trailers: {e}");
                        }
                    }
                } else {
                    println!("runner {} not found", RunnerType::Workflow.as_str_name());
                    return;
                }
            }
            Self::CreateWorkflow {
                name,
                workflow_file,
                workflow_data,
                context,
                channel,
                queue_type,
                response_type,
                store_success,
                store_failure,
                use_static,
                format,
                no_truncate,
            } => {
                let helper = JobCommandHelper::new(client.clone());
                let (_span, cx) =
                    self.create_parent_span("jobworkerp-client", "JobCommand::CreateWorkflow");
                let cx = Some(cx);

                let using = Some("create");
                let runner = match helper
                    .find_runner_by_name(
                        cx.as_ref(),
                        metadata.clone(),
                        RunnerType::Workflow.as_str_name(),
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("Failed to find WORKFLOW runner: {e:#}");
                        return;
                    }
                };
                let Some(Runner {
                    id: Some(rid),
                    data: Some(rdata),
                }) = runner
                else {
                    println!("runner {} not found", RunnerType::Workflow.as_str_name());
                    return;
                };

                // Build CreateWorkflowArgs JSON, then encode against the
                // dynamically fetched `create` method args descriptor.
                let worker_options = build_worker_options_json(
                    channel.as_deref(),
                    queue_type.as_ref(),
                    response_type.as_ref(),
                    *store_success,
                    *store_failure,
                    *use_static,
                );
                let job_args = match build_create_workflow_args_json(
                    name,
                    workflow_file.as_deref(),
                    workflow_data.as_deref(),
                    context.as_deref(),
                    worker_options,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("Invalid workflow arguments: {e:#}");
                        return;
                    }
                };
                let Some(args_descriptor) =
                    JobworkerpProto::parse_job_args_schema_descriptor(&rdata, using)
                        .unwrap_or_else(|e| {
                            eprintln!("Failed to parse job_args schema descriptor: {e:#?}");
                            None
                        })
                else {
                    println!("args_descriptor not found");
                    return;
                };
                let args = match JobworkerpProto::json_value_to_message(
                    args_descriptor,
                    &job_args,
                    true,
                    true,
                ) {
                    Ok(a) => a,
                    Err(e) => {
                        eprintln!("Failed to encode CreateWorkflowArgs: {e:#?}");
                        return;
                    }
                };
                let result_desc = JobworkerpProto::parse_result_schema_descriptor(&rdata, using)
                    .unwrap_or_else(|e| {
                        eprintln!("Failed to parse result schema descriptor: {e:#?}");
                        None
                    });

                // `create` is non-streaming: enqueue via an ephemeral worker and
                // collect a single output, then clean the temp worker up.
                let wname =
                    crate::client::helper::ephemeral_worker_name("JobworkerpClientWorkflowCreate");
                let worker_data = WorkerData {
                    name: wname.clone(),
                    runner_id: Some(rid),
                    runner_settings: vec![],
                    retry_policy: None,
                    channel: None,
                    queue_type: QueueType::Normal as i32,
                    response_type: ResponseType::Direct as i32,
                    store_success: false,
                    store_failure: false,
                    broadcast_results: false,
                    use_static: false,
                    ..Default::default()
                };
                let output = helper
                    .enqueue_and_get_output_worker_job(
                        cx.as_ref(),
                        metadata.clone(),
                        &worker_data,
                        args,
                        3600,
                        None,
                        None,
                        using,
                    )
                    .await;
                let _ = helper
                    .delete_worker_by_name(cx.as_ref(), metadata, wname.as_str())
                    .await;

                let output = match output {
                    Ok(o) => o,
                    Err(e) => {
                        eprintln!("Workflow worker creation failed: {e:#}");
                        return;
                    }
                };

                // Decode the CreateWorkflowResult and surface the created worker id.
                let result_json = result_desc.as_ref().and_then(|desc| {
                    ProtobufDescriptor::get_message_from_bytes(desc.clone(), &output)
                        .and_then(|m| ProtobufDescriptor::message_to_json_value(&m))
                        .ok()
                });
                let worker_id = result_json
                    .as_ref()
                    .and_then(extract_worker_id_from_create_result);

                let display_options = crate::display::DisplayOptions {
                    format: *format,
                    color_enabled: true,
                    max_field_length: None,
                    use_unicode: true,
                    no_truncate: *no_truncate,
                };
                match worker_id {
                    Some(id) => {
                        let row = serde_json::json!({ "worker_id": id, "name": name });
                        println!(
                            "{}",
                            crate::display::visualize_rows(&[row], &display_options)
                        );
                    }
                    None => {
                        eprintln!(
                            "Worker created but worker id could not be parsed from result: {:?}",
                            result_json
                        );
                    }
                }
            }

            Self::Find { id } => {
                let id = JobId { value: *id };
                let response = client.job_client().await.find(id).await.unwrap();
                let job = response.into_inner();
                if let Some(job) = job.data {
                    print_job_with_request(client, job).await.unwrap();
                } else {
                    println!("job not found");
                }
            }
            Self::List {
                offset,
                limit,
                format,
                no_truncate,
            } => {
                let request = FindListRequest {
                    offset: *offset,
                    limit: (*limit).map(|x| x as i32),
                };
                let response = client.job_client().await.find_list(request).await.unwrap();
                let mut inner = response.into_inner();

                // Collect all jobs into a vector for batch processing
                let mut jobs = Vec::new();
                loop {
                    match inner.message().await {
                        Ok(Some(job_data)) => {
                            // Get args descriptor for proper display
                            let args_descriptor = if let Some(data) = job_data.data.as_ref() {
                                if let Some(worker_id) = data.worker_id.as_ref() {
                                    let using_ref = data.using.as_deref();
                                    JobworkerpProto::find_runner_descriptors_by_worker(
                                        client,
                                        job_request::Worker::WorkerId(*worker_id),
                                        using_ref,
                                    )
                                    .await
                                    .ok()
                                    .and_then(|(_, args_desc, _)| args_desc)
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            let job_json = job_to_json(&job_data, None, args_descriptor, format);
                            jobs.push(job_json);
                        }
                        Ok(None) => {
                            // Stream ended
                            break;
                        }
                        Err(e) => {
                            eprintln!("Error reading from stream: {e}");
                            break;
                        }
                    }
                }

                // Display using the appropriate visualizer
                let options = DisplayOptions::new(*format)
                    .with_color(supports_color())
                    .with_no_truncate(*no_truncate);

                let output = match format {
                    crate::display::DisplayFormat::Table => {
                        let visualizer = TableVisualizer;
                        visualizer.visualize(&jobs, &options)
                    }
                    crate::display::DisplayFormat::Card => {
                        let visualizer = CardVisualizer;
                        visualizer.visualize(&jobs, &options)
                    }
                    crate::display::DisplayFormat::Json => {
                        let visualizer = JsonPrettyVisualizer;
                        visualizer.visualize(&jobs, &options)
                    }
                };

                println!("{output}");
            }
            Self::Delete { id } => {
                let id = JobId { value: *id };
                let response = client.job_client().await.delete(id).await.unwrap();
                println!("{response:#?}");
            }
            Self::Count {} => {
                let response = client
                    .job_client()
                    .await
                    .count(CountCondition {})
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
            Self::ListWithProcessingStatus {
                status,
                limit,
                format,
                no_truncate,
            } => {
                let request = FindListWithProcessingStatusRequest {
                    status: status.to_grpc() as i32,
                    limit: *limit,
                };
                let response = client
                    .job_client()
                    .await
                    .find_list_with_processing_status(request)
                    .await
                    .unwrap();
                let mut inner = response.into_inner();

                // Collect all jobs with processing status
                let mut jobs = Vec::new();
                loop {
                    match inner.message().await {
                        Ok(Some(job_and_status)) => {
                            if let Some(job) = job_and_status.job {
                                let processing_status = job_and_status
                                    .status
                                    .and_then(|s| JobProcessingStatus::try_from(s).ok());

                                // Get args descriptor for proper display
                                let args_descriptor = if let Some(data) = job.data.as_ref() {
                                    if let Some(worker_id) = data.worker_id.as_ref() {
                                        let using_ref = data.using.as_deref();
                                        JobworkerpProto::find_runner_descriptors_by_worker(
                                            client,
                                            job_request::Worker::WorkerId(*worker_id),
                                            using_ref,
                                        )
                                        .await
                                        .ok()
                                        .and_then(|(_, args_desc, _)| args_desc)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };

                                let job_json =
                                    job_to_json(&job, processing_status, args_descriptor, format);
                                jobs.push(job_json);
                            }
                        }
                        Ok(None) => {
                            // Stream ended
                            break;
                        }
                        Err(e) => {
                            eprintln!("Error reading from stream: {e}");
                            break;
                        }
                    }
                }

                // Display using the appropriate visualizer
                let options = DisplayOptions::new(*format)
                    .with_color(supports_color())
                    .with_no_truncate(*no_truncate);

                let output = match format {
                    crate::display::DisplayFormat::Table => {
                        let visualizer = TableVisualizer;
                        visualizer.visualize(&jobs, &options)
                    }
                    crate::display::DisplayFormat::Card => {
                        let visualizer = CardVisualizer;
                        visualizer.visualize(&jobs, &options)
                    }
                    crate::display::DisplayFormat::Json => {
                        let visualizer = JsonPrettyVisualizer;
                        visualizer.visualize(&jobs, &options)
                    }
                };

                println!("{output}");
            }
        }
        async fn print_job_with_request(
            client: &crate::client::JobworkerpClient,
            job: jobworkerp::data::Job,
        ) -> Result<()> {
            if let jobworkerp::data::Job {
                id: Some(jid),
                data: Some(jdat),
                metadata: _,
            } = job
            {
                println!("[job]:\n\t[id] {}", jid.value);
                if let Some(wid) = jdat.worker_id {
                    println!("\t[worker_id] {}", wid.value);
                    let using_ref = jdat.using.as_deref();
                    match JobworkerpProto::find_runner_descriptors_by_worker(
                        client,
                        job_request::Worker::WorkerId(wid),
                        using_ref,
                    )
                    .await
                    {
                        Ok((_, args_proto, _)) => {
                            if let Some(args_proto) = args_proto {
                                let args = ProtobufDescriptor::get_message_from_bytes(
                                    args_proto, &jdat.args,
                                )?;
                                println!("\t[args] ");
                                ProtobufDescriptor::print_dynamic_message(&args, false);
                            } else {
                                println!(
                                    "\t[args] {:?}",
                                    String::from_utf8_lossy(jdat.args.as_slice())
                                );
                            }
                        }
                        Err(e) => {
                            println!("\t[args (ERROR)]  {e:?}");
                        }
                    }
                    println!("\t[uniq_key] {:?}", jdat.uniq_key);
                    println!(
                        "\t[run_after_time] {}",
                        DateTime::from_timestamp_millis(jdat.run_after_time).unwrap_or_default()
                    );
                    println!("\t[priority] {:?}", jdat.priority().as_str_name());
                    println!("\t[timeout] {} msec", jdat.timeout);
                } else {
                    println!("\t[worker_id] not found");
                }
            } else {
                println!("[job]:\n\t[id] not found");
            }
            Ok(())
        }
    }
}
struct JobCommandHelper {
    client: crate::client::JobworkerpClient,
}
impl JobCommandHelper {
    pub const fn new(client: crate::client::JobworkerpClient) -> Self {
        Self { client }
    }
}

impl UseJobworkerpClient for JobCommandHelper {
    fn jobworkerp_client(&self) -> &crate::client::JobworkerpClient {
        &self.client
    }
}
impl UseJobworkerpClientHelper for JobCommandHelper {}
impl Tracing for JobCommandHelper {}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serde_json::json;

    #[derive(Parser, Debug)]
    struct TestRoot {
        #[clap(subcommand)]
        cmd: JobCommand,
    }

    // --- clap parse tests: EnqueueWorkflow workflow_source exclusivity ---

    #[test]
    fn enqueue_workflow_accepts_workflow_file() {
        let parsed = TestRoot::parse_from([
            "test",
            "enqueue-workflow",
            "--workflow-file",
            "wf.yaml",
            "--input",
            "{}",
        ]);
        match parsed.cmd {
            JobCommand::EnqueueWorkflow {
                workflow_file,
                workflow_data,
                ..
            } => {
                assert_eq!(workflow_file.as_deref(), Some("wf.yaml"));
                assert!(workflow_data.is_none());
            }
            other => panic!("expected EnqueueWorkflow, got {other:?}"),
        }
    }

    #[test]
    fn enqueue_workflow_accepts_workflow_data_and_execution_id() {
        let parsed = TestRoot::parse_from([
            "test",
            "enqueue-workflow",
            "--workflow-data",
            "do: []",
            "--input",
            "{}",
            "--execution-id",
            "exec-1",
        ]);
        match parsed.cmd {
            JobCommand::EnqueueWorkflow {
                workflow_file,
                workflow_data,
                execution_id,
                ..
            } => {
                assert!(workflow_file.is_none());
                assert_eq!(workflow_data.as_deref(), Some("do: []"));
                assert_eq!(execution_id.as_deref(), Some("exec-1"));
            }
            other => panic!("expected EnqueueWorkflow, got {other:?}"),
        }
    }

    #[test]
    fn enqueue_workflow_rejects_both_sources() {
        let res = TestRoot::try_parse_from([
            "test",
            "enqueue-workflow",
            "--workflow-file",
            "wf.yaml",
            "--workflow-data",
            "do: []",
            "--input",
            "{}",
        ]);
        assert!(res.is_err(), "both sources must be rejected");
    }

    #[test]
    fn enqueue_workflow_rejects_missing_source() {
        let res = TestRoot::try_parse_from(["test", "enqueue-workflow", "--input", "{}"]);
        assert!(res.is_err(), "missing source must be rejected");
    }

    // --- clap parse tests: CreateWorkflow ---

    #[test]
    fn create_workflow_parses_with_defaults() {
        let parsed = TestRoot::parse_from([
            "test",
            "create-workflow",
            "--name",
            "my-wf",
            "--workflow-file",
            "wf.yaml",
        ]);
        match parsed.cmd {
            JobCommand::CreateWorkflow {
                name,
                workflow_file,
                queue_type,
                response_type,
                ..
            } => {
                assert_eq!(name, "my-wf");
                assert_eq!(workflow_file.as_deref(), Some("wf.yaml"));
                assert!(queue_type.is_none());
                assert!(response_type.is_none());
            }
            other => panic!("expected CreateWorkflow, got {other:?}"),
        }
    }

    #[test]
    fn create_workflow_rejects_invalid_response_type() {
        let res = TestRoot::try_parse_from([
            "test",
            "create-workflow",
            "--name",
            "my-wf",
            "--workflow-file",
            "wf.yaml",
            "--response-type",
            "BOGUS",
        ]);
        assert!(res.is_err(), "invalid response type must be rejected");
    }

    // --- pure function tests: build_workflow_run_args_json ---

    #[test]
    fn run_args_uses_workflow_url_for_file() {
        let v = build_workflow_run_args_json(Some("wf.yaml"), None, "{}", None, None).unwrap();
        assert_eq!(v["workflow_url"], json!("wf.yaml"));
        assert!(v.get("workflow_data").is_none());
        assert_eq!(v["input"], json!("{}"));
    }

    #[test]
    fn run_args_uses_workflow_data_for_inline() {
        let v = build_workflow_run_args_json(None, Some("do: []"), "{}", None, None).unwrap();
        assert_eq!(v["workflow_data"], json!("do: []"));
        assert!(v.get("workflow_url").is_none());
    }

    #[test]
    fn run_args_omits_empty_context_and_execution_id() {
        let v = build_workflow_run_args_json(Some("wf"), None, "{}", Some(""), Some("")).unwrap();
        assert!(v.get("workflow_context").is_none());
        assert!(v.get("execution_id").is_none());
    }

    #[test]
    fn run_args_includes_context_and_execution_id() {
        let v =
            build_workflow_run_args_json(Some("wf"), None, "{}", Some("ctx"), Some("e1")).unwrap();
        assert_eq!(v["workflow_context"], json!("ctx"));
        assert_eq!(v["execution_id"], json!("e1"));
    }

    #[test]
    fn run_args_rejects_both_or_neither_sources() {
        assert!(build_workflow_run_args_json(Some("a"), Some("b"), "{}", None, None).is_err());
        assert!(build_workflow_run_args_json(None, None, "{}", None, None).is_err());
    }

    // --- pure function tests: build_worker_options_json ---

    #[test]
    fn worker_options_empty_when_all_none() {
        let v = build_worker_options_json(None, None, None, None, None, None);
        assert!(v.as_object().unwrap().is_empty());
    }

    #[test]
    fn worker_options_includes_only_set_keys_with_enum_names() {
        use crate::command::worker::{QueueTypeArg, ResponseTypeArg};
        let v = build_worker_options_json(
            Some("ch1"),
            Some(&QueueTypeArg::DbOnly),
            Some(&ResponseTypeArg::Direct),
            Some(true),
            None,
            None,
        );
        assert_eq!(v["channel"], json!("ch1"));
        // DbOnly maps to the server's FORCED_RDB enum name.
        assert_eq!(v["queue_type"], json!("FORCED_RDB"));
        assert_eq!(v["response_type"], json!("DIRECT"));
        assert_eq!(v["store_success"], json!(true));
        assert!(v.get("store_failure").is_none());
        assert!(v.get("use_static").is_none());
    }

    // --- pure function tests: build_create_workflow_args_json ---

    #[test]
    fn create_args_shapes_name_and_source_and_options() {
        let opts = build_worker_options_json(Some("ch"), None, None, None, None, None);
        let v = build_create_workflow_args_json("wname", Some("wf.yaml"), None, Some("ctx"), opts)
            .unwrap();
        assert_eq!(v["name"], json!("wname"));
        assert_eq!(v["workflow_url"], json!("wf.yaml"));
        assert_eq!(v["workflow_context"], json!("ctx"));
        assert_eq!(v["worker_options"]["channel"], json!("ch"));
    }

    #[test]
    fn create_args_omits_worker_options_when_empty() {
        let opts = build_worker_options_json(None, None, None, None, None, None);
        let v = build_create_workflow_args_json("w", None, Some("do: []"), None, opts).unwrap();
        assert!(v.get("worker_options").is_none());
        assert_eq!(v["workflow_data"], json!("do: []"));
    }

    // --- pure function tests: extract_worker_id_from_create_result ---

    #[test]
    fn extract_worker_id_wrapper_form() {
        let v = json!({ "worker_id": { "value": 42 } });
        assert_eq!(extract_worker_id_from_create_result(&v), Some(42));
    }

    #[test]
    fn extract_worker_id_camel_and_id_fallbacks() {
        assert_eq!(
            extract_worker_id_from_create_result(&json!({ "workerId": { "value": 7 } })),
            Some(7)
        );
        assert_eq!(
            extract_worker_id_from_create_result(&json!({ "id": { "value": 9 } })),
            Some(9)
        );
    }

    #[test]
    fn extract_worker_id_scalar_form() {
        assert_eq!(
            extract_worker_id_from_create_result(&json!({ "worker_id": 99 })),
            Some(99)
        );
    }

    #[test]
    fn extract_worker_id_missing_returns_none() {
        assert_eq!(
            extract_worker_id_from_create_result(&json!({ "other": 1 })),
            None
        );
    }
}
