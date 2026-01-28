// as job: valid commands are enqueue, find, list, delete, count
// -i, --id <id> id of the job (for find, delete)
// -w, --worker <id or name> worker id or name of the job (if string, treat as name, if number, treat as id)(for enqueue)
// --args <args json string> arguments of the worker runner (json string (transform to grpc message internally))
// -u, --unique-key <string> unique key of the job (for enqueue)
// -r, --run-after-time <number> execute unix time (milli-seconds) of the job (for enqueue)
// -p, --priority <priority> priority of the job (HIGH, MIDDLE, LOW)(for enqueue)
// -t, --timeout <timeout> timeout of the job (milli-seconds) (for enqueue)

use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hasher},
    str::FromStr,
    sync::Arc,
};

use super::WorkerIdOrName;
use crate::{
    client::{helper::UseJobworkerpClientHelper, UseJobworkerpClient},
    command::{job_result::JobResultCommand, to_request},
    display::{
        utils::supports_color, CardVisualizer, DisplayOptions, JsonPrettyVisualizer,
        JsonVisualizer, TableVisualizer,
    },
    jobworkerp::{
        self,
        data::{
            JobId, JobProcessingStatus, Priority, QueueType, ResponseType, Runner, RunnerType,
            WorkerData,
        },
        service::{
            job_request, CountCondition, FindListRequest, FindListWithProcessingStatusRequest,
            JobRequest,
        },
    },
    proto::JobworkerpProto,
};
use anyhow::Result;
use chrono::DateTime;
use clap::{Parser, ValueEnum};
use command_utils::trace::Tracing;
use command_utils::{protobuf::ProtobufDescriptor, util::datetime};
use opentelemetry::{global, trace::Span, Context};
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
        workflow_file: String,
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
    pub fn to_grpc(&self) -> Priority {
        match self {
            PriorityArg::High => Priority::High,
            PriorityArg::Middle => Priority::Medium,
            PriorityArg::Low => Priority::Low,
        }
    }
}

impl JobProcessingStatusArg {
    pub fn to_grpc(&self) -> JobProcessingStatus {
        match self {
            JobProcessingStatusArg::Unknown => JobProcessingStatus::Unknown,
            JobProcessingStatusArg::Pending => JobProcessingStatus::Pending,
            JobProcessingStatusArg::Running => JobProcessingStatus::Running,
            JobProcessingStatusArg::WaitResult => JobProcessingStatus::WaitResult,
            JobProcessingStatusArg::Cancelling => JobProcessingStatus::Cancelling,
        }
    }
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
            JobCommand::Enqueue {
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
                        JobworkerpProto::json_to_message(args_d, args.as_str()).unwrap()
                    } else {
                        args.as_bytes().to_vec()
                    },
                    uniq_key: unique_key.clone(),
                    run_after_time: *run_after_time,
                    priority: priority.clone().map(|p| p.to_grpc() as i32),
                    timeout: timeout.and_then(|t| if t > 0 { Some(t) } else { None }),
                    using: using.clone(),
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
                            && !output.items.is_empty() {
                                JobResultCommand::print_job_result_output(
                                    &output.items,
                                    result_desc,
                                );
                            }
                    }
                } else {
                    println!("{response:#?}");
                }
            }
            JobCommand::EnqueueForStream {
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
                        JobworkerpProto::json_to_message(args_d, args.as_str()).unwrap()
                    } else {
                        args.as_bytes().to_vec()
                    },
                    uniq_key: unique_key.clone(),
                    run_after_time: *run_after_time,
                    priority: priority.clone().map(|p| p.to_grpc() as i32),
                    timeout: timeout.and_then(|t| if t > 0 { Some(t) } else { None }),
                    using: using.clone(),
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
                    format: format.clone(),
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
                        Some(jobworkerp::data::result_output_item::Item::FinalCollected(v)) => {
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
                        None => {
                            // Skip empty items
                        }
                    }
                }

                // End streaming session
                JobResultCommand::end_streaming_session(item_count, format, &display_options);
                // if let Some(result) = response.result {
                //     JobResultCommand::print_job_result(&result, result_desc);
                // } else {
                //     println!("{:#?}", response);
                // }
            }
            JobCommand::EnqueueWorkflow {
                channel,
                context,
                input,
                priority,
                run_after_time,
                timeout,
                workflow_file,
                format,
                no_truncate,
            } => {
                let helper = JobCommandHelper::new(client.clone());
                let (_span, cx) =
                    self.create_parent_span("jobworkerp-client", "JobCommand::EnqueueWorkflow");
                let cx = Some(cx);

                let runner = helper
                    .find_runner_by_name(
                        cx.as_ref(),
                        metadata.clone(),
                        RunnerType::InlineWorkflow.as_str_name(),
                    )
                    .await
                    .unwrap();
                if let Some(Runner {
                    id: Some(rid),
                    data: Some(rdata),
                }) = runner
                {
                    let mut hasher = DefaultHasher::default();
                    hasher.write_i64(datetime::now_millis());
                    hasher.write_i64(rand::random()); // random
                    let wname = format!("{}_{:x}", "JobworkerpCilentWorkflow", hasher.finish());

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
                    let args = match JobworkerpProto::parse_job_args_schema_descriptor(&rdata, None)
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to parse job_args schema descriptor: {e:#?}"
                                )
                            })
                            .unwrap()
                    { Some(args_descriptor) => {
                        let context = context.as_deref().unwrap_or("");
                        let job_args = serde_json::json!({
                            "workflow_url": serde_json::Value::String(workflow_file.clone()),
                            "input": serde_json::Value::String(input.clone()),
                            // serde_json::from_str::<serde_json::Value>(input.as_str())
                            //     .unwrap_or_else(|_| serde_json::Value::String(input.clone())),
                            "workflow_context": context,
                        });
                        JobworkerpProto::json_value_to_message(args_descriptor, &job_args, true)
                            .map_err(|e| {
                                println!("Failed to parse job_args schema: {:#?}", &e);
                                anyhow::anyhow!("Failed to parse job_args schema: {e:#?}")
                            })
                            .unwrap()
                    } _ => {
                        println!("args_descriptor not found");
                        return;
                    }};
                    let result_desc = JobworkerpProto::parse_result_schema_descriptor(&rdata, None)
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to parse job_result schema descriptor: {e:#?}")
                        })
                        .unwrap();
                    let (meta, mut response) = helper
                        .enqueue_stream_worker_job(
                            cx.as_ref(),
                            metadata.clone(),
                            &worker_data,
                            args,
                            timeout.map(|t| (t / 1000) as u32).unwrap_or(3600),
                            *run_after_time,
                            priority.clone().map(|p| p.to_grpc()),
                            None, // using is None for workflow runners
                        )
                        .await
                        .inspect_err(|e| {
                            println!("enqueue_stream_worker_job error: {e:#?}");
                        })
                        .unwrap();
                    let _ = helper
                        .delete_worker_by_name(cx.as_ref(), metadata, wname.as_str())
                        .await;
                    // Check for job result header in initial response metadata
                    if let Some(id_bin) = meta.get_bin(JOB_ID_HEADER_NAME) {
                        match JobId::decode(id_bin.to_bytes().unwrap().as_ref()) {
                            Ok(job_id) => println!("Job ID header found: {job_id:?}"),
                            Err(e) => println!("Failed to decode job ID: {e}"),
                        }
                    }
                    // Create display options for workflow
                    let display_options = crate::display::DisplayOptions {
                        format: format.clone(),
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
                            Some(jobworkerp::data::result_output_item::Item::FinalCollected(v)) => {
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
                            None => {
                                // Skip empty items
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
                    println!(
                        "runner {} not found",
                        RunnerType::InlineWorkflow.as_str_name()
                    );
                    return;
                }
            }

            JobCommand::Find { id } => {
                let id = JobId { value: *id };
                let response = client.job_client().await.find(id).await.unwrap();
                let job = response.into_inner();
                if let Some(job) = job.data {
                    print_job_with_request(client, job).await.unwrap();
                } else {
                    println!("job not found");
                }
            }
            JobCommand::List {
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
                let options = DisplayOptions::new(format.clone())
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
            JobCommand::Delete { id } => {
                let id = JobId { value: *id };
                let response = client.job_client().await.delete(id).await.unwrap();
                println!("{response:#?}");
            }
            JobCommand::Count {} => {
                let response = client
                    .job_client()
                    .await
                    .count(CountCondition {})
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
            JobCommand::ListWithProcessingStatus {
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
                let options = DisplayOptions::new(format.clone())
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
                println!("[job]:\n\t[id] {}", &jid.value);
                if let Some(wid) = jdat.worker_id {
                    println!("\t[worker_id] {}", &wid.value);
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
                    println!("\t[uniq_key] {:?}", &jdat.uniq_key);
                    println!(
                        "\t[run_after_time] {}",
                        DateTime::from_timestamp_millis(jdat.run_after_time).unwrap_or_default()
                    );
                    println!("\t[priority] {:?}", &jdat.priority().as_str_name());
                    println!("\t[timeout] {} msec", &jdat.timeout);
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
    pub fn new(client: crate::client::JobworkerpClient) -> Self {
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
