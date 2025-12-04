use std::collections::HashMap;
use std::str::FromStr;

use super::{to_request, WorkerIdOrName};
use crate::jobworkerp;
use crate::jobworkerp::data::{JobId, JobResult, JobResultId};
use crate::jobworkerp::service::{
    CountCondition, FindListRequest, ListenByWorkerRequest, ListenRequest,
};
use crate::proto::JobworkerpProto;
use clap::Parser;
use command_utils::protobuf::ProtobufDescriptor;
use prost::Message;
use prost_reflect::MessageDescriptor;
use tonic::metadata::KeyAndValueRef;

pub mod display;

#[derive(Parser, Debug)]
pub struct JobResultArg {
    #[clap(subcommand)]
    pub cmd: JobResultCommand,
}

#[derive(Parser, Debug)]
pub enum JobResultCommand {
    Find {
        #[clap(short, long)]
        id: i64,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    Listen {
        #[clap(short, long)]
        job_id: i64,
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
        #[clap(short, long)]
        timeout: Option<u64>,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    ListenStream {
        #[clap(short, long)]
        job_id: i64,
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
        #[clap(short, long)]
        timeout: Option<u64>,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    ListenByWorker {
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    List {
        #[clap(short, long)]
        offset: Option<i64>,
        #[clap(short, long)]
        limit: Option<i32>,
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    ListByJobId {
        #[clap(short, long)]
        job_id: i64,
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
}

impl JobResultCommand {
    const RESULT_HEADER_NAME: &str = "x-job-result-bin";
    pub async fn execute(
        &self,
        client: &crate::client::JobworkerpClient,
        metadata: &HashMap<String, String>,
    ) {
        match self {
            JobResultCommand::Find {
                id,
                format,
                no_truncate,
            } => {
                let id = JobResultId { value: *id };
                let response = client
                    .job_result_client()
                    .await
                    .find(to_request(metadata, id).unwrap())
                    .await
                    .unwrap();
                match response.into_inner().data {
                    Some(job_res) => {
                        Self::print_job_result_formatted(client, job_res, format, *no_truncate)
                            .await;
                    }
                    None => {
                        println!("job result not found: id = {}", id.value);
                    }
                }
            }
            JobResultCommand::Listen {
                job_id,
                worker,
                timeout,
                format,
                no_truncate,
            } => {
                let req = ListenRequest {
                    job_id: Some(JobId { value: *job_id }),
                    worker: Some(worker.to_listen_worker()),
                    timeout: *timeout,
                };
                let response = client
                    .job_result_client()
                    .await
                    .listen(to_request(metadata, req).unwrap())
                    .await
                    .unwrap()
                    .into_inner();
                Self::print_job_result_formatted(client, response, format, *no_truncate).await;
            }
            JobResultCommand::ListenStream {
                job_id,
                worker,
                timeout,
                format,
                no_truncate,
            } => {
                let req = worker.to_job_worker();
                let (_, _args_desc, result_desc) =
                    JobworkerpProto::find_runner_descriptors_by_worker(client, req, None)
                        .await
                        .unwrap();

                let req = ListenRequest {
                    job_id: Some(JobId { value: *job_id }),
                    worker: Some(worker.to_listen_worker()),
                    timeout: *timeout,
                };
                let response = client
                    .job_result_client()
                    .await
                    .listen_stream(to_request(metadata, req).unwrap())
                    .await
                    .unwrap();

                let meta = response.metadata().clone();
                let mut response = response.into_inner();

                // Create display options
                let display_options = crate::display::DisplayOptions {
                    format: format.clone(),
                    color_enabled: true,
                    max_field_length: None,
                    use_unicode: true,
                    no_truncate: *no_truncate,
                };

                // result meta header
                JobResultCommand::print_job_result_metadata(&meta, result_desc.clone());

                // Start streaming session
                JobResultCommand::start_streaming_session("job result", format, &display_options);

                // print streaming response with improved formatting
                let mut item_count = 0;
                while let Some(item) = response.message().await.unwrap() {
                    if let Some(jobworkerp::data::result_output_item::Item::Data(v)) = item.item {
                        JobResultCommand::print_streaming_output(
                            v.as_slice(),
                            result_desc.clone(),
                            format,
                            &display_options,
                            item_count,
                        );
                        item_count += 1;
                    }
                }

                // End streaming session
                JobResultCommand::end_streaming_session(item_count, format, &display_options);
                // Self::print_job_result_with_request(client, response).await;
            }
            JobResultCommand::ListenByWorker {
                worker,
                format,
                no_truncate,
            } => {
                let req = ListenByWorkerRequest {
                    worker: Some(worker.to_listen_stream_worker()),
                };
                let mut response = client
                    .job_result_client()
                    .await
                    .listen_by_worker(to_request(metadata, req).unwrap())
                    .await
                    .unwrap()
                    .into_inner();
                println!("listening... (Ctrl+C to stop)");
                while let Some(res) = response.message().await.unwrap() {
                    Self::print_job_result_formatted(client, res, format, *no_truncate).await;
                }
            }
            JobResultCommand::List {
                offset,
                limit,
                format,
                no_truncate,
            } => {
                use self::display::job_result_to_json;
                use crate::display::{
                    utils::supports_color, CardVisualizer, DisplayOptions, JsonPrettyVisualizer,
                    JsonVisualizer, TableVisualizer,
                };

                let request = FindListRequest {
                    offset: *offset,
                    limit: *limit,
                };
                let response = client
                    .job_result_client()
                    .await
                    .find_list(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();
                let mut response = response.into_inner();

                // Collect all job results into a vector for batch processing
                let mut job_results_json = Vec::new();
                while let Some(job_res) = response.message().await.unwrap() {
                    let worker_name = job_res
                        .data
                        .as_ref()
                        .map(|d| d.worker_name.as_str())
                        .unwrap_or("");
                    let result_proto =
                        JobworkerpProto::resolve_result_descriptor(client, worker_name, None).await;
                    let job_result_json = job_result_to_json(&job_res, result_proto, format);
                    job_results_json.push(job_result_json);
                }

                // Display using the appropriate visualizer
                let options = DisplayOptions::new(format.clone())
                    .with_color(supports_color())
                    .with_no_truncate(*no_truncate);

                let output = match format {
                    crate::display::DisplayFormat::Table => {
                        let visualizer = TableVisualizer;
                        visualizer.visualize(&job_results_json, &options)
                    }
                    crate::display::DisplayFormat::Card => {
                        let visualizer = CardVisualizer;
                        visualizer.visualize(&job_results_json, &options)
                    }
                    crate::display::DisplayFormat::Json => {
                        let visualizer = JsonPrettyVisualizer;
                        visualizer.visualize(&job_results_json, &options)
                    }
                };

                println!("{output}");
            }
            JobResultCommand::ListByJobId {
                job_id,
                format,
                no_truncate,
            } => {
                use self::display::job_result_to_json;
                use crate::display::{
                    utils::supports_color, CardVisualizer, DisplayOptions, JsonPrettyVisualizer,
                    JsonVisualizer, TableVisualizer,
                };

                let request = jobworkerp::service::FindListByJobIdRequest {
                    job_id: Some(JobId { value: *job_id }),
                };
                let mut response = client
                    .job_result_client()
                    .await
                    .find_list_by_job_id(to_request(metadata, request).unwrap())
                    .await
                    .unwrap()
                    .into_inner();

                // Collect all job results into a vector for batch processing
                let mut job_results_json = Vec::new();
                while let Some(job_res) = response.message().await.unwrap() {
                    let worker_name = job_res
                        .data
                        .as_ref()
                        .map(|d| d.worker_name.as_str())
                        .unwrap_or("");
                    let result_proto =
                        JobworkerpProto::resolve_result_descriptor(client, worker_name, None).await;
                    let job_result_json = job_result_to_json(&job_res, result_proto, format);
                    job_results_json.push(job_result_json);
                }

                // Display using the appropriate visualizer
                let options = DisplayOptions::new(format.clone())
                    .with_color(supports_color())
                    .with_no_truncate(*no_truncate);

                let output = match format {
                    crate::display::DisplayFormat::Table => {
                        let visualizer = TableVisualizer;
                        visualizer.visualize(&job_results_json, &options)
                    }
                    crate::display::DisplayFormat::Card => {
                        let visualizer = CardVisualizer;
                        visualizer.visualize(&job_results_json, &options)
                    }
                    crate::display::DisplayFormat::Json => {
                        let visualizer = JsonPrettyVisualizer;
                        visualizer.visualize(&job_results_json, &options)
                    }
                };

                println!("{output}");
            }
            JobResultCommand::Delete { id } => {
                let id = JobResultId { value: *id };
                let response = client
                    .job_result_client()
                    .await
                    .delete(to_request(metadata, id).unwrap())
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
            JobResultCommand::Count {} => {
                let response = client
                    .job_result_client()
                    .await
                    .count(to_request(metadata, CountCondition {}).unwrap())
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
        }
    }
    /// Print job result with new format system
    async fn print_job_result_formatted(
        client: &crate::client::JobworkerpClient,
        job_result: jobworkerp::data::JobResult,
        format: &crate::display::DisplayFormat,
        no_truncate: bool,
    ) {
        use self::display::job_result_to_json;
        use crate::display::{
            utils::supports_color, CardVisualizer, DisplayOptions, JsonPrettyVisualizer,
            JsonVisualizer, TableVisualizer,
        };

        let worker_name = job_result
            .data
            .as_ref()
            .map(|d| d.worker_name.as_str())
            .unwrap_or("");
        let result_proto = JobworkerpProto::resolve_result_descriptor(client, worker_name, None).await;

        // Convert to JSON with proper formatting
        let job_result_json = job_result_to_json(&job_result, result_proto, format);
        let job_results = vec![job_result_json];

        // Display using the appropriate visualizer
        let options = DisplayOptions::new(format.clone())
            .with_color(supports_color())
            .with_no_truncate(no_truncate);

        let output = match format {
            crate::display::DisplayFormat::Table => {
                let visualizer = TableVisualizer;
                visualizer.visualize(&job_results, &options)
            }
            crate::display::DisplayFormat::Card => {
                let visualizer = CardVisualizer;
                visualizer.visualize(&job_results, &options)
            }
            crate::display::DisplayFormat::Json => {
                let visualizer = JsonPrettyVisualizer;
                visualizer.visualize(&job_results, &options)
            }
        };

        println!("{output}");
    }

    pub fn print_job_result_output(data: &[u8], result_proto: Option<MessageDescriptor>) {
        if let Some(proto) = result_proto {
            match ProtobufDescriptor::get_message_from_bytes(proto, data) {
                Ok(mes) => {
                    ProtobufDescriptor::print_dynamic_message(&mes, false);
                }
                Err(e) => {
                    println!("error: {e:#?}");
                }
            }
        } else {
            println!("\t[output]: |\n {}", String::from_utf8_lossy(data));
        }
    }
    pub fn print_job_result_metadata(
        metadata: &tonic::metadata::MetadataMap,
        _result_proto: Option<MessageDescriptor>,
    ) {
        println!("[metadata]:");
        for kv in metadata.iter() {
            match kv {
                KeyAndValueRef::Ascii(_key, _value) => {
                    // println!("Ascii: {:?}: {:?}", key, value)
                }
                KeyAndValueRef::Binary(ref key, ref value) => {
                    if key.as_str() == Self::RESULT_HEADER_NAME {
                        match value.to_bytes() {
                            Ok(bytes) => match JobResult::decode(bytes.as_ref()) {
                                Ok(res) => {
                                    // Use the legacy output for metadata context
                                    println!("[job_result] (from metadata header):");
                                    if let Some(rid) = &res.id {
                                        println!("\t[id] {}", rid.value);
                                    }
                                    if let Some(data) = &res.data {
                                        println!("\t[worker]: {}", data.worker_name);
                                        println!(
                                            "\t[job id]: {}",
                                            data.job_id.map(|j| j.value).unwrap_or_default()
                                        );
                                        println!("\t[status]: {}", data.status().as_str_name());
                                    }
                                }
                                Err(e) => {
                                    println!("Failed to decode JobResult from header: {e:#?}");
                                    println!("Raw bytes length: {}", bytes.len());
                                    println!("Raw bytes (hex): {:02x?}", bytes.as_ref());
                                }
                            },
                            Err(e) => {
                                println!("Failed to convert header value to bytes: {e:#?}");
                            }
                        }
                    } else {
                        println!("\t{key}: {value:?}");
                    }
                }
            }
        }
    }

    /// Print streaming output with improved formatting based on display options
    pub fn print_streaming_output(
        data: &[u8],
        result_descriptor: Option<MessageDescriptor>,
        format: &crate::display::DisplayFormat,
        options: &crate::display::DisplayOptions,
        item_index: usize,
    ) {
        use crate::command::job_result::display::streaming_output_to_json;
        use crate::display::visualizer::{
            StreamingCardVisualizer, StreamingTableVisualizer, StreamingVisualizer,
        };

        // Convert streaming data to JSON
        let json_item = streaming_output_to_json(data, result_descriptor, format);

        // Use appropriate streaming visualizer
        match format {
            crate::display::DisplayFormat::Table => {
                // For table format, we collect items in a static visualizer
                static TABLE_VISUALIZER: std::sync::OnceLock<StreamingTableVisualizer> =
                    std::sync::OnceLock::new();
                let visualizer = TABLE_VISUALIZER.get_or_init(StreamingTableVisualizer::new);
                visualizer.render_item(&json_item, item_index, options);
            }
            crate::display::DisplayFormat::Card => {
                let visualizer = StreamingCardVisualizer;
                visualizer.render_item(&json_item, item_index, options);
            }
            crate::display::DisplayFormat::Json => {
                // For JSON, we just print the item directly
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json_item)
                        .unwrap_or_else(|_| json_item.to_string())
                );
            }
        }
    }

    /// Start streaming session with appropriate visualizer
    pub fn start_streaming_session(
        stream_type: &str,
        format: &crate::display::DisplayFormat,
        options: &crate::display::DisplayOptions,
    ) {
        use crate::display::visualizer::{
            StreamingCardVisualizer, StreamingTableVisualizer, StreamingVisualizer,
        };

        match format {
            crate::display::DisplayFormat::Table => {
                static TABLE_VISUALIZER: std::sync::OnceLock<StreamingTableVisualizer> =
                    std::sync::OnceLock::new();
                let visualizer = TABLE_VISUALIZER.get_or_init(StreamingTableVisualizer::new);
                visualizer.start_stream(stream_type, options);
            }
            crate::display::DisplayFormat::Card => {
                let visualizer = StreamingCardVisualizer;
                visualizer.start_stream(stream_type, options);
            }
            crate::display::DisplayFormat::Json => {
                // JSON streaming doesn't need start message
            }
        }
    }

    /// End streaming session with appropriate visualizer
    pub fn end_streaming_session(
        total_count: usize,
        format: &crate::display::DisplayFormat,
        options: &crate::display::DisplayOptions,
    ) {
        use crate::display::visualizer::{
            StreamingCardVisualizer, StreamingTableVisualizer, StreamingVisualizer,
        };

        match format {
            crate::display::DisplayFormat::Table => {
                static TABLE_VISUALIZER: std::sync::OnceLock<StreamingTableVisualizer> =
                    std::sync::OnceLock::new();
                let visualizer = TABLE_VISUALIZER.get_or_init(StreamingTableVisualizer::new);
                visualizer.end_stream(total_count, options);
            }
            crate::display::DisplayFormat::Card => {
                let visualizer = StreamingCardVisualizer;
                visualizer.end_stream(total_count, options);
            }
            crate::display::DisplayFormat::Json => {
                // JSON streaming doesn't need end message
            }
        }
    }
}
// transform command to protobuf message of job_result service
