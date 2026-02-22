use super::UseJobworkerpClient;
use crate::command::to_request;
use crate::error::ClientError;
use crate::jobworkerp::data::{
    JobId, JobResultData, Priority, QueueType, ResponseType, ResultOutputItem, ResultStatus,
    RetryPolicy, RetryType, Runner, RunnerData, RunnerId, Worker, WorkerData, WorkerId,
};
use crate::jobworkerp::function::data::FunctionSpecs;
use crate::jobworkerp::function::service::{FindFunctionRequest, FindFunctionSetRequest};
use crate::jobworkerp::service::{
    CreateJobResponse, JobRequest, RunnerNameRequest, WorkerNameRequest,
};
use crate::proto::JobworkerpProto;
use anyhow::{Context, Result};
use command_utils::cache_ok;
use command_utils::protobuf::ProtobufDescriptor;
use command_utils::trace::Tracing;
use command_utils::util::datetime;
use command_utils::util::scoped_cache::ScopedCache;
use prost::Message;
use prost_reflect::MessageDescriptor;
use rand;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};
use std::sync::Arc;
use tokio_stream::StreamExt;

pub const DEFAULT_RETRY_POLICY: RetryPolicy = RetryPolicy {
    r#type: RetryType::Exponential as i32,
    interval: 800,
    max_interval: 60000,
    max_retry: 1,
    basis: 2.0,
};

/// Build WorkerData with default settings from runner
fn build_worker_data_default(
    name: &str,
    runner_id: RunnerId,
    runner_settings: Vec<u8>,
) -> WorkerData {
    WorkerData {
        name: name.to_string(),
        description: String::new(),
        runner_id: Some(runner_id),
        runner_settings,
        periodic_interval: 0,
        channel: None,
        queue_type: QueueType::Normal as i32,
        response_type: ResponseType::Direct as i32,
        store_success: false,
        store_failure: true,
        use_static: false,
        retry_policy: Some(DEFAULT_RETRY_POLICY),
        broadcast_results: true,
    }
}

/// Build WorkerData from JSON parameters
fn build_worker_data_from_json(
    name: &str,
    runner_id: RunnerId,
    runner_settings: Vec<u8>,
    params: &serde_json::Map<String, serde_json::Value>,
) -> WorkerData {
    WorkerData {
        name: params
            .get("name")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| name.to_string()),
        description: params
            .get("description")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default(),
        runner_id: Some(runner_id),
        runner_settings,
        periodic_interval: 0,
        channel: params
            .get("channel")
            .and_then(|v| v.as_str().map(|s| s.to_string())),
        queue_type: params
            .get("queue_type")
            .and_then(|v| v.as_str())
            .and_then(|s| QueueType::from_str_name(s).map(|q| q as i32))
            .unwrap_or(QueueType::Normal as i32),
        response_type: ResponseType::Direct as i32,
        store_success: params
            .get("store_success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        store_failure: params
            .get("store_failure")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        use_static: params
            .get("use_static")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        retry_policy: Some(DEFAULT_RETRY_POLICY),
        broadcast_results: true,
    }
}

/// Append a unique hash to the worker name for ephemeral workers
fn make_worker_name_unique(worker: &mut WorkerData) {
    let mut hasher = DefaultHasher::default();
    hasher.write_i64(datetime::now_millis());
    hasher.write_i64(rand::random());
    worker.name = format!("{}_{:x}", worker.name, hasher.finish());
    tracing::debug!("Worker name with hash: {}", &worker.name);
}

/// Build JobRequest from parameters
fn build_job_request(
    worker_id: WorkerId,
    args: Vec<u8>,
    timeout_sec: u32,
    run_after_time: Option<i64>,
    priority: Option<Priority>,
    using: Option<&str>,
) -> JobRequest {
    JobRequest {
        args,
        timeout: Some((timeout_sec * 1000).into()),
        worker: Some(crate::jobworkerp::service::job_request::Worker::WorkerId(
            worker_id,
        )),
        priority: priority.map(|p| p as i32),
        run_after_time,
        using: using.map(|s| s.to_string()),
        ..Default::default()
    }
}

/// Merge multiple decoded JSON chunks from streaming protobuf messages.
///
/// Each chunk is a decoded protobuf message (e.g. LLM streaming delta).
/// String fields are concatenated; the last non-null value wins for other types.
/// The first chunk provides the base structure.
fn merge_decoded_chunks(chunks: Vec<serde_json::Value>) -> serde_json::Value {
    if chunks.is_empty() {
        return serde_json::Value::Null;
    }
    if chunks.len() == 1 {
        return chunks.into_iter().next().unwrap();
    }

    let mut iter = chunks.into_iter();
    let mut merged = iter.next().unwrap();

    for chunk in iter {
        deep_merge_concat(&mut merged, chunk);
    }

    merged
}

/// Deep merge two JSON values, concatenating string and array fields.
/// Designed for LLM streaming deltas where each chunk carries incremental data:
/// strings are appended, arrays are extended, and objects are merged recursively.
fn deep_merge_concat(base: &mut serde_json::Value, overlay: serde_json::Value) {
    match (base, overlay) {
        (serde_json::Value::Object(base_map), serde_json::Value::Object(overlay_map)) => {
            for (key, overlay_val) in overlay_map {
                if let Some(base_val) = base_map.get_mut(&key) {
                    deep_merge_concat(base_val, overlay_val);
                } else {
                    base_map.insert(key, overlay_val);
                }
            }
        }
        (base @ serde_json::Value::String(_), serde_json::Value::String(s)) => {
            if let serde_json::Value::String(base_s) = base {
                base_s.push_str(&s);
            }
        }
        (serde_json::Value::Array(base_arr), serde_json::Value::Array(overlay_arr)) => {
            // LLM streaming deltas send array elements incrementally (e.g. tool_calls)
            base_arr.extend(overlay_arr);
        }
        (base, overlay) => {
            // For non-string, non-array scalars (numbers, bools, null), last value wins
            *base = overlay;
        }
    }
}

/// Decode output bytes to JSON using result schema descriptor
fn decode_output_to_json(
    output: &[u8],
    result_descriptor: Option<&MessageDescriptor>,
) -> Result<serde_json::Value> {
    if let Some(desc) = result_descriptor {
        match ProtobufDescriptor::get_message_from_bytes(desc.clone(), output) {
            Ok(m) => {
                let j = ProtobufDescriptor::message_to_json(&m)?;
                tracing::debug!("Result schema exists. decode message with proto: {:#?}", j);
                serde_json::from_str(j.as_str()).map_err(|e| {
                    ClientError::ParseError(format!(
                        "Failed to parse JSON from protobuf message: {e:#?}"
                    ))
                    .into()
                })
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to parse result with proto schema: {:#?}. Trying direct JSON parse.",
                    e
                );
                serde_json::from_slice(output).map_err(|e_slice| {
                    ClientError::ParseError(format!(
                        "Failed to parse output as JSON (slice): {e_slice:#?}"
                    ))
                    .into()
                })
            }
        }
    } else {
        let text = String::from_utf8_lossy(output);
        tracing::debug!("No result schema, treating as string: {}", text);
        Ok(serde_json::Value::String(text.into_owned()))
    }
}

pub trait UseJobworkerpClientHelper: UseJobworkerpClient + Send + Sync + Tracing {
    fn find_function_list<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        exclude_runner: bool,
        exclude_worker: bool,
    ) -> impl std::future::Future<Output = Result<Vec<FunctionSpecs>>> + Send + 'a {
        async move {
            let client_clone = self.jobworkerp_client().clone();
            Self::trace_grpc_client_with_request(
                cx.cloned(),
                "jobworkerp-client",
                "find_function_list",
                "find_list",
                tonic::Request::new(FindFunctionRequest {
                    exclude_runner,
                    exclude_worker,
                }),
                move |request| async move {
                    let response = client_clone
                        .function_client()
                        .await
                        .find_list(to_request(&metadata, request)?)
                        .await
                        .map_err(ClientError::from_tonic_status)?;
                    let mut functions = Vec::new();
                    let mut stream = response.into_inner();
                    while let Some(t) = stream.next().await {
                        match t {
                            Ok(t) => {
                                functions.push(t);
                            }
                            Err(e) => return Err(ClientError::from_tonic_status(e).into()),
                        }
                    }
                    Ok(functions)
                },
            )
            .await
        }
    }
    fn find_function_list_by_set<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        name: &'a str,
    ) -> impl std::future::Future<Output = Result<Vec<FunctionSpecs>>> + Send + 'a {
        async move {
            let client_clone = self.jobworkerp_client().clone();
            let name_owned = name.to_string();
            Self::trace_grpc_client_with_request(
                cx.cloned(),
                "jobworkerp-client",
                "find_function_list_by_set",
                "find_list_by_set",
                tonic::Request::new(FindFunctionSetRequest { name: name_owned }),
                move |request| async move {
                    let response = client_clone
                        .function_client()
                        .await
                        .find_list_by_set(to_request(&metadata, request)?)
                        .await
                        .map_err(ClientError::from_tonic_status)?;
                    let mut functions = Vec::new();
                    let mut stream = response.into_inner();
                    while let Some(t) = stream.next().await {
                        match t {
                            Ok(t) => {
                                functions.push(t);
                            }
                            Err(e) => return Err(ClientError::from_tonic_status(e).into()),
                        }
                    }
                    Ok(functions)
                },
            )
            .await
        }
    }
    fn find_runner_by_name<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        name: &'a str,
    ) -> impl std::future::Future<Output = Result<Option<Runner>>> + Send + 'a
    where
        Self: Send + Sync,
    {
        async move {
            let client_clone = self.jobworkerp_client().clone();
            let name_owned = name.to_string();
            Self::trace_grpc_client_with_request(
                cx.cloned(),
                "jobworkerp-client",
                "find_runner_by_name",
                "find_list",
                tonic::Request::new(RunnerNameRequest {
                    name: name_owned.clone(),
                }),
                move |request| async move {
                    let mut runner_client = client_clone.runner_client().await;
                    let res = runner_client
                        .find_by_name(to_request(&metadata, request)?)
                        .await
                        .map_err(ClientError::from_tonic_status)?
                        .into_inner();
                    Ok(res.data)
                },
            )
            .await
        }
    }
    fn find_worker_by_name<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        name: &'a str,
    ) -> impl std::future::Future<Output = Result<Option<(WorkerId, WorkerData)>>> + Send + 'a
    where
        Self: Send + Sync,
    {
        async move {
            let client_clone = self.jobworkerp_client().clone();
            let name_owned = name.to_string();
            Self::trace_grpc_client_with_request(
                cx.cloned(),
                "jobworkerp-client",
                "find_worker_by_name",
                "find_by_name",
                tonic::Request::new(WorkerNameRequest { name: name_owned }),
                move |request| async move {
                    let mut worker_cli = client_clone.worker_client().await;
                    let worker_response = worker_cli
                        .find_by_name(to_request(&metadata, request)?)
                        .await
                        .map_err(ClientError::from_tonic_status)?;

                    let worker = worker_response.into_inner().data;
                    Ok(worker.and_then(|w| {
                        if let Worker {
                            id: Some(wid),
                            data: Some(wdata),
                        } = w
                        {
                            Some((wid, wdata))
                        } else {
                            None
                        }
                    }))
                },
            )
            .await
        }
    }
    fn find_runner_by_name_with_cache(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        cache: &ScopedCache<String, Option<Runner>>,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Option<Runner>>> + Send
    where
        Self: Send + Sync,
    {
        async move {
            cache_ok!(
                cache,
                format!("runner:{}", name),
                self.find_runner_by_name(cx, metadata, name)
            )
        }
    }
    /// Find runner by name, returning error if not found
    fn find_runner_or_error<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        runner_name: &'a str,
    ) -> impl std::future::Future<Output = Result<(RunnerId, RunnerData)>> + Send + 'a
    where
        Self: Send + Sync,
    {
        async move {
            if let Some(Runner {
                id: Some(rid),
                data: Some(rdata),
            }) = self.find_runner_by_name(cx, metadata, runner_name).await?
            {
                Ok((rid, rdata))
            } else {
                Err(ClientError::NotFound(format!("runner not found: {runner_name}")).into())
            }
        }
    }
    fn find_or_create_worker<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker_data: &'a WorkerData,
    ) -> impl std::future::Future<Output = Result<Worker>> + Send + 'a {
        async move {
            let client_clone = self.jobworkerp_client().clone();
            let worker_name_for_find = worker_data.name.clone();

            let find_request = tonic::Request::new(WorkerNameRequest {
                name: worker_name_for_find,
            });

            let metadata_clone = metadata.clone();
            let found_worker_opt_res: Result<Option<Worker>> =
                Self::trace_grpc_client_with_request(
                    cx.cloned(),
                    "jobworkerp-client",
                    "find_or_create_worker.find_by_name",
                    "find_by_name",
                    find_request,
                    {
                        let client_clone_inner = client_clone.clone();
                        move |req| async move {
                            client_clone_inner
                                .worker_client()
                                .await
                                .find_by_name(to_request(&metadata_clone, req)?)
                                .await
                                .map(|response| response.into_inner().data)
                                .map_err(|e| ClientError::from_tonic_status(e).into())
                        }
                    },
                )
                .await;

            let found_worker_opt = found_worker_opt_res?;

            if let Some(worker) = found_worker_opt {
                Ok(worker)
            } else {
                tracing::debug!("worker {} not found. create new worker", &worker_data.name);
                let create_request_data = worker_data.clone();
                let create_tonic_request = tonic::Request::new(create_request_data);

                let metadata_clone = metadata.clone();
                let created_worker_id_opt_res: Result<Option<WorkerId>> =
                    Self::trace_grpc_client_with_request(
                        cx.cloned(),
                        "jobworkerp-client",
                        "find_or_create_worker.create",
                        "create",
                        create_tonic_request,
                        {
                            let client_clone_inner_create = client_clone.clone();
                            move |req| async move {
                                client_clone_inner_create
                                    .worker_client()
                                    .await
                                    .create(to_request(&metadata_clone, req)?)
                                    .await
                                    .map(|response| response.into_inner().id)
                                    .map_err(|e| ClientError::from_tonic_status(e).into())
                            }
                        },
                    )
                    .await;

                let created_worker_id = created_worker_id_opt_res?.ok_or_else(|| {
                    ClientError::RuntimeError(
                        "create worker response is empty or id is None".to_string(),
                    )
                })?;

                Ok(Worker {
                    id: Some(created_worker_id),
                    data: Some(worker_data.to_owned()),
                })
            }
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn enqueue_and_get_result_worker_job_with_runner<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        runner_name: &'a str,
        mut worker_data: WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
        using: Option<&'a str>,
    ) -> impl std::future::Future<Output = Result<JobResultData>> + Send + 'a {
        async move {
            let runner = self
                .find_runner_by_name(cx, metadata.clone(), runner_name)
                .await?;
            worker_data.runner_id = runner.and_then(|r| r.id);
            tracing::debug!("resolved runner_id: {:?}", &worker_data.runner_id);
            let res = self
                .enqueue_and_get_result_worker_job(
                    cx,
                    metadata,
                    &worker_data,
                    args,
                    timeout_sec,
                    run_after_time,
                    priority,
                    using,
                )
                .await?;
            if res.status() == ResultStatus::Success {
                Ok(res)
            } else {
                Err(ClientError::RuntimeError(format!(
                    "job failed: {:?}",
                    res.output
                        .map(|o| String::from_utf8_lossy(&o.items).into_owned())
                ))
                .into())
            }
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn enqueue_and_get_result_worker_job(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker_data: &WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
        using: Option<&str>,
    ) -> impl std::future::Future<Output = Result<JobResultData>> + Send {
        async move {
            Ok(self
                .enqueue_worker_job(
                    cx,
                    metadata,
                    worker_data,
                    args,
                    timeout_sec,
                    run_after_time,
                    priority,
                    using,
                )
                .await?
                .result
                .ok_or_else(|| ClientError::NotFound("result not found".to_string()))?
                .data
                .ok_or_else(|| ClientError::NotFound("result data not found".to_string()))?)
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn enqueue_and_get_output_worker_job(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker_data: &WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
        using: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        async move {
            let res = self
                .enqueue_and_get_result_worker_job(
                    cx,
                    metadata,
                    worker_data,
                    args,
                    timeout_sec,
                    run_after_time,
                    priority,
                    using,
                )
                .await?;
            if res.status() == ResultStatus::Success && res.output.is_some() {
                let output = res
                    .output
                    .as_ref()
                    .ok_or_else(|| {
                        ClientError::NotFound(format!("job result output is empty: {res:?}"))
                    })?
                    .items
                    .to_owned();
                Ok(output)
            } else {
                Err(ClientError::RuntimeError(format!(
                    "job failed: {:?}",
                    res.output
                        .map(|o| String::from_utf8_lossy(&o.items).into_owned())
                ))
                .into())
            }
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn enqueue_worker_job<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker_data: &'a WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
        using: Option<&'a str>,
    ) -> impl std::future::Future<Output = Result<CreateJobResponse>> + Send + 'a {
        async move {
            let worker = self
                .find_or_create_worker(cx, metadata.clone(), worker_data)
                .await?;
            let worker_id = worker.id.ok_or_else(|| {
                ClientError::InvalidParameter(
                    "Worker ID not found after find_or_create_worker".to_string(),
                )
            })?;
            let job_request_payload = build_job_request(
                worker_id,
                args,
                timeout_sec,
                run_after_time,
                priority,
                using,
            );
            let tonic_request = tonic::Request::new(job_request_payload);
            let client_clone = self.jobworkerp_client().clone();

            Self::trace_grpc_client_with_request(
                cx.cloned(),
                "jobworkerp-client",
                "enqueue_worker_job.enqueue",
                "enqueue",
                tonic_request,
                move |req| async move {
                    client_clone
                        .job_client()
                        .await
                        .enqueue(to_request(&metadata, req)?)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|e| ClientError::from_tonic_status(e).into())
                },
            )
            .await
            .context("enqueue_worker_job")
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn enqueue_stream_worker_job<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker_data: &'a WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
        using: Option<&'a str>,
    ) -> impl std::future::Future<
        Output = Result<(
            tonic::metadata::MetadataMap,
            tonic::Streaming<crate::jobworkerp::data::ResultOutputItem>,
        )>,
    > + Send
    + 'a {
        async move {
            let worker = self
                .find_or_create_worker(cx, metadata.clone(), worker_data)
                .await?;
            let worker_id = worker.id.ok_or_else(|| {
                ClientError::InvalidParameter(
                    "Worker ID not found after find_or_create_worker".to_string(),
                )
            })?;
            let job_request_payload = build_job_request(
                worker_id,
                args,
                timeout_sec,
                run_after_time,
                priority,
                using,
            );
            let tonic_request = tonic::Request::new(job_request_payload);
            let client_clone = self.jobworkerp_client().clone();
            let metadata_clone = metadata.clone();

            Self::trace_grpc_client_with_request(
                cx.cloned(),
                "jobworkerp-client",
                "enqueue_stream_worker_job.enqueue_for_stream",
                "enqueue_for_stream",
                tonic_request,
                move |req| async move {
                    let response = client_clone
                        .job_client()
                        .await
                        .enqueue_for_stream(to_request(&metadata_clone, req)?)
                        .await
                        .map_err(ClientError::from_tonic_status)?;
                    let metadata_map = response.metadata().clone();
                    let streaming = response.into_inner();
                    Ok((metadata_map, streaming))
                },
            )
            .await
            .context("enqueue_stream_worker_job")
        }
    }
    fn enqueue_job_and_get_output<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker_spec: crate::jobworkerp::service::job_request::Worker,
        args: Vec<u8>,
        timeout_sec: u32,
        using: Option<&'a str>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send + 'a {
        async move {
            tracing::debug!("enqueue_job_and_get_output: {:?}", &worker_spec);
            let job_request_payload = JobRequest {
                args,
                timeout: Some((timeout_sec * 1000).into()),
                worker: Some(worker_spec.clone()),
                priority: Some(Priority::High as i32),
                using: using.map(|s| s.to_string()),
                ..Default::default()
            };
            let tonic_request = tonic::Request::new(job_request_payload);
            let client_clone = self.jobworkerp_client().clone();

            let res_result: Result<JobResultData> = Self::trace_grpc_client_with_request(
                cx.cloned(),
                "jobworkerp-client",
                "enqueue_job_and_get_output.enqueue",
                "enqueue",
                tonic_request,
                move |req| async move {
                    let response = client_clone
                        .job_client()
                        .await
                        .enqueue(to_request(&metadata, req)?)
                        .await
                        .map_err(ClientError::from_tonic_status)?;
                    let job_response = response.into_inner();
                    let job_result = job_response.result.ok_or_else(|| {
                        ClientError::NotFound("result not found in CreateJobResponse".to_string())
                    })?;
                    let job_result_data = job_result.data.ok_or_else(|| {
                        ClientError::NotFound("result data not found in JobResult".to_string())
                    })?;
                    Ok(job_result_data)
                },
            )
            .await;

            let res = res_result?;

            if res.status() == ResultStatus::Success && res.output.is_some() {
                let output_item = res
                    .output
                    .ok_or_else(|| ClientError::NotFound("job result output is empty".to_string()))?
                    .items
                    .to_owned();
                Ok(output_item)
            } else {
                let error_message = res
                    .output
                    .as_ref()
                    .map(|e_bytes| String::from_utf8_lossy(&e_bytes.items).into_owned())
                    .unwrap_or_else(|| format!("job failed with status: {:?}", res.status()));
                Err(ClientError::RuntimeError(format!("job failed: {error_message}")).into())
            }
        }
    }
    /// Create WorkerData from runner ID and settings
    fn create_worker_with_runner_id(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker_name: &str,
        runner_id: RunnerId,
        runner_settings: Vec<u8>,
        worker_params: Option<serde_json::Value>,
    ) -> impl std::future::Future<Output = Result<Worker>> + Send {
        let worker_name = worker_name.to_owned();
        async move {
            let mut worker = if let Some(serde_json::Value::Object(obj)) = worker_params {
                build_worker_data_from_json(&worker_name, runner_id, runner_settings, &obj)
            } else {
                build_worker_data_default(&worker_name, runner_id, runner_settings)
            };

            if !worker.use_static {
                make_worker_name_unique(&mut worker);
            }

            self.find_or_create_worker(cx, metadata, &worker).await
        }
    }
    /// Create WorkerData by looking up runner by name
    fn create_worker_from_runner(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        runner_name: &str,
        runner_settings: Vec<u8>,
        worker_params: Option<serde_json::Value>,
    ) -> impl std::future::Future<Output = Result<Worker>> + Send {
        let runner_name = runner_name.to_owned();
        async move {
            let (runner_id, _) = self
                .find_runner_or_error(cx, metadata.clone(), &runner_name)
                .await?;

            self.create_worker_with_runner_id(
                cx,
                metadata,
                &runner_name,
                runner_id,
                runner_settings,
                worker_params,
            )
            .await
        }
    }
    /// Enqueue job with ephemeral worker and delete worker after completion
    fn enqueue_with_ephemeral_worker(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker: Worker,
        job_args: Vec<u8>,
        job_timeout_sec: u32,
        using: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        let using = using.map(|s| s.to_owned());
        async move {
            let (wid, wdata) = match worker {
                Worker {
                    id: Some(wid),
                    data: Some(wdata),
                } => (wid, wdata),
                _ => {
                    return Err(ClientError::InvalidParameter(
                        "Invalid worker: missing id or data".to_string(),
                    )
                    .into());
                }
            };

            let w = crate::jobworkerp::service::job_request::Worker::WorkerId(wid);
            let output = self
                .enqueue_job_and_get_output(
                    cx,
                    metadata.clone(),
                    w,
                    job_args,
                    job_timeout_sec,
                    using.as_deref(),
                )
                .await
                .inspect_err(|e| {
                    tracing::warn!("Execute task failed: enqueue job and get output: {:#?}", e)
                });

            // Delete ephemeral worker regardless of job result
            if !wdata.use_static {
                match to_request(&metadata, wid) {
                    Ok(req) => {
                        match self
                            .jobworkerp_client()
                            .worker_client()
                            .await
                            .delete(req)
                            .await
                        {
                            Ok(res) => {
                                if !res.into_inner().is_success {
                                    tracing::warn!("Failed to delete worker: {:#?}", wid);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Failed to delete worker {:#?}: {:#?}", wid, e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to create delete request for worker {:#?}: {:#?}",
                            wid,
                            e
                        );
                    }
                }
            }
            output
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn setup_worker_and_enqueue_with_raw_output(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        name: &str,
        runner_settings: Vec<u8>,
        worker_params: Option<serde_json::Value>,
        job_args: Vec<u8>,
        job_timeout_sec: u32,
        using: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        let name = name.to_owned();
        let using = using.map(|s| s.to_owned());
        async move {
            let worker = self
                .create_worker_from_runner(
                    cx,
                    metadata.clone(),
                    &name,
                    runner_settings,
                    worker_params,
                )
                .await?;
            self.enqueue_with_ephemeral_worker(
                cx,
                metadata,
                worker,
                job_args,
                job_timeout_sec,
                using.as_deref(),
            )
            .await
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn setup_worker_and_enqueue(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        runner_name: &str,
        runner_settings: Vec<u8>,
        worker_params: Option<serde_json::Value>,
        job_args: Vec<u8>,
        job_timeout_sec: u32,
        using: Option<&str>,
    ) -> impl std::future::Future<Output = Result<serde_json::Value>> + Send {
        let runner_name = runner_name.to_owned();
        let using = using.map(|s| s.to_owned());
        async move {
            let (rid, rdata) = self
                .find_runner_or_error(cx, metadata.clone(), &runner_name)
                .await?;

            let result_descriptor =
                JobworkerpProto::parse_result_schema_descriptor(&rdata, using.as_deref())?;

            let worker = self
                .create_worker_with_runner_id(
                    cx,
                    metadata.clone(),
                    &runner_name,
                    rid,
                    runner_settings,
                    worker_params,
                )
                .await?;
            let output = self
                .enqueue_with_ephemeral_worker(
                    cx,
                    metadata,
                    worker,
                    job_args,
                    job_timeout_sec,
                    using.as_deref(),
                )
                .await?;
            decode_output_to_json(&output, result_descriptor.as_ref())
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn setup_worker_and_enqueue_with_json(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        runner_name: &str,
        runner_settings: Option<serde_json::Value>,
        worker_params: Option<serde_json::Value>,
        job_args: serde_json::Value,
        job_timeout_sec: u32,
        using: Option<&str>,
    ) -> impl std::future::Future<Output = Result<serde_json::Value>> + Send {
        let runner_name = runner_name.to_owned();
        let using = using.map(|s| s.to_owned());
        async move {
            let (rid, rdata) = self
                .find_runner_or_error(cx, metadata.clone(), &runner_name)
                .await?;

            let runner_settings_descriptor =
                JobworkerpProto::parse_runner_settings_schema_descriptor(&rdata).map_err(|e| {
                    ClientError::ParseError(format!(
                        "Failed to parse runner_settings schema descriptor: {e:#?}"
                    ))
                })?;
            let args_descriptor =
                JobworkerpProto::parse_job_args_schema_descriptor(&rdata, using.as_deref())
                    .map_err(|e| {
                        ClientError::ParseError(format!(
                            "Failed to parse job_args schema descriptor: {e:#?}"
                        ))
                    })?;
            let result_descriptor =
                JobworkerpProto::parse_result_schema_descriptor(&rdata, using.as_deref())?;

            let runner_settings_bytes = if let Some(ope_desc) = runner_settings_descriptor {
                tracing::debug!("runner settings schema exists: {:#?}", &runner_settings);
                runner_settings
                    .map(|j| JobworkerpProto::json_value_to_message(ope_desc, &j, true, true))
                    .unwrap_or(Ok(vec![]))
                    .map_err(|e| {
                        ClientError::ParseError(format!(
                            "Failed to parse runner_settings schema: {e:#?}"
                        ))
                    })?
            } else {
                tracing::debug!("runner settings schema empty");
                vec![]
            };
            tracing::trace!("job args: {:#?}", &job_args);
            let job_args_bytes = match args_descriptor {
                Some(desc) => JobworkerpProto::json_value_to_message(desc, &job_args, true, true)
                    .map_err(|e| {
                    ClientError::ParseError(format!("Failed to parse job_args schema: {e:#?}"))
                })?,
                _ => serde_json::to_string(&job_args)
                    .map_err(|e| {
                        ClientError::ParseError(format!("Failed to serialize job_args: {e:#?}"))
                    })?
                    .into_bytes(),
            };

            let worker = self
                .create_worker_with_runner_id(
                    cx,
                    metadata.clone(),
                    &runner_name,
                    rid,
                    runner_settings_bytes,
                    worker_params,
                )
                .await?;
            let output = self
                .enqueue_with_ephemeral_worker(
                    cx,
                    metadata,
                    worker,
                    job_args_bytes,
                    job_timeout_sec,
                    using.as_deref(),
                )
                .await?;
            decode_output_to_json(&output, result_descriptor.as_ref())
        }
    }
    fn enqueue_with_json<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker_data: &'a WorkerData,
        job_args: serde_json::Value,
        job_timeout_sec: u32,
        using: Option<&'a str>,
    ) -> impl std::future::Future<Output = Result<serde_json::Value>> + Send + 'a {
        async move {
            let runner_id = worker_data.runner_id.ok_or_else(|| {
                ClientError::InvalidParameter(format!(
                    "runner_id not found in worker_data for {}",
                    worker_data.name
                ))
            })?;

            let client_clone = self.jobworkerp_client().clone();
            let find_runner_request = tonic::Request::new(runner_id);
            let metadata_clone = metadata.clone();

            let runner_response_opt_res: Result<Option<Runner>> =
                Self::trace_grpc_client_with_request(
                    cx.cloned(),
                    "jobworkerp-client",
                    "enqueue_with_json.find_runner",
                    "find",
                    find_runner_request,
                    {
                        let client_clone_inner = client_clone.clone();
                        move |req| async move {
                            client_clone_inner
                                .runner_client()
                                .await
                                .find(to_request(&metadata_clone, req)?)
                                .await
                                .map(|response| response.into_inner().data)
                                .map_err(|e| ClientError::from_tonic_status(e).into())
                        }
                    },
                )
                .await;

            let runner_opt = runner_response_opt_res?;

            if let Some(Runner {
                id: Some(_sid),
                data: Some(rdata),
            }) = runner_opt
            {
                let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(
                    &rdata, using,
                )
                .map_err(|e| {
                    ClientError::ParseError(format!(
                        "Failed to parse job_args schema descriptor: {e:#?}"
                    ))
                })?;

                tracing::trace!("job args (json): {:#?}", &job_args);
                let job_args_bytes = match args_descriptor {
                    Some(desc) => {
                        JobworkerpProto::json_value_to_message(desc, &job_args, true, true)
                            .map_err(|e| {
                                ClientError::ParseError(format!(
                                    "Failed to parse job_args schema: {e:#?}"
                                ))
                            })?
                    }
                    _ => serde_json::to_string(&job_args)
                        .map_err(|e| {
                            ClientError::ParseError(format!("Failed to serialize job_args: {e:#?}"))
                        })?
                        .into_bytes(),
                };

                let output_bytes = self
                    .enqueue_and_get_output_worker_job(
                        cx,
                        metadata,
                        worker_data,
                        job_args_bytes,
                        job_timeout_sec,
                        None,
                        None,
                        using,
                    )
                    .await?;

                let result_descriptor =
                    JobworkerpProto::parse_result_schema_descriptor(&rdata, using)?;
                decode_output_to_json(&output_bytes, result_descriptor.as_ref())
            } else {
                Err(ClientError::NotFound(format!(
                    "runner not found with id: {:?} for worker: {}",
                    runner_id, &worker_data.name
                ))
                .into())
            }
        }
    }
    fn delete_worker_by_name<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        name: &'a str,
    ) -> impl std::future::Future<Output = Result<bool>> + Send + 'a {
        async move {
            let client_clone = self.jobworkerp_client().clone();
            let worker_name_owned = name.to_string();

            let find_request = tonic::Request::new(WorkerNameRequest {
                name: worker_name_owned.clone(),
            });

            let metadata_clone = metadata.clone();
            let worker_to_delete_opt_res: Result<Option<Worker>> =
                Self::trace_grpc_client_with_request(
                    cx.cloned(),
                    "jobworkerp-client",
                    "delete_worker_by_name.find_by_name",
                    "find_by_name",
                    find_request,
                    {
                        let client_clone_inner_find = client_clone.clone();
                        move |req| async move {
                            client_clone_inner_find
                                .worker_client()
                                .await
                                .find_by_name(to_request(&metadata_clone, req)?)
                                .await
                                .map(|r| r.into_inner().data)
                                .map_err(|e| ClientError::from_tonic_status(e).into())
                        }
                    },
                )
                .await;

            let worker_to_delete_opt = worker_to_delete_opt_res?;

            if let Some(Worker { id: Some(wid), .. }) = worker_to_delete_opt {
                let delete_request = tonic::Request::new(wid);
                Self::trace_grpc_client_with_request(
                    cx.cloned(),
                    "jobworkerp-client",
                    "delete_worker_by_name.delete",
                    "delete",
                    delete_request,
                    {
                        move |req| async move {
                            client_clone
                                .worker_client()
                                .await
                                .delete(to_request(&metadata, req)?)
                                .await
                                .map(|r| r.into_inner().is_success)
                                .map_err(|e| ClientError::from_tonic_status(e).into())
                        }
                    },
                )
                .await
                .context("delete_worker_by_id")
            } else {
                Err(ClientError::NotFound(format!("worker not found: {name}")).into())
            }
        }
    }
    /// Enqueue a job with JSON args and return (JobId, Streaming, result_descriptor)
    /// for cancellable streaming processing.
    ///
    /// This combines the JSON-to-protobuf args conversion from `enqueue_with_json`
    /// with the streaming enqueue from `enqueue_stream_worker_job`, returning
    /// the JobId (from response metadata) so callers can cancel via `delete_job`.
    #[allow(clippy::too_many_arguments)]
    fn enqueue_stream_with_json<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        worker_data: &'a WorkerData,
        job_args: serde_json::Value,
        job_timeout_sec: u32,
        using: Option<&'a str>,
    ) -> impl std::future::Future<
        Output = Result<(
            Option<JobId>,
            tonic::Streaming<ResultOutputItem>,
            Option<MessageDescriptor>,
        )>,
    > + Send
    + 'a {
        async move {
            let runner_id = worker_data.runner_id.ok_or_else(|| {
                ClientError::InvalidParameter(format!(
                    "runner_id not found in worker_data for {}",
                    worker_data.name
                ))
            })?;

            let client_clone = self.jobworkerp_client().clone();
            let find_runner_request = tonic::Request::new(runner_id);
            let metadata_clone = metadata.clone();

            let runner_response_opt_res: Result<Option<Runner>> =
                Self::trace_grpc_client_with_request(
                    cx.cloned(),
                    "jobworkerp-client",
                    "enqueue_stream_with_json.find_runner",
                    "find",
                    find_runner_request,
                    {
                        let client_clone_inner = client_clone.clone();
                        move |req| async move {
                            client_clone_inner
                                .runner_client()
                                .await
                                .find(to_request(&metadata_clone, req)?)
                                .await
                                .map(|response| response.into_inner().data)
                                .map_err(|e| ClientError::from_tonic_status(e).into())
                        }
                    },
                )
                .await;

            let runner_opt = runner_response_opt_res?;

            if let Some(Runner {
                id: Some(_sid),
                data: Some(rdata),
            }) = runner_opt
            {
                let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(
                    &rdata, using,
                )
                .map_err(|e| {
                    ClientError::ParseError(format!(
                        "Failed to parse job_args schema descriptor: {e:#?}"
                    ))
                })?;

                tracing::trace!("job args (json): {:#?}", &job_args);
                let job_args_bytes = match args_descriptor {
                    Some(desc) => {
                        JobworkerpProto::json_value_to_message(desc, &job_args, true, true)
                            .map_err(|e| {
                                ClientError::ParseError(format!(
                                    "Failed to parse job_args schema: {e:#?}"
                                ))
                            })?
                    }
                    _ => serde_json::to_string(&job_args)
                        .map_err(|e| {
                            ClientError::ParseError(format!("Failed to serialize job_args: {e:#?}"))
                        })?
                        .into_bytes(),
                };

                let (meta_map, streaming) = self
                    .enqueue_stream_worker_job(
                        cx,
                        metadata,
                        worker_data,
                        job_args_bytes,
                        job_timeout_sec,
                        None,
                        None,
                        using,
                    )
                    .await?;

                // Extract JobId from response metadata
                let job_id = meta_map
                    .get_bin(crate::command::job::JOB_ID_HEADER_NAME)
                    .and_then(|id_bin| id_bin.to_bytes().ok())
                    .and_then(|bytes| JobId::decode(bytes.as_ref()).ok());

                if job_id.is_none() {
                    tracing::warn!("JobId not found in streaming response metadata");
                }

                let result_descriptor =
                    JobworkerpProto::parse_result_schema_descriptor(&rdata, using)?;

                Ok((job_id, streaming, result_descriptor))
            } else {
                Err(ClientError::NotFound(format!(
                    "runner not found with id: {:?} for worker: {}",
                    runner_id, &worker_data.name
                ))
                .into())
            }
        }
    }

    /// Delete job by ID
    fn delete_job<'a>(
        &'a self,
        cx: Option<&'a opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        job_id: JobId,
    ) -> impl std::future::Future<Output = Result<bool>> + Send + 'a {
        async move {
            let delete_request = tonic::Request::new(job_id);
            let client_clone = self.jobworkerp_client().clone();
            Self::trace_grpc_client_with_request(
                cx.cloned(),
                "jobworkerp-client",
                "delete_job.delete",
                "delete",
                delete_request,
                move |req| async move {
                    client_clone
                        .job_client()
                        .await
                        .delete(to_request(&metadata, req)?)
                        .await
                        .map(|r| r.into_inner().is_success)
                        .map_err(|e| ClientError::from_tonic_status(e).into())
                },
            )
            .await
            .context("delete_job")
        }
    }
}

/// Collect all output from a streaming job response and decode to JSON.
pub async fn collect_stream_result(
    stream: &mut tonic::Streaming<ResultOutputItem>,
    result_descriptor: Option<&MessageDescriptor>,
) -> Result<serde_json::Value> {
    let mut collected: Vec<u8> = Vec::new();
    // When result_descriptor is present, each Data chunk is an independent
    // protobuf message (e.g. LLM streaming delta). Concatenating raw bytes
    // causes later field values to overwrite earlier ones during protobuf
    // decode. Instead, decode each chunk individually and merge the JSON
    // text fields by string concatenation.
    let mut decoded_chunks: Vec<serde_json::Value> = Vec::new();
    let mut has_descriptor = result_descriptor.is_some();
    let mut finalized = false;

    while let Some(item_result) = stream.next().await {
        let item = item_result.map_err(ClientError::from_tonic_status)?;
        match item.item {
            Some(crate::jobworkerp::data::result_output_item::Item::Data(data)) => {
                if finalized {
                    tracing::warn!("Received Data chunk after FinalCollected, ignoring");
                    continue;
                }
                // Always accumulate raw bytes for fallback
                collected.extend_from_slice(&data);
                if has_descriptor {
                    // Decode each chunk independently
                    match decode_output_to_json(&data, result_descriptor) {
                        Ok(chunk_json) => decoded_chunks.push(chunk_json),
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "Failed to decode streaming chunk, falling back to raw accumulation"
                            );
                            decoded_chunks.clear();
                            has_descriptor = false;
                        }
                    }
                }
            }
            Some(crate::jobworkerp::data::result_output_item::Item::FinalCollected(data)) => {
                // FinalCollected (STREAMING_TYPE_INTERNAL only): complete aggregated result
                decoded_chunks.clear();
                collected = data;
                finalized = true;
            }
            Some(crate::jobworkerp::data::result_output_item::Item::End(_trailer)) => {
                break;
            }
            None => {}
        }
    }

    if !decoded_chunks.is_empty() {
        Ok(merge_decoded_chunks(decoded_chunks))
    } else {
        // When descriptor-based decoding failed (has_descriptor=false), the collected bytes
        // are concatenated raw chunks, not a valid single protobuf message.
        let desc = if has_descriptor {
            result_descriptor
        } else {
            None
        };
        decode_output_to_json(&collected, desc)
    }
}
