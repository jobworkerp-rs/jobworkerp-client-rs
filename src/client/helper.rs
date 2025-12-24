use super::UseJobworkerpClient;
use crate::command::to_request;
use crate::jobworkerp::data::{
    JobResultData, Priority, QueueType, ResponseType, ResultStatus, RetryPolicy, RetryType, Runner,
    Worker, WorkerData, WorkerId,
};
use crate::jobworkerp::function::data::FunctionSpecs;
use crate::jobworkerp::function::service::{FindFunctionRequest, FindFunctionSetRequest};
use crate::jobworkerp::service::{
    CreateJobResponse, JobRequest, RunnerNameRequest, WorkerNameRequest,
};
use crate::proto::JobworkerpProto;
use anyhow::{anyhow, Context, Result};
use command_utils::cache_ok;
use command_utils::protobuf::ProtobufDescriptor;
use command_utils::trace::Tracing;
use command_utils::util::datetime;
use command_utils::util::scoped_cache::ScopedCache;
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
                        .map_err(|e| anyhow!(e))?;
                    let mut functions = Vec::new();
                    let mut stream = response.into_inner();
                    while let Some(t) = stream.next().await {
                        match t {
                            Ok(t) => {
                                functions.push(t);
                            }
                            Err(e) => return Err(anyhow!(e)),
                        }
                    }
                    Ok(functions)
                },
            )
            .await
            .map_err(|e| anyhow!(e.to_string()))
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
                        .map_err(|e| anyhow!(e))?;
                    let mut functions = Vec::new();
                    let mut stream = response.into_inner();
                    while let Some(t) = stream.next().await {
                        match t {
                            Ok(t) => {
                                functions.push(t);
                            }
                            Err(e) => return Err(anyhow!(e)),
                        }
                    }
                    Ok(functions)
                },
            )
            .await
            .map_err(|e| anyhow!(e.to_string()))
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
                        .map_err(|e| anyhow!(e))?
                        .into_inner();
                    Ok(res.data)
                    // while let Some(item) = stream.next().await {
                    //     match item {
                    //         Ok(item) => {
                    //             if item.data.as_ref().is_some_and(|d| d.name == name_owned) {
                    //                 return Ok(Some(item));
                    //             }
                    //         }
                    //         Err(e) => return Err(anyhow!(e)),
                    //     }
                    // }
                    // Ok(None)
                },
            )
            .await
            .map_err(|e| anyhow!(e.to_string()))
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
                        .map_err(|e| anyhow!(e))?;

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
                                .map_err(|e| anyhow!(e))
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
                                    .map_err(|e| anyhow!(e))
                            }
                        },
                    )
                    .await;

                let created_worker_id = created_worker_id_opt_res?
                    .ok_or_else(|| anyhow!("create worker response is empty or id is None"))?;

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
                Err(anyhow!(
                    "job failed: {:?}",
                    res.output
                        .map(|o| String::from_utf8_lossy(&o.items).into_owned())
                ))
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
            self.enqueue_worker_job(
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
            .ok_or(anyhow!("result not found"))?
            .data
            .ok_or(anyhow!("result data not found"))
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
                    .ok_or(anyhow!("job result output is empty: {res:?}"))?
                    .items
                    .to_owned();
                Ok(output)
            } else {
                Err(anyhow!(
                    "job failed: {:?}",
                    res.output
                        .map(|o| String::from_utf8_lossy(&o.items).into_owned())
                ))
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
            let job_request_payload = JobRequest {
                args,
                timeout: Some((timeout_sec * 1000).into()),
                worker: Some(crate::jobworkerp::service::job_request::Worker::WorkerId(
                    worker
                        .id
                        .ok_or(anyhow!("Worker ID not found after find_or_create_worker"))?,
                )),
                priority: priority.map(|p| p as i32),
                run_after_time,
                using: using.map(|s| s.to_string()),
                ..Default::default()
            };
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
                        .map_err(|e| anyhow!(e))
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
            let job_request_payload = JobRequest {
                args,
                timeout: Some((timeout_sec * 1000).into()),
                worker: Some(crate::jobworkerp::service::job_request::Worker::WorkerId(
                    worker
                        .id
                        .ok_or(anyhow!("Worker ID not found after find_or_create_worker"))?,
                )),
                priority: priority.map(|p| p as i32),
                run_after_time,
                using: using.map(|s| s.to_string()),
                ..Default::default()
            };
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
                        .map_err(|e| anyhow!(e))?;
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
                        .map_err(|e| anyhow!(e))?;
                    let job_response = response.into_inner();
                    let job_result = job_response
                        .result
                        .ok_or_else(|| anyhow!("result not found in CreateJobResponse"))?;
                    let job_result_data = job_result
                        .data
                        .ok_or_else(|| anyhow!("result data not found in JobResult"))?;
                    Ok(job_result_data)
                },
            )
            .await;

            let res = res_result?;

            if res.status() == ResultStatus::Success && res.output.is_some() {
                let output_item = res
                    .output
                    .ok_or(anyhow!("job result output is empty"))?
                    .items
                    .to_owned();
                Ok(output_item)
            } else {
                let error_message = res
                    .output
                    .as_ref()
                    .map(|e_bytes| String::from_utf8_lossy(&e_bytes.items).into_owned())
                    .unwrap_or_else(|| format!("job failed with status: {:?}", res.status()));
                Err(anyhow!("job failed: {error_message}"))
            }
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
        async move {
            if let Some(Runner {
                id: Some(sid),
                data: Some(_sdata),
            }) = self
                .find_runner_by_name(cx, metadata.clone(), name.as_str())
                .await?
            {
                let mut worker: WorkerData =
                    if let Some(serde_json::Value::Object(obj)) = worker_params {
                        WorkerData {
                            name: obj
                                .get("name")
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                                .unwrap_or_else(|| name.to_string().clone()),
                            description: obj
                                .get("description")
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                                .unwrap_or_else(|| "".to_string()),
                            runner_id: Some(sid),
                            runner_settings,
                            periodic_interval: 0,
                            channel: obj
                                .get("channel")
                                .and_then(|v| v.as_str().map(|s| s.to_string())),
                            queue_type: obj
                                .get("queue_type")
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                                .and_then(|s| QueueType::from_str_name(&s).map(|q| q as i32))
                                .unwrap_or(QueueType::Normal as i32),
                            response_type: ResponseType::Direct as i32,
                            store_success: obj
                                .get("store_success")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false),
                            store_failure: obj
                                .get("store_success")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(true),
                            use_static: obj
                                .get("use_static")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false),
                            retry_policy: Some(DEFAULT_RETRY_POLICY),
                            broadcast_results: true,
                        }
                    } else {
                        WorkerData {
                            name: name.to_string().clone(),
                            description: "".to_string(),
                            runner_id: Some(sid),
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
                    };
                if !worker.use_static {
                    let mut hasher = DefaultHasher::default();
                    hasher.write_i64(datetime::now_millis());
                    hasher.write_i64(rand::random());
                    worker.name = format!("{}_{:x}", worker.name, hasher.finish());
                    tracing::debug!("Worker name with hash: {}", &worker.name);
                }
                if let Worker {
                    id: Some(wid),
                    data: Some(wdata),
                } = self
                    .find_or_create_worker(cx, metadata.clone(), &worker)
                    .await?
                {
                    let w = crate::jobworkerp::service::job_request::Worker::WorkerId(wid);
                    let output = self
                        .enqueue_job_and_get_output(
                            cx,
                            metadata.clone(),
                            w,
                            job_args,
                            job_timeout_sec,
                            using,
                        )
                        .await
                        .inspect_err(|e| {
                            tracing::warn!(
                                "Execute task failed: enqueue job and get output: {:#?}",
                                e
                            )
                        });
                    // use worker one-time
                    if !wdata.use_static {
                        let deleted = self
                            .jobworkerp_client()
                            .worker_client()
                            .await
                            .delete(to_request(&metadata, wid)?)
                            .await?
                            .into_inner();
                        if !deleted.is_success {
                            tracing::warn!("Failed to delete worker: {:#?}", wid);
                        }
                    }
                    output
                } else {
                    Err(anyhow::anyhow!(
                        "Failed to find or create worker: {worker:#?}"
                    ))
                }
            } else {
                Err(anyhow::anyhow!("Not found runner: {name}"))
            }
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
        async move {
            if let Some(Runner {
                id: Some(_sid),
                data: Some(sdata),
            }) = self
                .find_runner_by_name(cx, metadata.clone(), runner_name)
                .await?
            {
                let result_descriptor =
                    JobworkerpProto::parse_result_schema_descriptor(&sdata, using)?;
                let output = self
                    .setup_worker_and_enqueue_with_raw_output(
                        cx,
                        metadata,
                        runner_name,
                        runner_settings,
                        worker_params,
                        job_args,
                        job_timeout_sec,
                        using,
                    )
                    .await?;
                let output: Result<serde_json::Value> = if let Some(desc) = result_descriptor {
                    match ProtobufDescriptor::get_message_from_bytes(desc, &output) {
                        Ok(m) => {
                            let j = ProtobufDescriptor::message_to_json(&m)?;
                            tracing::debug!(
                                "Result schema exists. decode message with proto: {:#?}",
                                j
                            );
                            serde_json::from_str(j.as_str())
                        }
                        Err(e) => {
                            tracing::warn!("Failed to parse result schema: {:#?}", e);
                            serde_json::from_slice(&output)
                        }
                    }
                } else {
                    let text = String::from_utf8_lossy(&output);
                    tracing::debug!("No result schema: {}", text);
                    Ok(serde_json::Value::String(text.to_string()))
                }
                .map_err(|e| anyhow::anyhow!("Failed to parse output: {e:#?}"));
                output
            } else {
                Err(anyhow::anyhow!("Not found runner: {runner_name}"))
            }
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
        async move {
            if let Some(Runner {
                id: Some(_sid),
                data: Some(sdata),
            }) = self
                .find_runner_by_name(cx, metadata.clone(), runner_name)
                .await?
            {
                let runner_settings_descriptor =
                    JobworkerpProto::parse_runner_settings_schema_descriptor(&sdata).map_err(
                        |e| {
                            anyhow::anyhow!(
                                "Failed to parse runner_settings schema descriptor: {e:#?}"
                            )
                        },
                    )?;
                let args_descriptor =
                    JobworkerpProto::parse_job_args_schema_descriptor(&sdata, using).map_err(
                        |e| anyhow::anyhow!("Failed to parse job_args schema descriptor: {e:#?}"),
                    )?;

                let runner_settings = if let Some(ope_desc) = runner_settings_descriptor {
                    tracing::debug!("runner settings schema exists: {:#?}", &runner_settings);
                    runner_settings
                        .map(|j| JobworkerpProto::json_value_to_message(ope_desc, &j, true))
                        .unwrap_or(Ok(vec![]))
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to parse runner_settings schema: {e:#?}")
                        })?
                } else {
                    tracing::debug!("runner settings schema empty");
                    vec![]
                };
                tracing::debug!("job args: {:#?}", &job_args);
                let job_args = if let Some(desc) = args_descriptor.clone() {
                    JobworkerpProto::json_value_to_message(desc, &job_args, true)
                        .map_err(|e| anyhow::anyhow!("Failed to parse job_args schema: {e:#?}"))?
                } else {
                    serde_json::to_string(&job_args)
                        .map_err(|e| anyhow::anyhow!("Failed to serialize job_args: {e:#?}"))?
                        .as_bytes()
                        .to_vec()
                };
                self.setup_worker_and_enqueue(
                    cx,
                    metadata,
                    runner_name,
                    runner_settings,
                    worker_params,
                    job_args,
                    job_timeout_sec,
                    using,
                )
                .await
            } else {
                Err(
                    crate::error::ClientError::NotFound(format!("Not found runner: {runner_name}"))
                        .into(),
                )
            }
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
                anyhow!(
                    "runner_id not found in worker_data for {}",
                    worker_data.name
                )
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
                                .map_err(|e| anyhow!(e))
                        }
                    },
                )
                .await;

            let runner_opt = runner_response_opt_res?;

            if let Some(Runner {
                id: Some(_sid),
                data: Some(sdata),
            }) = runner_opt
            {
                let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(
                    &sdata, using,
                )
                .map_err(|e| anyhow!("Failed to parse job_args schema descriptor: {e:#?}"))?;

                tracing::debug!("job args (json): {:#?}", &job_args);
                let job_args_bytes = if let Some(desc) = args_descriptor.clone() {
                    JobworkerpProto::json_value_to_message(desc, &job_args, true)
                        .map_err(|e| anyhow!("Failed to parse job_args schema: {e:#?}"))?
                } else {
                    serde_json::to_string(&job_args)
                        .map_err(|e| anyhow!("Failed to serialize job_args: {e:#?}"))?
                        .into_bytes()
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
                    JobworkerpProto::parse_result_schema_descriptor(&sdata, using)?;
                if let Some(desc) = result_descriptor {
                    match ProtobufDescriptor::get_message_from_bytes(desc, &output_bytes) {
                        Ok(m) => {
                            let j_str = ProtobufDescriptor::message_to_json(&m)?;
                            tracing::debug!("Result schema exists. decode message with proto: {:#?}", j_str);
                            serde_json::from_str(&j_str)
                                .map_err(|e| anyhow!("Failed to parse JSON from protobuf message: {e:#?}"))
                        }
                        Err(e) => {
                            tracing::warn!("Failed to parse result with proto schema: {:#?}. Trying direct JSON parse.", e);
                            serde_json::from_slice(&output_bytes)
                                .map_err(|e_slice| anyhow!("Failed to parse output as JSON (slice): {e_slice:#?}"))
                        }
                    }
                } else {
                    let text = String::from_utf8_lossy(&output_bytes);
                    tracing::debug!("No result schema, treating as string: {}", text);
                    Ok(serde_json::Value::String(text.into_owned()))
                }
                .map_err(|e| anyhow!("Failed to parse output: {e:#?}"))
            } else {
                Err(anyhow!(
                    "Not found runner with id: {:?} for worker: {}",
                    runner_id,
                    &worker_data.name
                ))
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
                                .map_err(|e| anyhow!(e))
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
                                .map_err(|e| anyhow!(e))
                        }
                    },
                )
                .await
                .context("delete_worker_by_id")
            } else {
                Err(anyhow!("Not found worker to delete: {name}"))
            }
        }
    }
}
