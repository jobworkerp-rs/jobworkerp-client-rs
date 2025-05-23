use super::UseJobworkerpClient;
use crate::jobworkerp::data::{
    JobResultData, Priority, QueueType, ResponseType, ResultStatus, RetryPolicy, RetryType, Runner,
    Worker, WorkerData, WorkerId,
};
use crate::jobworkerp::function::data::FunctionSpecs;
use crate::jobworkerp::function::service::{FindFunctionRequest, FindFunctionSetRequest};
use crate::jobworkerp::service::{
    CreateJobResponse, FindListRequest, JobRequest, OptionalRunnerResponse, WorkerNameRequest,
};
use crate::proto::JobworkerpProto;
use anyhow::{anyhow, Context, Result};
use command_utils::cache_ok;
use command_utils::protobuf::ProtobufDescriptor;
use command_utils::util::datetime;
use command_utils::util::scoped_cache::ScopedCache;
use std::hash::{DefaultHasher, Hasher};
use tokio_stream::StreamExt;

pub const DEFAULT_RETRY_POLICY: RetryPolicy = RetryPolicy {
    r#type: RetryType::Exponential as i32,
    interval: 800,
    max_interval: 60000,
    max_retry: 1,
    basis: 2.0,
};
pub trait UseJobworkerpClientHelper: UseJobworkerpClient + Send + Sync {
    fn find_function_list(
        &self,
        exclude_runner: bool,
        exclude_worker: bool,
    ) -> impl std::future::Future<Output = Result<Vec<FunctionSpecs>>> + Send {
        async move {
            let response = self
                .jobworkerp_client()
                .function_client()
                .await
                .find_list(tonic::Request::new(FindFunctionRequest {
                    exclude_runner,
                    exclude_worker,
                }))
                .await?;
            let mut functions = Vec::new();
            let mut stream = response.into_inner();
            while let Some(t) = stream.next().await {
                match t {
                    Ok(t) => {
                        functions.push(t);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(functions)
        }
    }
    fn find_function_list_by_set(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Vec<FunctionSpecs>>> + Send {
        async move {
            let response = self
                .jobworkerp_client()
                .function_client()
                .await
                .find_list_by_set(tonic::Request::new(FindFunctionSetRequest {
                    name: name.to_string(),
                }))
                .await?;
            let mut functions = Vec::new();
            let mut stream = response.into_inner();
            while let Some(t) = stream.next().await {
                match t {
                    Ok(t) => {
                        functions.push(t);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(functions)
        }
    }
    fn find_runner_by_name_with_cache(
        &self,
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
                self.find_runner_by_name(name)
            )
        }
    }
    fn find_runner_by_name(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Option<Runner>>> + Send
    where
        Self: Send + Sync,
    {
        async move {
            // TODO memory cache
            let mut client = self.jobworkerp_client().runner_client().await;
            // TODO find by name
            let mut stream = client
                .find_list(tonic::Request::new(FindListRequest {
                    limit: None,
                    offset: None,
                }))
                .await?
                .into_inner();
            while let Some(item) = stream.next().await {
                match item {
                    Ok(item) => {
                        if item.data.as_ref().is_some_and(|d| d.name == name) {
                            return Ok(Some(item));
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(None)
        }
    }
    // delegate
    fn find_worker_by_name(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Option<(WorkerId, WorkerData)>>> + Send
    where
        Self: Send + Sync,
    {
        async move {
            let mut worker_cli = self.jobworkerp_client().worker_client().await;
            let worker = worker_cli
                .find_by_name(WorkerNameRequest {
                    name: name.to_string(),
                })
                .await?
                .into_inner()
                .data;
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
        }
    }
    fn find_or_create_worker(
        &self,
        worker_data: &WorkerData,
    ) -> impl std::future::Future<Output = Result<Worker>> + Send
    where
        Self: Send + Sync,
    {
        async move {
            let mut worker_cli = self.jobworkerp_client().worker_client().await;

            let worker = worker_cli
                .find_by_name(WorkerNameRequest {
                    name: worker_data.name.clone(),
                })
                .await?
                .into_inner()
                .data;

            // if not found, create sentence embedding worker
            let worker = if let Some(w) = worker {
                w
            } else {
                tracing::debug!("worker {} not found. create new worker", &worker_data.name,);
                let wid = worker_cli
                    .create(worker_data.clone())
                    .await?
                    .into_inner()
                    .id
                    .ok_or(anyhow!("create worker response is empty?"))?;
                // wait for worker creation? (replica db)
                // tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                Worker {
                    id: Some(wid),
                    data: Some(worker_data.to_owned()),
                }
            };
            Ok(worker)
        }
    }

    // fillin runner_id and enqueue job and get result data for worker
    fn enqueue_and_get_result_worker_job_with_runner(
        &self,
        runner_name: &str,
        mut worker_data: WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
    ) -> impl std::future::Future<Output = Result<JobResultData>> + Send {
        async move {
            let runner = self.find_runner_by_name(runner_name).await?;
            worker_data.runner_id = runner.and_then(|r| r.id);
            tracing::debug!("resolved runner_id: {:?}", &worker_data.runner_id);
            let res = self
                .enqueue_and_get_result_worker_job(
                    &worker_data,
                    args,
                    timeout_sec,
                    run_after_time,
                    priority,
                )
                .await?;
            if res.status() == ResultStatus::Success {
                Ok(res)
            } else {
                Err(anyhow!(
                    "job failed: {:?}",
                    res.output.and_then(|o| o
                        .items
                        .first()
                        .cloned()
                        .map(|e| String::from_utf8_lossy(&e).into_owned()))
                ))
            }
        }
    }
    // enqueue job and get result data for worker
    fn enqueue_and_get_result_worker_job(
        &self,
        worker_data: &WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
    ) -> impl std::future::Future<Output = Result<JobResultData>> + Send {
        async move {
            self.enqueue_worker_job(worker_data, args, timeout_sec, run_after_time, priority)
                .await?
                .result
                .ok_or(anyhow!("result not found"))?
                .data
                .ok_or(anyhow!("result data not found"))
        }
    }
    // enqueue job and get only output data for worker
    fn enqueue_and_get_output_worker_job(
        &self,
        worker_data: &WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        async move {
            let res = self
                .enqueue_and_get_result_worker_job(
                    worker_data,
                    args,
                    timeout_sec,
                    run_after_time,
                    priority,
                )
                .await?;
            if res.status() == ResultStatus::Success && res.output.is_some() {
                // output is Vec<Vec<u8>> but actually 1st Vec<u8> is valid.
                let output = res
                    .output
                    .as_ref()
                    .ok_or(anyhow!("job result output is empty: {:?}", res))?
                    .items
                    .first()
                    .ok_or(anyhow!(
                        "{} job result output first is empty: {:?}",
                        &worker_data.name,
                        res
                    ))?
                    .to_owned();
                Ok(output)
            } else {
                Err(anyhow!(
                    "job failed: {:?}",
                    res.output.and_then(|o| o
                        .items
                        .first()
                        .cloned()
                        .map(|e| String::from_utf8_lossy(&e).into_owned()))
                ))
            }
        }
    }
    // enqueue job for worker (use find_or_create_worker)
    fn enqueue_worker_job(
        &self,
        worker_data: &WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
    ) -> impl std::future::Future<Output = Result<CreateJobResponse>> + Send {
        async move {
            let worker = self.find_or_create_worker(worker_data).await?;
            let mut job_cli = self.jobworkerp_client().job_client().await;
            job_cli
                .enqueue(JobRequest {
                    args,
                    timeout: Some((timeout_sec * 1000).into()),
                    worker: Some(crate::jobworkerp::service::job_request::Worker::WorkerId(
                        worker.id.unwrap(),
                    )),
                    priority: priority.map(|p| p as i32), // higher priority for user slack response
                    run_after_time,
                    ..Default::default()
                })
                .await
                .map(|r| r.into_inner())
                .context("enqueue_worker_job")
        }
    }
    // enqueue job for worker (use find_or_create_worker)
    fn enqueue_stream_worker_job(
        &self,
        worker_data: &WorkerData,
        args: Vec<u8>,
        timeout_sec: u32,
        run_after_time: Option<i64>,
        priority: Option<Priority>,
    ) -> impl std::future::Future<
        Output = Result<tonic::Streaming<crate::jobworkerp::data::ResultOutputItem>>,
    > + Send {
        async move {
            let worker = self.find_or_create_worker(worker_data).await?;
            let mut job_cli = self.jobworkerp_client().job_client().await;
            job_cli
                .enqueue_for_stream(JobRequest {
                    args,
                    timeout: Some((timeout_sec * 1000).into()),
                    worker: Some(crate::jobworkerp::service::job_request::Worker::WorkerId(
                        worker.id.unwrap(),
                    )),
                    priority: priority.map(|p| p as i32), // higher priority for user slack response
                    run_after_time,
                    ..Default::default()
                })
                .await
                .map(|r| r.into_inner())
                .context("enqueue_worker_job")
        }
    }
    // enqueue job for worker and get output data
    fn enqueue_job_and_get_output(
        &self,
        worker: crate::jobworkerp::service::job_request::Worker,
        args: Vec<u8>,
        timeout_sec: u32,
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        async move {
            let mut job_cli = self.jobworkerp_client().job_client().await;
            tracing::debug!("enqueue_job_and_get_output: {:?}", &worker);
            let res = job_cli
                .enqueue(JobRequest {
                    args,
                    timeout: Some((timeout_sec * 1000).into()),
                    worker: Some(worker),
                    priority: Some(Priority::High as i32), // higher priority for user slack response
                    ..Default::default()
                })
                .await
                .map(|r| r.into_inner())
                .context("enqueue_worker_job")?
                .result
                .ok_or(anyhow!("result not found"))?
                .data
                .ok_or(anyhow!("result data not found"))?;
            if res.status() == ResultStatus::Success && res.output.is_some() {
                // output is Vec<Vec<u8>> but actually 1st Vec<u8> is valid.
                let output = res
                    .output
                    .as_ref()
                    .ok_or(anyhow!("job result output is empty: {:?}", res))?
                    .items
                    .first()
                    .unwrap_or(&vec![])
                    .to_owned();
                Ok(output)
            } else {
                Err(anyhow!(
                    "job failed: {:?}",
                    res.output.and_then(|o| o
                        .items
                        .first()
                        .cloned()
                        .map(|e| String::from_utf8_lossy(&e).into_owned()))
                ))
            }
        }
    }

    // enqueue job for worker and get result data
    // if worker not exists, create worker (use temporary worker name and delete worker after process if not use_static)
    // argument is json string or plain string (serce_json::Value::String)
    // return value is json string
    fn setup_worker_and_enqueue_with_raw_output(
        &self,
        name: &str,                               // runner(runner) name
        runner_settings: Vec<u8>,                 // runner_settings data
        worker_params: Option<serde_json::Value>, // worker parameters (if not exists, use default values)
        job_args: Vec<u8>,                        // enqueue job args
        job_timeout_sec: u32,                     // job timeout in seconds
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        let name = name.to_owned();
        // let job_args = job_args;
        async move {
            if let Some(Runner {
                id: Some(sid),
                data: Some(_sdata),
            }) = self.find_runner_by_name(name.as_str()).await?
            {
                let mut worker: WorkerData =
                    if let Some(serde_json::Value::Object(obj)) = worker_params {
                        // override values with workflow metadata
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
                                .unwrap_or(true), //
                            use_static: obj
                                .get("use_static")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false),
                            retry_policy: Some(DEFAULT_RETRY_POLICY), //TODO
                            broadcast_results: true,
                        }
                    } else {
                        // default values
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
                            store_failure: true, //
                            use_static: false,
                            retry_policy: Some(DEFAULT_RETRY_POLICY), //TODO
                            broadcast_results: true,
                        }
                    };
                // random name (temporary name for not static worker)
                if !worker.use_static {
                    let mut hasher = DefaultHasher::default();
                    hasher.write_i64(datetime::now_millis());
                    hasher.write_i64(rand::random()); // random
                    worker.name = format!("{}_{:x}", worker.name, hasher.finish());
                    tracing::debug!("Worker name with hash: {}", &worker.name);
                }
                // TODO unwind and delete not static worker if failed to enqueue job
                if let Worker {
                    id: Some(wid),
                    data: Some(wdata),
                } = self.find_or_create_worker(&worker).await?
                {
                    let w = crate::jobworkerp::service::job_request::Worker::WorkerId(wid);
                    let output = self
                        .enqueue_job_and_get_output(w, job_args, job_timeout_sec)
                        .await
                        .inspect_err(|e| {
                            tracing::warn!(
                                "Execute task failed: enqueue job and get output: {:#?}",
                                e
                            )
                        });
                    // use worker one-time
                    // XXX use_static means static worker in jobworkerp, not in workflow (but use as a temporary worker or not)
                    if !wdata.use_static {
                        let deleted = self
                            .jobworkerp_client()
                            .worker_client()
                            .await
                            .delete(wid)
                            .await?
                            .into_inner();
                        if !deleted.is_success {
                            tracing::warn!("Failed to delete worker: {:#?}", wid);
                        }
                    }
                    // eval error after deleting worker
                    // let output = output?;
                    // let output: Result<serde_json::Value> = if let Some(desc) = result_descriptor {
                    //     match ProtobufDescriptor::get_message_from_bytes(desc, &output) {
                    //         Ok(m) => {
                    //             let j = ProtobufDescriptor::message_to_json(&m)?;
                    //             tracing::debug!(
                    //                 "Result schema exists. decode message with proto: {:#?}",
                    //                 j
                    //             );
                    //             serde_json::from_str(j.as_str())
                    //         }
                    //         Err(e) => {
                    //             tracing::warn!("Failed to parse result schema: {:#?}", e);
                    //             serde_json::from_slice(&output)
                    //         }
                    //     }
                    // } else {
                    //     let text = String::from_utf8_lossy(&output);
                    //     tracing::debug!("No result schema: {}", text);
                    //     Ok(serde_json::Value::String(text.to_string()))
                    // }
                    // .map_err(|e| anyhow::anyhow!("Failed to parse output: {:#?}", e));
                    output
                } else {
                    Err(anyhow::anyhow!(
                        "Failed to find or create worker: {:#?}",
                        worker
                    ))
                }
            } else {
                Err(anyhow::anyhow!("Not found runner: {}", name))
            }
        }
    }
    fn setup_worker_and_enqueue(
        &self,
        runner_name: &str,                        // runner(runner) name
        runner_settings: Vec<u8>,                 // runner_settings data
        worker_params: Option<serde_json::Value>, // worker parameters (if not exists, use default values)
        job_args: Vec<u8>,                        // enqueue job args
        job_timeout_sec: u32,                     // job timeout in seconds
    ) -> impl std::future::Future<Output = Result<serde_json::Value>> + Send {
        async move {
            // use memory cache?
            if let Some(Runner {
                id: Some(_sid),
                data: Some(sdata),
            }) = self.find_runner_by_name(runner_name).await?
            {
                let result_descriptor = JobworkerpProto::parse_result_schema_descriptor(&sdata)?;
                let output = self
                    .setup_worker_and_enqueue_with_raw_output(
                        runner_name,
                        runner_settings,
                        worker_params,
                        job_args,
                        job_timeout_sec,
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
                .map_err(|e| anyhow::anyhow!("Failed to parse output: {:#?}", e));
                output
            // Ok(output)
            } else {
                Err(anyhow::anyhow!("Not found runner: {}", runner_name))
            }
        }
    }

    fn setup_worker_and_enqueue_with_json(
        &self,
        runner_name: &str,                          // runner(runner) name
        runner_settings: Option<serde_json::Value>, // runner_settings data
        worker_params: Option<serde_json::Value>, // worker parameters (if not exists, use default values)
        job_args: serde_json::Value,              // enqueue job args
        job_timeout_sec: u32,                     // job timeout in seconds
    ) -> impl std::future::Future<Output = Result<serde_json::Value>> + Send {
        async move {
            if let Some(Runner {
                id: Some(_sid),
                data: Some(sdata),
            }) = self.find_runner_by_name(runner_name).await?
            // TODO local cache? (2 times request in this function)
            {
                let runner_settings_descriptor =
                    JobworkerpProto::parse_runner_settings_schema_descriptor(&sdata).map_err(
                        |e| {
                            anyhow::anyhow!(
                                "Failed to parse runner_settings schema descriptor: {:#?}",
                                e
                            )
                        },
                    )?;
                let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(&sdata)
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to parse job_args schema descriptor: {:#?}", e)
                    })?;

                let runner_settings = if let Some(ope_desc) = runner_settings_descriptor {
                    tracing::debug!("runner settings schema exists: {:#?}", &runner_settings);
                    runner_settings
                        .map(|j| JobworkerpProto::json_value_to_message(ope_desc, &j, true))
                        .unwrap_or(Ok(vec![]))
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to parse runner_settings schema: {:#?}", e)
                        })?
                } else {
                    tracing::debug!("runner settings schema empty");
                    vec![]
                };
                tracing::debug!("job args: {:#?}", &job_args);
                let job_args = if let Some(desc) = args_descriptor.clone() {
                    JobworkerpProto::json_value_to_message(desc, &job_args, true)
                        .map_err(|e| anyhow::anyhow!("Failed to parse job_args schema: {:#?}", e))?
                } else {
                    serde_json::to_string(&job_args)
                        .map_err(|e| anyhow::anyhow!("Failed to serialize job_args: {:#?}", e))?
                        .as_bytes()
                        .to_vec()
                };
                self.setup_worker_and_enqueue(
                    runner_name,     // runner(runner) name
                    runner_settings, // runner_settings data
                    worker_params,   // worker parameters (if not exists, use default values)
                    job_args,        // enqueue job args
                    job_timeout_sec, // job timeout in seconds
                )
                .await
            } else {
                Err(crate::error::ClientError::NotFound(format!(
                    "Not found runner: {}",
                    runner_name
                ))
                .into())
            }
        }
    }

    fn enqueue_with_json(
        &self,
        worker_data: &WorkerData,
        job_args: serde_json::Value, // enqueue job args
        job_timeout_sec: u32,        // job timeout in seconds
    ) -> impl std::future::Future<Output = Result<serde_json::Value>> + Send {
        async move {
            let runner_id = worker_data
                .runner_id
                .ok_or(anyhow!("runner_id not found"))?;
            if let OptionalRunnerResponse {
                data:
                    Some(Runner {
                        id: Some(_sid),
                        data: Some(sdata),
                    }),
            } = self
                .jobworkerp_client()
                .runner_client()
                .await
                .find(runner_id)
                .await?
                .into_inner()
            // TODO local cache
            {
                let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(&sdata)
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to parse job_args schema descriptor: {:#?}", e)
                    })?;

                tracing::debug!("job args: {:#?}", &job_args);
                let job_args = if let Some(desc) = args_descriptor.clone() {
                    JobworkerpProto::json_value_to_message(desc, &job_args, true)
                        .map_err(|e| anyhow::anyhow!("Failed to parse job_args schema: {:#?}", e))?
                } else {
                    serde_json::to_string(&job_args)
                        .map_err(|e| anyhow::anyhow!("Failed to serialize job_args: {:#?}", e))?
                        .as_bytes()
                        .to_vec()
                };
                let output = self
                    .enqueue_and_get_output_worker_job(
                        worker_data,
                        job_args,
                        job_timeout_sec,
                        None,
                        None,
                    )
                    .await?;
                let result_descriptor = JobworkerpProto::parse_result_schema_descriptor(&sdata)?;
                if let Some(desc) = result_descriptor {
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
                .map_err(|e| anyhow::anyhow!("Failed to parse output: {:#?}", e))
            } else {
                Err(crate::error::ClientError::NotFound(format!(
                    "Not found runner of worker: {}",
                    &worker_data.name
                ))
                .into())
            }
        }
    }

    fn delete_worker_by_name(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<bool>> + Send
    where
        Self: Send + Sync,
    {
        async move {
            // TODO create delete_by_name rpc?
            let mut worker_cli = self.jobworkerp_client().worker_client().await;
            let w = worker_cli
                .find_by_name(WorkerNameRequest {
                    name: name.to_string(),
                })
                .await
                .map(|r| r.into_inner())
                .context("find_worker_by_name")?
                .data;
            if let Some(Worker { id: Some(wid), .. }) = w {
                worker_cli
                    .delete(wid)
                    .await
                    .map(|r| r.into_inner().is_success)
                    .context("delete_worker_by_id")
            } else {
                Err(anyhow::anyhow!("Not found worker: {}", name))
            }
        }
    }
}
