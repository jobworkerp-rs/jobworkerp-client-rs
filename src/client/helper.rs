use super::UseJobworkerpClient;
use crate::jobworkerp::data::{
    JobResultData, Priority, QueueType, ResponseType, ResultStatus, Runner, Worker, WorkerData,
};
use crate::jobworkerp::service::{
    CreateJobResponse, FindListRequest, JobRequest, WorkerNameRequest,
};
use crate::proto::JobworkerpProto;
use anyhow::{anyhow, Context, Result};
use command_utils::protobuf::ProtobufDescriptor;
use command_utils::util::datetime;
use command_utils::util::option::Exists;
use prost::Message;
use std::hash::{DefaultHasher, Hasher};
use tokio_stream::StreamExt;

pub trait UseJobworkerpClientHelper: UseJobworkerpClient + Send + Sync {
    fn find_runner_by_name(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Result<Option<Runner>>> + Send
    where
        Self: Send + Sync,
    {
        async move {
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
                        if item.data.as_ref().exists(|d| d.name == name) {
                            return Ok(Some(item));
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Ok(None)
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
    ) -> impl std::future::Future<Output = Result<JobResultData>> + Send {
        async move {
            let runner = self.find_runner_by_name(runner_name).await?;
            worker_data.runner_id = runner.and_then(|r| r.id);
            tracing::debug!("resolved runner_id: {:?}", &worker_data.runner_id);
            let res = self
                .enqueue_and_get_result_worker_job(&worker_data, args, timeout_sec)
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
    ) -> impl std::future::Future<Output = Result<JobResultData>> + Send {
        async move {
            self.enqueue_worker_job(worker_data, args, timeout_sec)
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
    ) -> impl std::future::Future<Output = Result<Vec<u8>>> + Send {
        async move {
            let res = self
                .enqueue_and_get_result_worker_job(worker_data, args, timeout_sec)
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
                    priority: Some(Priority::High as i32), // higher priority for user slack response
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
                    .ok_or(anyhow!("job result output first is empty: {:?}", res))?
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
    fn setup_worker_and_enqueue(
        &self,
        name: &str,                               // runner(runner) name
        runner_settings: Vec<u8>,                 // runner_settings data
        worker_params: Option<serde_json::Value>, // worker parameters (if not exists, use default values)
        job_args: Vec<u8>,                        // enqueue job args
        job_timeout_sec: u32,                     // job timeout in seconds
    ) -> impl std::future::Future<Output = Result<serde_json::Value>> + Send {
        let name = name.to_owned();
        // let job_args = job_args;
        async move {
            if let Some(Runner {
                id: Some(sid),
                data: Some(sdata),
            }) = self.find_runner_by_name(name.as_str()).await?
            {
                let result_descriptor = JobworkerpProto::parse_result_schema_descriptor(&sdata)?;
                let mut worker: WorkerData =
                    if let Some(serde_json::Value::Object(obj)) = worker_params {
                        // override values with workflow metadata
                        WorkerData {
                            name: obj
                                .get("name")
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                                .unwrap_or_else(|| name.to_string().clone()),
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
                            next_workers: vec![], // XXX use workflow task
                            use_static: obj
                                .get("use_static")
                                .and_then(|v| v.as_bool())
                                .unwrap_or(false),
                            retry_policy: None, //TODO
                        }
                    } else {
                        // default values
                        WorkerData {
                            name: name.to_string().clone(),
                            runner_id: Some(sid),
                            runner_settings,
                            periodic_interval: 0,
                            channel: None,
                            queue_type: QueueType::Normal as i32,
                            response_type: ResponseType::Direct as i32,
                            store_success: false,
                            store_failure: true, //
                            next_workers: vec![],
                            use_static: false,
                            retry_policy: None,
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
                    let output = output?;
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
    fn setup_worker_and_enqueue_with_json(
        &self,
        name: &str,                                 // runner(runner) name
        runner_settings: Option<serde_json::Value>, // runner_settings data
        worker_params: Option<serde_json::Value>, // worker parameters (if not exists, use default values)
        job_args: serde_json::Value,              // enqueue job args
        job_timeout_sec: u32,                     // job timeout in seconds
    ) -> impl std::future::Future<Output = Result<serde_json::Value>> + Send {
        async move {
            if let Some(Runner {
                id: Some(_sid),
                data: Some(sdata),
            }) = self.find_runner_by_name(name).await?
            // TODO local cache (2 times request in this function)
            {
                let runner_settings_descriptor =
                    JobworkerpProto::parse_runner_settings_schema_descriptor(&sdata)?;
                let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(&sdata)?;

                let runner_settings = if let Some(ope_desc) = runner_settings_descriptor {
                    let json = runner_settings.and_then(|json| {
                        serde_json::to_string(&json)
                            .map_err(|e| {
                                anyhow::anyhow!("Failed to serialize runner settings: {:#?}", e)
                            })
                            .ok()
                    });
                    tracing::debug!("runner settings schema exists: {:#?}", json);
                    json.map(|j| JobworkerpProto::json_to_message(ope_desc, &j))
                        .unwrap_or(Ok(vec![]))
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to parse runner_settings schema: {:#?}", e)
                        })?
                } else {
                    tracing::debug!("runner settings schema empty");
                    vec![]
                };
                let serialized_args = job_args.to_string();
                tracing::debug!(
                    "job desc: {:#?}, args: {:#?}",
                    &args_descriptor,
                    &serialized_args
                );
                let job_args = if let Some(desc) = args_descriptor.clone() {
                    JobworkerpProto::json_to_message(desc, &serialized_args)
                } else {
                    Ok(serialized_args.as_bytes().to_vec())
                }?;
                self.setup_worker_and_enqueue(
                    name,            // runner(runner) name
                    runner_settings, // runner_settings data
                    worker_params,   // worker parameters (if not exists, use default values)
                    job_args,        // enqueue job args
                    job_timeout_sec, // job timeout in seconds
                )
                .await
            } else {
                Err(anyhow::anyhow!("Not found runner: {}", name))
            }
        }
    }
}
