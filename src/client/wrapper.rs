#![allow(
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::option_if_let_else,
    clippy::missing_const_for_fn
)]

use super::helper::{
    JobTerminalOutcome, StreamEnqueueFailure, UseJobworkerpClientHelper,
    classify_stream_enqueue_error, collect_stream_result, encode_job_args_against_descriptor,
    ephemeral_worker_name, extract_job_id_from_metadata, extract_job_result_from_metadata,
};
use crate::{
    client::{JobworkerpClient, UseJobworkerpClient},
    command::to_request,
    error::ClientError,
    jobworkerp::data::{JobId, ResultStatus, Runner, RunnerType, WorkerData},
    proto::JobworkerpProto,
};
use anyhow::Result;
use command_utils::{protobuf::ProtobufDescriptor, trace::Tracing};
use futures::future::BoxFuture;
use prost_reflect::MessageDescriptor;
use serde_json::json;
use std::{collections::HashMap, sync::Arc, time::Duration};

// jobworker client wrapper
#[derive(Debug, Clone)]
pub struct JobworkerpClientWrapper {
    pub jobworkerp_client: JobworkerpClient,
}
impl UseJobworkerpClient for JobworkerpClientWrapper {
    fn jobworkerp_client(&self) -> &JobworkerpClient {
        &self.jobworkerp_client
    }
}
impl UseJobworkerpClientHelper for JobworkerpClientWrapper {}
impl Tracing for JobworkerpClientWrapper {}

impl From<JobworkerpClient> for JobworkerpClientWrapper {
    fn from(jobworkerp_client: JobworkerpClient) -> Self {
        Self { jobworkerp_client }
    }
}

impl JobworkerpClientWrapper {
    const DEFAULT_REQUEST_TIMEOUT_SEC: u32 = 1200;

    fn ensure_workflow_context_supported(desc: &prost_reflect::MessageDescriptor) -> Result<()> {
        if desc.get_field_by_name("workflow_context").is_none() {
            anyhow::bail!(
                "WORKFLOW runner on this jobworkerp server does not support workflow_context; upgrade jobworkerp before using prompt context injection"
            );
        }
        Ok(())
    }

    pub async fn new(address: &str, request_timeout_sec: Option<u32>) -> Result<Self> {
        let jobworkerp_client = JobworkerpClient::new(
            address.to_string(),
            request_timeout_sec.map(|s| Duration::from_secs(s.into())),
        )
        .await?;

        Ok(Self { jobworkerp_client })
    }
    pub async fn new_by_env(request_timeout_sec: Option<u32>) -> Result<Self> {
        let jobworkerp_client = JobworkerpClient::new(
            std::env::var("JOBWORKERP_ADDR").expect("JOBWORKERP_ADDR is not set"),
            request_timeout_sec.map(|s| Duration::from_secs(s.into())),
        )
        .await?;

        Ok(Self { jobworkerp_client })
    }
    #[must_use]
    pub fn address(&self) -> &str {
        self.jobworkerp_client.address.as_str()
    }
    #[must_use]
    pub const fn request_timeout(&self) -> Option<Duration> {
        self.jobworkerp_client.request_timeout
    }
    /// WorkerData params used to upsert the ephemeral WORKFLOW worker. Shared by the blocking
    /// (`execute_workflow`) and streaming-first (`execute_workflow_stream_first`) entry points so
    /// they stay in lockstep. `name` must be a per-execution unique name (see
    /// `ephemeral_worker_name`): `worker.name` is the `upsert_by_name` unique key, so a fixed name
    /// would make concurrent workflow executions share — and prematurely delete — one worker row.
    fn workflow_worker_params(name: &str, channel: Option<&str>) -> serde_json::Value {
        json!({
            "name": name,
            "channel": channel,
            "queue_type": "NORMAL",
            "store_success": false,
            "store_failure": true,
            "use_static": false,
        })
    }

    pub async fn execute_workflow(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        workflow_url: &str,
        input: &str,
        channel: Option<&str>,
    ) -> Result<serde_json::Value> {
        self.execute_workflow_with_context(cx, metadata, workflow_url, input, None, channel)
            .await
    }

    pub async fn execute_workflow_with_context(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        workflow_url: &str,
        input: &str,
        workflow_context: Option<&str>,
        channel: Option<&str>,
    ) -> Result<serde_json::Value> {
        let using = Some("run");
        let requires_workflow_context = workflow_context.is_some_and(|context| !context.is_empty());
        let mut job_args = json!({
            "workflow_url": workflow_url,
            "input": input,
        });
        if let Some(context) = workflow_context
            && !context.is_empty()
        {
            job_args["workflow_context"] = json!(context);
        }
        // Per-execution unique name so the ephemeral worker is created and deleted independently
        // of any concurrent workflow execution (see workflow_worker_params).
        let worker_name = ephemeral_worker_name(RunnerType::Workflow.as_str_name());
        let worker_params = Self::workflow_worker_params(&worker_name, channel);
        tracing::debug!("execute_workflow: {:?}", job_args);
        if let Some(Runner {
            id: Some(_sid),
            data: Some(sdata),
        }) = self
            .find_runner_by_name(cx, metadata.clone(), RunnerType::Workflow.as_str_name())
            .await?
        {
            let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(&sdata, using)?;
            let job_args = if let Some(desc) = args_descriptor.clone() {
                if requires_workflow_context {
                    Self::ensure_workflow_context_supported(&desc)?;
                }
                JobworkerpProto::json_value_to_message(desc, &job_args, true, true)
            } else {
                Ok(job_args.to_string().as_bytes().to_vec())
            }
            .inspect_err(|e| tracing::warn!("Failed to parse job_args: {:#?}", e))?;

            let output = self
                .setup_worker_and_enqueue_with_raw_output(
                    cx,
                    metadata,
                    RunnerType::Workflow.as_str_name(),
                    vec![],
                    Some(worker_params),
                    job_args,
                    self.request_timeout()
                        .map_or(Self::DEFAULT_REQUEST_TIMEOUT_SEC, |t| {
                            u32::try_from(t.as_secs()).unwrap_or(Self::DEFAULT_REQUEST_TIMEOUT_SEC)
                        }),
                    using,
                )
                .await?;
            let result_descriptor = JobworkerpProto::parse_result_schema_descriptor(&sdata, using)?;
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
            .map_err(|e| ClientError::ParseError(format!("Failed to parse output: {e:#?}")).into());
            output
        } else {
            tracing::error!("runner not found: WORKFLOW");
            Err(ClientError::NotFound("runner not found: WORKFLOW".to_string()).into())
        }
    }

    /// Execute any pre-registered worker by name with JSON arguments.
    /// Resolves `WorkerData` at runtime via `worker_name` (not `worker_id`, which is unstable).
    pub async fn execute_worker_by_name(
        &self,
        worker_name: &str,
        args_json: serde_json::Value,
        using: Option<&str>,
    ) -> Result<serde_json::Value> {
        let (_worker_id, worker_data) = self
            .find_worker_by_name(None, Arc::new(HashMap::new()), worker_name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Worker not found: worker_name='{worker_name}' on jobworkerp server"
                )
            })?;

        self.enqueue_with_json(
            None,
            Arc::new(HashMap::new()),
            &worker_data,
            args_json,
            self.request_timeout()
                .map_or(Self::DEFAULT_REQUEST_TIMEOUT_SEC, |t| {
                    u32::try_from(t.as_secs()).unwrap_or(Self::DEFAULT_REQUEST_TIMEOUT_SEC)
                }),
            using,
        )
        .await
    }

    /// Streaming-first enqueue of a pre-registered worker by name: resolves the worker by name and
    /// enqueues it via the streaming path so the job_id is available before the job finishes.
    /// Returns the immediate job_id plus the terminal-outcome future.
    pub async fn execute_worker_by_name_stream_first(
        &self,
        worker_name: &str,
        args_json: serde_json::Value,
        using: Option<&str>,
    ) -> Result<(
        Option<JobId>,
        BoxFuture<'static, Result<JobTerminalOutcome<serde_json::Value>>>,
    )> {
        let (_worker_id, worker_data) = self
            .find_worker_by_name(None, Arc::new(HashMap::new()), worker_name)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Worker not found: worker_name='{}' on jobworkerp server",
                    worker_name
                )
            })?;
        self.enqueue_stream_first(worker_data, args_json, using)
            .await
    }

    /// Execute the WORKFLOW runner streaming-first: upserts the WORKFLOW worker (idempotent by
    /// name) and enqueues it via the streaming path so the job_id is available before the workflow
    /// finishes.
    pub async fn execute_workflow_stream_first(
        &self,
        workflow_url: &str,
        input: &str,
        channel: Option<&str>,
    ) -> Result<(
        Option<JobId>,
        BoxFuture<'static, Result<JobTerminalOutcome<serde_json::Value>>>,
    )> {
        let using = Some("run");
        let job_args = json!({ "workflow_url": workflow_url, "input": input });
        // Per-execution unique name so this worker is created and deleted independently of any
        // concurrent workflow execution sharing the same fixed name (see workflow_worker_params).
        let worker_name = ephemeral_worker_name(RunnerType::Workflow.as_str_name());
        let worker_params = Self::workflow_worker_params(&worker_name, channel);
        // Upsert this execution's own WORKFLOW worker and enqueue it via the streaming path.
        let worker = self
            .create_worker_from_runner(
                None,
                Arc::new(HashMap::new()),
                RunnerType::Workflow.as_str_name(),
                vec![],
                Some(worker_params),
            )
            .await?;
        let worker_id = worker.id;
        let worker_data = worker
            .data
            .ok_or_else(|| ClientError::NotFound("WORKFLOW worker data not found".to_string()))?;
        let use_static = worker_data.use_static;
        // If the enqueue setup fails before a job starts (e.g. args encoding error, missing
        // runner_id), the terminal future that normally cleans this worker up is never built, so the
        // ephemeral worker would leak. Delete it here before propagating the error. On success the
        // terminal future owns the cleanup (the worker must outlive the running job).
        let (job_id, terminal) = match self
            .enqueue_stream_first(worker_data, job_args, using)
            .await
        {
            Ok(enqueued) => enqueued,
            Err(e) => {
                if let Some(worker_id) = worker_id {
                    self.delete_non_static_worker(Arc::new(HashMap::new()), worker_id, use_static)
                        .await;
                }
                return Err(e);
            }
        };

        // The WORKFLOW worker is ephemeral (per-execution unique name, use_static=false): delete it
        // once the job reaches a terminal state, mirroring the blocking
        // `enqueue_with_ephemeral_worker` cleanup so the streaming path does not leak a per-execution
        // worker (and its runtime channel) into the worker table. Deletion is deferred into the
        // terminal future because the worker must outlive the running job, and is keyed by worker id
        // so it only removes this execution's worker. Best-effort; never overrides the job outcome.
        let terminal = match worker_id {
            Some(worker_id) => {
                let client = self.clone();
                let fut: BoxFuture<'static, Result<JobTerminalOutcome<serde_json::Value>>> =
                    Box::pin(async move {
                        let outcome = terminal.await;
                        client
                            .delete_non_static_worker(
                                Arc::new(HashMap::new()),
                                worker_id,
                                use_static,
                            )
                            .await;
                        outcome
                    });
                fut
            }
            None => terminal,
        };
        Ok((job_id, terminal))
    }

    /// Streaming-first enqueue: try `enqueue_for_stream` so the assigned `job_id` is available
    /// immediately (from the response header), before the job reaches a terminal state, then
    /// resolve the terminal outcome by consuming the stream. Returns the immediate `job_id`
    /// (when streaming succeeds) plus a future that resolves to the terminal `JobTerminalOutcome`.
    ///
    /// When the runner does not support streaming (`check_worker_streaming` rejects the
    /// `enqueue_for_stream`), this transparently falls back to the blocking Direct
    /// `enqueue_with_json_and_job_id`; in that case the returned immediate `job_id` is `None`
    /// (it is only known once the terminal future resolves).
    pub async fn enqueue_stream_first(
        &self,
        worker_data: WorkerData,
        args_json: serde_json::Value,
        using: Option<&str>,
    ) -> Result<(
        Option<JobId>,
        BoxFuture<'static, Result<JobTerminalOutcome<serde_json::Value>>>,
    )> {
        let timeout_sec = self
            .request_timeout()
            .map(|t| t.as_secs() as u32)
            .unwrap_or(Self::DEFAULT_REQUEST_TIMEOUT_SEC);
        // Own `using` so the fallback future (which may outlive this call) can capture it.
        let using_owned = using.map(std::string::ToString::to_string);

        // Resolve the runner's args/result schema, then encode args against the args schema
        // (same as the Direct path does).
        let runner_id = worker_data.runner_id.ok_or_else(|| {
            ClientError::InvalidParameter(format!(
                "runner_id not found in worker_data for {}",
                worker_data.name
            ))
        })?;
        let (args_bytes, result_descriptor) = self
            .encode_job_args_and_result_descriptor(&runner_id, &args_json, using)
            .await?;

        let raw = self
            .enqueue_stream_worker_job_raw(
                Arc::new(HashMap::new()),
                &worker_data,
                args_bytes,
                timeout_sec,
                None,
                None,
                using,
            )
            .await;

        match raw {
            Ok((meta, mut stream)) => {
                let job_id = extract_job_id_from_metadata(&meta);
                // Prefer the terminal JobResult carried on the initial header when present; the
                // status may still be provisional, so the stream trailer / follow-up find below
                // takes precedence when it yields a terminal JobResult.
                let header_result = extract_job_result_from_metadata(&meta);
                let client = self.clone();
                // `job_id` is `Option<JobId>` (Copy): the future captures it by copy, so it remains
                // available for the returned tuple below.
                let fut: BoxFuture<'static, Result<JobTerminalOutcome<serde_json::Value>>> =
                    Box::pin(async move {
                        let output =
                            collect_stream_result(&mut stream, result_descriptor.as_ref()).await?;
                        // Terminal status: trailer JobResult > initial header JobResult >
                        // find_list_by_job_id > Success (workflow leaves no result only on success).
                        let trailer_result = match stream.trailers().await {
                            Ok(Some(tr)) => extract_job_result_from_metadata(&tr),
                            _ => None,
                        };
                        let status = client
                            .resolve_terminal_status(trailer_result.or(header_result), job_id)
                            .await;
                        Ok(JobTerminalOutcome {
                            job_id,
                            status,
                            output,
                        })
                    });
                Ok((job_id, fut))
            }
            Err(status) => match classify_stream_enqueue_error(&status) {
                StreamEnqueueFailure::Terminal {
                    job_id,
                    status: result_status,
                    output,
                } => {
                    // The outcome is already fully resolved here; wrap it in a ready future rather
                    // than allocating an async block that immediately returns.
                    let outcome = JobTerminalOutcome {
                        job_id,
                        status: result_status,
                        output: serde_json::Value::String(
                            String::from_utf8_lossy(&output).into_owned(),
                        ),
                    };
                    let fut: BoxFuture<'static, Result<JobTerminalOutcome<serde_json::Value>>> =
                        Box::pin(futures::future::ready(Ok(outcome)));
                    Ok((job_id, fut))
                }
                StreamEnqueueFailure::NotStreamable(e) => {
                    // Runner does not support streaming: fall back to the blocking Direct path.
                    // job_id is only known once that terminal future resolves.
                    tracing::debug!("stream enqueue not available, falling back to Direct: {e}");
                    let client = self.clone();
                    let fut: BoxFuture<'static, Result<JobTerminalOutcome<serde_json::Value>>> =
                        Box::pin(async move {
                            client
                                .enqueue_with_json_and_job_id(
                                    None,
                                    Arc::new(HashMap::new()),
                                    &worker_data,
                                    args_json,
                                    timeout_sec,
                                    using_owned.as_deref(),
                                )
                                .await
                        });
                    Ok((None, fut))
                }
            },
        }
    }

    /// Resolve the runner by id, encode JSON job args against its job_args schema, and return the
    /// encoded bytes together with the result schema descriptor (used to decode the stream output).
    async fn encode_job_args_and_result_descriptor(
        &self,
        runner_id: &crate::jobworkerp::data::RunnerId,
        job_args: &serde_json::Value,
        using: Option<&str>,
    ) -> Result<(Vec<u8>, Option<MessageDescriptor>)> {
        let runner = self
            .jobworkerp_client()
            .runner_client()
            .await
            .find(to_request(&HashMap::new(), *runner_id)?)
            .await
            .map(|r| r.into_inner().data)
            .map_err(ClientError::from_tonic_status)?
            .ok_or_else(|| ClientError::NotFound(format!("runner not found: id={runner_id:?}")))?;
        let rdata = runner
            .data
            .ok_or_else(|| ClientError::NotFound("runner data not found".to_string()))?;
        let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(&rdata, using)?;
        let result_descriptor = JobworkerpProto::parse_result_schema_descriptor(&rdata, using)?;
        let args_bytes = encode_job_args_against_descriptor(args_descriptor, job_args)?;
        Ok((args_bytes, result_descriptor))
    }

    /// Determine the terminal status of a streaming job. Prefer a terminal `JobResult` recovered
    /// from the stream trailer / header; otherwise look it up via `find_list_by_job_id`; if no
    /// result was persisted, treat as Success (workflow workers run with store_failure=true, so a
    /// missing result implies success).
    async fn resolve_terminal_status(
        &self,
        terminal_result: Option<crate::jobworkerp::data::JobResult>,
        job_id: Option<JobId>,
    ) -> ResultStatus {
        if let Some(status) = terminal_result.and_then(|r| r.data).map(|d| d.status()) {
            return status;
        }
        if let Some(jid) = job_id {
            let fetched = self.find_first_job_result(jid).await;
            if let Ok(Some(result)) = fetched
                && let Some(d) = result.data
            {
                return d.status();
            }
        }
        ResultStatus::Success
    }

    /// Fetch the first persisted JobResult for a job_id (first item of the FindListByJobId stream).
    async fn find_first_job_result(
        &self,
        job_id: JobId,
    ) -> Result<Option<crate::jobworkerp::data::JobResult>> {
        use crate::jobworkerp::service::FindListByJobIdRequest;
        let request = FindListByJobIdRequest {
            job_id: Some(job_id),
        };
        let response = self
            .jobworkerp_client()
            .job_result_client()
            .await
            .find_list_by_job_id(to_request(&HashMap::new(), request)?)
            .await
            .map_err(ClientError::from_tonic_status)?;
        let mut stream = response.into_inner();
        // First persisted result, or None on stream-end / error.
        Ok(stream.message().await.ok().flatten())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Requires a running jobworkerp at localhost:9000 with the COMMAND runner. Verifies that the
    // streaming-first enqueue returns the job_id immediately (well before the long-running job
    // terminates), which is the property the enqueue-time ExecutionRef recording relies on.
    // Run with: cargo test -p jobworkerp-client stream_first_returns_job_id_before_terminal -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn stream_first_returns_job_id_before_terminal() {
        use crate::client::helper::UseJobworkerpClientHelper;
        use crate::jobworkerp::data::{QueueType, ResponseType, RetryType, WorkerData};
        use std::time::Instant;

        let client = JobworkerpClientWrapper::new("http://localhost:9000", Some(60))
            .await
            .expect("connect jobworkerp");

        // Resolve COMMAND runner id and upsert a streaming-capable worker.
        let runner = client
            .find_runner_by_name(None, Arc::new(HashMap::new()), "COMMAND")
            .await
            .expect("find COMMAND runner")
            .expect("COMMAND runner exists");
        let runner_id = runner.id.expect("runner id");
        let worker = client
            .upsert_worker(
                None,
                Arc::new(HashMap::new()),
                &WorkerData {
                    name: "test-stream-first-verify".to_string(),
                    runner_id: Some(runner_id),
                    runner_settings: vec![],
                    response_type: ResponseType::Direct as i32,
                    queue_type: QueueType::Normal as i32,
                    store_success: false,
                    store_failure: true,
                    retry_policy: Some(crate::jobworkerp::data::RetryPolicy {
                        r#type: RetryType::None as i32,
                        basis: 2.0,
                        ..Default::default()
                    }),
                    broadcast_results: true,
                    ..Default::default()
                },
            )
            .await
            .expect("upsert worker");
        let worker_data = worker.data.clone().expect("worker data");

        let started = Instant::now();
        let (job_id, terminal) = client
            .enqueue_stream_first(
                worker_data,
                serde_json::json!({"command": "sleep", "args": ["5"]}),
                None,
            )
            .await
            .expect("stream-first enqueue");

        // The job_id must be available well before the 5s sleep finishes.
        let enqueue_elapsed = started.elapsed();
        assert!(
            job_id.is_some(),
            "job_id must be returned immediately from the streaming header"
        );
        assert!(
            enqueue_elapsed.as_secs() < 3,
            "job_id should be returned before the job terminates (elapsed: {enqueue_elapsed:?})"
        );

        // The terminal outcome resolves only after the job completes (~5s).
        let outcome = terminal.await.expect("terminal outcome");
        assert_eq!(outcome.job_id, job_id, "terminal job_id matches the header");
        assert!(
            started.elapsed().as_secs() >= 4,
            "terminal outcome should resolve after the sleep completes"
        );
        assert!(outcome.is_success(), "sleep 5 should succeed");

        // Cleanup the worker.
        if let Some(wid) = worker.id {
            let _ = client
                .jobworkerp_client()
                .worker_client()
                .await
                .delete(to_request(&HashMap::new(), wid).unwrap())
                .await;
        }
    }

    // Requires a running jobworkerp at localhost:9000 with the COMMAND runner. Verifies the
    // worker cleanup shared by the streaming-first path: a use_static=false worker is deleted,
    // while a use_static=true worker is left intact.
    // Run with: cargo test -p jobworkerp-client delete_non_static_worker_removes_only_non_static -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn delete_non_static_worker_removes_only_non_static() {
        use crate::client::helper::UseJobworkerpClientHelper;
        use crate::jobworkerp::data::WorkerData;

        let client = JobworkerpClientWrapper::new("http://localhost:9000", Some(60))
            .await
            .expect("connect jobworkerp");
        let runner = client
            .find_runner_by_name(None, Arc::new(HashMap::new()), "COMMAND")
            .await
            .expect("find COMMAND runner")
            .expect("COMMAND runner exists");
        let runner_id = runner.id.expect("runner id");

        let upsert = |name: &str, use_static: bool| {
            let client = client.clone();
            let data = WorkerData {
                name: name.to_string(),
                runner_id: Some(runner_id),
                use_static,
                ..Default::default()
            };
            async move {
                client
                    .upsert_worker(None, Arc::new(HashMap::new()), &data)
                    .await
                    .expect("upsert worker")
            }
        };
        let exists = |name: &'static str| {
            let client = client.clone();
            async move {
                client
                    .find_worker_by_name(None, Arc::new(HashMap::new()), name)
                    .await
                    .expect("find worker")
                    .is_some()
            }
        };

        // Non-static worker: deleted by delete_non_static_worker.
        let ephemeral = upsert("test-non-static-delete", false).await;
        let ephemeral_id = ephemeral.id.expect("worker id");
        assert!(exists("test-non-static-delete").await);
        client
            .delete_non_static_worker(Arc::new(HashMap::new()), ephemeral_id, false)
            .await;
        assert!(
            !exists("test-non-static-delete").await,
            "non-static worker must be deleted"
        );

        // Static worker: left intact (no-op).
        let static_worker = upsert("test-static-keep", true).await;
        let static_id = static_worker.id.expect("worker id");
        client
            .delete_non_static_worker(Arc::new(HashMap::new()), static_id, true)
            .await;
        assert!(
            exists("test-static-keep").await,
            "static worker must be left intact"
        );

        // Cleanup the static worker.
        let _ = client
            .jobworkerp_client()
            .worker_client()
            .await
            .delete(to_request(&HashMap::new(), static_id).unwrap())
            .await;
    }
}

#[cfg(test)]
mod workflow_context_tests {
    use super::JobworkerpClientWrapper;
    use command_utils::protobuf::ProtobufDescriptor;

    #[test]
    fn workflow_context_support_check_rejects_legacy_args_schema() {
        let proto = r#"
        syntax = "proto3";
        message WorkflowRunArgs {
          string input = 3;
        }
        "#;
        let descriptor = ProtobufDescriptor::new(&proto.to_string()).unwrap();
        let msg = descriptor.get_message_by_name("WorkflowRunArgs").unwrap();
        let err = JobworkerpClientWrapper::ensure_workflow_context_supported(&msg).unwrap_err();
        assert!(
            err.to_string()
                .contains("does not support workflow_context")
        );
    }

    #[test]
    fn workflow_context_support_check_accepts_current_args_schema() {
        let proto = r#"
        syntax = "proto3";
        message WorkflowRunArgs {
          string input = 3;
          optional string workflow_context = 4;
        }
        "#;
        let descriptor = ProtobufDescriptor::new(&proto.to_string()).unwrap();
        let msg = descriptor.get_message_by_name("WorkflowRunArgs").unwrap();
        JobworkerpClientWrapper::ensure_workflow_context_supported(&msg).unwrap();
    }
}
