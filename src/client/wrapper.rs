#![allow(
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::option_if_let_else,
    clippy::missing_const_for_fn
)]

use super::helper::{
    JobTerminalOutcome, StreamChunkMerge, StreamEnqueueFailure, UseJobworkerpClientHelper,
    classify_stream_enqueue_error, collect_stream_result_with, encode_job_args_against_descriptor,
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
            // WORKFLOW runner: surface logical failure (Faulted/Cancelled/...) as Err instead of
            // returning a "successful" result whose embedded status is non-success.
            output.and_then(check_workflow_output_status)
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
        let is_workflow_runner = self.is_workflow_runner(&worker_data).await?;
        let args_json =
            shape_args_for_workflow_runner(is_workflow_runner, &worker_data.name, args_json)?;

        let output = self
            .enqueue_with_json(
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
            .await?;
        if is_workflow_runner {
            check_workflow_output_status(output)
        } else {
            Ok(output)
        }
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
        let is_workflow_runner = self.is_workflow_runner(&worker_data).await?;
        let args_json =
            shape_args_for_workflow_runner(is_workflow_runner, &worker_data.name, args_json)?;
        // WORKFLOW workers stream snapshot-style chunks (each chunk is a full WorkflowResult);
        // every other runner type today streams deltas (LLM token chunks). Picking the wrong
        // merge collapses snapshots into garbled concatenations (e.g.
        // `status: "RunningRunningFaulted"`).
        let chunk_merge = if is_workflow_runner {
            StreamChunkMerge::LastWins
        } else {
            StreamChunkMerge::Concat
        };
        let (job_id, terminal) = self
            .enqueue_stream_first(worker_data, args_json, using, chunk_merge)
            .await?;
        let terminal = wrap_terminal_with_workflow_status_check(terminal, is_workflow_runner);
        Ok((job_id, terminal))
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
        // WORKFLOW URL path is always a WORKFLOW worker → snapshot semantics.
        let (job_id, terminal) = match self
            .enqueue_stream_first(worker_data, job_args, using, StreamChunkMerge::LastWins)
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
        // WORKFLOW URL path: the worker is always the WORKFLOW runner, so apply the embedded
        // workflow-status check unconditionally.
        let terminal = wrap_terminal_with_workflow_status_check(terminal, true);
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
        chunk_merge: StreamChunkMerge,
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
                        let output = collect_stream_result_with(
                            &mut stream,
                            result_descriptor.as_ref(),
                            chunk_merge,
                        )
                        .await?;
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

    /// Resolve the worker's runner row and return true when it is the WORKFLOW runner. Used by
    /// both the args-shaping (`shape_args_for_workflow_runner`) and the output-status check
    /// (`check_workflow_output_status`) so they stay in lockstep about which workers count as
    /// WORKFLOW workers.
    async fn is_workflow_runner(&self, worker_data: &WorkerData) -> Result<bool> {
        let Some(runner_id) = worker_data.runner_id else {
            return Ok(false);
        };
        let runner = self
            .jobworkerp_client()
            .runner_client()
            .await
            .find(to_request(&HashMap::new(), runner_id)?)
            .await
            .map(|r| r.into_inner().data)
            .map_err(ClientError::from_tonic_status)?;
        Ok(runner
            .as_ref()
            .and_then(|r| r.data.as_ref())
            .is_some_and(|d| d.runner_type == RunnerType::Workflow as i32))
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

/// `WorkflowResult.status` values that count as **failure** for the purposes of client-side error
/// surfacing — see `runner/protobuf/jobworkerp/runner/workflow_result.proto`. Only `Faulted` is
/// classified as an error here, because the other non-`Completed` statuses are not failures:
///   - `Cancelled` — explicit cancellation initiated by a user; logged but not raised as an error
///     so cancel paths don't get double-reported.
///   - `Waiting`   — paused for an external condition (notably user approval before a
///     `checkpoint`-based resume). Treating this as an error would abort the very flow that is
///     supposed to be suspended for later resumption.
///   - `Pending` / `Running` — should not appear in a terminal `WorkflowResult` per the proto
///     comment ("States below should not appear in final results"); be defensive and pass them
///     through rather than aborting on what is almost certainly a sentinel value.
///
/// Adding more statuses here would risk turning resume-style flows (Waiting) into hard errors.
const FAILURE_WORKFLOW_STATUSES: &[&str] = &["Faulted"];

/// `WorkflowResult.status` values that are expected to appear in a terminal output: a success
/// (`Completed`), the recognised failure (`Faulted`), or a deliberate non-error suspension
/// (`Cancelled`, `Waiting`). Anything else — `Pending` / `Running` per the proto comment, or a
/// status string the server side may add in the future — is a sentinel that should not show up at
/// the terminal boundary. We pass it through (to avoid breaking unknown-but-correct flows) but
/// emit a `warn!` so it surfaces in logs for investigation.
const KNOWN_TERMINAL_WORKFLOW_STATUSES: &[&str] = &["Completed", "Faulted", "Cancelled", "Waiting"];

/// Emit a `warn!` when a `WorkflowResult.status` arrives at the terminal boundary that we did not
/// expect to see there. Helpful for catching server-side regressions (e.g. an in-flight status
/// leaking into the final result) without changing client-side behaviour. Only the WORKFLOW path
/// calls this, and only at the terminal boundary, so the warning never fires for mid-stream
/// events.
fn warn_if_unexpected_terminal_workflow_status(status: &str, context: &str) {
    if KNOWN_TERMINAL_WORKFLOW_STATUSES.contains(&status) {
        return;
    }
    tracing::warn!(
        workflow_status = %status,
        context = %context,
        "unexpected workflow terminal status; passing through (treat as success) but this likely indicates a server-side bug or a status value the client does not yet handle"
    );
}

/// Inspect a workflow output JSON (shape of `WorkflowResult`) and return `Err` only when the
/// workflow reported a hard failure (`Faulted`). Returns `Ok(output)` for `Completed` (success)
/// and for the non-success-but-not-error statuses described on
/// [`FAILURE_WORKFLOW_STATUSES`].
///
/// `JobResult.status == Success` alone is not enough to declare a workflow run successful: a
/// workflow that surfaces `Faulted` inside `WorkflowResult` still returns a protobuf-clean
/// response, so callers (cron, slack handlers) would otherwise log it as success. This collapses
/// the two layers of status into one error decision for client-side callers — without disturbing
/// `Cancelled` / `Waiting` flows that are intentionally non-`Completed`.
fn check_workflow_output_status(output: serde_json::Value) -> Result<serde_json::Value> {
    let Some(status) = output.get("status").and_then(|v| v.as_str()) else {
        return Ok(output);
    };
    warn_if_unexpected_terminal_workflow_status(status, "blocking workflow output");
    if !FAILURE_WORKFLOW_STATUSES.contains(&status) {
        return Ok(output);
    }
    let error_message = extract_workflow_error_message(&output)
        .unwrap_or_else(|| format!("workflow status: {status}"));
    Err(ClientError::RuntimeError(format!("workflow failed ({status}): {error_message}")).into())
}

/// Extract a human-readable diagnostic from a `WorkflowResult`-shaped JSON. Looks for the proto
/// field name (`error_message`) *and* its protobuf-JSON-mapped variant (`errorMessage`) because
/// the descriptor-based decode in `decode_output_to_json` runs through `message_to_json` with the
/// default `SerializeOptions`, which converts snake_case to lowerCamelCase. Reading only the
/// snake form would silently drop the diagnostic for descriptor-decoded outputs and surface only
/// the generic "workflow status: Faulted" message instead of the real validation / runtime error.
/// Falls back to the `output` field (same name in both representations) when no error_message is
/// present.
fn extract_workflow_error_message(output: &serde_json::Value) -> Option<String> {
    output
        .get("error_message")
        .or_else(|| output.get("errorMessage"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            output
                .get("output")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
}

/// Wrap a streaming-first terminal future so that, for WORKFLOW workers, a `ResultStatus::Success`
/// outcome whose embedded `WorkflowResult.status` is `Faulted` is downgraded to
/// `ResultStatus::ErrorAndRetry` with `is_success = false`. This keeps the streaming-first path
/// (cron stream-first, slack handlers) consistent with the blocking path so callers logging
/// `outcome.is_success()` don't treat a Faulted workflow as success.
///
/// The wrapper is a no-op when `is_workflow_runner` is false or the outcome was already a
/// non-success / Err, AND only acts on terminal `Faulted`: `Waiting` (paused for user approval
/// before a checkpoint-based resume) and `Cancelled` (explicit cancellation) are not failures and
/// must keep their original status so resume / cancel flows behave correctly.
fn wrap_terminal_with_workflow_status_check(
    terminal: BoxFuture<'static, Result<JobTerminalOutcome<serde_json::Value>>>,
    is_workflow_runner: bool,
) -> BoxFuture<'static, Result<JobTerminalOutcome<serde_json::Value>>> {
    if !is_workflow_runner {
        return terminal;
    }
    Box::pin(async move {
        let outcome = terminal.await?;
        Ok(downgrade_workflow_outcome_if_failed(outcome))
    })
}

/// Downgrade an otherwise-successful `JobTerminalOutcome` when the workflow body reports
/// `Faulted` — flips `status` to `ErrorAndRetry` so `JobTerminalOutcome::is_success()` agrees with
/// the workflow's view of itself. Leaves `job_id` and `output` intact so the caller can still
/// record / display the failed execution. Non-`Faulted` statuses (including `Waiting`,
/// `Cancelled`) are deliberately preserved as success-equivalent terminal states.
fn downgrade_workflow_outcome_if_failed(
    outcome: JobTerminalOutcome<serde_json::Value>,
) -> JobTerminalOutcome<serde_json::Value> {
    if outcome.status != ResultStatus::Success {
        return outcome;
    }
    let status = outcome
        .output
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    // Empty status here means the streaming output was not WorkflowResult-shaped (e.g. a
    // descriptor mismatch fell back to raw bytes). Treat that as "no signal" and skip the warn,
    // so the noise floor stays low for non-workflow data paths.
    if !status.is_empty() {
        warn_if_unexpected_terminal_workflow_status(status, "stream-first workflow terminal");
    }
    if !FAILURE_WORKFLOW_STATUSES.contains(&status) {
        return outcome;
    }
    JobTerminalOutcome {
        status: ResultStatus::ErrorAndRetry,
        ..outcome
    }
}

// --- WORKFLOW worker args shaping ----------------------------------------------------------
//
// Three concerns are split into three functions so the caller's intent is explicit at the call
// site:
//
//   * `wrap_args_as_workflow_input`        — "treat this JSON as the workflow's input, always
//                                            wrap it as {input: <stringified>}".
//   * `pass_args_as_workflow_run_args`     — "this JSON is already a WorkflowRunArgs, send it
//                                            verbatim" (with a type-shape sanity check so we
//                                            don't silently misinterpret a free-form payload).
//   * `shape_args_for_workflow_runner`     — auto-detect: if the JSON has the type-shape of a
//                                            WorkflowRunArgs, pass through; otherwise wrap.
//
// The pure `looks_like_workflow_run_args` predicate centralises the schema check so the
// auto-detect and the explicit pass-through agree.

/// `WorkflowRunArgs` JSON field keys with their expected protobuf-JSON-mapped value kind. Both
/// the proto-name (`workflow_url`, `from_checkpoint`, ...) and the protobuf-JSON-mapped camelCase
/// (`workflowUrl`, `fromCheckpoint`, ...) forms are accepted because callers may have authored
/// the JSON either way; see `runner/protobuf/jobworkerp/runner/workflow_run_args.proto`.
///
/// A key by itself is not enough — the value at that key must also have the expected kind
/// (string / object), otherwise the entry is a spurious key collision in a free-form workflow
/// input rather than a real `WorkflowRunArgs` field. Without this type guard,
/// `{"input": {"text": "..."}}` (object at a string field) would encode-error, and
/// `{"execution_id": "...", "payload": ...}` would silently send an empty input because
/// `ignore_unknown_fields=true` drops `payload`.
#[derive(Debug, Clone, Copy)]
enum WorkflowRunArgsValueKind {
    String,
    Object,
}

const WORKFLOW_RUN_ARGS_FIELDS: &[(&str, WorkflowRunArgsValueKind)] = &[
    // input: string (carries the stringified workflow input)
    ("input", WorkflowRunArgsValueKind::String),
    // workflow_source oneof — the group name takes an object ({"workflowUrl": "..."}), while the
    // concrete branches `workflow_url` / `workflow_data` (and their JSON-mapped camelCase
    // siblings) take a string.
    ("workflow_source", WorkflowRunArgsValueKind::Object),
    ("workflowSource", WorkflowRunArgsValueKind::Object),
    ("workflow_url", WorkflowRunArgsValueKind::String),
    ("workflowUrl", WorkflowRunArgsValueKind::String),
    ("workflow_data", WorkflowRunArgsValueKind::String),
    ("workflowData", WorkflowRunArgsValueKind::String),
    // Optional control fields.
    ("workflow_context", WorkflowRunArgsValueKind::String),
    ("workflowContext", WorkflowRunArgsValueKind::String),
    ("execution_id", WorkflowRunArgsValueKind::String),
    ("executionId", WorkflowRunArgsValueKind::String),
    ("from_checkpoint", WorkflowRunArgsValueKind::Object),
    ("fromCheckpoint", WorkflowRunArgsValueKind::Object),
];

/// True when `args_json` carries at least one top-level key whose value matches the type-shape of
/// the corresponding `WorkflowRunArgs` field. Used by the auto-detect and the explicit
/// pass-through to agree on the same definition of "looks like WorkflowRunArgs".
///
/// A key whose value kind does NOT match the protobuf field type does not count: it indicates a
/// spurious collision (a free-form workflow input that happens to use the same key name with a
/// different shape). Such inputs must be wrapped, not passed through, or the protobuf JSON
/// deserialise will either error out (e.g. object value at a string field) or silently drop the
/// rest of the input (unknown fields elsewhere in the object).
fn looks_like_workflow_run_args(args_json: &serde_json::Value) -> bool {
    let serde_json::Value::Object(map) = args_json else {
        return false;
    };
    WORKFLOW_RUN_ARGS_FIELDS.iter().any(|(key, kind)| {
        let Some(value) = map.get(*key) else {
            return false;
        };
        match kind {
            WorkflowRunArgsValueKind::String => value.is_string(),
            WorkflowRunArgsValueKind::Object => value.is_object(),
        }
    })
}

/// Wrap the caller's JSON as the workflow's `input` string field, producing a `WorkflowRunArgs`
/// payload of the form `{"input": "<stringified>"}`. Use this when the JSON is the workflow input
/// (the cron / event-handler path), not a hand-built `WorkflowRunArgs`.
///
/// Bare strings are kept verbatim so jq/template expressions etc. survive into the workflow
/// runner without a re-serialise round trip.
fn wrap_args_as_workflow_input(
    worker_name: &str,
    args_json: serde_json::Value,
) -> Result<serde_json::Value> {
    let input_str = match &args_json {
        serde_json::Value::String(s) => s.clone(),
        other => serde_json::to_string(other).map_err(|e| {
            ClientError::ParseError(format!(
                "Failed to serialize workflow input for worker '{worker_name}': {e:#?}"
            ))
        })?,
    };
    Ok(json!({ "input": input_str }))
}

/// Send the caller's JSON verbatim as a `WorkflowRunArgs`. Use this when the caller has
/// explicitly built the protobuf args (e.g. checkpoint resume with `from_checkpoint`, or workflow
/// URL override). Errors if the JSON does not look like a `WorkflowRunArgs` — catches the case
/// where a free-form input was misrouted here, which would otherwise produce an encode error or
/// (worse) silently drop fields under `ignore_unknown_fields=true`.
#[allow(dead_code)]
fn pass_args_as_workflow_run_args(
    worker_name: &str,
    args_json: serde_json::Value,
) -> Result<serde_json::Value> {
    if !looks_like_workflow_run_args(&args_json) {
        return Err(ClientError::ParseError(format!(
            "WorkflowRunArgs shape expected for worker '{worker_name}' but no WorkflowRunArgs-typed field was found; \
             use `wrap_args_as_workflow_input` if this JSON is the workflow input rather than the args envelope"
        ))
        .into());
    }
    Ok(args_json)
}

/// Auto-detect: if `args_json` already looks like a `WorkflowRunArgs` (a known field is present
/// AND its value has the expected type-shape), pass it through; otherwise wrap it as the
/// workflow's `input` field via [`wrap_args_as_workflow_input`].
///
/// Type-shape verification is what guards against the two failure modes the original keys-only
/// check missed:
///   * `{"input": {"text": "..."}}` — `input` at object would encode-error against the string
///     field; we wrap it instead so the entire object lands inside `input` as a stringified JSON.
///   * `{"execution_id": "...", "payload": ...}` — `execution_id` matches a key, but the caller's
///     real input is in `payload`. Without the type check we would pass it through and the
///     unknown `payload` would be dropped; with the check (still passing here because
///     execution_id is correctly typed as string) the caller gets the documented WorkflowRunArgs
///     semantics. Callers who actually want `payload` to be the input must use
///     [`wrap_args_as_workflow_input`] explicitly.
fn shape_args_for_workflow_runner(
    is_workflow_runner: bool,
    worker_name: &str,
    args_json: serde_json::Value,
) -> Result<serde_json::Value> {
    if !is_workflow_runner {
        return Ok(args_json);
    }
    if looks_like_workflow_run_args(&args_json) {
        return Ok(args_json);
    }
    wrap_args_as_workflow_input(worker_name, args_json)
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
                StreamChunkMerge::Concat,
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

#[cfg(test)]
mod shape_args_for_workflow_runner_tests {
    use super::shape_args_for_workflow_runner;
    use serde_json::json;

    // Non-WORKFLOW workers (COMMAND, HTTP_REQUEST, etc.) must receive untouched JSON args so their
    // own runner-specific schemas still encode correctly.
    #[test]
    fn non_workflow_runner_passes_through() {
        let args = json!({"command": "echo", "args": ["hi"]});
        let out = shape_args_for_workflow_runner(false, "w", args.clone()).unwrap();
        assert_eq!(out, args);
    }

    // Free-form object (the only shape the cron UI produces today) must be wrapped into
    // {"input": "<stringified>"} so it lands in WorkflowRunArgs.input instead of being silently
    // dropped as unknown fields by the encoder. This is the exact regression that produced the
    // "instance: String(\"\")" workflow-input validation error.
    #[test]
    fn workflow_runner_wraps_free_form_object_as_input_string() {
        let args = json!({"memories_grpc_host": "h", "summary_model": "qwen3.6:27b"});
        let out = shape_args_for_workflow_runner(true, "w", args.clone()).unwrap();
        let serde_json::Value::Object(ref map) = out else {
            panic!("expected object, got {out:?}");
        };
        assert_eq!(map.len(), 1, "only the wrapped `input` key should remain");
        let input = map.get("input").expect("input key");
        let serde_json::Value::String(input_str) = input else {
            panic!("input must be stringified JSON, got {input:?}");
        };
        let round_trip: serde_json::Value = serde_json::from_str(input_str).unwrap();
        assert_eq!(round_trip, args);
    }

    // Already-shaped WorkflowRunArgs (URL execution path) must not be double-wrapped — otherwise
    // workflow_url would be lost and input would be the stringified outer object.
    #[test]
    fn workflow_runner_does_not_double_wrap_when_input_present() {
        let args = json!({"workflow_url": "u", "input": "{\"x\":1}"});
        let out = shape_args_for_workflow_runner(true, "w", args.clone()).unwrap();
        assert_eq!(out, args);
    }

    #[test]
    fn workflow_runner_preserves_workflow_url_only_shape() {
        let args = json!({"workflow_url": "u"});
        let out = shape_args_for_workflow_runner(true, "w", args.clone()).unwrap();
        assert_eq!(out, args);
    }

    // A bare string is treated as the already-stringified workflow input — preserve it verbatim so
    // jq/template expressions etc. survive into the workflow runner.
    #[test]
    fn workflow_runner_wraps_bare_string_without_re_serializing() {
        let out = shape_args_for_workflow_runner(true, "w", json!("hello")).unwrap();
        assert_eq!(out, json!({"input": "hello"}));
    }

    // Regression: a WorkflowRunArgs whose only set keys are control fields (no `input` /
    // `workflow_url`) must pass through. Wrapping it as {"input": <stringified>} would silently
    // drop `from_checkpoint` (no resume), `execution_id` (no tracking), and `workflow_context`
    // (no per-run context override). Cover each control field in isolation.
    #[test]
    fn workflow_runner_preserves_args_with_only_from_checkpoint() {
        let args = json!({
            "from_checkpoint": {"position": "/foo", "data": {}}
        });
        let out = shape_args_for_workflow_runner(true, "w", args.clone()).unwrap();
        assert_eq!(out, args);
    }

    #[test]
    fn workflow_runner_preserves_args_with_only_execution_id() {
        let args = json!({"execution_id": "01J0X..."});
        let out = shape_args_for_workflow_runner(true, "w", args.clone()).unwrap();
        assert_eq!(out, args);
    }

    #[test]
    fn workflow_runner_preserves_args_with_only_workflow_context() {
        let args = json!({"workflow_context": "{\"user_id\":42}"});
        let out = shape_args_for_workflow_runner(true, "w", args.clone()).unwrap();
        assert_eq!(out, args);
    }

    // The same JSON may be authored using protobuf-JSON-mapped (lowerCamelCase) field names since
    // some callers will round-trip through the proto-JSON converter. Cover the camelCase variants
    // of each control field so they are recognised too.
    #[test]
    fn workflow_runner_preserves_args_with_camel_case_control_fields() {
        for args in [
            json!({"fromCheckpoint": {"position": "/p"}}),
            json!({"executionId": "01J0X..."}),
            json!({"workflowContext": "{}"}),
            json!({"workflowUrl": "u"}),
            json!({"workflowData": "{}"}),
            json!({"workflowSource": {"workflowUrl": "u"}}),
        ] {
            let out = shape_args_for_workflow_runner(true, "w", args.clone()).unwrap();
            assert_eq!(out, args, "args={args}");
        }
    }

    // Regression for the keys-only-check review finding: a free-form workflow input that *happens*
    // to use a WorkflowRunArgs-reserved key with a mismatched type must be wrapped (so the whole
    // object lands inside the workflow's `input` field as stringified JSON), not passed through
    // verbatim. Otherwise:
    //   - {"input": {"text": "..."}}  → `input` expects a string; pass-through would encode-error.
    //   - {"workflow_url": 42}         → number at a string field; pass-through would encode-error.
    //   - {"from_checkpoint": "ckpt"}  → string at a message field; pass-through would encode-error.
    #[test]
    fn workflow_runner_wraps_when_reserved_key_value_type_mismatches() {
        let cases = [
            json!({"input": {"text": "hi"}}),
            json!({"input": ["a", "b"]}),
            json!({"input": 7}),
            json!({"input": null}),
            json!({"workflow_url": 42}),
            json!({"workflowUrl": false}),
            json!({"from_checkpoint": "ckpt"}),
            json!({"fromCheckpoint": ["pos"]}),
            json!({"workflow_source": "u"}),
            json!({"workflowSource": null}),
            json!({"execution_id": 123}),
            json!({"executionId": []}),
            json!({"workflow_context": {"user_id": 42}}),
            json!({"workflowContext": 0}),
        ];
        for args in cases {
            let out = shape_args_for_workflow_runner(true, "w", args.clone()).unwrap();
            // Wrapping always produces {"input": "<stringified args>"} — verify that round-tripping
            // through the `input` string recovers the original free-form payload byte-for-byte.
            let serde_json::Value::Object(ref out_map) = out else {
                panic!("expected wrapped object, got {out:?}");
            };
            assert_eq!(
                out_map.len(),
                1,
                "expected only `input` key after wrap, got {out:?}"
            );
            let input = out_map.get("input").expect("input key");
            let serde_json::Value::String(input_str) = input else {
                panic!("input must be a stringified JSON value, got {input:?}");
            };
            let round_trip: serde_json::Value = serde_json::from_str(input_str).unwrap();
            assert_eq!(round_trip, args, "round-trip mismatch for {args}");
        }
    }

    // Regression for the silent-drop scenario: a free-form workflow input that happens to use a
    // reserved key whose value type DOES match (e.g. {"execution_id": "abc", "payload": ...}) is
    // currently passed through. Document that behaviour and direct affected callers at the
    // explicit `wrap_args_as_workflow_input` entry point: type-matching key collisions are
    // ambiguous from a pure-JSON standpoint and would require parsing the workflow definition to
    // disambiguate.
    #[test]
    fn workflow_runner_passes_through_type_matching_reserved_key_even_with_extras() {
        let args = json!({"execution_id": "abc", "payload": {"x": 1}});
        let out = shape_args_for_workflow_runner(true, "w", args.clone()).unwrap();
        // Documented behaviour: type-matching reserved key wins; callers that need {"payload": ...}
        // as the workflow input must call `wrap_args_as_workflow_input` directly.
        assert_eq!(out, args);
    }
}

#[cfg(test)]
mod workflow_args_helpers_tests {
    use super::{
        looks_like_workflow_run_args, pass_args_as_workflow_run_args, wrap_args_as_workflow_input,
    };
    use serde_json::json;

    // wrap_args_as_workflow_input always produces {"input": <stringified-args>} and never
    // double-stringifies a bare string — preserves jq/template expressions.
    #[test]
    fn wrap_stringifies_object_and_preserves_string_verbatim() {
        let out = wrap_args_as_workflow_input("w", json!({"a": 1})).unwrap();
        assert_eq!(out, json!({"input": "{\"a\":1}"}));
        let bare = wrap_args_as_workflow_input("w", json!("hello")).unwrap();
        assert_eq!(bare, json!({"input": "hello"}));
    }

    // pass_args_as_workflow_run_args is the explicit "I built WorkflowRunArgs myself" entrypoint:
    // valid shapes go through, anything else is rejected so misuse cannot silently mis-encode.
    #[test]
    fn pass_through_accepts_well_shaped_workflow_run_args() {
        let args = json!({"workflow_url": "u", "input": "{}"});
        let out = pass_args_as_workflow_run_args("w", args.clone()).unwrap();
        assert_eq!(out, args);
    }

    #[test]
    fn pass_through_rejects_free_form_input() {
        let err = pass_args_as_workflow_run_args("w", json!({"foo": "bar"})).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("WorkflowRunArgs shape expected"), "got: {msg}");
    }

    #[test]
    fn pass_through_rejects_reserved_key_with_wrong_value_type() {
        let err =
            pass_args_as_workflow_run_args("w", json!({"input": {"text": "hi"}})).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("WorkflowRunArgs shape expected"), "got: {msg}");
    }

    // The predicate is the single source of truth shared by auto-detect and explicit pass-through;
    // make sure key-only collisions with the wrong value type do NOT register as WorkflowRunArgs.
    #[test]
    fn looks_like_predicate_distinguishes_type_match_from_key_collision() {
        assert!(looks_like_workflow_run_args(
            &json!({"input": "stringified"})
        ));
        assert!(looks_like_workflow_run_args(
            &json!({"from_checkpoint": {"position": "/p"}})
        ));
        assert!(!looks_like_workflow_run_args(&json!({"input": {"obj": 1}})));
        assert!(!looks_like_workflow_run_args(
            &json!({"from_checkpoint": "string-not-message"})
        ));
        assert!(!looks_like_workflow_run_args(&json!({"foo": "bar"})));
        assert!(!looks_like_workflow_run_args(&json!("bare-string")));
    }
}

#[cfg(test)]
mod check_workflow_output_status_tests {
    use super::{check_workflow_output_status, downgrade_workflow_outcome_if_failed};
    use crate::client::helper::JobTerminalOutcome;
    use crate::jobworkerp::data::{JobId, ResultStatus};
    use serde_json::json;

    // Completed is the success state — pass through untouched so downstream callers receive
    // the workflow result (output / position / id) intact.
    #[test]
    fn completed_workflow_passes_through() {
        let output = json!({"status": "Completed", "output": "{\"ok\": true}"});
        let out = check_workflow_output_status(output.clone()).unwrap();
        assert_eq!(out, output);
    }

    // A protobuf-clean response whose embedded workflow status is Faulted is the exact scenario
    // the user reported: previously the caller saw success because JobResult.status was Success.
    // Verify this now surfaces as Err with the embedded error_message attached so the cron job log
    // shows the real failure reason.
    #[test]
    fn faulted_workflow_becomes_err_with_error_message() {
        let output = json!({
            "status": "Faulted",
            "error_message": "Workflow input validation failed: ..."
        });
        let err = check_workflow_output_status(output).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("workflow failed (Faulted)"), "got: {msg}");
        assert!(
            msg.contains("Workflow input validation failed"),
            "got: {msg}"
        );
    }

    // Faulted with no error_message falls back to the `output` field so we still surface whatever
    // diagnostic the workflow produced.
    #[test]
    fn faulted_workflow_falls_back_to_output_field() {
        let output = json!({"status": "Faulted", "output": "stack trace..."});
        let err = check_workflow_output_status(output).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("workflow failed (Faulted)"), "got: {msg}");
        assert!(msg.contains("stack trace"), "got: {msg}");
    }

    // The descriptor-based protobuf-JSON decode produces lowerCamelCase keys per the standard JSON
    // mapping (snake_case `error_message` becomes `errorMessage`). Reading only the snake form
    // would surface only the generic "workflow status: Faulted" message instead of the real
    // diagnostic — exact regression flagged in code review. Verify the camelCase form is honoured.
    #[test]
    fn faulted_workflow_reads_camel_case_error_message() {
        let output = json!({
            "status": "Faulted",
            "errorMessage": "Workflow input validation failed: missing field"
        });
        let err = check_workflow_output_status(output).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("workflow failed (Faulted)"), "got: {msg}");
        assert!(
            msg.contains("Workflow input validation failed: missing field"),
            "got: {msg}"
        );
    }

    // If both keys are present (defensive — shouldn't happen in practice), prefer the snake form
    // so callers that already pass proto-name JSON aren't surprised by which value wins.
    #[test]
    fn faulted_workflow_prefers_snake_when_both_present() {
        let output = json!({
            "status": "Faulted",
            "error_message": "snake",
            "errorMessage": "camel"
        });
        let err = check_workflow_output_status(output).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains(": snake"),
            "expected snake-case to win, got: {msg}"
        );
    }

    // Cancelled is an explicit user-initiated terminal state, NOT a failure. Surfacing it as an
    // error would double-report the cancel path; pass it through.
    #[test]
    fn cancelled_workflow_passes_through() {
        let output = json!({"status": "Cancelled", "output": "user requested cancel"});
        let out = check_workflow_output_status(output.clone()).unwrap();
        assert_eq!(out, output);
    }

    // Waiting is the user-approval-before-resume state used by checkpoint flows. Treating it as
    // an error would abort the very flow that is supposed to be suspended; pass it through.
    #[test]
    fn waiting_workflow_passes_through() {
        let output = json!({"status": "Waiting", "position": "/foo"});
        let out = check_workflow_output_status(output.clone()).unwrap();
        assert_eq!(out, output);
    }

    // Per the proto comment Pending / Running should not appear in a final result; defensively
    // pass them through rather than fail on what is almost certainly a sentinel value.
    #[test]
    fn pending_and_running_pass_through() {
        for status in ["Pending", "Running"] {
            let output = json!({"status": status});
            let out = check_workflow_output_status(output.clone()).unwrap();
            assert_eq!(out, output, "status={status}");
        }
    }

    // KNOWN_TERMINAL_WORKFLOW_STATUSES is the "do not warn at the terminal boundary" set. A
    // regression that drops Waiting from this set would start emitting noisy warnings for the
    // very flows we intentionally pass through, so guard the membership explicitly.
    #[test]
    fn known_terminal_workflow_statuses_cover_intentional_non_completed_states() {
        for status in ["Completed", "Faulted", "Cancelled", "Waiting"] {
            assert!(
                super::KNOWN_TERMINAL_WORKFLOW_STATUSES.contains(&status),
                "expected `{status}` in KNOWN_TERMINAL_WORKFLOW_STATUSES so it never trips the warn path"
            );
        }
        for status in ["Pending", "Running", "SomeFutureState"] {
            assert!(
                !super::KNOWN_TERMINAL_WORKFLOW_STATUSES.contains(&status),
                "`{status}` should NOT be a known terminal status so the warn fires when it leaks to the terminal boundary"
            );
        }
    }

    // Non-WorkflowResult shaped output (e.g. a COMMAND runner's stdout JSON) must not be touched —
    // we only have a meaningful classification for known workflow status strings.
    #[test]
    fn non_workflow_output_passes_through() {
        let output = json!({"exit_code": 0, "stdout": "hi"});
        let out = check_workflow_output_status(output.clone()).unwrap();
        assert_eq!(out, output);
    }

    // Unknown status string: don't fail — be conservative so a future status added on the server
    // side doesn't suddenly turn every job into an error.
    #[test]
    fn unknown_status_passes_through() {
        let output = json!({"status": "SomeFutureState"});
        let out = check_workflow_output_status(output.clone()).unwrap();
        assert_eq!(out, output);
    }

    // Streaming-first parity: a JobTerminalOutcome that the jobworkerp server reported as
    // Success but whose embedded workflow status is Faulted must be downgraded to
    // ErrorAndRetry so outcome.is_success() lines up with the workflow's actual outcome.
    #[test]
    fn outcome_downgrade_faulted_terminal_flips_status() {
        let outcome = JobTerminalOutcome {
            job_id: Some(JobId { value: 1 }),
            status: ResultStatus::Success,
            output: json!({"status": "Faulted", "error_message": "boom"}),
        };
        let downgraded = downgrade_workflow_outcome_if_failed(outcome);
        assert_eq!(downgraded.status, ResultStatus::ErrorAndRetry);
        assert!(!downgraded.is_success());
        assert_eq!(downgraded.job_id.map(|j| j.value), Some(1));
    }

    // Waiting terminal outcomes are kept as Success — checkpoint flows depend on the caller
    // observing a non-failed terminal so they can resume from the checkpoint.
    #[test]
    fn outcome_downgrade_keeps_waiting_terminal_as_success() {
        let outcome = JobTerminalOutcome {
            job_id: Some(JobId { value: 2 }),
            status: ResultStatus::Success,
            output: json!({"status": "Waiting", "position": "/approve"}),
        };
        let kept = downgrade_workflow_outcome_if_failed(outcome);
        assert_eq!(kept.status, ResultStatus::Success);
        assert!(kept.is_success());
    }

    // Cancelled terminal outcomes are also kept as Success — cancellation is an explicit user
    // action and is handled by the cancel path, not by the success/failure classification here.
    #[test]
    fn outcome_downgrade_keeps_cancelled_terminal_as_success() {
        let outcome = JobTerminalOutcome {
            job_id: Some(JobId { value: 3 }),
            status: ResultStatus::Success,
            output: json!({"status": "Cancelled"}),
        };
        let kept = downgrade_workflow_outcome_if_failed(outcome);
        assert_eq!(kept.status, ResultStatus::Success);
        assert!(kept.is_success());
    }

    // Already non-success outcomes are not touched: we never want to mask a real ErrorAndRetry as
    // some other terminal state, and we want the original status preserved for cancellation paths.
    #[test]
    fn outcome_downgrade_preserves_already_failed_status() {
        let outcome = JobTerminalOutcome {
            job_id: None,
            status: ResultStatus::Cancelled,
            output: json!({"status": "Cancelled"}),
        };
        let preserved = downgrade_workflow_outcome_if_failed(outcome);
        assert_eq!(preserved.status, ResultStatus::Cancelled);
    }

    // Genuine successful workflow: untouched.
    #[test]
    fn outcome_downgrade_keeps_completed_terminal_as_success() {
        let outcome = JobTerminalOutcome {
            job_id: Some(JobId { value: 9 }),
            status: ResultStatus::Success,
            output: json!({"status": "Completed", "output": "{}"}),
        };
        let kept = downgrade_workflow_outcome_if_failed(outcome);
        assert_eq!(kept.status, ResultStatus::Success);
        assert!(kept.is_success());
    }
}
