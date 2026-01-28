use super::helper::UseJobworkerpClientHelper;
use crate::{
    client::{JobworkerpClient, UseJobworkerpClient},
    jobworkerp::data::{Runner, RunnerType},
    proto::JobworkerpProto,
};
use anyhow::Result;
use command_utils::{protobuf::ProtobufDescriptor, trace::Tracing};
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

impl JobworkerpClientWrapper {
    const DEFAULT_REQUEST_TIMEOUT_SEC: u32 = 1200;
    pub async fn new(
        address: &str,
        request_timeout_sec: Option<u32>,
    ) -> Result<JobworkerpClientWrapper> {
        let jobworkerp_client = JobworkerpClient::new(
            address.to_string(),
            request_timeout_sec.map(|s| Duration::from_secs(s.into())),
        )
        .await?;

        Ok(JobworkerpClientWrapper { jobworkerp_client })
    }
    pub async fn new_by_env(request_timeout_sec: Option<u32>) -> Result<JobworkerpClientWrapper> {
        let jobworkerp_client = JobworkerpClient::new(
            std::env::var("JOBWORKERP_ADDR").expect("JOBWORKERP_ADDR is not set"),
            request_timeout_sec.map(|s| Duration::from_secs(s.into())),
        )
        .await?;

        Ok(JobworkerpClientWrapper { jobworkerp_client })
    }
    pub fn address(&self) -> &str {
        self.jobworkerp_client.address.as_str()
    }
    pub fn request_timeout(&self) -> Option<Duration> {
        self.jobworkerp_client.request_timeout
    }
    pub async fn execute_workflow(
        &self,
        cx: Option<&opentelemetry::Context>,
        metadata: Arc<HashMap<String, String>>,
        workflow_url: &str,
        input: &str,
        channel: Option<&str>,
        using: Option<&str>,
    ) -> Result<serde_json::Value> {
        let job_args = json!({
            "workflow_url": workflow_url,
            "input": input,
        });
        let worker_params = json!({
            "channel": channel,
            "queue_type": "NORMAL",
            "store_success": false,
            "store_failure": false,
            "use_static": false,
        });
        tracing::debug!("execute_workflow: {:?}", job_args);
        if let Some(Runner {
            id: Some(_sid),
            data: Some(sdata),
        }) = self
            .find_runner_by_name(
                cx,
                metadata.clone(),
                RunnerType::InlineWorkflow.as_str_name(),
            )
            .await?
        {
            // InlineWorkflow is a single-method runner, so using is None (auto-selected)
            let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(&sdata, None)?;
            let job_args = match args_descriptor.clone() { Some(desc) => {
                JobworkerpProto::json_value_to_message(desc, &job_args, true)
            } _ => {
                Ok(job_args.to_string().as_bytes().to_vec())
            }}
            .inspect_err(|e| tracing::warn!("Failed to parse job_args: {:#?}", e))?;

            let output = self
                .setup_worker_and_enqueue_with_raw_output(
                    cx,
                    metadata,
                    RunnerType::InlineWorkflow.as_str_name(),
                    vec![],
                    Some(worker_params),
                    job_args,
                    self.request_timeout()
                        .map(|t| t.as_secs() as u32)
                        .unwrap_or(Self::DEFAULT_REQUEST_TIMEOUT_SEC),
                    using, // using: InlineWorkflow is single-method, auto-selected
                )
                .await?;
            // InlineWorkflow is a single-method runner, so using is None (auto-selected)
            let result_descriptor = JobworkerpProto::parse_result_schema_descriptor(&sdata, None)?;
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
            tracing::error!("runner not found: INLINE_WORKFLOW");
            Err(anyhow::anyhow!("runner not found: INLINE_WORKFLOW"))
        }
    }
}
