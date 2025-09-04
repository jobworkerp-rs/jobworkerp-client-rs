use super::helper::UseJobworkerpClientHelper;
use crate::client::{JobworkerpClient, UseJobworkerpClient};
use anyhow::Result;
use command_utils::trace::Tracing;
use std::time::Duration;

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
}
