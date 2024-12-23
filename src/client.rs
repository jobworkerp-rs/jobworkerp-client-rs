pub mod wrapper;

use crate::grpc::GrpcConnection;
use crate::jobworkerp::data::{
    JobResultData, Priority, ResultStatus, Worker, WorkerData, WorkerSchema,
};
use crate::jobworkerp::service::{
    job_restore_service_client::JobRestoreServiceClient,
    job_result_service_client::JobResultServiceClient, job_service_client::JobServiceClient,
    job_status_service_client::JobStatusServiceClient,
    worker_schema_service_client::WorkerSchemaServiceClient,
    worker_service_client::WorkerServiceClient,
};
use crate::jobworkerp::service::{CreateJobResponse, JobRequest, WorkerNameRequest};
use anyhow::{anyhow, Context, Result};
use command_utils::util::option::Exists;
use std::time::Duration;

pub struct JobworkerpClient {
    connection: GrpcConnection,
}

impl JobworkerpClient {
    pub async fn new(addr: String, request_timeout: Option<Duration>) -> Result<Self> {
        let con = GrpcConnection::new(addr, request_timeout).await?;
        Ok(Self { connection: con })
    }
    pub async fn init_grpc_connection(&self) -> Result<()> {
        // TODO create new conection only when connection test failed
        self.connection.reconnect().await
    }
    pub async fn worker_schema_client(
        &self,
    ) -> WorkerSchemaServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        WorkerSchemaServiceClient::new(cell.clone())
    }
    pub async fn worker_client(&self) -> WorkerServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        WorkerServiceClient::new(cell.clone())
    }
    pub async fn job_client(&self) -> JobServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobServiceClient::new(cell.clone())
    }
    pub async fn job_status_client(&self) -> JobStatusServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobStatusServiceClient::new(cell.clone())
    }
    pub async fn job_restore_client(&self) -> JobRestoreServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobRestoreServiceClient::new(cell.clone())
    }
    pub async fn job_result_client(&self) -> JobResultServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobResultServiceClient::new(cell.clone())
    }
}

pub trait UseJobworkerpClient {
    fn jobworkerp_client(&self) -> &JobworkerpClient;
}

pub trait UseJobworkerpClientHelper: UseJobworkerpClient {
    async fn find_worker_schema_by_name(&self, name: &str) -> Result<Option<WorkerSchema>> {
        let mut client = self.jobworkerp_client().worker_schema_client().await;
        // TODO find by name
        let res = client
            .find_list(tonic::Request::new(Default::default()))
            .await?
            .into_inner()
            .message()
            .await
            .into_iter()
            .flatten()
            .find(|x| x.data.as_ref().exists(|x| x.name.as_str() == name));
        Ok(res)
    }
    async fn find_or_create_worker(&self, worker_data: &WorkerData) -> Result<Worker> {
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

    // enqueue job and get result data for worker
    async fn enqueue_and_get_result_worker_job(
        &self,
        worker_data: &WorkerData,
        arg: Vec<u8>,
        timeout_sec: u32,
    ) -> Result<JobResultData> {
        self.enqueue_worker_job(worker_data, arg, timeout_sec)
            .await?
            .result
            .ok_or(anyhow!("result not found"))?
            .data
            .ok_or(anyhow!("result data not found"))
    }
    // enqueue job and get only output data for worker
    async fn enqueue_and_get_output_worker_job(
        &self,
        worker_data: &WorkerData,
        arg: Vec<u8>,
        timeout_sec: u32,
    ) -> Result<Vec<u8>> {
        let res = self
            .enqueue_and_get_result_worker_job(worker_data, arg, timeout_sec)
            .await?;
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
            Err(anyhow!("job failed: {:?}", res))
        }
    }
    // enqueue job for worker (use find_or_create_worker)
    async fn enqueue_worker_job(
        &self,
        worker_data: &WorkerData,
        arg: Vec<u8>,
        timeout_sec: u32,
    ) -> Result<CreateJobResponse> {
        let worker = self.find_or_create_worker(worker_data).await?;
        let mut job_cli = self.jobworkerp_client().job_client().await;
        job_cli
            .enqueue(JobRequest {
                arg,
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
