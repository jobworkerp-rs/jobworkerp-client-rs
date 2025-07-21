pub mod helper;
pub mod wrapper;

use crate::grpc::GrpcConnection;
use crate::jobworkerp::function::service::{
    function_service_client::FunctionServiceClient,
    function_set_service_client::FunctionSetServiceClient,
};
use crate::jobworkerp::service::{
    job_restore_service_client::JobRestoreServiceClient,
    job_result_service_client::JobResultServiceClient, job_service_client::JobServiceClient,
    job_status_service_client::JobStatusServiceClient, runner_service_client::RunnerServiceClient,
    worker_service_client::WorkerServiceClient,
};
use anyhow::Result;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct JobworkerpClient {
    pub address: String,
    pub request_timeout: Option<Duration>,
    connection: GrpcConnection,
}

impl JobworkerpClient {
    pub async fn new(addr: String, request_timeout: Option<Duration>) -> Result<Self> {
        let use_tls = addr.starts_with("https://");
        let con = GrpcConnection::new(addr.clone(), request_timeout, use_tls).await?;
        Ok(Self {
            address: addr,
            request_timeout,
            connection: con,
        })
    }
    pub async fn init_grpc_connection(&self) -> Result<()> {
        // TODO create new conection only when connection test failed
        self.connection.reconnect().await
    }
    pub async fn runner_client(&self) -> RunnerServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        RunnerServiceClient::new(cell.clone())
    }
    pub async fn worker_client(&self) -> WorkerServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        WorkerServiceClient::new(cell.clone()).max_decoding_message_size(128 * 1024 * 1024)
    }
    pub async fn function_client(&self) -> FunctionServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        FunctionServiceClient::new(cell.clone())
    }
    pub async fn function_set_client(&self) -> FunctionSetServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        FunctionSetServiceClient::new(cell.clone())
    }
    pub async fn job_client(&self) -> JobServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobServiceClient::new(cell.clone())
            .max_decoding_message_size(128 * 1024 * 1024) // 128MB for large responses
            .max_encoding_message_size(64 * 1024 * 1024) // 64MB for large requests
    }
    pub async fn job_status_client(&self) -> JobStatusServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobStatusServiceClient::new(cell.clone()).max_decoding_message_size(128 * 1024 * 1024)
    }
    pub async fn job_restore_client(&self) -> JobRestoreServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobRestoreServiceClient::new(cell.clone()).max_decoding_message_size(128 * 1024 * 1024)
    }
    pub async fn job_result_client(&self) -> JobResultServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobResultServiceClient::new(cell.clone()).max_decoding_message_size(128 * 1024 * 1024)
    }
}

pub trait UseJobworkerpClient {
    fn jobworkerp_client(&self) -> &JobworkerpClient;
}
