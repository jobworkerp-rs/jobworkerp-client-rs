pub mod helper;
pub mod wrapper;

use crate::grpc::GrpcConnection;
use crate::jobworkerp::service::{
    job_restore_service_client::JobRestoreServiceClient,
    job_result_service_client::JobResultServiceClient, job_service_client::JobServiceClient,
    job_status_service_client::JobStatusServiceClient,
    worker_schema_service_client::WorkerSchemaServiceClient,
    worker_service_client::WorkerServiceClient,
};
use anyhow::Result;
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
        WorkerSchemaServiceClient::new(cell.clone()).max_decoding_message_size(128 * 1024 * 1024)
    }
    pub async fn worker_client(&self) -> WorkerServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        WorkerServiceClient::new(cell.clone()).max_decoding_message_size(128 * 1024 * 1024)
    }
    pub async fn job_client(&self) -> JobServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobServiceClient::new(cell.clone()).max_decoding_message_size(128 * 1024 * 1024)
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
