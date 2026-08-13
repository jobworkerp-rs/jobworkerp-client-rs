#![allow(clippy::missing_errors_doc)]

pub mod function_set_yaml;
pub mod helper;
pub mod manifest_yaml;
pub mod worker_yaml;
pub mod wrapper;
pub mod yaml_common;

use crate::grpc::GrpcConnection;
use crate::jobworkerp::function::service::{
    function_service_client::FunctionServiceClient,
    function_set_service_client::FunctionSetServiceClient,
};
use crate::jobworkerp::service::{
    job_processing_status_service_client::JobProcessingStatusServiceClient,
    job_restore_service_client::JobRestoreServiceClient,
    job_result_service_client::JobResultServiceClient, job_service_client::JobServiceClient,
    runner_service_client::RunnerServiceClient,
    worker_instance_service_client::WorkerInstanceServiceClient,
    worker_service_client::WorkerServiceClient,
};
use anyhow::Result;
use std::time::Duration;

// RunnerData includes self-contained protobuf schemas for every exposed method.
// Keep this bounded while accommodating multi-method plugin runners.
const RUNNER_MAX_DECODING_MESSAGE_SIZE: usize = 16 * 1024 * 1024 - 1;

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
        self.connection.reconnect().await
    }
    pub async fn runner_client(&self) -> RunnerServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        RunnerServiceClient::new(cell.clone())
            .max_decoding_message_size(RUNNER_MAX_DECODING_MESSAGE_SIZE)
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
    pub async fn job_processing_status_client(
        &self,
    ) -> JobProcessingStatusServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobProcessingStatusServiceClient::new(cell.clone())
            .max_decoding_message_size(128 * 1024 * 1024)
    }
    pub async fn job_restore_client(&self) -> JobRestoreServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobRestoreServiceClient::new(cell.clone()).max_decoding_message_size(128 * 1024 * 1024)
    }
    pub async fn job_result_client(&self) -> JobResultServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        JobResultServiceClient::new(cell.clone()).max_decoding_message_size(128 * 1024 * 1024)
    }
    pub async fn worker_instance_client(
        &self,
    ) -> WorkerInstanceServiceClient<tonic::transport::Channel> {
        let cell = self.connection.read_channel().await;
        WorkerInstanceServiceClient::new(cell.clone())
    }
}

pub trait UseJobworkerpClient {
    fn jobworkerp_client(&self) -> &JobworkerpClient;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobworkerp::{
        data::{Runner, RunnerData, RunnerType},
        service::{
            CountCondition, CountResponse, CreateRunnerRequest, CreateRunnerResponse,
            FindListRequest, FindRunnerListRequest, OptionalRunnerResponse, RunnerNameRequest,
            SuccessResponse,
            runner_service_server::{RunnerService, RunnerServiceServer},
        },
    };
    use futures::Stream;
    use std::{pin::Pin, sync::Arc};
    use tokio::{sync::oneshot, time};
    use tonic::{Request, Response, Status};

    #[derive(Debug)]
    struct LargeRunnerService {
        runner: Runner,
    }

    #[tonic::async_trait]
    impl RunnerService for LargeRunnerService {
        type FindListStream = Pin<Box<dyn Stream<Item = Result<Runner, Status>> + Send>>;
        type FindListByStream = Pin<Box<dyn Stream<Item = Result<Runner, Status>> + Send>>;

        async fn create(
            &self,
            _request: Request<CreateRunnerRequest>,
        ) -> Result<Response<CreateRunnerResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn delete(
            &self,
            _request: Request<crate::jobworkerp::data::RunnerId>,
        ) -> Result<Response<SuccessResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn find(
            &self,
            _request: Request<crate::jobworkerp::data::RunnerId>,
        ) -> Result<Response<OptionalRunnerResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn find_by_name(
            &self,
            _request: Request<RunnerNameRequest>,
        ) -> Result<Response<OptionalRunnerResponse>, Status> {
            Ok(Response::new(OptionalRunnerResponse {
                data: Some(self.runner.clone()),
            }))
        }

        async fn find_list(
            &self,
            _request: Request<FindListRequest>,
        ) -> Result<Response<Self::FindListStream>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn count(
            &self,
            _request: Request<CountCondition>,
        ) -> Result<Response<CountResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn find_list_by(
            &self,
            _request: Request<FindRunnerListRequest>,
        ) -> Result<Response<Self::FindListByStream>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn count_by(
            &self,
            _request: Request<crate::jobworkerp::service::CountRunnerRequest>,
        ) -> Result<Response<CountResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
    }

    #[tokio::test]
    async fn runner_client_decodes_a_runner_response_larger_than_tonics_default_limit() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("get test listener address");
        drop(listener);

        let runner = Runner {
            id: None,
            data: Some(RunnerData {
                name: "large-schema-runner".to_string(),
                description: "x".repeat(5 * 1024 * 1024),
                runner_type: RunnerType::Plugin as i32,
                runner_settings_proto: String::new(),
                definition: String::new(),
                method_proto_map: None,
            }),
        };
        let service = Arc::new(LargeRunnerService { runner });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RunnerServiceServer::from_arc(service))
                .serve_with_shutdown(address, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve test runner service");
        });

        let endpoint = format!("http://{address}");
        let client = time::timeout(time::Duration::from_secs(2), async {
            loop {
                match JobworkerpClient::new(endpoint.clone(), None).await {
                    Ok(client) => break client,
                    Err(_) => time::sleep(time::Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("connect to test runner service");
        let response = client
            .runner_client()
            .await
            .find_by_name(RunnerNameRequest {
                name: "large-schema-runner".to_string(),
            })
            .await
            .expect("decode a response larger than the default 4 MiB limit")
            .into_inner();

        assert_eq!(
            response
                .data
                .expect("runner response")
                .data
                .expect("runner data")
                .name,
            "large-schema-runner"
        );

        shutdown_tx.send(()).expect("stop test runner service");
        server.await.expect("join test runner service");
    }
}
