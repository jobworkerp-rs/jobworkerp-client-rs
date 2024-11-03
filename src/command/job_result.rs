use crate::jobworkerp;
use crate::jobworkerp::data::{JobId, JobResultId, Worker, WorkerSchema};
use crate::jobworkerp::service::{CountCondition, FindListRequest, WorkerNameRequest};
use chrono::DateTime;
use clap::Parser;
use infra_utils::infra::protobuf::ProtobufDescriptor;
use prost_reflect::MessageDescriptor;

#[derive(Parser, Debug)]
pub struct JobResultArg {
    #[clap(subcommand)]
    pub cmd: JobResultCommand,
}

#[derive(Parser, Debug)]
pub enum JobResultCommand {
    Find {
        #[clap(short, long)]
        id: i64,
    },
    List {
        #[clap(short, long)]
        offset: Option<i64>,
        #[clap(short, long)]
        limit: Option<i32>,
    },
    ListByJobId {
        #[clap(short, long)]
        job_id: i64,
    },
    Delete {
        #[clap(short, long)]
        id: i64,
    },
    Count {},
}

impl JobResultCommand {
    pub async fn execute(&self, client: &crate::client::JobworkerpClient) {
        match self {
            JobResultCommand::Find { id } => {
                let id = JobResultId { value: *id };
                let response = client.job_result_client().await.find(id).await.unwrap();
                println!("{:#?}", response);
            }
            JobResultCommand::List { offset, limit } => {
                let request = FindListRequest {
                    offset: *offset,
                    limit: *limit,
                };
                let response = client
                    .job_result_client()
                    .await
                    .find_list(request)
                    .await
                    .unwrap();
                let mut response = response.into_inner();
                while let Some(job_res) = response.message().await.unwrap() {
                    Self::print_job_result_with_request(client, job_res).await;
                }
            }
            JobResultCommand::ListByJobId { job_id } => {
                let request = jobworkerp::service::FindListByJobIdRequest {
                    job_id: Some(JobId { value: *job_id }),
                };
                let mut response = client
                    .job_result_client()
                    .await
                    .find_list_by_job_id(tonic::Request::new(request))
                    .await
                    .unwrap()
                    .into_inner();
                while let Some(job_res) = response.message().await.unwrap() {
                    Self::print_job_result_with_request(client, job_res).await;
                }
            }
            JobResultCommand::Delete { id } => {
                let id = JobResultId { value: *id };
                let response = client.job_result_client().await.delete(id).await.unwrap();
                println!("{:#?}", response);
            }
            JobResultCommand::Count {} => {
                let response = client
                    .job_result_client()
                    .await
                    .count(CountCondition {}) // TODO
                    .await
                    .unwrap();
                println!("{:#?}", response);
            }
        }
    }
    async fn print_job_result_with_request(
        client: &crate::client::JobworkerpClient,
        job_result: jobworkerp::data::JobResult,
    ) {
        let result_proto = Self::resolve_result_proto(client, "worker_name").await;
        Self::print_job_result(&job_result, result_proto);
    }
    async fn resolve_result_proto(
        client: &crate::client::JobworkerpClient,
        worker_name: &str,
    ) -> Option<MessageDescriptor> {
        if let Some(Worker {
            id: Some(_wid),
            data: Some(wdata),
        }) = client
            .worker_client()
            .await
            .find_by_name(WorkerNameRequest {
                name: worker_name.to_string(),
            })
            .await
            .unwrap()
            .into_inner()
            .data
        {
            if let Some(WorkerSchema {
                id: Some(_sid),
                data: Some(sdata),
            }) = client
                .worker_schema_client()
                .await
                .find(wdata.schema_id.unwrap())
                .await
                .unwrap()
                .into_inner()
                .data
            {
                sdata.result_output_proto.map(|p| {
                    ProtobufDescriptor::new(&p)
                        .unwrap()
                        .get_messages()
                        .first()
                        .unwrap()
                        .clone()
                })
            } else {
                println!("schema not found: {:#?}", &wdata.schema_id);
                None
            }
        } else {
            println!("worker not found: {:#?}", &worker_name);
            None
        }
    }
    pub fn print_job_result(
        job_result: &jobworkerp::data::JobResult,
        result_proto: Option<MessageDescriptor>,
    ) {
        if let jobworkerp::data::JobResult {
            id: Some(rid),
            data: Some(rdata),
        } = job_result
        {
            println!("[job_result]:\n [id] {}", &rid.value);
            println!("\t[worker]: {}", &rdata.worker_name);
            println!(
                "\t[job id]: {}",
                &rdata.job_id.map(|j| j.value).unwrap_or_default()
            );
            println!("\t[status]: {}", &rdata.status().as_str_name());
            println!(
                "\t[start-end]: {} - {}",
                DateTime::from_timestamp_millis(rdata.start_time)
                    .map(|d| d.to_string())
                    .unwrap_or_default(),
                DateTime::from_timestamp_millis(rdata.end_time)
                    .map(|d| d.to_string())
                    .unwrap_or_default()
            );
            let output = rdata.output.as_ref().unwrap();
            if let Some(proto) = result_proto.as_ref() {
                for item in output.items.iter() {
                    match ProtobufDescriptor::get_message_from_bytes(proto.clone(), item.as_slice())
                    {
                        Ok(mes) => {
                            ProtobufDescriptor::print_dynamic_message(&mes);
                        }
                        Err(e) => {
                            println!("error: {:#?}", e);
                        }
                    }
                }
            } else if !output.items.is_empty() && !output.items[0].is_empty() {
                for item in output.items.iter() {
                    println!("\t[output]: |\n {}", String::from_utf8_lossy(item));
                }
            }
        }
        // .unwrap_or_else(|| {
        //     println!("result: None: {:#?}", &job_result.id);
        // });
    }
}
// transform command to protobuf message of job_result service
