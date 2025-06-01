use std::collections::HashMap;
use std::str::FromStr;

use super::{to_request, WorkerIdOrName};
use crate::jobworkerp;
use crate::jobworkerp::data::{JobId, JobResult, JobResultId};
use crate::jobworkerp::service::{
    CountCondition, FindListRequest, ListenByWorkerRequest, ListenRequest,
};
use crate::proto::JobworkerpProto;
use chrono::DateTime;
use clap::Parser;
use command_utils::protobuf::ProtobufDescriptor;
use prost::Message;
use prost_reflect::MessageDescriptor;
use tonic::metadata::KeyAndValueRef;

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
    Listen {
        #[clap(short, long)]
        job_id: i64,
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
        #[clap(short, long)]
        timeout: Option<u64>,
    },
    ListenStream {
        #[clap(short, long)]
        job_id: i64,
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
        #[clap(short, long)]
        timeout: Option<u64>,
    },
    ListenByWorker {
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
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
    const RESULT_HEADER_NAME: &str = "x-job-result-bin";
    pub async fn execute(
        &self,
        client: &crate::client::JobworkerpClient,
        metadata: &HashMap<String, String>,
    ) {
        match self {
            JobResultCommand::Find { id } => {
                let id = JobResultId { value: *id };
                let response = client
                    .job_result_client()
                    .await
                    .find(to_request(metadata, id).unwrap())
                    .await
                    .unwrap();
                match response.into_inner().data {
                    Some(job_res) => {
                        Self::print_job_result_with_request(client, job_res).await;
                    }
                    None => {
                        println!("job result not found: id = {}", id.value);
                    }
                }
            }
            JobResultCommand::Listen {
                job_id,
                worker,
                timeout,
            } => {
                let req = ListenRequest {
                    job_id: Some(JobId { value: *job_id }),
                    worker: Some(worker.to_listen_worker()),
                    timeout: *timeout,
                };
                let response = client
                    .job_result_client()
                    .await
                    .listen(to_request(metadata, req).unwrap())
                    .await
                    .unwrap()
                    .into_inner();
                Self::print_job_result_with_request(client, response).await;
            }
            JobResultCommand::ListenStream {
                job_id,
                worker,
                timeout,
            } => {
                let req = worker.to_job_worker();
                let (_, _args_desc, result_desc) =
                    JobworkerpProto::find_runner_descriptors_by_worker(client, req)
                        .await
                        .unwrap();

                let req = ListenRequest {
                    job_id: Some(JobId { value: *job_id }),
                    worker: Some(worker.to_listen_worker()),
                    timeout: *timeout,
                };
                let response = client
                    .job_result_client()
                    .await
                    .listen_stream(to_request(metadata, req).unwrap())
                    .await
                    .unwrap();

                let meta = response.metadata().clone();
                let mut response = response.into_inner();
                // result meta header
                JobResultCommand::print_job_result_metadata(&meta, result_desc.clone());
                // print streaming response
                while let Some(item) = response.message().await.unwrap() {
                    if let Some(jobworkerp::data::result_output_item::Item::Data(v)) = item.item {
                        JobResultCommand::print_job_result_output(
                            v.as_slice(),
                            result_desc.clone(),
                        );
                    }
                }
                // Self::print_job_result_with_request(client, response).await;
            }
            JobResultCommand::ListenByWorker { worker } => {
                let req = ListenByWorkerRequest {
                    worker: Some(worker.to_listen_stream_worker()),
                };
                let mut response = client
                    .job_result_client()
                    .await
                    .listen_by_worker(to_request(metadata, req).unwrap())
                    .await
                    .unwrap()
                    .into_inner();
                println!("listening... (Ctrl+C to stop)");
                while let Some(res) = response.message().await.unwrap() {
                    Self::print_job_result_with_request(client, res).await;
                }
            }
            JobResultCommand::List { offset, limit } => {
                let request = FindListRequest {
                    offset: *offset,
                    limit: *limit,
                };
                let response = client
                    .job_result_client()
                    .await
                    .find_list(to_request(metadata, request).unwrap())
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
                    .find_list_by_job_id(to_request(metadata, request).unwrap())
                    .await
                    .unwrap()
                    .into_inner();
                while let Some(job_res) = response.message().await.unwrap() {
                    Self::print_job_result_with_request(client, job_res).await;
                }
            }
            JobResultCommand::Delete { id } => {
                let id = JobResultId { value: *id };
                let response = client
                    .job_result_client()
                    .await
                    .delete(to_request(metadata, id).unwrap())
                    .await
                    .unwrap();
                println!("{:#?}", response);
            }
            JobResultCommand::Count {} => {
                let response = client
                    .job_result_client()
                    .await
                    .count(to_request(metadata, CountCondition {}).unwrap()) // TODO
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
        let worker_name = job_result
            .data
            .as_ref()
            .map(|d| d.worker_name.as_str())
            .unwrap_or("");
        let result_proto = JobworkerpProto::resolve_result_descriptor(client, worker_name).await;
        Self::print_job_result(&job_result, result_proto);
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
            println!("[job_result]:\n\t[id] {}", &rid.value);
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
            if rdata.output.is_none() {
                println!("\t[output]: None");
                return;
            }
            let output = rdata.output.as_ref().unwrap();
            if let Some(proto) = result_proto.as_ref() {
                for item in output.items.iter() {
                    match ProtobufDescriptor::get_message_from_bytes(proto.clone(), item.as_slice())
                    {
                        Ok(mes) => {
                            ProtobufDescriptor::print_dynamic_message(&mes, false);
                        }
                        Err(e) => {
                            println!("decode error: {:#?}", e);
                            println!(
                                "original response as string: {:#?}",
                                String::from_utf8_lossy(item)
                            );
                        }
                    }
                }
            } else if !output.items.is_empty() && !output.items[0].is_empty() {
                for item in output.items.iter() {
                    println!("\t[output]: |\n {}", String::from_utf8_lossy(item));
                }
            }
            // flush stdout
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
        // .unwrap_or_else(|| {
        //     println!("result: None: {:#?}", &job_result.id);
        // });
    }

    pub fn print_job_result_output(data: &[u8], result_proto: Option<MessageDescriptor>) {
        if let Some(proto) = result_proto {
            match ProtobufDescriptor::get_message_from_bytes(proto, data) {
                Ok(mes) => {
                    ProtobufDescriptor::print_dynamic_message(&mes, false);
                }
                Err(e) => {
                    println!("error: {:#?}", e);
                }
            }
        } else {
            println!("\t[output]: |\n {}", String::from_utf8_lossy(data));
        }
    }
    pub fn print_job_result_metadata(
        metadata: &tonic::metadata::MetadataMap,
        result_proto: Option<MessageDescriptor>,
    ) {
        println!("[metadata]:");
        for kv in metadata.iter() {
            match kv {
                KeyAndValueRef::Ascii(_key, _value) => {
                    // println!("Ascii: {:?}: {:?}", key, value)
                }
                KeyAndValueRef::Binary(ref key, ref value) => {
                    if key.as_str() == Self::RESULT_HEADER_NAME {
                        let res =
                            JobResult::decode(value.to_bytes().unwrap().iter().as_slice()).unwrap();
                        Self::print_job_result(&res, result_proto.clone());
                        // println!("\t[result]: {:#?}", res);
                    } else {
                        println!("\t{}: {:?}", key, value);
                    }
                }
            }
        }
    }
}
// transform command to protobuf message of job_result service
