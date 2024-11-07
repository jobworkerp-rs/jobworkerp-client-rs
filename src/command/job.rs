// as job: valid commands are enqueue, find, list, delete, count
// -i, --id <id> id of the job (for find, delete)
// -w, --worker <id or name> worker id or name of the job (if string, treat as name, if number, treat as id)(for enqueue)
// --arg <arg json string> arguments of the worker runner (json string (transform to grpc message internally))
// -u, --unique-key <string> unique key of the job (for enqueue)
// -r, --run-after-time <number> execute unix time (milli-seconds) of the job (for enqueue)
// -p, --priority <priority> priority of the job (HIGH, MIDDLE, LOW)(for enqueue)
// -t, --timeout <timeout> timeout of the job (milli-seconds) (for enqueue)

use std::str::FromStr;

use super::WorkerIdOrName;
use crate::{
    command::{job_result::JobResultCommand, worker_schema::WorkerSchemaCommand},
    jobworkerp::{
        self,
        data::{JobId, Priority},
        service::{CountCondition, FindListRequest, JobRequest},
    },
};
use anyhow::Result;
use chrono::DateTime;
use clap::{Parser, ValueEnum};
use command_utils::util::option::FlatMap;
use infra_utils::infra::protobuf::ProtobufDescriptor;

#[derive(Parser, Debug)]
pub struct JobArg {
    #[clap(subcommand)]
    pub cmd: JobCommand,
}

#[derive(Parser, Debug, Clone)]
pub enum JobCommand {
    Enqueue {
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
        #[clap(short, long)]
        arg: String,
        #[clap(short, long)]
        unique_key: Option<String>,
        #[clap(short, long)]
        run_after_time: Option<i64>,
        #[clap(short, long)]
        priority: Option<PriorityArg>,
        #[clap(short, long)]
        timeout: Option<u64>,
    },
    Find {
        #[clap(short, long)]
        id: i64,
    },
    List {
        #[clap(short, long)]
        offset: Option<i64>,
        #[clap(short, long)]
        limit: Option<u32>,
    },
    Delete {
        #[clap(short, long)]
        id: i64,
    },
    Count {},
}

#[derive(ValueEnum, Debug, Clone)]
pub enum PriorityArg {
    High,
    Middle,
    Low,
}
impl PriorityArg {
    pub fn to_grpc(&self) -> Priority {
        match self {
            PriorityArg::High => Priority::High,
            PriorityArg::Middle => Priority::Medium,
            PriorityArg::Low => Priority::Low,
        }
    }
}

impl JobCommand {
    pub async fn execute(&self, client: &crate::client::JobworkerpClient) {
        match self {
            JobCommand::Enqueue {
                worker,
                arg,
                unique_key,
                run_after_time,
                priority,
                timeout,
            } => {
                let req = worker.to_job_worker();
                let (_, arg_desc, result_desc) =
                    WorkerSchemaCommand::find_descriptors_by_worker(client, req)
                        .await
                        .unwrap();
                let request = JobRequest {
                    worker: Some(worker.to_job_worker()),
                    arg: WorkerSchemaCommand::json_to_message(arg_desc, arg.as_str()).unwrap(),
                    uniq_key: unique_key.clone(),
                    run_after_time: *run_after_time,
                    priority: priority.clone().map(|p| p.to_grpc() as i32),
                    timeout: timeout.flat_map(|t| if t > 0 { Some(t) } else { None }),
                };
                let response = client
                    .job_client()
                    .await
                    .enqueue(request)
                    .await
                    .unwrap()
                    .into_inner();
                if let Some(result) = response.result {
                    JobResultCommand::print_job_result(&result, result_desc);
                } else {
                    println!("{:#?}", response);
                }
            }
            JobCommand::Find { id } => {
                let id = JobId { value: *id };
                let response = client.job_client().await.find(id).await.unwrap();
                let job = response.into_inner();
                if let Some(job) = job.data {
                    print_job_with_request(client, job).await.unwrap();
                } else {
                    println!("job not found");
                }
            }
            JobCommand::List { offset, limit } => {
                let request = FindListRequest {
                    offset: *offset,
                    limit: (*limit).map(|x| x as i32),
                };
                let response = client.job_client().await.find_list(request).await.unwrap();
                let meta = response.metadata().clone();
                let mut inner = response.into_inner();
                println!("{:#?}", meta);
                while let Some(data) = inner.message().await.unwrap() {
                    print_job_with_request(client, data).await.unwrap();
                }
            }
            JobCommand::Delete { id } => {
                let id = JobId { value: *id };
                let response = client.job_client().await.delete(id).await.unwrap();
                println!("{:#?}", response);
            }
            JobCommand::Count {} => {
                let response = client
                    .job_client()
                    .await
                    .count(CountCondition {}) // TODO
                    .await
                    .unwrap();
                println!("{:#?}", response);
            }
        }
        async fn print_job_with_request(
            client: &crate::client::JobworkerpClient,
            job: jobworkerp::data::Job,
        ) -> Result<()> {
            if let jobworkerp::data::Job {
                id: Some(jid),
                data: Some(jdat),
            } = job
            {
                println!("[job]:\n\t[id] {}", &jid.value);
                if let Some(wid) = jdat.worker_id {
                    println!("\t[worker_id] {}", &wid.value);
                    match super::resolve_protos_by_worker_id(client, &wid).await {
                        Ok((_, arg_proto, _)) => {
                            let arg =
                                ProtobufDescriptor::get_message_from_bytes(arg_proto, &jdat.arg)?;
                            println!("\t[arg] ");
                            ProtobufDescriptor::print_dynamic_message(&arg);
                        }
                        Err(e) => {
                            println!("\t[arg (ERROR)]  {:?}", e);
                        }
                    }
                    println!("\t[uniq_key] {:?}", &jdat.uniq_key);
                    println!(
                        "\t[run_after_time] {}",
                        DateTime::from_timestamp_millis(jdat.run_after_time).unwrap_or_default()
                    );
                    println!("\t[priority] {:?}", &jdat.priority().as_str_name());
                    println!("\t[timeout] {} msec", &jdat.timeout);
                } else {
                    println!("\t[worker_id] not found");
                }
            } else {
                println!("[job]:\n\t[id] not found");
            }
            Ok(())
        }
    }
}
