// as job: valid commands are enqueue, find, list, delete, count
// -i, --id <id> id of the job (for find, delete)
// -w, --worker <id or name> worker id or name of the job (if string, treat as name, if number, treat as id)(for enqueue)
// --args <args json string> arguments of the worker runner (json string (transform to grpc message internally))
// -u, --unique-key <string> unique key of the job (for enqueue)
// -r, --run-after-time <number> execute unix time (milli-seconds) of the job (for enqueue)
// -p, --priority <priority> priority of the job (HIGH, MIDDLE, LOW)(for enqueue)
// -t, --timeout <timeout> timeout of the job (milli-seconds) (for enqueue)

use std::{
    hash::{DefaultHasher, Hasher},
    str::FromStr,
};

use super::WorkerIdOrName;
use crate::{
    client::{helper::UseJobworkerpClientHelper, UseJobworkerpClient},
    command::job_result::JobResultCommand,
    jobworkerp::{
        self,
        data::{JobId, Priority, QueueType, ResponseType, Runner, RunnerType, WorkerData},
        service::{job_request, CountCondition, FindListRequest, JobRequest},
    },
    proto::JobworkerpProto,
};
use anyhow::Result;
use chrono::DateTime;
use clap::{Parser, ValueEnum};
use command_utils::util::option::FlatMap;
use command_utils::{protobuf::ProtobufDescriptor, util::datetime};

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
        args: String,
        #[clap(short, long)]
        unique_key: Option<String>,
        #[clap(short, long)]
        run_after_time: Option<i64>,
        #[clap(short, long)]
        priority: Option<PriorityArg>,
        #[clap(short, long)]
        timeout: Option<u64>,
    },
    EnqueueForStream {
        #[clap(short, long, value_parser = WorkerIdOrName::from_str)]
        worker: WorkerIdOrName,
        #[clap(short, long)]
        args: String,
        #[clap(short, long)]
        unique_key: Option<String>,
        #[clap(short, long)]
        run_after_time: Option<i64>,
        #[clap(short, long)]
        priority: Option<PriorityArg>,
        #[clap(short, long)]
        timeout: Option<u64>,
    },
    EnqueueWorkflow {
        #[clap(short, long)]
        channel: Option<String>,
        #[clap(long)]
        context: Option<String>,
        #[clap(short, long)]
        input: String,
        #[clap(short, long)]
        priority: Option<PriorityArg>,
        #[clap(short, long)]
        run_after_time: Option<i64>,
        #[clap(short, long)]
        timeout: Option<u64>,
        #[clap(short, long)]
        workflow_file: String,
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
                args,
                unique_key,
                run_after_time,
                priority,
                timeout,
            } => {
                let req = worker.to_job_worker();
                let (_, args_desc, result_desc) =
                    JobworkerpProto::find_runner_descriptors_by_worker(client, req)
                        .await
                        .unwrap();
                let request = JobRequest {
                    worker: Some(worker.to_job_worker()),
                    args: if let Some(args_d) = args_desc {
                        JobworkerpProto::json_to_message(args_d, args.as_str()).unwrap()
                    } else {
                        args.as_bytes().to_vec()
                    },
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
            JobCommand::EnqueueForStream {
                worker,
                args,
                unique_key,
                run_after_time,
                priority,
                timeout,
            } => {
                let req = worker.to_job_worker();
                let (_, args_desc, result_desc) =
                    JobworkerpProto::find_runner_descriptors_by_worker(client, req)
                        .await
                        .unwrap();
                let request = JobRequest {
                    worker: Some(worker.to_job_worker()),
                    args: if let Some(args_d) = args_desc {
                        JobworkerpProto::json_to_message(args_d, args.as_str()).unwrap()
                    } else {
                        args.as_bytes().to_vec()
                    },
                    uniq_key: unique_key.clone(),
                    run_after_time: *run_after_time,
                    priority: priority.clone().map(|p| p.to_grpc() as i32),
                    timeout: timeout.flat_map(|t| if t > 0 { Some(t) } else { None }),
                };
                let response = client
                    .job_client()
                    .await
                    .enqueue_for_stream(request)
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
                // if let Some(result) = response.result {
                //     JobResultCommand::print_job_result(&result, result_desc);
                // } else {
                //     println!("{:#?}", response);
                // }
            }
            JobCommand::EnqueueWorkflow {
                channel,
                context,
                input,
                priority,
                run_after_time,
                timeout,
                workflow_file,
            } => {
                let helper = JobCommandHelper::new(client.clone());
                let runner = helper
                    .find_runner_by_name(RunnerType::SimpleWorkflow.as_str_name())
                    .await
                    .unwrap();
                if let Some(Runner {
                    id: Some(rid),
                    data: Some(rdata),
                }) = runner
                {
                    let mut hasher = DefaultHasher::default();
                    hasher.write_i64(datetime::now_millis());
                    hasher.write_i64(rand::random()); // random
                                                      // create random worker name
                    let wname = format!("{}_{:x}", "JobworkerpCilentWorkflow", hasher.finish());

                    let worker_data = WorkerData {
                        name: wname.clone(),
                        runner_id: Some(rid),
                        runner_settings: vec![],
                        retry_policy: None, // XXX
                        channel: channel.clone(),
                        queue_type: QueueType::Normal as i32,
                        response_type: ResponseType::Direct as i32,
                        store_success: false,
                        store_failure: false,
                        output_as_stream: false,
                        use_static: false,
                        ..Default::default()
                    };
                    let args = if let Some(args_descriptor) =
                        JobworkerpProto::parse_job_args_schema_descriptor(&rdata)
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to parse job_args schema descriptor: {:#?}",
                                    e
                                )
                            })
                            .unwrap()
                    {
                        let context = context.as_deref().unwrap_or("");
                        let job_args = serde_json::json![{
                            "workflow_url": workflow_file.clone(),
                            "input": input.clone(),
                            "context": context,
                        }];
                        JobworkerpProto::json_value_to_message(args_descriptor, &job_args, true)
                            .map_err(|e| {
                                anyhow::anyhow!("Failed to parse job_args schema: {:#?}", e)
                            })
                            .unwrap()
                    } else {
                        println!("args_descriptor not found");
                        return;
                    };
                    let result_desc = JobworkerpProto::parse_result_schema_descriptor(&rdata)
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to parse job_result schema descriptor: {:#?}",
                                e
                            )
                        })
                        .unwrap();

                    let response = helper
                        .enqueue_worker_job(
                            &worker_data,
                            args,
                            timeout.map(|t| (t / 1000) as u32).unwrap_or_default(),
                            run_after_time.clone(),
                            priority.clone().map(|p| p.to_grpc()),
                        )
                        .await
                        .unwrap();
                    let _ = helper.delete_worker_by_name(wname.as_str()).await;
                    if let Some(result) = response.result {
                        JobResultCommand::print_job_result(&result, result_desc);
                    } else {
                        println!("{:#?}", response);
                    }
                } else {
                    println!(
                        "runner {} not found",
                        RunnerType::SimpleWorkflow.as_str_name()
                    );
                    return;
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
                    match JobworkerpProto::find_runner_descriptors_by_worker(
                        client,
                        job_request::Worker::WorkerId(wid),
                    )
                    .await
                    {
                        Ok((_, args_proto, _)) => {
                            if let Some(args_proto) = args_proto {
                                let args = ProtobufDescriptor::get_message_from_bytes(
                                    args_proto, &jdat.args,
                                )?;
                                println!("\t[args] ");
                                ProtobufDescriptor::print_dynamic_message(&args, false);
                            } else {
                                println!(
                                    "\t[args] {:?}",
                                    String::from_utf8_lossy(jdat.args.as_slice())
                                );
                            }
                        }
                        Err(e) => {
                            println!("\t[args (ERROR)]  {:?}", e);
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
struct JobCommandHelper {
    client: crate::client::JobworkerpClient,
}
impl JobCommandHelper {
    pub fn new(client: crate::client::JobworkerpClient) -> Self {
        Self { client }
    }
}

impl UseJobworkerpClient for JobCommandHelper {
    fn jobworkerp_client(&self) -> &crate::client::JobworkerpClient {
        &self.client
    }
}
impl UseJobworkerpClientHelper for JobCommandHelper {}
