// worker: valid commands are create, find, list, update, delete, count
// -i, --id <number> id of the job (for find, update, delete)
// -n, --name <string> name of the worker (for create, update)
// -r, --runner-id <number> worker_schama id of the worker (for find, update, delete)
// -s, --settings <runner_settings json string> runner_settings of the worker runner (json string (transform to grpc message internally))(for create, update)
// -p, --periodic <periodic millis number> periodic of the worker runner (for create, update)(default: 0)
// -c, --channel <string> channel of the worker runner (for create, update)(optional)
// -q, --queue-type <queue type> queue type of the worker runner (REDIS, RDB, HYBRID) (for create, update)
// -r, --response-type <response type> response type of the worker (NO_RESULT, DIRECT, LISTEN_AFTER) (for create, update) (default: DIRECT)
// --store-success <bool> store success result to job_result (for create, update) (default: false)
// --store-failure <bool> store failure result to job_result (for create, update) (default: false)
// --next-workers <number array> next workers of the worker (for create, update) (optional)
// --use-static <bool> use static worker (for create, update) (default: false)

use crate::{
    client::helper::DEFAULT_RETRY_POLICY,
    command::to_request,
    jobworkerp::{
        self,
        data::{QueueType, ResponseType, RunnerId, WorkerData, WorkerId},
        service::{CountCondition, WorkerNameRequest},
    },
    proto::JobworkerpProto,
};
use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};
use command_utils::protobuf::ProtobufDescriptor;
use std::{collections::HashMap, process::exit};

#[derive(Parser, Debug)]
pub struct WorkerArg {
    #[clap(subcommand)]
    pub cmd: WorkerCommand,
}

#[derive(Parser, Debug)]
pub enum WorkerCommand {
    Create {
        #[clap(short, long)]
        name: String,
        #[clap(short, long)]
        description: String,
        #[clap(short, long)]
        runner_id: i64,
        #[clap(short, long)]
        settings: String,
        #[clap(short, long, default_value = "0")]
        periodic: u32,
        #[clap(short, long)]
        channel: Option<String>,
        #[clap(short, long, value_parser = QueueTypeArg::parse, default_value = "NORMAL")]
        queue_type: QueueTypeArg,
        #[clap(long, value_parser = ResponseTypeArg::parse, default_value = "DIRECT")]
        response_type: ResponseTypeArg,
        #[clap(long, default_value = "false")]
        store_success: bool,
        #[clap(long, default_value = "false")]
        store_failure: bool,
        #[clap(long, default_value = "false")]
        use_static: bool,
        #[clap(long, default_value = "false")]
        broadcast_results: bool,
    },
    Find {
        #[clap(short, long)]
        id: i64,
    },
    FindByName {
        #[clap(short, long)]
        name: String,
    },
    List {
        #[clap(short, long)]
        offset: Option<i64>,
        #[clap(short, long)]
        limit: Option<i32>,
    },
    Update {
        #[clap(short, long)]
        id: i64,
        #[clap(short, long)]
        name: Option<String>,
        #[clap(short, long)]
        description: Option<String>,
        #[clap(short, long)]
        runner_id: Option<i64>,
        #[clap(short, long)]
        settings: Option<String>,
        #[clap(short, long)]
        periodic: Option<u32>,
        #[clap(short, long)]
        channel: Option<Option<String>>,
        #[clap(short, long, value_parser = QueueTypeArg::parse)]
        queue_type: Option<QueueTypeArg>,
        #[clap(long, value_parser = ResponseTypeArg::parse)]
        response_type: Option<ResponseTypeArg>,
        #[clap(long)]
        store_success: Option<bool>,
        #[clap(long)]
        store_failure: Option<bool>,
        #[clap(long)]
        use_static: Option<bool>,
        #[clap(long)]
        broadcast_results: Option<bool>,
    },
    Delete {
        #[clap(short, long)]
        id: i64,
    },
    Count {},
}

#[derive(ValueEnum, Debug, Clone)]
pub enum QueueTypeArg {
    Normal,
    ForcedRdb,
    WithBackup,
}
impl QueueTypeArg {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "NORMAL" => Ok(Self::Normal),
            "FORCED_RDB" => Ok(Self::ForcedRdb),
            "WITH_BACKUP" => Ok(Self::WithBackup),
            _ => Err(anyhow!("unknown queue type: {}", s)),
        }
    }
}

#[derive(ValueEnum, Debug, Clone)]
pub enum ResponseTypeArg {
    NoResult,
    Direct,
}
impl ResponseTypeArg {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "NO_RESULT" => Ok(Self::NoResult),
            "DIRECT" => Ok(Self::Direct),
            _ => Err(anyhow!("unknown response type: {}", s)),
        }
    }
}

impl WorkerCommand {
    pub async fn execute(
        &self,
        client: &crate::client::JobworkerpClient,
        metadata: &HashMap<String, String>,
    ) {
        match self {
            WorkerCommand::Create {
                name,
                description,
                runner_id,
                settings,
                periodic,
                channel,
                queue_type,
                response_type,
                store_success,
                store_failure,
                use_static,
                broadcast_results,
            } => {
                let runner_settings =
                    match JobworkerpProto::find_worker_runner_settings_descriptors(
                        client,
                        RunnerId { value: *runner_id },
                    )
                    .await
                    {
                        Ok(Some(ope_desc)) => {
                            JobworkerpProto::json_to_message(ope_desc, settings.as_str())
                                .map_err(|e| {
                                    println!(
                                        "failed to parse runner_settings json to message: {e:?}"
                                    );
                                    exit(0x0100);
                                })
                                .unwrap()
                        }
                        Ok(None) => {
                            // empty runner_settings means string argument as Vec<u8>
                            settings.as_bytes().to_vec()
                        }
                        Err(e) => {
                            println!("failed to find runner: {e:?}");
                            exit(0x0100);
                        }
                    };
                let request = WorkerData {
                    name: name.clone(),
                    description: description.clone(),
                    runner_id: Some(RunnerId { value: *runner_id }),
                    runner_settings,
                    periodic_interval: *periodic,
                    channel: channel.clone(),
                    queue_type: match queue_type {
                        QueueTypeArg::Normal => QueueType::Normal as i32,
                        QueueTypeArg::ForcedRdb => QueueType::ForcedRdb as i32,
                        QueueTypeArg::WithBackup => QueueType::WithBackup as i32,
                    },
                    response_type: match response_type {
                        ResponseTypeArg::NoResult => ResponseType::NoResult as i32,
                        ResponseTypeArg::Direct => ResponseType::Direct as i32,
                    },
                    store_success: *store_success,
                    store_failure: *store_failure,
                    use_static: *use_static,
                    retry_policy: Some(DEFAULT_RETRY_POLICY),
                    broadcast_results: *broadcast_results,
                };
                let response = client
                    .worker_client()
                    .await
                    .create(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
            WorkerCommand::Find { id } => {
                let id = WorkerId { value: *id };
                let response = client
                    .worker_client()
                    .await
                    .find(to_request(metadata, id).unwrap())
                    .await
                    .unwrap()
                    .into_inner()
                    .data;
                if let Some(worker) = response {
                    print_worker(client, worker).await.unwrap();
                } else {
                    println!("worker not found");
                }
            }
            WorkerCommand::FindByName { name } => {
                let name = WorkerNameRequest { name: name.clone() };
                let response = client
                    .worker_client()
                    .await
                    .find_by_name(to_request(metadata, name).unwrap())
                    .await
                    .unwrap()
                    .into_inner()
                    .data;
                if let Some(worker) = response {
                    print_worker(client, worker).await.unwrap();
                } else {
                    println!("worker not found");
                }
            }
            WorkerCommand::List { offset, limit } => {
                let response = client
                    .worker_client()
                    .await
                    .find_list(
                        to_request(
                            metadata,
                            jobworkerp::service::FindWorkerListRequest {
                                offset: *offset,
                                limit: *limit,
                                ..Default::default()
                            },
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                println!("meta: {:#?}", response.metadata());
                let mut data = response.into_inner();
                while let Some(worker) = data.message().await.unwrap() {
                    print_worker(client, worker).await.unwrap();
                }
                println!(
                    "trailers: {:#?}",
                    data.trailers().await.unwrap().unwrap_or_default()
                );
            }
            WorkerCommand::Update {
                id,
                name,
                description,
                runner_id,
                settings,
                periodic,
                channel,
                queue_type,
                response_type,
                store_success,
                store_failure,
                use_static,
                broadcast_results,
            } => {
                // find by id and update all fields if some
                let res = client
                    .worker_client()
                    .await
                    .find(to_request(metadata, WorkerId { value: *id }).unwrap())
                    .await
                    .unwrap();
                let worker_opt = res.into_inner().data;
                if let Some(mut worker_data) = worker_opt.and_then(|w| w.data) {
                    worker_data.name = name.clone().unwrap_or(worker_data.name);
                    worker_data.description =
                        description.clone().unwrap_or(worker_data.description);
                    worker_data.runner_id = runner_id
                        .map(|s| RunnerId { value: s })
                        .or(worker_data.runner_id);
                    // TODO runner_settings is json string and should be transformed to grpc message bytes.(use runner_settings_proto from runner)
                    worker_data.runner_settings = settings
                        .clone()
                        .map(|o| o.bytes().collect())
                        .unwrap_or(worker_data.runner_settings.clone());
                    worker_data.periodic_interval =
                        periodic.unwrap_or(worker_data.periodic_interval);
                    worker_data.channel = channel.clone().unwrap_or(worker_data.channel);
                    worker_data.queue_type = queue_type
                        .clone()
                        .map(|q| q as i32)
                        .unwrap_or(worker_data.queue_type);
                    worker_data.response_type = response_type
                        .clone()
                        .map(|r| r as i32)
                        .unwrap_or(worker_data.response_type);
                    worker_data.store_success = store_success.unwrap_or(worker_data.store_success);
                    worker_data.store_failure = store_failure.unwrap_or(worker_data.store_failure);
                    worker_data.use_static = use_static.unwrap_or(worker_data.use_static);
                    worker_data.broadcast_results =
                        broadcast_results.unwrap_or(worker_data.broadcast_results);
                    // worker_data.retry_policy = worker_data.retry_policy; //TODO
                    let response = client
                        .worker_client()
                        .await
                        .update(
                            to_request(
                                metadata,
                                jobworkerp::data::Worker {
                                    id: Some(WorkerId { value: *id }),
                                    data: Some(worker_data),
                                },
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    println!("{response:#?}");
                } else {
                    println!("worker not found");
                }
            }
            WorkerCommand::Delete { id } => {
                let id = WorkerId { value: *id };
                let response = client
                    .worker_client()
                    .await
                    .delete(to_request(metadata, id).unwrap())
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
            WorkerCommand::Count {} => {
                let response = client
                    .worker_client()
                    .await
                    .count(to_request(metadata, CountCondition {}).unwrap())
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
        }
        async fn print_worker(
            client: &crate::client::JobworkerpClient,
            worker: jobworkerp::data::Worker,
        ) -> Result<()> {
            if let jobworkerp::data::Worker {
                id: Some(wid),
                data: Some(wdat),
            } = worker.clone()
            {
                let op = JobworkerpProto::find_worker_runner_settings_descriptors(
                    client,
                    wdat.runner_id.unwrap(),
                )
                .await
                .ok()
                .flatten();
                println!("[worker]:\n\t[id] {}", &wid.value);
                println!("\t[name] {}", &wdat.name);
                println!("\t[description] {}", &wdat.description);
                println!(
                    "\t[runner_id] {}",
                    wdat.runner_id.map(|s| s.value).unwrap_or_default()
                );
                if let Some(op) = op {
                    match ProtobufDescriptor::get_message_from_bytes(op, &wdat.runner_settings) {
                        Ok(msg) => {
                            println!("\t[runner_settings] |");
                            ProtobufDescriptor::print_dynamic_message(&msg, false);
                        }
                        Err(e) => {
                            println!(
                                "\t[runner_settings (error)] failed to parse runner_settings message: {e:?}"
                            );
                        }
                    }
                } else {
                    println!(
                        "\t[runner_settings] {}",
                        String::from_utf8_lossy(wdat.runner_settings.as_slice())
                    );
                }
                println!("\t[periodic] {}", wdat.periodic_interval);
                println!("\t[channel] {:?}", wdat.channel);
                println!("\t[queue_type] {:?}", wdat.queue_type);
                println!("\t[response_type] {:?}", wdat.response_type);
                println!("\t[store_success] {}", wdat.store_success);
                println!("\t[store_failure] {}", wdat.store_failure);
                println!("\t[use_static] {}", wdat.use_static);
                println!("\t[broadcast_results] {}", wdat.broadcast_results);
            } else {
                println!("worker not found");
            }
            Ok(())
        }
    }
}
