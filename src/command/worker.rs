// worker: valid commands are create, find, list, update, delete, count
// -i, --id <number> id of the job (for find, update, delete)
// -n, --name <string> name of the worker (for create, update)
// -s, --schema-id <number> worker_schama id of the worker (for find, update, delete)
// -o, --operation <operation json string> operation of the worker runner (json string (transform to grpc message internally))(for create, update)
// -p, --periodic <periodic millis number> periodic of the worker runner (for create, update)(default: 0)
// -c, --channel <string> channel of the worker runner (for create, update)(optional)
// -q, --queue-type <queue type> queue type of the worker runner (REDIS, RDB, HYBRID) (for create, update)
// -r, --response-type <response type> response type of the worker (NO_RESULT, DIRECT, LISTEN_AFTER) (for create, update) (default: DIRECT)
// --store-success <bool> store success result to job_result (for create, update) (default: false)
// --store-failure <bool> store failure result to job_result (for create, update) (default: false)
// --next-workers <number array> next workers of the worker (for create, update) (optional)
// --use-static <bool> use static worker (for create, update) (default: false)

use crate::{
    command::worker_schema::WorkerSchemaCommand,
    jobworkerp::{
        self,
        data::{QueueType, ResponseType, WorkerData, WorkerId, WorkerSchemaId},
        service::CountCondition,
    },
};
use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};
use command_utils::util::option::FlatMap;
use infra_utils::infra::protobuf::ProtobufDescriptor;
use std::process::exit;

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
        schema_id: i64,
        #[clap(short, long)]
        operation: String,
        #[clap(short, long, default_value = "0")]
        periodic: u32,
        #[clap(short, long)]
        channel: Option<String>,
        #[clap(short, long, value_parser = QueueTypeArg::parse, default_value = "NORMAL")]
        queue_type: QueueTypeArg,
        #[clap(short, long, value_parser = ResponseTypeArg::parse, default_value = "DIRECT")]
        response_type: ResponseTypeArg,
        #[clap(long, default_value = "false")]
        store_success: bool,
        #[clap(long, default_value = "false")]
        store_failure: bool,
        #[clap(long)]
        next_workers: Option<Vec<i64>>,
        #[clap(long, default_value = "false")]
        use_static: bool,
    },
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
    Update {
        #[clap(short, long)]
        id: i64,
        #[clap(short, long)]
        name: Option<String>,
        #[clap(short, long)]
        schema_id: Option<i64>,
        #[clap(short, long)]
        operation: Option<String>,
        #[clap(short, long)]
        periodic: Option<u32>,
        #[clap(short, long)]
        channel: Option<Option<String>>,
        #[clap(short, long, value_parser = QueueTypeArg::parse)]
        queue_type: Option<QueueTypeArg>,
        #[clap(short, long, value_parser = ResponseTypeArg::parse)]
        response_type: Option<ResponseTypeArg>,
        #[clap(long)]
        store_success: Option<bool>,
        #[clap(long)]
        store_failure: Option<bool>,
        #[clap(long)]
        next_workers: Option<Vec<i64>>,
        #[clap(long)]
        use_static: Option<bool>,
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
    ListenAfter,
}
impl ResponseTypeArg {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "NO_RESULT" => Ok(Self::NoResult),
            "DIRECT" => Ok(Self::Direct),
            "LISTEN_AFTER" => Ok(Self::ListenAfter),
            _ => Err(anyhow!("unknown response type: {}", s)),
        }
    }
}

impl WorkerCommand {
    pub async fn execute(&self, client: &crate::client::JobworkerpClient) {
        match self {
            WorkerCommand::Create {
                name,
                schema_id,
                operation,
                periodic,
                channel,
                queue_type,
                response_type,
                store_success,
                store_failure,
                next_workers,
                use_static,
            } => {
                let (ope_desc, _, _) = WorkerSchemaCommand::find_descriptors(
                    client,
                    WorkerSchemaId { value: *schema_id },
                )
                .await
                .unwrap();
                let operation = WorkerSchemaCommand::json_to_message(ope_desc, operation.as_str())
                    .map_err(|e| {
                        println!("failed to parse operation json to message: {:?}", e);
                        exit(0x0100);
                    })
                    .unwrap();
                let request = WorkerData {
                    name: name.clone(),
                    schema_id: Some(WorkerSchemaId { value: *schema_id }),
                    operation,
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
                        ResponseTypeArg::ListenAfter => ResponseType::ListenAfter as i32,
                    },
                    store_success: *store_success,
                    store_failure: *store_failure,
                    next_workers: next_workers
                        .clone()
                        .unwrap_or_default()
                        .iter()
                        .map(|n| WorkerId { value: *n })
                        .collect(),
                    use_static: *use_static,
                    retry_policy: None,
                };
                let response = client.worker_client().await.create(request).await.unwrap();
                println!("{:#?}", response);
            }
            WorkerCommand::Find { id } => {
                let id = WorkerId { value: *id };
                let response = client
                    .worker_client()
                    .await
                    .find(id)
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
                    .find_list(jobworkerp::service::FindListRequest {
                        offset: *offset,
                        limit: *limit,
                    })
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
                schema_id,
                operation,
                periodic,
                channel,
                queue_type,
                response_type,
                store_success,
                store_failure,
                next_workers,
                use_static,
            } => {
                // find by id and update all fields if some
                let res = client
                    .worker_client()
                    .await
                    .find(WorkerId { value: *id })
                    .await
                    .unwrap();
                let worker_opt = res.into_inner().data;
                if let Some(mut worker_data) = worker_opt.flat_map(|w| w.data) {
                    worker_data.name = name.clone().unwrap_or(worker_data.name);
                    worker_data.schema_id = schema_id
                        .map(|s| WorkerSchemaId { value: s })
                        .or(worker_data.schema_id);
                    // TODO operation is json string and should be transformed to grpc message bytes.(use operation_proto from worker_schema)
                    worker_data.operation = operation
                        .clone()
                        .map(|o| o.bytes().collect())
                        .unwrap_or(worker_data.operation.clone());
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
                    worker_data.next_workers = next_workers
                        .clone()
                        .unwrap_or_default()
                        .iter()
                        .map(|n| WorkerId { value: *n })
                        .collect();
                    worker_data.use_static = use_static.unwrap_or(worker_data.use_static);
                    // worker_data.retry_policy = worker_data.retry_policy; //TODO
                    let response = client
                        .worker_client()
                        .await
                        .update(jobworkerp::data::Worker {
                            id: Some(WorkerId { value: *id }),
                            data: Some(worker_data),
                        })
                        .await
                        .unwrap();
                    println!("{:#?}", response);
                } else {
                    println!("worker not found");
                }
            }
            WorkerCommand::Delete { id } => {
                let id = WorkerId { value: *id };
                let response = client.worker_client().await.delete(id).await.unwrap();
                println!("{:#?}", response);
            }
            WorkerCommand::Count {} => {
                let response = client
                    .worker_client()
                    .await
                    .count(CountCondition {})
                    .await
                    .unwrap();
                println!("{:#?}", response);
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
                let (op, _, _) =
                    WorkerSchemaCommand::find_descriptors(client, wdat.schema_id.unwrap()).await?;
                println!("[worker]:\n\t[id] {}", &wid.value);
                println!("\t[name] {}", &wdat.name);
                println!(
                    "\t[schema_id] {}",
                    wdat.schema_id.map(|s| s.value).unwrap_or_default()
                );
                let operation = ProtobufDescriptor::get_message_from_bytes(op, &wdat.operation)?;
                println!("\t[operation] |");
                ProtobufDescriptor::print_dynamic_message(&operation);
                println!("\t[periodic] {}", wdat.periodic_interval);
                println!("\t[channel] {:?}", wdat.channel);
                println!("\t[queue_type] {:?}", wdat.queue_type);
                println!("\t[response_type] {:?}", wdat.response_type);
                println!("\t[store_success] {}", wdat.store_success);
                println!("\t[store_failure] {}", wdat.store_failure);
                println!("\t[next_workers] {:?}", wdat.next_workers);
                println!("\t[use_static] {}", wdat.use_static);
            } else {
                println!("worker not found");
            }
            Ok(())
        }
    }
}
