// as worker_schema: valid commands are find, list, delete, count
// -i, --id <id> id of the job (for find, delete)
// --offset <offset> offset of the list (for list)
// --limit <limit> limit of the list (for list)

use crate::{
    client::JobworkerpClient,
    jobworkerp::{
        data::{WorkerSchema, WorkerSchemaData, WorkerSchemaId},
        service::{job_request, CountCondition, FindListRequest, WorkerNameRequest},
    },
};
use anyhow::Result;
use clap::Parser;
use command_utils::util::option::FlatMap;
use infra_utils::infra::protobuf::ProtobufDescriptor;
use prost::Message;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde_json::Deserializer;

#[derive(Parser, Debug)]
pub struct WorkerSchemaArg {
    #[clap(subcommand)]
    pub cmd: WorkerSchemaCommand,
}

#[derive(Parser, Debug)]
pub enum WorkerSchemaCommand {
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
    Delete {
        #[clap(short, long)]
        id: i64,
    },
    Count {},
}

impl WorkerSchemaCommand {
    pub async fn execute(&self, client: &JobworkerpClient) {
        match self {
            WorkerSchemaCommand::Find { id } => {
                let id = WorkerSchemaId { value: *id };
                let response = client
                    .worker_schema_client()
                    .await
                    .find(id)
                    .await
                    .unwrap()
                    .into_inner()
                    .data;
                if let Some(data) = response {
                    Self::print_worker_schema(&data);
                } else {
                    println!("schema not found");
                }
            }
            WorkerSchemaCommand::List { offset, limit } => {
                let request = FindListRequest {
                    offset: *offset,
                    limit: *limit,
                };
                let response = client
                    .worker_schema_client()
                    .await
                    .find_list(request)
                    .await
                    .unwrap();
                println!("meta: {:#?}", response.metadata());
                let mut data = response.into_inner();
                while let Some(data) = data.message().await.unwrap() {
                    Self::print_worker_schema(&data);
                }
                println!(
                    "trailer: {:#?}",
                    data.trailers().await.unwrap().unwrap_or_default()
                );
            }
            WorkerSchemaCommand::Delete { id } => {
                let id = WorkerSchemaId { value: *id };
                let response = client
                    .worker_schema_client()
                    .await
                    .delete(id)
                    .await
                    .unwrap();
                println!("{:#?}", response);
            }
            WorkerSchemaCommand::Count {} => {
                let response = client
                    .worker_schema_client()
                    .await
                    .count(CountCondition {})
                    .await
                    .unwrap();
                println!("{:#?}", response.into_inner().total);
            }
        }
    }
    pub async fn find_descriptors(
        client: &JobworkerpClient,
        schema_id: WorkerSchemaId,
    ) -> Result<(
        MessageDescriptor,
        MessageDescriptor,
        Option<MessageDescriptor>,
    )> {
        let response = client
            .worker_schema_client()
            .await
            .find(schema_id)
            .await?
            .into_inner()
            .data
            .flat_map(|r| r.data);
        if let Some(schema) = response {
            let operation_descriptor = Self::parse_operation_schema_descriptor(&schema)?;
            let arg_descriptor = Self::parse_arg_schema_descriptor(&schema)?;
            let result_descriptor = Self::parse_result_schema_descriptor(&schema)?;
            Ok((operation_descriptor, arg_descriptor, result_descriptor))
        } else {
            Err(anyhow::anyhow!("schema not found"))
        }
    }
    pub async fn find_descriptors_by_worker(
        client: &JobworkerpClient,
        worker: job_request::Worker,
    ) -> Result<(
        MessageDescriptor,
        MessageDescriptor,
        Option<MessageDescriptor>,
    )> {
        let schema_id = match worker {
            job_request::Worker::WorkerId(id) => client.worker_client().await.find(id).await?,
            job_request::Worker::WorkerName(name) => {
                client
                    .worker_client()
                    .await
                    .find_by_name(WorkerNameRequest { name })
                    .await?
            }
        }
        .into_inner()
        .data
        .flat_map(|r| r.data.flat_map(|r| r.schema_id))
        .ok_or(anyhow::anyhow!("schema not found"))?;
        Self::find_descriptors(client, schema_id).await
    }
    pub fn json_to_message(descriptor: MessageDescriptor, json_str: &str) -> Result<Vec<u8>> {
        let mut deserializer = Deserializer::from_str(json_str);
        let dynamic_message = DynamicMessage::deserialize(descriptor, &mut deserializer)?;
        deserializer.end()?;
        Ok(dynamic_message.encode_to_vec())
    }
    pub fn parse_operation_schema_descriptor(
        schema: &WorkerSchemaData,
    ) -> Result<MessageDescriptor> {
        let descriptor = ProtobufDescriptor::new(&schema.operation_proto)?;
        descriptor
            .get_messages()
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("message not found"))
    }
    pub fn parse_arg_schema_descriptor(schema: &WorkerSchemaData) -> Result<MessageDescriptor> {
        let descriptor = ProtobufDescriptor::new(&schema.job_arg_proto)?;
        descriptor
            .get_messages()
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("message not found"))
    }
    pub fn parse_result_schema_descriptor(
        schema: &WorkerSchemaData,
    ) -> Result<Option<MessageDescriptor>> {
        if let Some(proto) = &schema.result_output_proto {
            if proto.is_empty() {
                Ok(None)
            } else {
                let descriptor = ProtobufDescriptor::new(proto)?;
                descriptor
                    .get_messages()
                    .first()
                    .map(|m| Some(m.clone()))
                    .ok_or_else(|| anyhow::anyhow!("message not found"))
            }
        } else {
            Ok(None)
        }
    }
    fn print_worker_schema(schema: &WorkerSchema) {
        if let WorkerSchema {
            id: Some(_id),
            data: Some(data),
        } = schema
        {
            println!("[worker_schema]:\n\t[id] {}", &_id.value);
            println!("\t[name] {}", &data.name);
            println!("\t[runner_type] {}", &data.runner_type().as_str_name());
            println!("\t[operation_proto] |\n---\n{}", &data.operation_proto);
            println!("\t[job_arg_proto] |\n---\n{}", &data.job_arg_proto);
            println!(
                "\t[result_output_proto] |\n---\n{}",
                &data
                    .result_output_proto
                    .clone()
                    .unwrap_or("(None)".to_string())
            );
        } else {
            println!("[worker_schema]:\n\tdata is empty");
        }
    }
}
