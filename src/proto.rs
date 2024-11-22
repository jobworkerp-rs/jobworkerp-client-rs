use crate::jobworkerp::data::{Worker, WorkerSchema};
use crate::jobworkerp::service::WorkerNameRequest;
use crate::{
    client::JobworkerpClient,
    jobworkerp::{
        data::{WorkerSchemaData, WorkerSchemaId},
        service::job_request,
    },
};
use anyhow::Result;
use command_utils::util::option::FlatMap;
use infra_utils::infra::protobuf::ProtobufDescriptor;
use prost::Message;
use prost_reflect::{DynamicMessage, MessageDescriptor};
use serde_json::Deserializer;

pub struct JobworkerpProto {}

impl JobworkerpProto {
    pub async fn find_worker_operation_descriptors(
        client: &JobworkerpClient,
        schema_id: WorkerSchemaId,
    ) -> Result<Option<MessageDescriptor>> {
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
            Ok(operation_descriptor)
        } else {
            Err(anyhow::anyhow!("schema not found"))
        }
    }
    pub async fn find_worker_schema_descriptors(
        client: &JobworkerpClient,
        schema_id: WorkerSchemaId,
    ) -> Result<(
        Option<MessageDescriptor>,
        Option<MessageDescriptor>,
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
    pub async fn find_worker_schema_descriptors_by_worker(
        client: &JobworkerpClient,
        worker: job_request::Worker,
    ) -> Result<(
        Option<MessageDescriptor>,
        Option<MessageDescriptor>,
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
        Self::find_worker_schema_descriptors(client, schema_id).await
    }
    pub fn json_to_message(descriptor: MessageDescriptor, json_str: &str) -> Result<Vec<u8>> {
        let mut deserializer = Deserializer::from_str(json_str);
        let dynamic_message = DynamicMessage::deserialize(descriptor, &mut deserializer)?;
        deserializer.end()?;
        Ok(dynamic_message.encode_to_vec())
    }
    pub fn parse_operation_schema_descriptor(
        schema: &WorkerSchemaData,
    ) -> Result<Option<MessageDescriptor>> {
        if schema.operation_proto.is_empty() {
            Ok(None)
        } else {
            let descriptor = ProtobufDescriptor::new(&schema.operation_proto)?;
            descriptor
                .get_messages()
                .first()
                .map(|m| Some(m.clone()))
                .ok_or_else(|| anyhow::anyhow!("message not found"))
        }
    }
    pub fn parse_arg_schema_descriptor(
        schema: &WorkerSchemaData,
    ) -> Result<Option<MessageDescriptor>> {
        if schema.job_arg_proto.is_empty() {
            Ok(None)
        } else {
            let descriptor = ProtobufDescriptor::new(&schema.job_arg_proto)?;
            descriptor
                .get_messages()
                .first()
                .map(|m| Some(m.clone()))
                .ok_or_else(|| anyhow::anyhow!("message not found"))
        }
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
    pub async fn resolve_result_descriptor(
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
}
