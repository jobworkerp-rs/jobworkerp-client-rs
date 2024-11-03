use crate::jobworkerp::{data::WorkerId, service::job_request};
use anyhow::Result;
use prost_reflect::MessageDescriptor;
use worker_schema::WorkerSchemaCommand;

pub mod job;
pub mod job_result;
pub mod worker;
pub mod worker_schema;

pub async fn resolve_protos_by_worker_name(
    client: &crate::client::JobworkerpClient,
    worker_name: &str,
) -> Result<(
    MessageDescriptor,
    MessageDescriptor,
    Option<MessageDescriptor>,
)> {
    WorkerSchemaCommand::find_descriptors_by_worker(
        client,
        job_request::Worker::WorkerName(worker_name.to_string()),
    )
    .await
}

pub async fn resolve_protos_by_worker_id(
    client: &crate::client::JobworkerpClient,
    worker_id: &WorkerId,
) -> Result<(
    MessageDescriptor,
    MessageDescriptor,
    Option<MessageDescriptor>,
)> {
    WorkerSchemaCommand::find_descriptors_by_worker(
        client,
        job_request::Worker::WorkerId(*worker_id),
    )
    .await
}
