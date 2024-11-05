use std::str::FromStr;

use crate::jobworkerp::{
    data::WorkerId,
    service::{job_request, listen_request},
};
use anyhow::Result;
use prost_reflect::MessageDescriptor;
use serde::Deserialize;
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

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum WorkerIdOrName {
    Id(i64),
    Name(String),
}
impl WorkerIdOrName {
    pub fn to_job_worker(&self) -> job_request::Worker {
        match self {
            WorkerIdOrName::Id(id) => job_request::Worker::WorkerId(WorkerId { value: *id }),
            WorkerIdOrName::Name(name) => job_request::Worker::WorkerName(name.clone()),
        }
    }
    pub fn to_listen_worker(&self) -> listen_request::Worker {
        match self {
            WorkerIdOrName::Id(id) => listen_request::Worker::WorkerId(WorkerId { value: *id }),
            WorkerIdOrName::Name(name) => listen_request::Worker::WorkerName(name.clone()),
        }
    }
}

impl FromStr for WorkerIdOrName {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(id) = s.parse::<i64>() {
            Ok(WorkerIdOrName::Id(id))
        } else {
            Ok(WorkerIdOrName::Name(s.to_string()))
        }
    }
}
