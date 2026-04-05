// module for command line interface
#![allow(
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    clippy::module_name_repetitions,
    clippy::redundant_field_names,
    clippy::items_after_statements,
    clippy::similar_names,
    clippy::option_if_let_else,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::implicit_hasher
)]

use crate::jobworkerp::{
    data::WorkerId,
    service::{job_request, listen_by_worker_request, listen_request},
};
use anyhow::Result;
use serde::Deserialize;
use std::{collections::HashMap, str::FromStr};
use tonic::metadata;

pub mod function;
pub mod function_set;
pub mod job;
pub mod job_result;
pub mod job_status;
pub mod runner;
pub mod worker;
pub mod worker_instance;

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum WorkerIdOrName {
    Id(i64),
    Name(String),
}
impl WorkerIdOrName {
    #[must_use]
    pub fn to_job_worker(&self) -> job_request::Worker {
        match self {
            Self::Id(id) => job_request::Worker::WorkerId(WorkerId { value: *id }),
            Self::Name(name) => job_request::Worker::WorkerName(name.clone()),
        }
    }
    #[must_use]
    pub fn to_listen_worker(&self) -> listen_request::Worker {
        match self {
            Self::Id(id) => listen_request::Worker::WorkerId(WorkerId { value: *id }),
            Self::Name(name) => listen_request::Worker::WorkerName(name.clone()),
        }
    }
    #[must_use]
    pub fn to_listen_stream_worker(&self) -> listen_by_worker_request::Worker {
        match self {
            Self::Id(id) => listen_by_worker_request::Worker::WorkerId(WorkerId { value: *id }),
            Self::Name(name) => listen_by_worker_request::Worker::WorkerName(name.clone()),
        }
    }
}

impl FromStr for WorkerIdOrName {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<i64>()
            .map_or_else(|_| Ok(Self::Name(s.to_string())), |id| Ok(Self::Id(id)))
    }
}

/// # Errors
///
/// Returns an error if metadata keys or values are invalid.
pub fn to_request<T>(
    metadata: &HashMap<String, String>,
    request: impl tonic::IntoRequest<T>,
) -> Result<tonic::Request<T>> {
    let mut request = request.into_request();
    if !metadata.is_empty() {
        for (key, value) in metadata {
            request.metadata_mut().insert(
                metadata::MetadataKey::from_bytes(key.as_bytes())?,
                metadata::MetadataValue::try_from(value.as_str())?,
            );
        }
    }
    Ok(request)
}
