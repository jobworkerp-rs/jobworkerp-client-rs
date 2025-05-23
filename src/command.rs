// module for command line interface

use crate::jobworkerp::{
    data::WorkerId,
    service::{job_request, listen_by_worker_request, listen_request},
};
use anyhow::Result;
use serde::Deserialize;
use std::str::FromStr;

pub mod function;
pub mod function_set;
pub mod job;
pub mod job_result;
pub mod runner;
pub mod worker;

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
    pub fn to_listen_stream_worker(&self) -> listen_by_worker_request::Worker {
        match self {
            WorkerIdOrName::Id(id) => {
                listen_by_worker_request::Worker::WorkerId(WorkerId { value: *id })
            }
            WorkerIdOrName::Name(name) => {
                listen_by_worker_request::Worker::WorkerName(name.clone())
            }
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
