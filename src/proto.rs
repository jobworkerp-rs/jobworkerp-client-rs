use crate::jobworkerp::data::{JobResultData, MethodProtoMap, Runner, Worker};
use crate::jobworkerp::service::WorkerNameRequest;
use crate::{
    client::JobworkerpClient,
    jobworkerp::{
        data::{RunnerData, RunnerId},
        service::job_request,
    },
};
use anyhow::{Context, Result};
use command_utils::protobuf::ProtobufDescriptor;
use prost_reflect::MessageDescriptor;

/// Get MethodSchema from method_proto_map with using parameter
///
/// # Arguments
/// * `method_proto_map` - Optional map of method names to schemas
/// * `using` - Optional method name to select
///
/// # Returns
/// * `Ok(Some(MethodSchema))` - Method found
/// * `Ok(None)` - method_proto_map is empty or None
/// * `Err` - using specified but method not found, or multiple methods without using
fn get_method_schema<'a>(
    method_proto_map: &'a Option<MethodProtoMap>,
    using: Option<&str>,
) -> Result<Option<&'a crate::jobworkerp::data::MethodSchema>> {
    let map = match method_proto_map {
        Some(m) => m,
        None => return Ok(None),
    };

    if map.schemas.is_empty() {
        return Ok(None);
    }

    // If using is specified, find that method
    if let Some(method_name) = using {
        return map.schemas.get(method_name).map(Some).ok_or_else(|| {
            anyhow::anyhow!(
                "Method '{}' not found. Available: [{}]",
                method_name,
                map.schemas
                    .keys()
                    .map(|k| k.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        });
    }

    // If using is None, auto-select if only one method exists
    if map.schemas.len() == 1 {
        Ok(map.schemas.values().next())
    } else {
        Err(anyhow::anyhow!(
            "Multiple methods available, 'using' required. Available: [{}]",
            map.schemas
                .keys()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }
}

pub struct JobworkerpProto {}

impl JobworkerpProto {
    pub async fn find_worker_runner_settings_descriptors(
        client: &JobworkerpClient,
        runner_id: RunnerId,
    ) -> Result<Option<MessageDescriptor>> {
        let response = client
            .runner_client()
            .await
            .find(runner_id)
            .await?
            .into_inner()
            .data
            .and_then(|r| r.data);
        if let Some(runner_data) = response {
            let runner_settings_descriptor =
                Self::parse_runner_settings_schema_descriptor(&runner_data)?;
            Ok(runner_settings_descriptor)
        } else {
            Err(anyhow::anyhow!(
                "runner not found: runner_id = {}",
                runner_id.value
            ))
        }
    }
    pub async fn find_runner_descriptors(
        client: &JobworkerpClient,
        runner_id: RunnerId,
        using: Option<&str>,
    ) -> Result<(
        Option<MessageDescriptor>,
        Option<MessageDescriptor>,
        Option<MessageDescriptor>,
    )> {
        let response = client
            .runner_client()
            .await
            .find(runner_id)
            .await?
            .into_inner()
            .data
            .and_then(|r| r.data);
        if let Some(runner_data) = response {
            let runner_settings_descriptor =
                Self::parse_runner_settings_schema_descriptor(&runner_data)?;
            let args_descriptor = Self::parse_job_args_schema_descriptor(&runner_data, using)?;
            let result_descriptor = Self::parse_result_schema_descriptor(&runner_data, using)?;
            Ok((
                runner_settings_descriptor,
                args_descriptor,
                result_descriptor,
            ))
        } else {
            Err(anyhow::anyhow!(
                "runner not found: runner_id = {}",
                runner_id.value
            ))
        }
    }
    pub async fn find_runner_descriptors_by_worker(
        client: &JobworkerpClient,
        worker: job_request::Worker,
        using: Option<&str>,
    ) -> Result<(
        Option<MessageDescriptor>,
        Option<MessageDescriptor>,
        Option<MessageDescriptor>,
    )> {
        let runner_id = match worker.clone() {
            job_request::Worker::WorkerId(id) => client.worker_client().await.find(id).await?,
            job_request::Worker::WorkerName(name) => client
                .worker_client()
                .await
                .find_by_name(WorkerNameRequest { name })
                .await
                .context("failed to find worker by name (find_runner_descriptors_by_worker)")?,
        }
        .into_inner()
        .data
        .and_then(|r| r.data.and_then(|r| r.runner_id))
        .ok_or(anyhow::anyhow!("runner not found: worker: {:?}", &worker))?;
        Self::find_runner_descriptors(client, runner_id, using).await
    }
    pub fn json_value_to_message(
        descriptor: MessageDescriptor,
        json_value: &serde_json::Value,
        normalize_enum: bool,
        _ignore_unknown_fields: bool,
    ) -> Result<Vec<u8>> {
        ProtobufDescriptor::json_value_to_message(descriptor, json_value, normalize_enum)
    }
    pub fn json_to_message(
        descriptor: MessageDescriptor,
        json_str: &str,
        _normalize_enum: bool,
    ) -> Result<Vec<u8>> {
        ProtobufDescriptor::json_to_message(descriptor, json_str)
    }
    pub fn parse_runner_settings_schema_descriptor(
        runner_data: &RunnerData,
    ) -> Result<Option<MessageDescriptor>> {
        if runner_data.runner_settings_proto.is_empty() {
            Ok(None)
        } else {
            let descriptor = ProtobufDescriptor::new(&runner_data.runner_settings_proto)?;
            descriptor
                .get_messages()
                .first()
                .map(|m| Some(m.clone()))
                .ok_or_else(|| anyhow::anyhow!("message not found"))
        }
    }
    pub fn parse_job_args_schema_descriptor(
        runner_data: &RunnerData,
        using: Option<&str>,
    ) -> Result<Option<MessageDescriptor>> {
        let method_schema = get_method_schema(&runner_data.method_proto_map, using)?;

        match method_schema {
            Some(schema) if !schema.args_proto.is_empty() => {
                let descriptor = ProtobufDescriptor::new(&schema.args_proto)?;
                descriptor
                    .get_messages()
                    .first()
                    .map(|m| Some(m.clone()))
                    .ok_or_else(|| anyhow::anyhow!("message not found in args_proto"))
            }
            _ => Ok(None),
        }
    }
    pub fn parse_result_schema_descriptor(
        runner_data: &RunnerData,
        using: Option<&str>,
    ) -> Result<Option<MessageDescriptor>> {
        let method_schema = get_method_schema(&runner_data.method_proto_map, using)?;

        match method_schema {
            Some(schema) if !schema.result_proto.is_empty() => {
                let descriptor = ProtobufDescriptor::new(&schema.result_proto)?;
                descriptor
                    .get_messages()
                    .first()
                    .map(|m| Some(m.clone()))
                    .ok_or_else(|| anyhow::anyhow!("message not found in result_proto"))
            }
            _ => Ok(None),
        }
    }
    pub async fn resolve_result_descriptor(
        client: &crate::client::JobworkerpClient,
        worker_name: &str,
        using: Option<&str>,
    ) -> Option<MessageDescriptor> {
        match client
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
            Some(Worker {
                id: Some(_wid),
                data: Some(wdata),
            }) => {
                tracing::debug!("worker {} found: {:#?}", worker_name, &wdata);
                match client
                    .runner_client()
                    .await
                    .find(wdata.runner_id.unwrap())
                    .await
                    .unwrap()
                    .into_inner()
                    .data
                {
                    Some(Runner {
                        id: Some(_sid),
                        data: Some(sdata),
                    }) => {
                        tracing::debug!("runner for worker {} found: {:#?}", worker_name, &sdata);
                        // Use method_proto_map instead of result_output_proto
                        match Self::parse_result_schema_descriptor(&sdata, using) {
                            Ok(Some(descriptor)) => Some(descriptor),
                            Ok(None) => {
                                tracing::debug!("no result schema for worker: {}", worker_name);
                                None
                            }
                            Err(e) => {
                                tracing::warn!("failed to parse result schema: {:#?}", e);
                                None
                            }
                        }
                    }
                    _ => {
                        tracing::warn!("runner not found: {:#?}", &wdata.runner_id);
                        None
                    }
                }
            }
            _ => {
                tracing::warn!("worker not found: {:#?}", &worker_name);
                None
            }
        }
    }
    pub async fn resolve_result_output_to_json(
        client: &crate::client::JobworkerpClient,
        worker_name: &str,
        result_data: &JobResultData,
        using: Option<&str>,
    ) -> Result<serde_json::Value> {
        let output_text: serde_json::Value = match result_data.output.as_ref() {
            Some(output) if !output.items.is_empty() => {
                let result_proto =
                    JobworkerpProto::resolve_result_descriptor(client, worker_name, using).await;
                if let Some(proto) = result_proto.as_ref() {
                    let mut output_array = Vec::new();
                    let item = output.items.as_slice();
                    if !item.is_empty() {
                        tracing::debug!(
                            "protobuf decode item: {}, worker: {}",
                            item.len(),
                            worker_name
                        );
                        match ProtobufDescriptor::get_message_from_bytes(proto.clone(), item) {
                            Ok(mes) => {
                                // using result for warning only
                                let _ = ProtobufDescriptor::message_to_json_value(&mes)
                                    .map(|json| {
                                        output_array.push(json);
                                    })
                                    .map_err(|e| {
                                        tracing::warn!("protobuf decode error: {:#?}", e);
                                    });
                            }
                            Err(e) => {
                                tracing::warn!("protobuf decode error: {:#?}", e);
                            }
                        }
                    }
                    if output_array.is_empty() {
                        serde_json::Value::String("".to_string())
                    } else if output_array.len() == 1 {
                        output_array[0].clone()
                    } else {
                        serde_json::Value::Array(output_array)
                    }
                } else if !output.items.is_empty() {
                    tracing::debug!("empty proto (means raw bytes): worker={}", worker_name);
                    serde_json::Value::String(String::from_utf8_lossy(&output.items).into_owned())
                } else {
                    tracing::debug!("empty proto, empty item: worker={}", worker_name);
                    serde_json::Value::Null
                }
            }
            Some(_output) => serde_json::Value::String("".to_string()),
            None => serde_json::Value::Null,
        };
        Ok(output_text)
    }
    pub async fn resolve_result_output_to_string(
        client: &crate::client::JobworkerpClient,
        worker_name: &str,
        result_data: &JobResultData,
        using: Option<&str>,
    ) -> Result<String> {
        let output_text: String = match result_data.output.as_ref() {
            Some(output) if !output.items.is_empty() => {
                let result_proto =
                    JobworkerpProto::resolve_result_descriptor(client, worker_name, using).await;
                if let Some(proto) = result_proto.as_ref() {
                    let mut output_text = "".to_string();
                    let item = output.items.clone();
                    if !item.is_empty() {
                        tracing::debug!(
                            "protobuf decode item: {}, worker: {}",
                            output.items.len(),
                            worker_name
                        );
                        tracing::debug!(
                            "protobuf decode item: {}, worker: {}",
                            item.len(),
                            worker_name
                        );
                        match ProtobufDescriptor::get_message_from_bytes(
                            proto.clone(),
                            item.as_slice(),
                        ) {
                            Ok(mes) => {
                                output_text +=
                                    ProtobufDescriptor::dynamic_message_to_string(&mes, false)
                                        .as_str();
                            }
                            Err(e) => {
                                output_text += format!("protobuf decode error: {e:#?}").as_str();
                            }
                        }
                    }
                    output_text
                } else if !output.items.is_empty() {
                    tracing::debug!("empty proto (means raw bytes): worker={}", worker_name);
                    String::from_utf8_lossy(&output.items).into_owned()
                } else {
                    tracing::debug!("empty proto, empty item: worker={}", worker_name);
                    "".to_string()
                }
            }
            Some(output) => String::from_utf8_lossy(output.items.as_slice()).into_owned(),
            None => "".to_string(),
        };
        Ok(output_text)
    }
}
