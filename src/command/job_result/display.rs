#![allow(
    clippy::doc_markdown,
    clippy::must_use_candidate,
    clippy::option_if_let_else,
    clippy::cast_precision_loss
)]

//! `JobResult`-specific display functionality
//!
//! This module handles the conversion of `JobResult` data structures to JSON format
//! with appropriate enum decoration and streaming output support.

use crate::display::{DisplayFormat, format::EnumFormatter};
use crate::jobworkerp::data::{JobResult, ResultStatus, StreamError};
use crate::stream_error::{code_name, problem_fields};
use chrono::DateTime;
use command_utils::protobuf::ProtobufDescriptor;
use prost_reflect::MessageDescriptor;
use serde_json::Value as JsonValue;

/// Formatter for `ResultStatus` enum
pub struct ResultStatusFormatter;

impl EnumFormatter<ResultStatus> for ResultStatusFormatter {
    fn format(&self, status: ResultStatus, format: &DisplayFormat) -> String {
        match format {
            DisplayFormat::Table => match status {
                ResultStatus::Success => "Success",
                ResultStatus::ErrorAndRetry => "Error (Retry)",
                ResultStatus::FatalError => "Fatal Error",
                ResultStatus::Abort => "Aborted",
                ResultStatus::MaxRetry => "Max Retry",
                ResultStatus::OtherError => "Other Error",
                ResultStatus::Cancelled => "Cancelled",
            }
            .to_string(),
            DisplayFormat::Card => match status {
                ResultStatus::Success => "✅ Success",
                ResultStatus::ErrorAndRetry => "⚠️ Error (Retry)",
                ResultStatus::FatalError => "❌ Fatal Error",
                ResultStatus::Abort => "🛑 Aborted",
                ResultStatus::MaxRetry => "🔄 Max Retry",
                ResultStatus::OtherError => "❌ Other Error",
                ResultStatus::Cancelled => "❌ Cancelled",
            }
            .to_string(),
            DisplayFormat::Json => status.as_str_name().to_string(),
        }
    }
}

/// Format timestamp for display
#[must_use]
pub fn format_timestamp(timestamp_millis: i64, format: &DisplayFormat) -> String {
    match format {
        DisplayFormat::Json => timestamp_millis.to_string(),
        _ => DateTime::from_timestamp_millis(timestamp_millis)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_default(),
    }
}

/// Calculate and format duration from start and end timestamps
#[must_use]
pub fn format_duration(start_millis: i64, end_millis: i64, format: &DisplayFormat) -> String {
    match format {
        DisplayFormat::Json => (end_millis - start_millis).to_string(),
        _ => {
            if end_millis > start_millis {
                let duration_ms = end_millis - start_millis;
                if duration_ms < 1000 {
                    format!("{duration_ms}ms")
                } else {
                    format!("{:.2}s", duration_ms as f64 / 1000.0)
                }
            } else {
                "-".to_string()
            }
        }
    }
}

/// Convert `JobResult` to JSON format with appropriate decoration
#[must_use]
pub fn job_result_to_json(
    job_result: &JobResult,
    result_descriptor: Option<MessageDescriptor>,
    format: &DisplayFormat,
) -> JsonValue {
    let mut result_json = serde_json::json!({});

    if let Some(id) = &job_result.id {
        result_json["result_id"] = serde_json::json!(id.value);
    }

    if let Some(data) = &job_result.data {
        result_json["job_id"] =
            serde_json::json!(data.job_id.as_ref().map(|j| j.value).unwrap_or_default());
        result_json["worker_name"] = serde_json::json!(data.worker_name);

        // Format status with decoration
        let status_formatter = ResultStatusFormatter;
        result_json["status"] = serde_json::json!(status_formatter.format(data.status(), format));

        // Format timestamps
        result_json["start_time"] = serde_json::json!(format_timestamp(data.start_time, format));
        result_json["end_time"] = serde_json::json!(format_timestamp(data.end_time, format));
        result_json["duration"] =
            serde_json::json!(format_duration(data.start_time, data.end_time, format));

        // Process output data
        if let Some(output) = &data.output {
            if let Some(descriptor) = result_descriptor {
                // Try to parse as protobuf message
                match ProtobufDescriptor::get_message_from_bytes(descriptor, &output.items) {
                    Ok(message) => match ProtobufDescriptor::message_to_json_value(&message) {
                        Ok(output_json) => {
                            result_json["output"] = output_json;
                        }
                        Err(_) => {
                            result_json["output"] =
                                serde_json::json!(String::from_utf8_lossy(&output.items));
                        }
                    },
                    Err(_) => {
                        result_json["output"] =
                            serde_json::json!(String::from_utf8_lossy(&output.items));
                    }
                }
            } else {
                // No descriptor, treat as string
                result_json["output"] = serde_json::json!(String::from_utf8_lossy(&output.items));
            }
        } else {
            result_json["output"] = serde_json::json!(null);
        }

        if let Some(error) = &data.error {
            result_json["error"] = stream_error_to_json(error);
        }
    }

    result_json
}

/// JSON form of a failure classification, with the code by name as in the server's wording (the
/// derived serialization would show the bare enum number). The problem keeps its fields in RFC 9457
/// order and appears only when typed.
fn stream_error_to_json(error: &StreamError) -> JsonValue {
    let mut json = serde_json::json!({
        "code": code_name(error.code),
        "origin": error.origin,
        "message": error.message,
    });
    let problem = error
        .problem
        .as_ref()
        .map(problem_fields)
        .unwrap_or_default();
    if !problem.is_empty() {
        json["problem"] = problem
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect::<serde_json::Map<_, _>>()
            .into();
    }
    json
}

/// Convert streaming output data to JSON format
#[must_use]
pub fn streaming_output_to_json(
    data: &[u8],
    result_descriptor: Option<MessageDescriptor>,
    _format: &DisplayFormat,
) -> JsonValue {
    if let Some(descriptor) = result_descriptor {
        // Try to parse as protobuf message
        match ProtobufDescriptor::get_message_from_bytes(descriptor, data) {
            Ok(message) => match ProtobufDescriptor::message_to_json_value(&message) {
                Ok(output_json) => output_json,
                Err(_) => serde_json::json!(String::from_utf8_lossy(data)),
            },
            Err(_) => serde_json::json!(String::from_utf8_lossy(data)),
        }
    } else {
        // No descriptor, treat as string
        serde_json::json!(String::from_utf8_lossy(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobworkerp::data::{JobResultData, ProblemDetails, ResultOutput, StreamErrorCode};

    fn job_result(error: Option<StreamError>) -> JobResult {
        JobResult {
            id: None,
            data: Some(JobResultData {
                status: ResultStatus::MaxRetry as i32,
                output: Some(ResultOutput {
                    items: b"failed".to_vec(),
                }),
                error,
                ..Default::default()
            }),
            metadata: std::collections::HashMap::new(),
        }
    }

    // Successes and results read back from the RDB have no classification: keep the output shape.
    #[test]
    fn job_result_json_omits_error_without_classification() {
        let json = job_result_to_json(&job_result(None), None, &DisplayFormat::Json);
        assert!(json.get("error").is_none(), "got: {json}");
    }

    #[test]
    fn job_result_json_shows_the_classification_and_problem() {
        let error = StreamError {
            code: StreamErrorCode::Timeout as i32,
            message: "timed out".to_string(),
            origin: "WORKFLOW".to_string(),
            problem: Some(ProblemDetails {
                r#type: "urn:t".to_string(),
                instance: Some("/do/0".to_string()),
                ..Default::default()
            }),
        };
        let json = job_result_to_json(&job_result(Some(error)), None, &DisplayFormat::Json);
        assert_eq!(
            json["error"],
            serde_json::json!({
                "code": "TIMEOUT",
                "origin": "WORKFLOW",
                "message": "timed out",
                "problem": {"type": "urn:t", "instance": "/do/0"}
            })
        );
    }

    #[test]
    fn stream_error_json_names_the_code() {
        let error = StreamError {
            code: StreamErrorCode::InvalidArgument as i32,
            message: "bad".to_string(),
            origin: "GRPC".to_string(),
            ..Default::default()
        };
        assert_eq!(
            stream_error_to_json(&error),
            serde_json::json!({"code": "INVALID_ARGUMENT", "origin": "GRPC", "message": "bad"})
        );
        let unknown = StreamError {
            code: 99,
            ..Default::default()
        };
        assert_eq!(
            stream_error_to_json(&unknown)["code"],
            serde_json::json!("99")
        );
    }

    // A problem without a type identifies nothing, so it is left out.
    #[test]
    fn stream_error_json_omits_an_untyped_problem() {
        let error = StreamError {
            problem: Some(ProblemDetails {
                status: 500,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(stream_error_to_json(&error).get("problem").is_none());
    }
}
