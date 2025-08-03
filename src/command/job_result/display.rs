//! JobResult-specific display functionality
//!
//! This module handles the conversion of JobResult data structures to JSON format
//! with appropriate enum decoration and streaming output support.

use crate::display::{format::EnumFormatter, DisplayFormat};
use crate::jobworkerp::data::{JobResult, ResultStatus};
use chrono::DateTime;
use command_utils::protobuf::ProtobufDescriptor;
use prost_reflect::MessageDescriptor;
use serde_json::Value as JsonValue;

/// Formatter for ResultStatus enum
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
pub fn format_timestamp(timestamp_millis: i64, format: &DisplayFormat) -> String {
    match format {
        DisplayFormat::Json => timestamp_millis.to_string(),
        _ => DateTime::from_timestamp_millis(timestamp_millis)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_default(),
    }
}

/// Calculate and format duration from start and end timestamps
pub fn format_duration(start_millis: i64, end_millis: i64, format: &DisplayFormat) -> String {
    match format {
        DisplayFormat::Json => (end_millis - start_millis).to_string(),
        _ => {
            if end_millis > start_millis {
                let duration_ms = end_millis - start_millis;
                if duration_ms < 1000 {
                    format!("{}ms", duration_ms)
                } else {
                    format!("{:.2}s", duration_ms as f64 / 1000.0)
                }
            } else {
                "-".to_string()
            }
        }
    }
}

/// Convert JobResult to JSON format with appropriate decoration
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
    }

    result_json
}

/// Convert streaming output data to JSON format
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
