//! Job-specific display functionality
//!
//! This module handles the conversion of Job data structures to JSON format
//! with appropriate enum decoration based on display format.

use crate::display::{format::EnumFormatter, DisplayFormat};
use crate::jobworkerp::data::{Job, JobProcessingStatus, Priority};
use chrono::DateTime;
use command_utils::protobuf::ProtobufDescriptor;
use prost_reflect::MessageDescriptor;
use serde_json::Value as JsonValue;

/// Formatter for JobProcessingStatus enum
pub struct JobProcessingStatusFormatter;

impl EnumFormatter<JobProcessingStatus> for JobProcessingStatusFormatter {
    fn format(&self, status: JobProcessingStatus, format: &DisplayFormat) -> String {
        match format {
            DisplayFormat::Table => match status {
                JobProcessingStatus::Running => "Running",
                JobProcessingStatus::Pending => "Pending",
                JobProcessingStatus::WaitResult => "Wait Result",
                JobProcessingStatus::Cancelling => "Cancelling",
                JobProcessingStatus::Unknown => "Unknown",
            }
            .to_string(),
            DisplayFormat::Card => match status {
                JobProcessingStatus::Running => "🟢 Running",
                JobProcessingStatus::Pending => "🟡 Pending",
                JobProcessingStatus::WaitResult => "🔵 Wait Result",
                JobProcessingStatus::Cancelling => "🔴 Cancelling",
                JobProcessingStatus::Unknown => "⚫ Unknown",
            }
            .to_string(),
            DisplayFormat::Json => status.as_str_name().to_string(),
        }
    }
}

/// Formatter for Priority enum
pub struct PriorityFormatter;

impl EnumFormatter<Priority> for PriorityFormatter {
    fn format(&self, priority: Priority, format: &DisplayFormat) -> String {
        match format {
            DisplayFormat::Table => match priority {
                Priority::High => "High",
                Priority::Medium => "Medium",
                Priority::Low => "Low",
            }
            .to_string(),
            DisplayFormat::Card => match priority {
                Priority::High => "🔴 High",
                Priority::Medium => "🟠 Medium",
                Priority::Low => "Low",
            }
            .to_string(),
            DisplayFormat::Json => priority.as_str_name().to_string(),
        }
    }
}

/// Convert Job to JSON with format-specific enum decoration
pub fn job_to_json(
    job: &Job,
    processing_status: Option<JobProcessingStatus>,
    args_descriptor: Option<MessageDescriptor>,
    format: &DisplayFormat,
) -> JsonValue {
    let status_formatter = JobProcessingStatusFormatter;
    let priority_formatter = PriorityFormatter;

    let mut job_json = serde_json::json!({
        "id": job.id.as_ref().map(|id| id.value),
        "worker_id": job.data.as_ref().and_then(|d| d.worker_id.as_ref().map(|w| w.value)),
        "run_after": job.data.as_ref().map(|d| {
            if d.run_after_time == 0 {
                "".to_string()
            } else {
                format_timestamp(d.run_after_time)
            }
        }),
        "timeout_seconds": job.data.as_ref().map(|d| d.timeout / 1000),
        "unique_key": job.data.as_ref().and_then(|d| d.uniq_key.clone()),
    });

    // Add processing status with format-specific decoration
    if let Some(status) = processing_status {
        job_json["status"] = JsonValue::String(status_formatter.format(status, format));
    }

    // Add priority with format-specific decoration
    if let Some(data) = job.data.as_ref() {
        job_json["priority"] =
            JsonValue::String(priority_formatter.format(data.priority(), format));
    }

    // Add protobuf arguments as-is (using existing message_to_json_value)
    if let (Some(data), Some(descriptor)) = (job.data.as_ref(), args_descriptor) {
        if let Ok(args_message) = ProtobufDescriptor::get_message_from_bytes(descriptor, &data.args)
        {
            if let Ok(args_json) = ProtobufDescriptor::message_to_json_value(&args_message) {
                job_json["arguments"] = args_json;
            }
        }
    }

    job_json
}

/// Format Unix timestamp (milliseconds) to ISO 8601 string
fn format_timestamp(timestamp_ms: i64) -> String {
    let timestamp_secs = timestamp_ms / 1000;
    let nanosecs = ((timestamp_ms % 1000) * 1_000_000) as u32;

    match DateTime::from_timestamp(timestamp_secs, nanosecs) {
        Some(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        None => "Invalid timestamp".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_processing_status_formatter() {
        let formatter = JobProcessingStatusFormatter;

        // Test table format
        assert_eq!(
            formatter.format(JobProcessingStatus::Running, &DisplayFormat::Table),
            "Running"
        );

        // Test JSON format
        assert_eq!(
            formatter.format(JobProcessingStatus::Running, &DisplayFormat::Json),
            "RUNNING"
        );
    }

    #[test]
    fn test_priority_formatter() {
        let formatter = PriorityFormatter;

        // Test card format
        assert_eq!(
            formatter.format(Priority::High, &DisplayFormat::Card),
            "🔴 High"
        );

        // Test JSON format
        assert_eq!(
            formatter.format(Priority::High, &DisplayFormat::Json),
            "PRIORITY_HIGH"
        );
    }

    #[test]
    fn test_format_timestamp() {
        // Test valid timestamp
        let timestamp = 1642680000000; // 2022-01-20T12:00:00.000Z
        let result = format_timestamp(timestamp);
        assert!(result.contains("2022-01-20T12:00:00"));

        // Test zero timestamp
        let result = format_timestamp(0);
        assert_eq!(result, "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn test_run_after_zero_handling() {
        use crate::display::DisplayFormat;
        use crate::jobworkerp::data::{Job, JobData, JobId, Priority};
        use std::collections::HashMap;

        // Create a job with run_after_time = 0
        let job = Job {
            id: Some(JobId { value: 123 }),
            data: Some(JobData {
                worker_id: None,
                args: vec![],
                uniq_key: None,
                enqueue_time: 1640000000000,
                grabbed_until_time: None,
                run_after_time: 0,
                retried: 0,
                priority: Priority::Medium as i32,
                timeout: 30000,
                request_streaming: false,
            }),
            metadata: HashMap::new(),
        };

        let result = job_to_json(&job, None, None, &DisplayFormat::Json);

        // run_after should be empty string when run_after_time is 0
        assert_eq!(result["run_after"], "");
    }

    #[test]
    fn test_run_after_non_zero_handling() {
        use crate::display::DisplayFormat;
        use crate::jobworkerp::data::{Job, JobData, JobId, Priority};
        use std::collections::HashMap;

        // Create a job with non-zero run_after_time
        let job = Job {
            id: Some(JobId { value: 123 }),
            data: Some(JobData {
                worker_id: None,
                args: vec![],
                uniq_key: None,
                enqueue_time: 1640000000000,
                grabbed_until_time: None,
                run_after_time: 1642680000000, // 2022-01-20T12:00:00.000Z
                retried: 0,
                priority: Priority::Medium as i32,
                timeout: 30000,
                request_streaming: false,
            }),
            metadata: HashMap::new(),
        };

        let result = job_to_json(&job, None, None, &DisplayFormat::Json);

        // run_after should be formatted timestamp when run_after_time is not 0
        assert!(result["run_after"]
            .as_str()
            .unwrap()
            .contains("2022-01-20T12:00:00"));
    }
}
