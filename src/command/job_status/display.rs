#![allow(clippy::doc_markdown, clippy::must_use_candidate)]

//! `JobProcessingStatus`-specific display functionality
//!
//! This module handles the conversion of `JobProcessingStatus` data structures to JSON format
//! with appropriate enum decoration and formatting.

use crate::display::DisplayFormat;
use crate::display::format::EnumFormatter;
use crate::jobworkerp::data::JobProcessingStatus;
use crate::jobworkerp::service::{JobProcessingStatusDetailResponse, JobProcessingStatusResponse};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

/// Formatter for `JobProcessingStatus` enum
pub struct JobProcessingStatusFormatter;

impl EnumFormatter<JobProcessingStatus> for JobProcessingStatusFormatter {
    fn format(&self, status: JobProcessingStatus, format: &DisplayFormat) -> String {
        match format {
            DisplayFormat::Table => match status {
                JobProcessingStatus::Unknown => "UNKNOWN",
                JobProcessingStatus::Pending => "PENDING",
                JobProcessingStatus::Running => "RUNNING",
                JobProcessingStatus::WaitResult => "WAIT_RESULT",
                JobProcessingStatus::Cancelling => "CANCELLING",
            }
            .to_string(),
            DisplayFormat::Card => match status {
                JobProcessingStatus::Unknown => "? UNKNOWN",
                JobProcessingStatus::Pending => "... PENDING",
                JobProcessingStatus::Running => "> RUNNING",
                JobProcessingStatus::WaitResult => "~ WAIT_RESULT",
                JobProcessingStatus::Cancelling => "x CANCELLING",
            }
            .to_string(),
            DisplayFormat::Json => status.as_str_name().to_string(),
        }
    }
}

/// Format timestamp (milliseconds) as ISO 8601 string
fn format_timestamp(millis: i64, format: DisplayFormat) -> JsonValue {
    if format == DisplayFormat::Json {
        serde_json::json!(millis)
    } else {
        DateTime::<Utc>::from_timestamp_millis(millis).map_or_else(
            || serde_json::json!(millis),
            |dt| serde_json::json!(dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
        )
    }
}

/// Format optional timestamp
fn format_optional_timestamp(millis: Option<i64>, format: DisplayFormat) -> JsonValue {
    millis.map_or(serde_json::json!(null), |m| format_timestamp(m, format))
}

/// Convert `JobProcessingStatusResponse` to JSON with format-specific enum decoration
#[must_use]
pub fn job_processing_status_to_json(
    response: &JobProcessingStatusResponse,
    format: &DisplayFormat,
) -> JsonValue {
    let status_formatter = JobProcessingStatusFormatter;

    serde_json::json!({
        "id": response.id.as_ref().map(|id| id.value),
        "status": status_formatter.format(response.status(), format),
    })
}

/// Convert `JobProcessingStatusDetailResponse` to JSON with format-specific decoration
#[must_use]
pub fn job_processing_status_detail_to_json(
    response: &JobProcessingStatusDetailResponse,
    format: &DisplayFormat,
) -> JsonValue {
    let status_formatter = JobProcessingStatusFormatter;

    let mut json = serde_json::json!({
        "id": response.id.as_ref().map(|id| id.value),
        "status": status_formatter.format(response.status(), format),
        "worker_id": response.worker_id,
        "channel": response.channel,
        "priority": response.priority,
        "enqueue_time": format_timestamp(response.enqueue_time, *format),
        "updated_at": format_timestamp(response.updated_at, *format),
    });

    // Optional fields
    if response.start_time.is_some() {
        json["start_time"] = format_optional_timestamp(response.start_time, *format);
    }
    if response.pending_time.is_some() {
        json["pending_time"] = format_optional_timestamp(response.pending_time, *format);
    }
    if let Some(is_streamable) = response.is_streamable {
        json["is_streamable"] = serde_json::json!(is_streamable);
    }
    if let Some(broadcast_results) = response.broadcast_results {
        json["broadcast_results"] = serde_json::json!(broadcast_results);
    }

    json
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
            "RUNNING"
        );

        // Test card format
        assert_eq!(
            formatter.format(JobProcessingStatus::Pending, &DisplayFormat::Card),
            "... PENDING"
        );

        // Test JSON format
        assert_eq!(
            formatter.format(JobProcessingStatus::WaitResult, &DisplayFormat::Json),
            "WAIT_RESULT"
        );
    }

    #[test]
    fn test_format_timestamp() {
        let millis = 1_703_001_600_000_i64; // 2023-12-19 12:00:00 UTC

        // JSON returns raw millis
        let json_result = format_timestamp(millis, DisplayFormat::Json);
        assert_eq!(json_result, serde_json::json!(millis));

        // Other formats return formatted string
        let table_result = format_timestamp(millis, DisplayFormat::Table);
        assert!(table_result.is_string());
    }
}
