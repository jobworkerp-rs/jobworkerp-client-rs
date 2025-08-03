//! Worker-specific display functionality
//!
//! This module handles the conversion of Worker data structures to JSON format
//! with appropriate enum decoration and formatting.

use crate::display::{format::EnumFormatter, DisplayFormat};
use crate::jobworkerp::data::{QueueType, ResponseType, Worker};
use command_utils::protobuf::ProtobufDescriptor;
use prost_reflect::MessageDescriptor;
use serde_json::Value as JsonValue;

/// Formatter for QueueType enum
pub struct QueueTypeFormatter;

impl EnumFormatter<QueueType> for QueueTypeFormatter {
    fn format(&self, queue_type: QueueType, format: &DisplayFormat) -> String {
        match format {
            DisplayFormat::Table => match queue_type {
                QueueType::Normal => "NORMAL",
                QueueType::ForcedRdb => "FORCED_RDB",
                QueueType::WithBackup => "WITH_BACKUP",
            }
            .to_string(),
            DisplayFormat::Card => match queue_type {
                QueueType::Normal => "📝 NORMAL",
                QueueType::ForcedRdb => "🗃️ FORCED_RDB",
                QueueType::WithBackup => "💾 WITH_BACKUP",
            }
            .to_string(),
            DisplayFormat::Json => queue_type.as_str_name().to_string(),
        }
    }
}

/// Formatter for ResponseType enum
pub struct ResponseTypeFormatter;

impl EnumFormatter<ResponseType> for ResponseTypeFormatter {
    fn format(&self, response_type: ResponseType, format: &DisplayFormat) -> String {
        match format {
            DisplayFormat::Table => match response_type {
                ResponseType::NoResult => "NO_RESULT",
                ResponseType::Direct => "DIRECT",
            }
            .to_string(),
            DisplayFormat::Card => match response_type {
                ResponseType::NoResult => "🚫 NO_RESULT",
                ResponseType::Direct => "⚡ DIRECT",
            }
            .to_string(),
            DisplayFormat::Json => response_type.as_str_name().to_string(),
        }
    }
}

/// Format boolean values with appropriate decoration
pub fn format_boolean(value: bool, format: &DisplayFormat) -> String {
    match format {
        DisplayFormat::Table => value.to_string(),
        DisplayFormat::Card => {
            if value {
                "✅ true".to_string()
            } else {
                "❌ false".to_string()
            }
        }
        DisplayFormat::Json => value.to_string(),
    }
}

/// Convert Worker to JSON with format-specific enum decoration
pub fn worker_to_json(
    worker: &Worker,
    settings_descriptor: Option<MessageDescriptor>,
    format: &DisplayFormat,
) -> JsonValue {
    let queue_type_formatter = QueueTypeFormatter;
    let response_type_formatter = ResponseTypeFormatter;

    let mut worker_json = serde_json::json!({
        "id": worker.id.as_ref().map(|id| id.value),
    });

    if let Some(data) = &worker.data {
        worker_json["name"] = serde_json::json!(data.name);
        worker_json["description"] = serde_json::json!(data.description);
        worker_json["runner_id"] = serde_json::json!(data.runner_id.as_ref().map(|r| r.value));
        worker_json["periodic_interval"] = serde_json::json!(data.periodic_interval);
        worker_json["channel"] = serde_json::json!(data.channel);

        // Format enums with decoration
        worker_json["queue_type"] =
            serde_json::json!(queue_type_formatter.format(data.queue_type(), format));
        worker_json["response_type"] =
            serde_json::json!(response_type_formatter.format(data.response_type(), format));

        // Format boolean values with decoration
        worker_json["store_success"] =
            serde_json::json!(format_boolean(data.store_success, format));
        worker_json["store_failure"] =
            serde_json::json!(format_boolean(data.store_failure, format));
        worker_json["use_static"] = serde_json::json!(format_boolean(data.use_static, format));
        worker_json["broadcast_results"] =
            serde_json::json!(format_boolean(data.broadcast_results, format));

        // Process runner_settings with descriptor if available
        if let Some(descriptor) = settings_descriptor {
            match ProtobufDescriptor::get_message_from_bytes(descriptor, &data.runner_settings) {
                Ok(settings_message) => {
                    match ProtobufDescriptor::message_to_json_value(&settings_message) {
                        Ok(settings_json) => {
                            worker_json["runner_settings"] = settings_json;
                        }
                        Err(_) => {
                            worker_json["runner_settings"] =
                                serde_json::json!(String::from_utf8_lossy(&data.runner_settings));
                        }
                    }
                }
                Err(_) => {
                    worker_json["runner_settings"] =
                        serde_json::json!(String::from_utf8_lossy(&data.runner_settings));
                }
            }
        } else {
            worker_json["runner_settings"] =
                serde_json::json!(String::from_utf8_lossy(&data.runner_settings));
        }
    }

    worker_json
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_type_formatter() {
        let formatter = QueueTypeFormatter;

        // Test table format
        assert_eq!(
            formatter.format(QueueType::Normal, &DisplayFormat::Table),
            "NORMAL"
        );

        // Test card format
        assert_eq!(
            formatter.format(QueueType::ForcedRdb, &DisplayFormat::Card),
            "🗃️ FORCED_RDB"
        );

        // Test JSON format
        assert_eq!(
            formatter.format(QueueType::WithBackup, &DisplayFormat::Json),
            "WITH_BACKUP"
        );
    }

    #[test]
    fn test_response_type_formatter() {
        let formatter = ResponseTypeFormatter;

        // Test table format
        assert_eq!(
            formatter.format(ResponseType::Direct, &DisplayFormat::Table),
            "DIRECT"
        );

        // Test card format
        assert_eq!(
            formatter.format(ResponseType::NoResult, &DisplayFormat::Card),
            "🚫 NO_RESULT"
        );

        // Test JSON format
        assert_eq!(
            formatter.format(ResponseType::Direct, &DisplayFormat::Json),
            "DIRECT"
        );
    }

    #[test]
    fn test_format_boolean() {
        // Test table format
        assert_eq!(format_boolean(true, &DisplayFormat::Table), "true");

        // Test card format
        assert_eq!(format_boolean(false, &DisplayFormat::Card), "❌ false");
        assert_eq!(format_boolean(true, &DisplayFormat::Card), "✅ true");

        // Test JSON format
        assert_eq!(format_boolean(false, &DisplayFormat::Json), "false");
    }
}
