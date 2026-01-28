use crate::display::DisplayFormat;
use crate::display::format::{EnumFormatter, StreamingOutputTypeFormatter};
use crate::jobworkerp::data::StreamingOutputType;
use serde_json::{Value as JsonValue, json};

/// Convert Function to JSON format for display
pub fn function_to_json(
    function: &crate::jobworkerp::function::data::FunctionSpecs,
    format: &DisplayFormat,
) -> JsonValue {
    let formatter = StreamingOutputTypeFormatter;

    let mut json_obj = json!({
        "name": function.name,
        "description": function.description,
    });

    // Add ID based on type (runner or worker)
    if let Some(runner_id) = &function.runner_id {
        json_obj["runner_id"] = json!(runner_id.value);
        json_obj["type"] = json!("RUNNER");
    }
    if let Some(worker_id) = &function.worker_id {
        json_obj["worker_id"] = json!(worker_id.value);
        json_obj["type"] = json!("WORKER");
    }

    // Add settings_schema
    if !function.settings_schema.is_empty() {
        json_obj["settings_schema"] = match format {
            DisplayFormat::Json => json!(function.settings_schema),
            _ => {
                if function.settings_schema.len() > 200 {
                    json!(format!(
                        "{}... [truncated]",
                        &function.settings_schema[..197]
                    ))
                } else {
                    json!(function.settings_schema)
                }
            }
        };
    }

    // Process methods
    if let Some(method_map) = &function.methods {
        let method_count = method_map.schemas.len();

        if method_count == 0 {
            json_obj["methods"] = json!(null);
        } else if method_count == 1 {
            // Single method - include full details
            let (method_name, method_schema) = method_map.schemas.iter().next().unwrap();

            json_obj["method"] = json!(method_name);

            if let Some(desc) = &method_schema.description {
                json_obj["method_description"] = json!(desc);
            }

            json_obj["arguments_schema"] = match format {
                DisplayFormat::Json => json!(method_schema.arguments_schema),
                _ => {
                    if method_schema.arguments_schema.len() > 200 {
                        json!(format!(
                            "{}... [truncated]",
                            &method_schema.arguments_schema[..197]
                        ))
                    } else {
                        json!(method_schema.arguments_schema)
                    }
                }
            };

            if let Some(result_schema) = &method_schema.result_schema {
                json_obj["result_schema"] = match format {
                    DisplayFormat::Json => json!(result_schema),
                    _ => {
                        if result_schema.len() > 200 {
                            json!(format!("{}... [truncated]", &result_schema[..197]))
                        } else {
                            json!(result_schema)
                        }
                    }
                };
            }

            let output_type = StreamingOutputType::try_from(method_schema.output_type)
                .unwrap_or(StreamingOutputType::NonStreaming);
            json_obj["output_type"] = json!(formatter.format(output_type, format));

            if let Some(annotations) = &method_schema.annotations
                && matches!(format, DisplayFormat::Json)
            {
                let mut ann_obj = json!({});
                if let Some(title) = &annotations.title {
                    ann_obj["title"] = json!(title);
                }
                if let Some(read_only) = annotations.read_only_hint {
                    ann_obj["read_only_hint"] = json!(read_only);
                }
                if let Some(destructive) = annotations.destructive_hint {
                    ann_obj["destructive_hint"] = json!(destructive);
                }
                if let Some(idempotent) = annotations.idempotent_hint {
                    ann_obj["idempotent_hint"] = json!(idempotent);
                }
                if let Some(open_world) = annotations.open_world_hint {
                    ann_obj["open_world_hint"] = json!(open_world);
                }
                json_obj["annotations"] = ann_obj;
            }
        } else {
            // Multiple methods - summary view (sorted alphabetically)
            json_obj["method_count"] = json!(method_count);

            let mut method_names: Vec<_> = method_map.schemas.keys().collect();
            method_names.sort();
            let methods: Vec<JsonValue> = method_names
                .iter()
                .map(|name| {
                    let schema = &method_map.schemas[*name];
                    let mut method_obj = json!({
                        "name": name,
                    });

                    if let Some(desc) = &schema.description {
                        method_obj["description"] = json!(desc);
                    }

                    let output_type = StreamingOutputType::try_from(schema.output_type)
                        .unwrap_or(StreamingOutputType::NonStreaming);
                    method_obj["output_type"] = json!(formatter.format(output_type, format));

                    if matches!(format, DisplayFormat::Json) {
                        method_obj["arguments_schema"] = json!(schema.arguments_schema);
                        if let Some(result_schema) = &schema.result_schema {
                            method_obj["result_schema"] = json!(result_schema);
                        }
                    }

                    method_obj
                })
                .collect();

            json_obj["methods"] = json!(methods);
        }
    } else {
        json_obj["methods"] = json!(null);
    }

    json_obj
}

/// Convert FunctionResult to JSON format for display
pub fn function_result_to_json(
    result: &crate::jobworkerp::function::data::FunctionResult,
    format: &DisplayFormat,
) -> JsonValue {
    let mut json_obj = json!({
        "output": result.output
    });

    if let Some(status) = &result.status {
        let status_value = *status;
        let status_result = crate::jobworkerp::data::ResultStatus::try_from(status_value)
            .unwrap_or(crate::jobworkerp::data::ResultStatus::OtherError);

        json_obj["status"] = match format {
            DisplayFormat::Table | DisplayFormat::Card => {
                // Add emoji decoration for status
                match status_result {
                    crate::jobworkerp::data::ResultStatus::Success => json!("✅ SUCCESS"),
                    crate::jobworkerp::data::ResultStatus::ErrorAndRetry => {
                        json!("🔄 ERROR_AND_RETRY")
                    }
                    crate::jobworkerp::data::ResultStatus::FatalError => json!("💥 FATAL_ERROR"),
                    crate::jobworkerp::data::ResultStatus::Abort => json!("🛑 ABORT"),
                    crate::jobworkerp::data::ResultStatus::MaxRetry => json!("🔥 MAX_RETRY"),
                    crate::jobworkerp::data::ResultStatus::OtherError => json!("❌ OTHER_ERROR"),
                    crate::jobworkerp::data::ResultStatus::Cancelled => json!("🚫 CANCELLED"),
                }
            }
            DisplayFormat::Json => json!(status_result.as_str_name()),
        };
    }

    if let Some(error_message) = &result.error_message {
        json_obj["error_message"] = json!(error_message);
    }

    if let Some(error_code) = &result.error_code {
        json_obj["error_code"] = json!(error_code);
    }

    if let Some(last_info) = &result.last_info {
        json_obj["execution_info"] = json!({
            "job_id": last_info.job_id,
            "started_at": last_info.started_at,
            "completed_at": last_info.completed_at,
            "execution_time_ms": last_info.execution_time_ms,
            "metadata": if last_info.metadata.is_empty() {
                json!(null)
            } else {
                json!(last_info.metadata)
            }
        });
    }

    json_obj
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobworkerp::data::RunnerId;
    use crate::jobworkerp::function::data::{FunctionSpecs, MethodSchema, MethodSchemaMap};
    use std::collections::HashMap;

    #[test]
    fn test_function_to_json_single_method() {
        let mut schemas = HashMap::new();
        schemas.insert(
            "run".to_string(),
            MethodSchema {
                description: Some("Test method description".to_string()),
                arguments_schema:
                    r#"{"type": "object", "properties": {"arg1": {"type": "string"}}}"#.to_string(),
                result_schema: Some(
                    r#"{"type": "object", "properties": {"result": {"type": "string"}}}"#
                        .to_string(),
                ),
                output_type: StreamingOutputType::Streaming as i32,
                annotations: None,
            },
        );

        let function = FunctionSpecs {
            runner_type: crate::jobworkerp::data::RunnerType::Command as i32,
            runner_id: Some(RunnerId { value: 1001 }),
            worker_id: None,
            name: "test_function".to_string(),
            description: "Test function description".to_string(),
            settings_schema: r#"{"type": "object", "properties": {"api_key": {"type": "string"}}}"#
                .to_string(),
            methods: Some(MethodSchemaMap { schemas }),
        };

        let json = function_to_json(&function, &DisplayFormat::Table);
        assert_eq!(json["name"], "test_function");
        assert_eq!(json["runner_id"], 1001);
        assert_eq!(json["type"], "RUNNER");
        assert_eq!(json["method"], "run");
        assert_eq!(json["output_type"], "STREAMING");
        assert!(json["arguments_schema"].is_string());
    }

    #[test]
    fn test_function_to_json_multiple_methods() {
        let mut schemas = HashMap::new();
        schemas.insert(
            "fetch_html".to_string(),
            MethodSchema {
                description: Some("Fetch HTML from URL".to_string()),
                arguments_schema:
                    r#"{"type": "object", "properties": {"url": {"type": "string"}}}"#.to_string(),
                result_schema: Some(r#"{"type": "string"}"#.to_string()),
                output_type: StreamingOutputType::NonStreaming as i32,
                annotations: None,
            },
        );
        schemas.insert(
            "get_current_time".to_string(),
            MethodSchema {
                description: Some("Get current server time".to_string()),
                arguments_schema: r#"{"type": "object"}"#.to_string(),
                result_schema: Some(
                    r#"{"type": "object", "properties": {"timestamp": {"type": "number"}}}"#
                        .to_string(),
                ),
                output_type: StreamingOutputType::NonStreaming as i32,
                annotations: None,
            },
        );

        let function = FunctionSpecs {
            runner_type: crate::jobworkerp::data::RunnerType::McpServer as i32,
            runner_id: Some(RunnerId { value: 2001 }),
            worker_id: None,
            name: "mcp_server_function".to_string(),
            description: "MCP Server with multiple tools".to_string(),
            settings_schema: "".to_string(),
            methods: Some(MethodSchemaMap { schemas }),
        };

        let json = function_to_json(&function, &DisplayFormat::Card);
        assert_eq!(json["name"], "mcp_server_function");
        assert_eq!(json["method_count"], 2);
        assert!(json["methods"].is_array());
        assert_eq!(json["methods"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_function_result_to_json() {
        use crate::jobworkerp::function::data::FunctionResult;

        let result = FunctionResult {
            output: "test output".to_string(),
            status: Some(crate::jobworkerp::data::ResultStatus::Success as i32),
            error_message: None,
            error_code: None,
            last_info: None,
        };

        let json = function_result_to_json(&result, &DisplayFormat::Card);
        assert_eq!(json["output"], "test output");
        assert_eq!(json["status"], "✅ SUCCESS");
    }
}
