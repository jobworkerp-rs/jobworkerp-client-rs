use crate::display::{format::EnumFormatter, DisplayFormat};
use crate::jobworkerp::data::StreamingOutputType;
use serde_json::{json, Value as JsonValue};

/// Formatter for StreamingOutputType enum with emoji decoration
pub struct StreamingOutputTypeFormatter;
impl EnumFormatter<StreamingOutputType> for StreamingOutputTypeFormatter {
    fn format(&self, output_type: StreamingOutputType, format: &DisplayFormat) -> String {
        match format {
            DisplayFormat::Table | DisplayFormat::Card => match output_type {
                StreamingOutputType::NonStreaming => "📄 NON_STREAMING".to_string(),
                StreamingOutputType::Streaming => "📊 STREAMING".to_string(),
                StreamingOutputType::Both => "📊🔄 BOTH".to_string(),
            },
            DisplayFormat::Json => output_type.as_str_name().to_string(),
        }
    }
}

/// Convert Function to JSON format for display
pub fn function_to_json(
    function: &crate::jobworkerp::function::data::FunctionSpecs,
    format: &DisplayFormat,
) -> JsonValue {
    let formatter = StreamingOutputTypeFormatter;
    let output_type = StreamingOutputType::try_from(function.output_type)
        .unwrap_or(StreamingOutputType::NonStreaming);

    let mut json_obj = json!({
        "name": function.name,
        "description": function.description,
        "output_type": formatter.format(output_type, format)
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

    // Process schema based on type
    match &function.schema {
        Some(crate::jobworkerp::function::data::function_specs::Schema::SingleSchema(schema)) => {
            let mut schema_obj = json!({});

            if let Some(settings) = &schema.settings {
                schema_obj["settings"] = match format {
                    DisplayFormat::Json => json!(settings),
                    _ => {
                        // For Card/Table, truncate long schemas
                        if settings.len() > 200 {
                            json!(format!("{}... [truncated]", &settings[..197]))
                        } else {
                            json!(settings)
                        }
                    }
                };
            }

            schema_obj["arguments"] = match format {
                DisplayFormat::Json => json!(schema.arguments),
                _ => {
                    // For Card/Table, truncate long schemas
                    if schema.arguments.len() > 200 {
                        json!(format!("{}... [truncated]", &schema.arguments[..197]))
                    } else {
                        json!(schema.arguments)
                    }
                }
            };

            if let Some(result_schema) = &schema.result_output_schema {
                schema_obj["result_output_schema"] = match format {
                    DisplayFormat::Json => json!(result_schema),
                    _ => {
                        // For Card/Table, truncate long schemas
                        if result_schema.len() > 200 {
                            json!(format!("{}... [truncated]", &result_schema[..197]))
                        } else {
                            json!(result_schema)
                        }
                    }
                };
            }

            json_obj["schema"] = schema_obj;
        }
        Some(crate::jobworkerp::function::data::function_specs::Schema::McpTools(mcp_tools)) => {
            let tools: Vec<JsonValue> = mcp_tools
                .list
                .iter()
                .map(|tool| {
                    json!({
                        "name": tool.name,
                        "description": tool.description.as_ref().unwrap_or(&String::new()),
                        "input_schema": match format {
                            DisplayFormat::Json => tool.input_schema.clone(),
                            _ => {
                                // For Card/Table, truncate long schemas
                                let schema = &tool.input_schema;
                                if schema.len() > 150 {
                                    format!("{}... [truncated]", &schema[..147])
                                } else {
                                    schema.clone()
                                }
                            }
                        }
                    })
                })
                .collect();

            json_obj["mcp_tools"] = json!({
                "count": tools.len(),
                "tools": tools
            });
        }
        None => {
            json_obj["schema"] = json!(null);
        }
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
    use crate::jobworkerp::function::data::{FunctionSchema, FunctionSpecs};

    #[test]
    fn test_streaming_output_type_formatter() {
        let formatter = StreamingOutputTypeFormatter;

        // Test Table format
        assert_eq!(
            formatter.format(StreamingOutputType::Streaming, &DisplayFormat::Table),
            "📊 STREAMING"
        );
        assert_eq!(
            formatter.format(StreamingOutputType::NonStreaming, &DisplayFormat::Card),
            "📄 NON_STREAMING"
        );

        // Test JSON format
        assert_eq!(
            formatter.format(StreamingOutputType::Streaming, &DisplayFormat::Json),
            "STREAMING"
        );
    }

    #[test]
    fn test_function_to_json_runner() {
        let function = FunctionSpecs {
            runner_type: crate::jobworkerp::data::RunnerType::Command as i32,
            runner_id: Some(RunnerId { value: 1001 }),
            worker_id: None,
            name: "test_function".to_string(),
            description: "Test function description".to_string(),
            output_type: StreamingOutputType::Streaming as i32,
            schema: Some(
                crate::jobworkerp::function::data::function_specs::Schema::SingleSchema(
                    FunctionSchema {
                        settings: Some("{}".to_string()),
                        arguments: "{\"type\": \"object\"}".to_string(),
                        result_output_schema: None,
                    },
                ),
            ),
        };

        let json = function_to_json(&function, &DisplayFormat::Table);
        assert_eq!(json["name"], "test_function");
        assert_eq!(json["runner_id"], 1001);
        assert_eq!(json["type"], "RUNNER");
        assert_eq!(json["output_type"], "📊 STREAMING");
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
