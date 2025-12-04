//! Runner-specific display functionality
//!
//! This module handles the conversion of Runner data structures to JSON format
//! with appropriate enum decoration and formatting.

use crate::display::format::{EnumFormatter, StreamingOutputTypeFormatter};
use crate::display::DisplayFormat;
use crate::jobworkerp::data::{Runner, RunnerType};
use serde_json::Value as JsonValue;

/// Formatter for RunnerType enum
pub struct RunnerTypeFormatter;

impl EnumFormatter<RunnerType> for RunnerTypeFormatter {
    fn format(&self, runner_type: RunnerType, format: &DisplayFormat) -> String {
        match format {
            DisplayFormat::Table => match runner_type {
                RunnerType::Plugin => "PLUGIN",
                RunnerType::Command => "COMMAND",
                RunnerType::HttpRequest => "HTTP_REQUEST",
                RunnerType::GrpcUnary => "GRPC_UNARY",
                RunnerType::Docker => "DOCKER",
                RunnerType::SlackPostMessage => "SLACK_POST_MESSAGE",
                RunnerType::PythonCommand => "PYTHON_COMMAND",
                RunnerType::McpServer => "MCP_SERVER",
                RunnerType::LlmChat => "LLM_CHAT",
                RunnerType::LlmCompletion => "LLM_COMPLETION",
                RunnerType::InlineWorkflow => "INLINE_WORKFLOW",
                RunnerType::ReusableWorkflow => "REUSABLE_WORKFLOW",
                RunnerType::CreateWorkflow => "CREATE_WORKFLOW",
            }
            .to_string(),
            DisplayFormat::Card => match runner_type {
                RunnerType::Plugin => "⚡ PLUGIN",
                RunnerType::Command => "💻 COMMAND",
                RunnerType::HttpRequest => "🌐 HTTP_REQUEST",
                RunnerType::GrpcUnary => "🔗 GRPC_UNARY",
                RunnerType::Docker => "🐳 DOCKER",
                RunnerType::SlackPostMessage => "💬 SLACK_POST_MESSAGE",
                RunnerType::PythonCommand => "🐍 PYTHON_COMMAND",
                RunnerType::McpServer => "🔧 MCP_SERVER",
                RunnerType::LlmChat => "🤖 LLM_CHAT",
                RunnerType::LlmCompletion => "📝 LLM_COMPLETION",
                RunnerType::InlineWorkflow => "🔄 INLINE_WORKFLOW",
                RunnerType::ReusableWorkflow => "🔄 REUSABLE_WORKFLOW",
                RunnerType::CreateWorkflow => "🔄 CREATE_WORKFLOW",
            }
            .to_string(),
            DisplayFormat::Json => runner_type.as_str_name().to_string(),
        }
    }
}

/// Truncate proto definition for display
fn truncate_proto_definition(
    definition: &str,
    format: &DisplayFormat,
    no_truncate: bool,
) -> String {
    match format {
        DisplayFormat::Json => definition.to_string(),
        _ => {
            if no_truncate {
                definition.to_string()
            } else {
                let max_length = match format {
                    DisplayFormat::Table => 50,
                    DisplayFormat::Card => 200,
                    DisplayFormat::Json => return definition.to_string(),
                };

                if definition.len() > max_length {
                    format!("{}...", &definition[..max_length.saturating_sub(3)])
                } else {
                    definition.to_string()
                }
            }
        }
    }
}

/// Convert Runner to JSON with format-specific enum decoration
pub fn runner_to_json(runner: &Runner, format: &DisplayFormat, no_truncate: bool) -> JsonValue {
    let runner_type_formatter = RunnerTypeFormatter;
    let output_type_formatter = StreamingOutputTypeFormatter;

    let mut runner_json = serde_json::json!({
        "id": runner.id.as_ref().map(|id| id.value),
    });

    if let Some(data) = &runner.data {
        runner_json["name"] = serde_json::json!(data.name);
        runner_json["description"] = serde_json::json!(data.description);

        // Format enums with decoration
        runner_json["runner_type"] =
            serde_json::json!(runner_type_formatter.format(data.runner_type(), format));

        // Format proto definition with truncation
        runner_json["definition"] = serde_json::json!(truncate_proto_definition(
            &data.definition,
            format,
            no_truncate
        ));

        // Add runner_settings_proto
        runner_json["runner_settings_proto"] = serde_json::json!(truncate_proto_definition(
            &data.runner_settings_proto,
            format,
            no_truncate
        ));

        // Display method_proto_map
        if let Some(method_map) = &data.method_proto_map {
            let methods: Vec<String> = method_map.schemas.keys().cloned().collect();
            runner_json["methods"] = serde_json::json!(methods);

            match format {
                DisplayFormat::Json => {
                    // Detailed information for JSON format
                    let mut method_details = serde_json::Map::new();
                    for (name, schema) in &method_map.schemas {
                        method_details.insert(
                            name.clone(),
                            serde_json::json!({
                                "args_proto": truncate_proto_definition(&schema.args_proto, format, no_truncate),
                                "result_proto": truncate_proto_definition(&schema.result_proto, format, no_truncate),
                                "description": schema.description,
                                "output_type": output_type_formatter.format(schema.output_type(), format),
                            }),
                        );
                    }
                    runner_json["method_schemas"] = serde_json::json!(method_details);
                }
                DisplayFormat::Card => {
                    if method_map.schemas.len() == 1 {
                        let (name, schema) = method_map.schemas.iter().next().unwrap();
                        runner_json["method"] = serde_json::json!(name);
                        runner_json["output_type"] = serde_json::json!(
                            output_type_formatter.format(schema.output_type(), format)
                        );
                    } else {
                        runner_json["method_count"] = serde_json::json!(method_map.schemas.len());
                    }
                }
                DisplayFormat::Table => {
                    runner_json["method_count"] = serde_json::json!(method_map.schemas.len());
                }
            }
        } else {
            runner_json["methods"] = serde_json::json!([]);
        }
    }

    runner_json
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runner_type_formatter() {
        let formatter = RunnerTypeFormatter;

        // Test table format
        assert_eq!(
            formatter.format(RunnerType::McpServer, &DisplayFormat::Table),
            "MCP_SERVER"
        );

        // Test card format
        assert_eq!(
            formatter.format(RunnerType::Plugin, &DisplayFormat::Card),
            "⚡ PLUGIN"
        );

        // Test JSON format
        assert_eq!(
            formatter.format(RunnerType::InlineWorkflow, &DisplayFormat::Json),
            "INLINE_WORKFLOW"
        );
    }

    #[test]
    fn test_streaming_output_type_formatter() {
        use crate::jobworkerp::data::StreamingOutputType;

        let formatter = StreamingOutputTypeFormatter;

        // Test table format
        assert_eq!(
            formatter.format(StreamingOutputType::Streaming, &DisplayFormat::Table),
            "STREAMING"
        );

        // Test card format
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
    fn test_truncate_proto_definition() {
        let long_definition = "syntax = \"proto3\";\n\npackage test;\n\nmessage TestMessage {\n  string field1 = 1;\n  int32 field2 = 2;\n  bool field3 = 3;\n}";

        // Test table format with truncation
        let result = truncate_proto_definition(long_definition, &DisplayFormat::Table, false);
        assert!(result.len() <= 50);
        assert!(result.ends_with("..."));

        // Test no truncate
        let result = truncate_proto_definition(long_definition, &DisplayFormat::Table, true);
        assert_eq!(result, long_definition);

        // Test JSON format (no truncation)
        let result = truncate_proto_definition(long_definition, &DisplayFormat::Json, false);
        assert_eq!(result, long_definition);
    }
}
