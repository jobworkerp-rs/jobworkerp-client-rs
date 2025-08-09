//! Utility functions for display formatting
//!
//! This module provides helper functions for string manipulation,
//! JSON hierarchy formatting, and other display-related utilities.

use serde_json::Value as JsonValue;

/// Truncate a string to specified length with ellipsis
pub fn truncate_string(text: &str, max_length: Option<usize>) -> String {
    match max_length {
        Some(max_len) if text.len() > max_len => {
            if max_len <= 3 {
                "...".to_string()
            } else {
                format!("{}...", &text[..max_len - 3])
            }
        }
        _ => text.to_string(),
    }
}

/// Format JSON value for hierarchical display with indentation
pub fn format_json_hierarchy(
    value: &JsonValue,
    indent_level: usize,
    max_field_length: Option<usize>,
) -> String {
    let indent = "  ".repeat(indent_level);

    let child_indent = indent_level + 1;

    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => {
            format!("\"{}\"", truncate_string(s, max_field_length))
        }
        JsonValue::Array(arr) => {
            if arr.is_empty() {
                return "[]".to_string();
            }

            let mut result = "[\n".to_string();
            for (i, item) in arr.iter().enumerate() {
                let formatted_item = format_json_hierarchy(item, child_indent, max_field_length);
                result.push_str(&format!("{indent}  {formatted_item}"));
                if i < arr.len() - 1 {
                    result.push(',');
                }
                result.push('\n');
            }
            result.push_str(&format!("{indent}]"));
            result
        }
        JsonValue::Object(obj) => {
            if obj.is_empty() {
                return "{}".to_string();
            }

            let mut result = "{\n".to_string();
            let entries: Vec<_> = obj.iter().collect();
            for (i, (key, value)) in entries.iter().enumerate() {
                let formatted_value = format_json_hierarchy(value, child_indent, max_field_length);
                result.push_str(&format!("{indent}  \"{key}\": {formatted_value}"));
                if i < entries.len() - 1 {
                    result.push(',');
                }
                result.push('\n');
            }
            result.push_str(&format!("{indent}}}"));
            result
        }
    }
}

/// Convert JsonValue to a flat key-value representation for table display
pub fn flatten_json_for_table(value: &JsonValue, prefix: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();

    match value {
        JsonValue::Object(obj) => {
            for (key, val) in obj {
                let full_key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };

                match val {
                    JsonValue::Object(_) | JsonValue::Array(_) => {
                        result.extend(flatten_json_for_table(val, &full_key));
                    }
                    _ => {
                        result.push((full_key, json_value_to_string(val)));
                    }
                }
            }
        }
        JsonValue::Array(arr) => {
            for (i, item) in arr.iter().enumerate() {
                let indexed_key = format!("{prefix}[{i}]");
                match item {
                    JsonValue::Object(_) | JsonValue::Array(_) => {
                        result.extend(flatten_json_for_table(item, &indexed_key));
                    }
                    _ => {
                        result.push((indexed_key, json_value_to_string(item)));
                    }
                }
            }
        }
        _ => {
            result.push((prefix.to_string(), json_value_to_string(value)));
        }
    }

    result
}

/// Convert JsonValue to display string
pub fn json_value_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => s.clone(),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            // For complex nested structures, show compact JSON
            serde_json::to_string(value).unwrap_or_else(|_| "Invalid JSON".to_string())
        }
    }
}

/// Check if terminal output supports colors
pub fn supports_color() -> bool {
    // Check if output is to a TTY and color environment variables
    atty::is(atty::Stream::Stdout)
        && (std::env::var("NO_COLOR").is_err() || std::env::var("NO_COLOR") == Ok("".to_string()))
        && std::env::var("TERM")
            .map(|term| term != "dumb")
            .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_truncate_string() {
        assert_eq!(truncate_string("hello", Some(10)), "hello");
        assert_eq!(truncate_string("hello world", Some(8)), "hello...");
        assert_eq!(truncate_string("hi", Some(2)), "hi");
        assert_eq!(truncate_string("hello", Some(2)), "...");
        assert_eq!(truncate_string("hello", None), "hello");
    }

    #[test]
    fn test_json_value_to_string() {
        assert_eq!(json_value_to_string(&json!(null)), "null");
        assert_eq!(json_value_to_string(&json!(true)), "true");
        assert_eq!(json_value_to_string(&json!(42)), "42");
        assert_eq!(json_value_to_string(&json!("test")), "test");
    }

    #[test]
    fn test_flatten_json_for_table() {
        let json_obj = json!({
            "name": "test",
            "nested": {
                "value": 42,
                "array": [1, 2, 3]
            }
        });

        let flattened = flatten_json_for_table(&json_obj, "");
        assert_eq!(flattened.len(), 5);

        // Check if all expected keys are present
        let keys: Vec<String> = flattened.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"nested.value".to_string()));
        assert!(keys.contains(&"nested.array[0]".to_string()));
    }

    #[test]
    fn test_format_json_hierarchy_simple() {
        let json_obj = json!({
            "key": "value",
            "number": 42
        });

        let formatted = format_json_hierarchy(&json_obj, 0, Some(50));
        assert!(formatted.contains("\"key\": \"value\""));
        assert!(formatted.contains("\"number\": 42"));
        assert!(formatted.starts_with("{"));
        assert!(formatted.ends_with("}"));
    }

    #[test]
    fn test_format_json_hierarchy_nested() {
        let json_obj = json!({
            "parent": {
                "child": "value"
            }
        });

        let formatted = format_json_hierarchy(&json_obj, 0, Some(50));
        assert!(formatted.contains("\"parent\":"));
        assert!(formatted.contains("\"child\": \"value\""));
    }
}
