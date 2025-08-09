//! Visualizer implementations for different output formats
//!
//! This module provides concrete implementations of visualizers for table,
//! card, and JSON output formats.

use crate::display::utils::{format_json_hierarchy, truncate_string};
use crate::display::{format::color, DisplayOptions};
use comfy_table::{Attribute, Cell, ColumnConstraint, ContentArrangement, Table, Width};
use serde_json::Value as JsonValue;

/// Trait for JSON visualizers
pub trait JsonVisualizer {
    fn visualize(&self, data: &[JsonValue], options: &DisplayOptions) -> String;
}

/// Trait for streaming output visualizers
pub trait StreamingVisualizer {
    fn start_stream(&self, stream_type: &str, options: &DisplayOptions);
    fn render_item(&self, item: &JsonValue, index: usize, options: &DisplayOptions);
    fn end_stream(&self, total_count: usize, options: &DisplayOptions);
}

/// Table format visualizer using comfy-table
pub struct TableVisualizer;

impl JsonVisualizer for TableVisualizer {
    fn visualize(&self, data: &[JsonValue], options: &DisplayOptions) -> String {
        if data.is_empty() {
            return "No data".to_string();
        }

        // Process data for argument expansion if needed
        let processed_data = if options.no_truncate {
            expand_arguments_fields(data)
        } else {
            data.to_vec()
        };

        let mut table = Table::new();

        // Use the most stable content arrangement
        table.set_content_arrangement(ContentArrangement::Disabled);

        // Collect all unique keys
        let all_keys = collect_all_keys(&processed_data);

        // Create headers - use plain text to avoid ANSI issues
        let headers: Vec<Cell> = all_keys
            .iter()
            .map(|key| {
                let cell = if options.color_enabled {
                    Cell::new(key).fg(comfy_table::Color::Cyan)
                } else {
                    Cell::new(key)
                };
                cell.add_attribute(Attribute::Bold)
            })
            .collect();
        table.set_header(headers);

        // Add data rows
        for json_obj in &processed_data {
            let row: Vec<Cell> = all_keys
                .iter()
                .map(|key| {
                    let value = get_value_by_key(json_obj, key);
                    let display_value = format_cell_value_for_comfy_table(&value, options);

                    if options.color_enabled {
                        apply_cell_color(Cell::new(display_value), &value)
                    } else {
                        Cell::new(display_value)
                    }
                })
                .collect();
            table.add_row(row);
        }

        // Use simple preset to avoid complex formatting issues
        table.load_preset(comfy_table::presets::NOTHING);

        // Set column constraints based on content and no-truncate option
        for (i, key) in all_keys.iter().enumerate() {
            if let Some(column) = table.column_mut(i) {
                let constraint = if options.no_truncate {
                    match key.as_str() {
                        "id" => ColumnConstraint::LowerBoundary(Width::Fixed(20)),
                        _ => ColumnConstraint::ContentWidth,
                    }
                } else {
                    match key.as_str() {
                        "id" => ColumnConstraint::Boundaries {
                            lower: Width::Fixed(10),
                            upper: Width::Fixed(20),
                        },
                        "arguments" => ColumnConstraint::UpperBoundary(Width::Fixed(25)),
                        key if key.starts_with("arg_") => {
                            ColumnConstraint::UpperBoundary(Width::Fixed(25))
                        }
                        "run_after" => ColumnConstraint::UpperBoundary(Width::Fixed(21)),
                        _ => ColumnConstraint::ContentWidth,
                    }
                };
                column.set_constraint(constraint);
            }
        }

        table.to_string()
    }
}

/// Card format visualizer with detailed individual entries  
pub struct CardVisualizer;

impl JsonVisualizer for CardVisualizer {
    fn visualize(&self, data: &[JsonValue], options: &DisplayOptions) -> String {
        if data.is_empty() {
            return "No data".to_string();
        }

        let mut result = String::new();
        let separator = if options.use_unicode { "━" } else { "-" };
        let bullet = if options.use_unicode { "• " } else { "- " };

        for (index, json_obj) in data.iter().enumerate() {
            if index > 0 {
                result.push_str(&format!("\n{}\n", separator.repeat(50)));
            }

            // Card header
            let header = format!("Item #{}", index + 1);
            if options.color_enabled {
                result.push_str(&color::colorize_text(&header, "cyan", true));
            } else {
                result.push_str(&header);
            }
            result.push('\n');

            // Format as hierarchical structure
            let formatted = format_json_hierarchy(json_obj, 0, options.effective_max_length());

            // Add bullets to each line
            for line in formatted.lines() {
                if !line.trim().is_empty() {
                    result.push_str(&format!("{bullet}{line}\n"));
                }
            }
        }

        result
    }
}

/// Pretty JSON format visualizer
pub struct JsonPrettyVisualizer;

/// Streaming table format visualizer (displays each item as individual table)
pub struct StreamingTableVisualizer;

/// Streaming card format visualizer
pub struct StreamingCardVisualizer;

/// Streaming JSON format visualizer  
pub struct StreamingJsonVisualizer;

impl JsonVisualizer for JsonPrettyVisualizer {
    fn visualize(&self, data: &[JsonValue], _options: &DisplayOptions) -> String {
        if data.is_empty() {
            return "[]".to_string();
        }

        if data.len() == 1 {
            // Single object - don't wrap in array
            match serde_json::to_string_pretty(&data[0]) {
                Ok(json) => json,
                Err(_) => "Invalid JSON".to_string(),
            }
        } else {
            // Multiple objects - return as JSON array
            match serde_json::to_string_pretty(&JsonValue::Array(data.to_vec())) {
                Ok(json) => json,
                Err(_) => "Invalid JSON".to_string(),
            }
        }
    }
}

// // /// Create a simple table with proper alignment
// // #[allow(dead_code)]
// fn create_simple_table(data: &[JsonValue], options: &DisplayOptions) -> String {
//     if data.is_empty() {
//         return "No data".to_string();
//     }

//     // If no-truncate is enabled, expand arguments fields
//     let processed_data = if options.no_truncate {
//         expand_arguments_fields(data)
//     } else {
//         data.to_vec()
//     };

//     let all_keys = collect_all_keys(&processed_data);

//     // Calculate column widths
//     let mut col_widths: Vec<usize> = all_keys.iter()
//         .map(|header| header.len())
//         .collect();

//     // Check data widths (without color codes for accurate width calculation)
//     for json_obj in &processed_data {
//         for (i, key) in all_keys.iter().enumerate() {
//             let value = get_value_by_key(json_obj, key);
//             let display_value = format_cell_value(&value, &options.clone().with_color(false));
//             col_widths[i] = col_widths[i].max(display_value.len());
//         }
//     }

//     // Ensure minimum widths for specific columns to prevent issues
//     for (i, key) in all_keys.iter().enumerate() {
//         match key.as_str() {
//             "id" => col_widths[i] = col_widths[i].max(20), // int64 maximum: 9223372036854775807 (19 digits)
//             "worker_id" => col_widths[i] = col_widths[i].max(8),
//             _ => {}
//         }
//     }

//     // Apply maximum width limits to prevent excessive widths (only if truncation is enabled)
//     if !options.no_truncate {
//         for (i, key) in all_keys.iter().enumerate() {
//             match key.as_str() {
//                 "arguments" => col_widths[i] = col_widths[i].min(25),
//                 "run_after" => col_widths[i] = col_widths[i].min(21),
//                 "unique_key" => col_widths[i] = col_widths[i].min(20),
//                 "id" => col_widths[i] = col_widths[i].min(20), // int64 can be up to 19 digits
//                 _ => {}
//             }
//         }
//     } else {
//         // When no-truncate is enabled, ensure minimum reasonable widths for readability
//         for (i, key) in all_keys.iter().enumerate() {
//             match key.as_str() {
//                 "id" => col_widths[i] = col_widths[i].max(20), // Ensure enough space for int64
//                 _ => {}
//             }
//         }
//     }

//     let mut result = String::new();

//     // Top border
//     result.push('┌');
//     for (i, width) in col_widths.iter().enumerate() {
//         result.push_str(&"─".repeat(width + 2));
//         if i < col_widths.len() - 1 {
//             result.push('┬');
//         }
//     }
//     result.push_str("┐\n");

//     // Header
//     result.push('│');
//     for (key, width) in all_keys.iter().zip(&col_widths) {
//         let header_text = if options.color_enabled {
//             color::colorize_text(key, "cyan", true)
//         } else {
//             key.clone()
//         };
//         result.push_str(&format!(" {:width$} ", header_text, width = width));
//         result.push('│');
//     }
//     result.push('\n');

//     // Header separator
//     result.push('├');
//     for (i, width) in col_widths.iter().enumerate() {
//         result.push_str(&"─".repeat(width + 2));
//         if i < col_widths.len() - 1 {
//             result.push('┼');
//         }
//     }
//     result.push_str("┤\n");

//     // Data rows
//     for json_obj in &processed_data {
//         result.push('│');
//         for (key, width) in all_keys.iter().zip(&col_widths) {
//             let value = get_value_by_key(json_obj, key);
//             let display_value = format_cell_value(&value, options);

//             // Handle truncation if needed
//             let display_text = if options.color_enabled {
//                 // For colored text, work with uncolored version for width calculation
//                 let uncolored_value = format_cell_value(&value, &options.clone().with_color(false));
//                 if !options.no_truncate && uncolored_value.len() > *width {
//                     let truncated = truncate_string(&uncolored_value, Some(*width));
//                     format_cell_value_for_truncated(&value, options, &truncated)
//                 } else {
//                     display_value
//                 }
//             } else {
//                 if !options.no_truncate && display_value.len() > *width {
//                     truncate_string(&display_value, Some(*width))
//                 } else {
//                     display_value
//                 }
//             };

//             // Calculate actual display width without ANSI codes
//             let actual_display_width = if options.color_enabled {
//                 // Strip ANSI codes to get actual display width
//                 strip_ansi_codes(&display_text).chars().count()
//             } else {
//                 display_text.chars().count()
//             };

//             // Apply proper padding
//             let padding_needed = width.saturating_sub(actual_display_width);
//             result.push_str(&format!(" {}{} ", display_text, " ".repeat(padding_needed)));
//             result.push('│');
//         }
//         result.push('\n');
//     }

//     // Bottom border
//     result.push('└');
//     for (i, width) in col_widths.iter().enumerate() {
//         result.push_str(&"─".repeat(width + 2));
//         if i < col_widths.len() - 1 {
//             result.push('┴');
//         }
//     }
//     result.push_str("┘\n");

//     result
// }

/// Expand arguments fields in JSON data for detailed view
fn expand_arguments_fields(data: &[JsonValue]) -> Vec<JsonValue> {
    data.iter()
        .map(|item| {
            if let JsonValue::Object(obj) = item {
                let mut expanded = obj.clone();

                // If arguments field exists, expand it based on its type
                if let Some(JsonValue::Object(args_obj)) = obj.get("arguments") {
                    // Remove the original arguments field
                    expanded.remove("arguments");

                    // Add each argument field with "arg_" prefix
                    for (key, value) in args_obj {
                        let prefixed_key = format!("arg_{key}");
                        expanded.insert(prefixed_key, value.clone());
                    }
                }

                JsonValue::Object(expanded)
            } else {
                item.clone()
            }
        })
        .collect()
}

/// Collect all unique keys from array of JSON objects
fn collect_all_keys(data: &[JsonValue]) -> Vec<String> {
    let mut keys = std::collections::BTreeSet::new();

    for item in data {
        if let JsonValue::Object(obj) = item {
            for key in obj.keys() {
                keys.insert(key.clone());
            }
        }
    }

    keys.into_iter().collect()
}

/// Get value from JSON object by key, handling nested paths
fn get_value_by_key(json: &JsonValue, key: &str) -> JsonValue {
    match json {
        JsonValue::Object(obj) => obj.get(key).cloned().unwrap_or(JsonValue::Null),
        _ => JsonValue::Null,
    }
}

// // /// Format cell value for table display with proper truncation and coloring
// // #[allow(dead_code)]
// fn format_cell_value(value: &JsonValue, options: &DisplayOptions) -> String {
//     let text = match value {
//         JsonValue::Null => "null".to_string(),
//         JsonValue::Bool(b) => b.to_string(),
//         JsonValue::Number(n) => n.to_string(),
//         JsonValue::String(s) => s.clone(),
//         JsonValue::Array(arr) => {
//             if arr.is_empty() {
//                 "[]".to_string()
//             } else if options.no_truncate {
//                 // Show array content when no-truncate is enabled
//                 format!("[{}]", arr.iter()
//                     .map(|v| format_cell_value(v, &options.clone().with_color(false)))
//                     .collect::<Vec<_>>()
//                     .join(", "))
//             } else {
//                 format!("[{} items]", arr.len())
//             }
//         }
//         JsonValue::Object(obj) => {
//             if obj.is_empty() {
//                 "{}".to_string()
//             } else if options.no_truncate {
//                 // Show object content when no-truncate is enabled
//                 format!("{{{}}}", obj.iter()
//                     .map(|(k, v)| format!("{}: {}", k, format_cell_value(v, &options.clone().with_color(false))))
//                     .collect::<Vec<_>>()
//                     .join(", "))
//             } else {
//                 format!("{{{} fields}}", obj.len())
//             }
//         }
//     };

//     let truncated = truncate_string(&text, options.effective_max_length());

//     // Apply color formatting based on content
//     if options.color_enabled {
//         apply_value_coloring(&truncated, value)
//     } else {
//         truncated
//     }
// }

// // /// Strip ANSI escape codes from text for accurate width calculation
// // #[allow(dead_code)]
// fn strip_ansi_codes(text: &str) -> String {
//     // Simple ANSI escape sequence pattern: \x1B[...m
//     let mut result = String::new();
//     let mut chars = text.chars();

//     while let Some(ch) = chars.next() {
//         if ch == '\x1B' {
//             // Skip until 'm' is found (end of ANSI sequence)
//             if chars.next() == Some('[') {
//                 while let Some(c) = chars.next() {
//                     if c == 'm' {
//                         break;
//                     }
//                 }
//             }
//         } else {
//             result.push(ch);
//         }
//     }

//     result
// }

/// Format cell value specifically for comfy-table (without external ANSI codes)
fn format_cell_value_for_comfy_table(value: &JsonValue, options: &DisplayOptions) -> String {
    let text = match value {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => s.clone(),
        JsonValue::Array(arr) => {
            if arr.is_empty() {
                "[]".to_string()
            } else if options.no_truncate {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(|v| format_cell_value_for_comfy_table(
                            v,
                            &options.clone().with_color(false)
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!("[{} items]", arr.len())
            }
        }
        JsonValue::Object(obj) => {
            if obj.is_empty() {
                "{}".to_string()
            } else if options.no_truncate {
                format!(
                    "{{{}}}",
                    obj.iter()
                        .map(|(k, v)| format!(
                            "{}: {}",
                            k,
                            format_cell_value_for_comfy_table(
                                v,
                                &options.clone().with_color(false)
                            )
                        ))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!("{{{} fields}}", obj.len())
            }
        }
    };

    // Apply truncation if needed (no external color codes)
    if !options.no_truncate {
        truncate_string(&text, options.effective_max_length())
    } else {
        text
    }
}

/// Apply cell color using comfy-table's built-in coloring
fn apply_cell_color(cell: Cell, value: &JsonValue) -> Cell {
    match value {
        JsonValue::Null => cell.fg(comfy_table::Color::Magenta),
        JsonValue::Bool(true) => cell.fg(comfy_table::Color::Green),
        JsonValue::Bool(false) => cell.fg(comfy_table::Color::Red),
        JsonValue::Number(_) => cell.fg(comfy_table::Color::Blue),
        JsonValue::String(s) => {
            if s.contains("Running") {
                cell.fg(comfy_table::Color::Green)
            } else if s.contains("Pending") {
                cell.fg(comfy_table::Color::Yellow)
            } else if s.contains("Wait") {
                cell.fg(comfy_table::Color::Blue)
            } else if s.contains("Cancel") || s.contains("High") {
                cell.fg(comfy_table::Color::Red)
            } else if s.contains("Medium") {
                cell.fg(comfy_table::Color::Yellow)
            } else if s.contains("Low") {
                cell.fg(comfy_table::Color::Blue)
            } else {
                cell
            }
        }
        _ => cell,
    }
}

/// StreamingTableVisualizer implementation  
impl Default for StreamingTableVisualizer {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingTableVisualizer {
    pub fn new() -> Self {
        Self
    }
}

impl StreamingVisualizer for StreamingTableVisualizer {
    fn start_stream(&self, stream_type: &str, options: &DisplayOptions) {
        let start_msg = format!("🔄 Streaming {stream_type} results in table format...");
        if options.color_enabled {
            println!("{}", color::colorize_text(&start_msg, "cyan", true));
        } else {
            println!("{start_msg}");
        }
        println!(); // Add blank line for readability
    }

    fn render_item(&self, item: &JsonValue, index: usize, options: &DisplayOptions) {
        // Create a single-item table for each result
        let table_visualizer = TableVisualizer;
        let single_item = vec![item.clone()];
        let table_output = table_visualizer.visualize(&single_item, options);

        // Add a header for each result
        let result_header = format!("Result #{}", index + 1);
        if options.color_enabled {
            println!("{}", color::colorize_text(&result_header, "blue", true));
        } else {
            println!("{result_header}");
        }

        println!("{table_output}");
        println!(); // Add blank line between results
    }

    fn end_stream(&self, total_count: usize, options: &DisplayOptions) {
        let end_msg = format!("✅ Table streaming completed ({total_count} results)");
        if options.color_enabled {
            println!("{}", color::colorize_text(&end_msg, "green", true));
        } else {
            println!("{end_msg}");
        }
    }
}

/// StreamingCardVisualizer implementation
impl StreamingVisualizer for StreamingCardVisualizer {
    fn start_stream(&self, stream_type: &str, options: &DisplayOptions) {
        let start_msg = format!("🔄 Streaming {stream_type} results...");
        if options.color_enabled {
            println!("{}", color::colorize_text(&start_msg, "cyan", true));
        } else {
            println!("{start_msg}");
        }
    }

    fn render_item(&self, item: &JsonValue, index: usize, options: &DisplayOptions) {
        let border = if options.use_unicode { "─" } else { "-" };
        let corner_tl = if options.use_unicode { "╭" } else { "+" };
        let corner_tr = if options.use_unicode { "╮" } else { "+" };
        let corner_bl = if options.use_unicode { "╰" } else { "+" };
        let corner_br = if options.use_unicode { "╯" } else { "+" };
        let vertical = if options.use_unicode { "│" } else { "|" };

        let title = format!(" Result #{} ", index + 1);
        let border_width: usize = 60;
        let title_padding = border_width.saturating_sub(title.len() + 2);
        let left_padding = title_padding / 2;
        let right_padding = title_padding - left_padding;

        // Top border with title
        println!(
            "{}{}{}{}{}",
            corner_tl,
            border.repeat(left_padding),
            title,
            border.repeat(right_padding),
            corner_tr
        );

        // Format content hierarchically
        let formatted = format_json_hierarchy(item, 0, options.effective_max_length());

        // Print content with borders
        for line in formatted.lines() {
            if !line.trim().is_empty() {
                let padded_line = format!(" {line}");
                let padding_needed = border_width.saturating_sub(padded_line.chars().count() + 1);
                println!(
                    "{}{}{} {}",
                    vertical,
                    padded_line,
                    " ".repeat(padding_needed),
                    vertical
                );
            }
        }

        // Bottom border
        println!("{}{}{}", corner_bl, border.repeat(border_width), corner_br);
        println!(); // Empty line between items
    }

    fn end_stream(&self, total_count: usize, options: &DisplayOptions) {
        let end_msg = format!("✅ Stream completed ({total_count} results received)");
        if options.color_enabled {
            println!("{}", color::colorize_text(&end_msg, "green", true));
        } else {
            println!("{end_msg}");
        }
    }
}

/// StreamingJsonVisualizer implementation
impl StreamingVisualizer for StreamingJsonVisualizer {
    fn start_stream(&self, _stream_type: &str, _options: &DisplayOptions) {
        // JSON streaming doesn't need a start message
    }

    fn render_item(&self, item: &JsonValue, _index: usize, _options: &DisplayOptions) {
        match serde_json::to_string_pretty(item) {
            Ok(json) => println!("{json}"),
            Err(_) => println!("Invalid JSON"),
        }
        println!(); // Empty line between items
    }

    fn end_stream(&self, _total_count: usize, _options: &DisplayOptions) {
        // JSON streaming doesn't need an end message
    }
}

// // /// Format cell value for truncated text with proper coloring
// // #[allow(dead_code)]
// fn format_cell_value_for_truncated(value: &JsonValue, options: &DisplayOptions, truncated_text: &str) -> String {
//     if options.color_enabled {
//         apply_value_coloring(truncated_text, value)
//     } else {
//         truncated_text.to_string()
//     }
// }

// // /// Apply appropriate coloring to values based on their type and content
// // #[allow(dead_code)]
// fn apply_value_coloring(text: &str, value: &JsonValue) -> String {
//     match value {
//         JsonValue::Null => color::colorize_text(text, "magenta", true),
//         JsonValue::Bool(true) => color::colorize_text(text, "green", true),
//         JsonValue::Bool(false) => color::colorize_text(text, "red", true),
//         JsonValue::Number(_) => color::colorize_text(text, "blue", true),
//         JsonValue::String(s) => {
//             // Special handling for status and priority strings
//             if s.contains("Running") {
//                 color::colorize_text(text, "green", true)
//             } else if s.contains("Pending") {
//                 color::colorize_text(text, "yellow", true)
//             } else if s.contains("Wait") {
//                 color::colorize_text(text, "blue", true)
//             } else if s.contains("Cancel") {
//                 color::colorize_text(text, "red", true)
//             } else if s.contains("High") {
//                 color::colorize_text(text, "red", true)
//             } else if s.contains("Medium") {
//                 color::colorize_text(text, "yellow", true)
//             } else if s.contains("Low") {
//                 color::colorize_text(text, "blue", true)
//             } else {
//                 text.to_string()
//             }
//         }
//         _ => text.to_string(),
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_table_visualizer_empty() {
        let visualizer = TableVisualizer;
        let options = DisplayOptions::default();
        let result = visualizer.visualize(&[], &options);
        assert_eq!(result, "No data");
    }

    #[test]
    fn test_table_visualizer_single_object() {
        let visualizer = TableVisualizer;
        let options = DisplayOptions::default();
        let data = vec![json!({"name": "test", "value": 42})];
        let result = visualizer.visualize(&data, &options);

        assert!(result.contains("name"));
        assert!(result.contains("value"));
        assert!(result.contains("test"));
        assert!(result.contains("42"));
    }

    #[test]
    fn test_card_visualizer_basic() {
        let visualizer = CardVisualizer;
        let options = DisplayOptions::default();
        let data = vec![json!({"name": "test"})];
        let result = visualizer.visualize(&data, &options);

        assert!(result.contains("Item #1"));
        assert!(result.contains("name"));
        assert!(result.contains("test"));
    }

    #[test]
    fn test_json_pretty_visualizer() {
        let visualizer = JsonPrettyVisualizer;
        let options = DisplayOptions::default();
        let data = vec![json!({"test": "value"})];
        let result = visualizer.visualize(&data, &options);

        assert!(result.contains("test"));
        assert!(result.contains("value"));
        // Should be pretty-printed JSON
        assert!(result.contains("{\n"));
    }

    #[test]
    fn test_collect_all_keys() {
        let data = vec![json!({"a": 1, "b": 2}), json!({"b": 3, "c": 4})];
        let keys = collect_all_keys(&data);
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    // #[test]
    // fn test_format_cell_value() {
    //     let options = DisplayOptions::default().with_color(false);

    //     assert_eq!(format_cell_value(&json!(null), &options), "null");
    //     assert_eq!(format_cell_value(&json!(true), &options), "true");
    //     assert_eq!(format_cell_value(&json!(42), &options), "42");
    //     assert_eq!(format_cell_value(&json!("test"), &options), "test");
    // }
}
