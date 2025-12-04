//! Display format definitions and enum formatting functionality

use clap::ValueEnum;
use crate::jobworkerp::data::StreamingOutputType;

/// Available display formats for CLI output
#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum DisplayFormat {
    /// Table format with columns and borders
    #[value(name = "table")]
    Table,
    /// Card format with detailed individual entries
    #[value(name = "card")]
    Card,
    /// JSON format for machine processing
    #[value(name = "json")]
    Json,
}

impl std::fmt::Display for DisplayFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DisplayFormat::Table => write!(f, "table"),
            DisplayFormat::Card => write!(f, "card"),
            DisplayFormat::Json => write!(f, "json"),
        }
    }
}

/// Trait for formatting enum values based on display format
pub trait EnumFormatter<T> {
    fn format(&self, value: T, format: &DisplayFormat) -> String;
}

/// Formatter for StreamingOutputType enum
pub struct StreamingOutputTypeFormatter;

impl EnumFormatter<StreamingOutputType> for StreamingOutputTypeFormatter {
    fn format(&self, output_type: StreamingOutputType, format: &DisplayFormat) -> String {
        match format {
            DisplayFormat::Table => match output_type {
                StreamingOutputType::Streaming => "STREAMING",
                StreamingOutputType::NonStreaming => "NON_STREAMING",
                StreamingOutputType::Both => "BOTH",
            }
            .to_string(),
            DisplayFormat::Card => match output_type {
                StreamingOutputType::Streaming => "📊 STREAMING",
                StreamingOutputType::NonStreaming => "📄 NON_STREAMING",
                StreamingOutputType::Both => "🔄 BOTH",
            }
            .to_string(),
            DisplayFormat::Json => output_type.as_str_name().to_string(),
        }
    }
}

/// Utility functions for color formatting
pub mod color {
    use yansi::Paint;

    /// Apply color to text if color is enabled
    pub fn colorize_text(text: &str, color: &str, enabled: bool) -> String {
        if !enabled {
            return text.to_string();
        }

        match color {
            "red" => Paint::red(text).to_string(),
            "green" => Paint::green(text).to_string(),
            "yellow" => Paint::yellow(text).to_string(),
            "blue" => Paint::blue(text).to_string(),
            "cyan" => Paint::cyan(text).to_string(),
            "magenta" => Paint::magenta(text).to_string(),
            "white" => Paint::white(text).to_string(),
            _ => text.to_string(),
        }
    }

    /// Colorize status text with appropriate color
    pub fn colorize_status(status: &str, enabled: bool) -> String {
        let color = match status.to_lowercase().as_str() {
            s if s.contains("running") => "green",
            s if s.contains("pending") => "yellow",
            s if s.contains("wait") || s.contains("result") => "blue",
            s if s.contains("cancel") || s.contains("fail") => "red",
            _ => "white",
        };
        colorize_text(status, color, enabled)
    }

    /// Colorize priority text with appropriate color
    pub fn colorize_priority(priority: &str, enabled: bool) -> String {
        let color = match priority.to_lowercase().as_str() {
            s if s.contains("high") => "red",
            s if s.contains("medium") => "yellow",
            s if s.contains("low") => "blue",
            _ => "white",
        };
        colorize_text(priority, color, enabled)
    }
}
