//! Display formatting module for CLI output
//! 
//! This module provides unified display functionality for various CLI commands,
//! supporting multiple output formats (table, card, JSON) with customizable options.

pub mod format;
pub mod visualizer;
pub mod utils;
#[cfg(test)]
mod test;

// Re-exports for public API
pub use format::DisplayFormat;
pub use visualizer::{JsonVisualizer, TableVisualizer, CardVisualizer, JsonPrettyVisualizer};
pub use utils::{truncate_string, format_json_hierarchy};

/// Display configuration options
#[derive(Debug, Clone)]
pub struct DisplayOptions {
    /// Output format (table, card, json)
    pub format: DisplayFormat,
    /// Enable color output
    pub color_enabled: bool,
    /// Maximum field length for truncation (None = no limit)
    pub max_field_length: Option<usize>,
    /// Use Unicode characters for formatting
    pub use_unicode: bool,
    /// Disable string truncation (for debugging)
    pub no_truncate: bool,
}

impl Default for DisplayOptions {
    fn default() -> Self {
        Self {
            format: DisplayFormat::Table,
            color_enabled: true,
            max_field_length: Some(50),
            use_unicode: true,
            no_truncate: false,
        }
    }
}

impl DisplayOptions {
    /// Create new DisplayOptions with specified format
    /// JSON format automatically disables colors for clean output
    pub fn new(format: DisplayFormat) -> Self {
        Self {
            format: format.clone(),
            color_enabled: match format {
                DisplayFormat::Json => false,  // Always disable colors for JSON
                _ => true,  // Default enabled for other formats
            },
            ..Default::default()
        }
    }

    /// Set color enabled/disabled
    /// Note: JSON format automatically disables colors regardless of this setting
    pub fn with_color(mut self, enabled: bool) -> Self {
        self.color_enabled = match self.format {
            DisplayFormat::Json => false,  // Always disable colors for JSON
            _ => enabled,  // Use the provided setting for other formats
        };
        self
    }

    /// Set no_truncate option
    pub fn with_no_truncate(mut self, no_truncate: bool) -> Self {
        self.no_truncate = no_truncate;
        self
    }

    /// Get effective max field length considering no_truncate option
    pub fn effective_max_length(&self) -> Option<usize> {
        if self.no_truncate {
            None
        } else {
            self.max_field_length
        }
    }
}