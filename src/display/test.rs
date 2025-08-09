//! Tests to verify JSON output contains no ANSI escape sequences

#[cfg(test)]
mod tests {
    use super::super::{DisplayFormat, DisplayOptions};

    #[test]
    fn test_json_format_disables_colors_automatically() {
        let options = DisplayOptions::new(DisplayFormat::Json);
        assert!(!options.color_enabled);
        assert_eq!(options.format, DisplayFormat::Json);
    }

    #[test]
    fn test_json_format_ignores_with_color_override() {
        let options = DisplayOptions::new(DisplayFormat::Json).with_color(true); // Try to force colors on

        // Should still be false for JSON format
        assert!(!options.color_enabled);
    }

    #[test]
    fn test_other_formats_respect_color_setting() {
        let table_options = DisplayOptions::new(DisplayFormat::Table).with_color(false);
        assert!(!table_options.color_enabled);

        let card_options = DisplayOptions::new(DisplayFormat::Card).with_color(true);
        assert!(card_options.color_enabled);
    }
}
