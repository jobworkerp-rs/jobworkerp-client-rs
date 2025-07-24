//! Tests to verify JSON output contains no ANSI escape sequences

#[cfg(test)]
mod tests {
    use super::super::{DisplayOptions, DisplayFormat};
    
    #[test]
    fn test_json_format_disables_colors_automatically() {
        let options = DisplayOptions::new(DisplayFormat::Json);
        assert_eq!(options.color_enabled, false);
        assert_eq!(options.format, DisplayFormat::Json);
    }
    
    #[test]
    fn test_json_format_ignores_with_color_override() {
        let options = DisplayOptions::new(DisplayFormat::Json)
            .with_color(true);  // Try to force colors on
        
        // Should still be false for JSON format
        assert_eq!(options.color_enabled, false);
    }
    
    #[test]
    fn test_other_formats_respect_color_setting() {
        let table_options = DisplayOptions::new(DisplayFormat::Table)
            .with_color(false);
        assert_eq!(table_options.color_enabled, false);
        
        let card_options = DisplayOptions::new(DisplayFormat::Card)
            .with_color(true);
        assert_eq!(card_options.color_enabled, true);
    }
}