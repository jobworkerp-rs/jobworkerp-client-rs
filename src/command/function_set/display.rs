use serde_json::{json, Value as JsonValue};
use crate::display::DisplayFormat;

/// Convert FunctionSet to JSON format for display
pub fn function_set_to_json(
    function_set: &crate::jobworkerp::function::data::FunctionSet,
    format: &DisplayFormat,
) -> JsonValue {
    let mut json_obj = json!({});

    // Add ID information
    if let Some(id) = &function_set.id {
        json_obj["id"] = json!(id.value);
    }

    // Add data information if present
    if let Some(data) = &function_set.data {
        json_obj["name"] = json!(data.name);
        json_obj["description"] = json!(data.description);
        
        // Handle category as an integer
        json_obj["category"] = match format {
            DisplayFormat::Json => json!(data.category),
            _ => json!(format!("📂 Category {}", data.category))
        };

        // Handle targets (runner/worker references)
        if !data.targets.is_empty() {
            let targets_json: Vec<JsonValue> = data.targets.iter().map(|target| {
                let type_display = match format {
                    DisplayFormat::Json => {
                        match target.r#type() {
                            crate::jobworkerp::function::data::FunctionType::Runner => "RUNNER",
                            crate::jobworkerp::function::data::FunctionType::Worker => "WORKER",
                        }.to_string()
                    }
                    _ => {
                        match target.r#type() {
                            crate::jobworkerp::function::data::FunctionType::Runner => "🚀 RUNNER",
                            crate::jobworkerp::function::data::FunctionType::Worker => "⚙️ WORKER",
                        }.to_string()
                    }
                };
                
                json!({
                    "id": target.id,
                    "type": type_display
                })
            }).collect();
            
            json_obj["targets"] = match format {
                DisplayFormat::Json => json!(targets_json),
                _ => json!(format!("🎯 {} targets", targets_json.len()))
            };
            
            // For detailed view, also show target details
            if matches!(format, DisplayFormat::Card) || matches!(format, DisplayFormat::Json) {
                json_obj["target_details"] = json!(targets_json);
            }
        }
    }

    json_obj
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobworkerp::function::data::{FunctionSet, FunctionSetData, FunctionSetId, FunctionTarget, FunctionType};

    #[test]
    fn test_function_set_to_json_basic() {
        let function_set = FunctionSet {
            id: Some(FunctionSetId { value: 123 }),
            data: Some(FunctionSetData {
                name: "test_set".to_string(),
                description: "Test function set".to_string(),
                category: 1,
                targets: vec![
                    FunctionTarget {
                        id: 1001,
                        r#type: FunctionType::Runner as i32,
                    },
                    FunctionTarget {
                        id: 2001,
                        r#type: FunctionType::Worker as i32,
                    },
                ],
            }),
        };

        let json = function_set_to_json(&function_set, &DisplayFormat::Table);
        assert_eq!(json["id"], 123);
        assert_eq!(json["name"], "test_set");
        assert_eq!(json["description"], "Test function set");
        assert_eq!(json["category"], "📂 Category 1");
        assert_eq!(json["targets"], "🎯 2 targets");
    }

    #[test]
    fn test_function_set_to_json_minimal() {
        let function_set = FunctionSet {
            id: Some(FunctionSetId { value: 456 }),
            data: Some(FunctionSetData {
                name: "minimal_set".to_string(),
                description: "Minimal function set".to_string(),
                category: 0,
                targets: vec![],
            }),
        };

        let json = function_set_to_json(&function_set, &DisplayFormat::Json);
        assert_eq!(json["id"], 456);
        assert_eq!(json["name"], "minimal_set");
        assert_eq!(json["description"], "Minimal function set");
        assert_eq!(json["category"], 0);
        assert!(json.get("targets").is_none());
    }

    #[test]
    fn test_function_set_json_format() {
        let function_set = FunctionSet {
            id: Some(FunctionSetId { value: 789 }),
            data: Some(FunctionSetData {
                name: "json_test".to_string(),
                description: "JSON format test".to_string(),
                category: 2,
                targets: vec![
                    FunctionTarget {
                        id: 3001,
                        r#type: FunctionType::Runner as i32,
                    },
                ],
            }),
        };

        // Test JSON format (should not have emoji decorations)
        let json = function_set_to_json(&function_set, &DisplayFormat::Json);
        assert_eq!(json["category"], 2);
        if let Some(targets) = json.get("target_details") {
            assert_eq!(targets[0]["type"], "RUNNER");
        }
        
        // Test Card format (should have emoji decorations)  
        let card_json = function_set_to_json(&function_set, &DisplayFormat::Card);
        assert_eq!(card_json["category"], "📂 Category 2");
        if let Some(targets) = card_json.get("target_details") {
            assert_eq!(targets[0]["type"], "🚀 RUNNER");
        }
    }
}