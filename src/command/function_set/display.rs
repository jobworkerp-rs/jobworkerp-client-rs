use crate::display::DisplayFormat;
use serde_json::{json, Value as JsonValue};

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
            _ => json!(format!("📂 Category {}", data.category)),
        };

        // Handle targets (runner/worker references)
        if !data.targets.is_empty() {
            let targets_json: Vec<JsonValue> = data
                .targets
                .iter()
                .map(|target| {
                    use crate::jobworkerp::function::data::function_id;

                    let (id_value, type_display) = match target.function_id.as_ref().and_then(|fid| fid.id.as_ref()) {
                        Some(function_id::Id::RunnerId(rid)) => {
                            let type_str = match format {
                                DisplayFormat::Json => "RUNNER",
                                _ => "🚀 RUNNER",
                            };
                            (rid.value, type_str.to_string())
                        }
                        Some(function_id::Id::WorkerId(wid)) => {
                            let type_str = match format {
                                DisplayFormat::Json => "WORKER",
                                _ => "⚙️ WORKER",
                            };
                            (wid.value, type_str.to_string())
                        }
                        None => (0, "UNKNOWN".to_string()),
                    };

                    json!({
                        "id": id_value,
                        "type": type_display
                    })
                })
                .collect();

            json_obj["targets"] = match format {
                DisplayFormat::Json => json!(targets_json),
                _ => json!(format!("🎯 {} targets", targets_json.len())),
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
    use crate::jobworkerp::{
        data::{RunnerId, WorkerId},
        function::data::{function_id, FunctionId, FunctionSet, FunctionSetData, FunctionSetId},
    };

    #[test]
    fn test_function_set_to_json_basic() {
        use crate::jobworkerp::function::data::FunctionUsing;

        let function_set = FunctionSet {
            id: Some(FunctionSetId { value: 123 }),
            data: Some(FunctionSetData {
                name: "test_set".to_string(),
                description: "Test function set".to_string(),
                category: 1,
                targets: vec![
                    FunctionUsing {
                        function_id: Some(FunctionId {
                            id: Some(function_id::Id::RunnerId(RunnerId { value: 1001 })),
                        }),
                        using: None,
                    },
                    FunctionUsing {
                        function_id: Some(FunctionId {
                            id: Some(function_id::Id::WorkerId(WorkerId { value: 2001 })),
                        }),
                        using: None,
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
        use crate::jobworkerp::function::data::FunctionUsing;

        let function_set = FunctionSet {
            id: Some(FunctionSetId { value: 789 }),
            data: Some(FunctionSetData {
                name: "json_test".to_string(),
                description: "JSON format test".to_string(),
                category: 2,
                targets: vec![FunctionUsing {
                    function_id: Some(FunctionId {
                        id: Some(function_id::Id::RunnerId(RunnerId { value: 3001 })),
                    }),
                    using: None,
                }],
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
