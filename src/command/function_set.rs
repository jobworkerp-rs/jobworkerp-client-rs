// function_set: valid commands are create, find, find_by_name, list, update, delete, count
// -i, --id <number> id of the function set (for find, update, delete)
// -n, --name <string> name of the function set (for create, update, find_by_name)
// -d, --description <string> description of the function set (for create, update)
// -c, --category <number> category of the function set (for create, update)
// -t, --targets <json array string> targets of the function set (for create, update)

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use crate::{
    client::{helper::UseJobworkerpClientHelper, wrapper::JobworkerpClientWrapper},
    command::{id_map_to_rows, to_request},
    display::{DisplayOptions, utils::supports_color, visualize_rows},
    jobworkerp::{
        data::{RunnerId, WorkerId},
        function::{
            data::{
                FunctionId, FunctionSet, FunctionSetData, FunctionSetId, FunctionUsing, function_id,
            },
            service::FindByNameRequest,
        },
    },
};
use clap::Parser;
use serde_json::Value;

pub mod display;

#[derive(Parser, Debug)]
pub struct FunctionSetArg {
    #[clap(subcommand)]
    pub cmd: FunctionSetCommand,
}

#[derive(Parser, Debug)]
pub enum FunctionSetCommand {
    Create {
        #[clap(short, long)]
        name: String,
        #[clap(short, long)]
        description: String,
        #[clap(short, long, default_value_t = 0)]
        category: i32,
        #[clap(short, long)]
        targets: String, // JSON array string
    },
    Find {
        #[clap(short, long)]
        id: i64,
    },
    FindByName {
        #[clap(short, long)]
        name: String,
        #[clap(long, value_enum, default_value = "card")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    List {
        #[clap(short, long)]
        offset: Option<i64>,
        #[clap(short, long)]
        limit: Option<i32>,
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
    Update {
        #[clap(short, long)]
        id: i64,
        #[clap(short, long)]
        name: Option<String>,
        #[clap(short, long)]
        description: Option<String>,
        #[clap(short, long)]
        category: Option<i32>,
        #[clap(short, long)]
        targets: Option<String>, // JSON array string
    },
    Delete {
        #[clap(short, long)]
        id: i64,
    },
    Count {},
    /// Apply function_set definitions from a YAML manifest file.
    Apply {
        /// Path to the function_set YAML file (see docs/function-set-yaml.md).
        file: PathBuf,
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
}

#[allow(
    clippy::too_many_lines,
    clippy::missing_panics_doc,
    clippy::items_after_statements,
    clippy::similar_names
)]
impl FunctionSetCommand {
    pub async fn execute(
        &self,
        client: &crate::client::JobworkerpClient,
        metadata: &HashMap<String, String>,
    ) {
        match self {
            Self::Create {
                name,
                description,
                category,
                targets,
            } => {
                let targets = parse_targets(targets);
                let request = FunctionSetData {
                    name: name.clone(),
                    description: description.clone(),
                    category: *category,
                    targets,
                };
                let response = client
                    .function_set_client()
                    .await
                    .create(to_request(metadata, request).unwrap())
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
            Self::Find { id } => {
                let id = FunctionSetId { value: *id };
                let response = client
                    .function_set_client()
                    .await
                    .find(to_request(metadata, id).unwrap())
                    .await
                    .unwrap()
                    .into_inner()
                    .data;
                if let Some(function_set) = response {
                    print_function_set(function_set);
                } else {
                    println!("function set not found");
                }
            }
            Self::FindByName {
                name,
                format,
                no_truncate,
            } => {
                use self::display::function_set_to_json;
                use crate::display::{
                    CardVisualizer, DisplayOptions, JsonPrettyVisualizer, JsonVisualizer,
                    TableVisualizer, utils::supports_color,
                };

                let request = FindByNameRequest { name: name.clone() };
                let response = client
                    .function_set_client()
                    .await
                    .find_by_name(to_request(metadata, request).unwrap())
                    .await
                    .unwrap()
                    .into_inner()
                    .data;
                if let Some(function_set) = response {
                    let function_set_json = function_set_to_json(&function_set, format);
                    let function_sets_vec = vec![function_set_json];

                    let options = DisplayOptions::new(*format)
                        .with_color(supports_color())
                        .with_no_truncate(*no_truncate);

                    let output = match format {
                        crate::display::DisplayFormat::Table => {
                            let visualizer = TableVisualizer;
                            visualizer.visualize(&function_sets_vec, &options)
                        }
                        crate::display::DisplayFormat::Card => {
                            let visualizer = CardVisualizer;
                            visualizer.visualize(&function_sets_vec, &options)
                        }
                        crate::display::DisplayFormat::Json => {
                            let visualizer = JsonPrettyVisualizer;
                            visualizer.visualize(&function_sets_vec, &options)
                        }
                    };

                    println!("{output}");
                } else {
                    println!("function set not found");
                }
            }
            Self::List {
                offset,
                limit,
                format,
                no_truncate,
            } => {
                use self::display::function_set_to_json;
                use crate::display::{
                    CardVisualizer, DisplayOptions, JsonPrettyVisualizer, JsonVisualizer,
                    TableVisualizer, utils::supports_color,
                };

                let response = client
                    .function_set_client()
                    .await
                    .find_list(
                        to_request(
                            metadata,
                            crate::jobworkerp::service::FindListRequest {
                                offset: *offset,
                                limit: *limit,
                            },
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();

                println!("meta: {:#?}", response.metadata());
                let mut data = response.into_inner();

                // Collect all function sets into a vector for batch processing
                let mut function_sets_json = Vec::new();
                while let Some(function_set) = data.message().await.unwrap() {
                    let function_set_json = function_set_to_json(&function_set, format);
                    function_sets_json.push(function_set_json);
                }

                // Display using the appropriate visualizer
                let options = DisplayOptions::new(*format)
                    .with_color(supports_color())
                    .with_no_truncate(*no_truncate);

                let output = match format {
                    crate::display::DisplayFormat::Table => {
                        let visualizer = TableVisualizer;
                        visualizer.visualize(&function_sets_json, &options)
                    }
                    crate::display::DisplayFormat::Card => {
                        let visualizer = CardVisualizer;
                        visualizer.visualize(&function_sets_json, &options)
                    }
                    crate::display::DisplayFormat::Json => {
                        let visualizer = JsonPrettyVisualizer;
                        visualizer.visualize(&function_sets_json, &options)
                    }
                };

                println!("{output}");
                println!(
                    "trailers: {:#?}",
                    data.trailers().await.unwrap().unwrap_or_default()
                );
            }
            Self::Update {
                id,
                name,
                description,
                category,
                targets,
            } => {
                // Find existing function set
                let res = client
                    .function_set_client()
                    .await
                    .find(to_request(metadata, FunctionSetId { value: *id }).unwrap())
                    .await
                    .unwrap();
                let function_set_opt = res.into_inner().data;
                if let Some(mut function_set) = function_set_opt {
                    if let Some(data) = &mut function_set.data {
                        data.name = name.clone().unwrap_or_else(|| data.name.clone());
                        data.description = description
                            .clone()
                            .unwrap_or_else(|| data.description.clone());
                        data.category = category.unwrap_or(data.category);
                        if let Some(targets_str) = targets {
                            data.targets = parse_targets(targets_str);
                        }
                    }
                    let response = client
                        .function_set_client()
                        .await
                        .update(to_request(metadata, function_set).unwrap())
                        .await
                        .unwrap();
                    println!("{response:#?}");
                } else {
                    println!("function set not found");
                }
            }
            Self::Delete { id } => {
                let id = FunctionSetId { value: *id };
                let response = client
                    .function_set_client()
                    .await
                    .delete(to_request(metadata, id).unwrap())
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
            Self::Count {} => {
                let response = client
                    .function_set_client()
                    .await
                    .count(
                        to_request(metadata, crate::jobworkerp::service::CountCondition {})
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                println!("{response:#?}");
            }
            Self::Apply {
                file,
                format,
                no_truncate,
            } => {
                apply_function_sets(client, metadata, file, *format, *no_truncate).await;
            }
        }
    }
}

async fn apply_function_sets(
    client: &crate::client::JobworkerpClient,
    metadata: &HashMap<String, String>,
    file: &std::path::Path,
    format: crate::display::DisplayFormat,
    no_truncate: bool,
) {
    let wrapper: JobworkerpClientWrapper = client.clone().into();
    let registered = match wrapper
        .register_function_sets_from_yaml(None, Arc::new(metadata.clone()), file)
        .await
    {
        Ok(map) => map,
        Err(err) => {
            eprintln!("function-set apply failed: {err:#}");
            std::process::exit(1);
        }
    };

    // Always route through visualize_rows even when empty so `--format json`
    // emits a valid `[]` rather than a human-readable sentinel that would
    // break downstream script parsers.
    let rows = id_map_to_rows(registered, "function_set_id");
    let options = DisplayOptions::new(format)
        .with_color(supports_color())
        .with_no_truncate(no_truncate);
    println!("{}", visualize_rows(&rows, &options));
}

fn parse_targets(targets_json: &str) -> Vec<FunctionUsing> {
    let targets_value: Value = serde_json::from_str(targets_json).expect("Invalid JSON format");
    let targets_array = targets_value.as_array().expect("Targets must be an array");

    targets_array
        .iter()
        .map(|target| {
            let id = target
                .get("id")
                .expect("Target must have id")
                .as_i64()
                .expect("id must be a number");

            let type_value = target
                .get("type")
                .expect("Target must have type")
                .as_str()
                .expect("type must be a string (RUNNER or WORKER)");

            let function_id_inner = match type_value {
                "RUNNER" | "0" => function_id::Id::RunnerId(RunnerId { value: id }),
                "WORKER" | "1" => function_id::Id::WorkerId(WorkerId { value: id }),
                _ => panic!("Invalid function type: {type_value}. Must be RUNNER or WORKER"),
            };

            FunctionUsing {
                function_id: Some(FunctionId {
                    id: Some(function_id_inner),
                }),
                using: None,
            }
        })
        .collect()
}

fn print_function_set(function_set: FunctionSet) {
    if let (Some(id), Some(data)) = (function_set.id, function_set.data) {
        println!("[function_set]:");
        println!("\t[id] {}", id.value);
        println!("\t[name] {}", data.name);
        println!("\t[description] {}", data.description);
        println!("\t[category] {}", data.category);
        println!("\t[targets]:");
        for (i, target) in data.targets.iter().enumerate() {
            let (id_value, type_str) =
                match target.function_id.as_ref().and_then(|fid| fid.id.as_ref()) {
                    Some(function_id::Id::RunnerId(rid)) => (rid.value, "RUNNER"),
                    Some(function_id::Id::WorkerId(wid)) => (wid.value, "WORKER"),
                    None => (0, "UNKNOWN"),
                };
            println!("\t\t[{i}] id: {id_value}, type: {type_str}");
        }
    } else {
        println!("Invalid function set data");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct TestRoot {
        #[clap(subcommand)]
        cmd: FunctionSetCommand,
    }

    #[test]
    fn parses_apply_with_format_and_no_truncate() {
        let parsed = TestRoot::parse_from([
            "test",
            "apply",
            "./fs.yaml",
            "--format",
            "json",
            "--no-truncate",
        ]);
        match parsed.cmd {
            FunctionSetCommand::Apply {
                file,
                format,
                no_truncate,
            } => {
                assert_eq!(file, PathBuf::from("./fs.yaml"));
                assert!(matches!(format, crate::display::DisplayFormat::Json));
                assert!(no_truncate);
            }
            other => panic!("expected Apply, got {other:?}"),
        }
    }

    #[test]
    fn apply_defaults_to_table_format() {
        let parsed = TestRoot::parse_from(["test", "apply", "./fs.yaml"]);
        match parsed.cmd {
            FunctionSetCommand::Apply {
                format,
                no_truncate,
                ..
            } => {
                assert!(matches!(format, crate::display::DisplayFormat::Table));
                assert!(!no_truncate);
            }
            other => panic!("expected Apply, got {other:?}"),
        }
    }

    #[test]
    fn apply_rejects_unknown_format() {
        let err = TestRoot::try_parse_from(["test", "apply", "./fs.yaml", "--format", "xml"])
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("xml") || msg.contains("invalid value"));
    }
}
