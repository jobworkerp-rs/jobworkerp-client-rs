// manifest: apply a combined worker + function_set YAML manifest.
// See docs/manifest-yaml.md for the YAML schema.

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use crate::{
    client::{
        JobworkerpClient, helper::UseJobworkerpClientHelper, manifest_yaml::ManifestResult,
        wrapper::JobworkerpClientWrapper,
    },
    command::id_map_to_rows,
    display::{DisplayOptions, utils::supports_color, visualize_rows},
};
use clap::Parser;

#[derive(Parser, Debug)]
pub struct ManifestArg {
    #[clap(subcommand)]
    pub cmd: ManifestCommand,
}

#[derive(Parser, Debug)]
pub enum ManifestCommand {
    /// Apply a manifest YAML file that combines workers and function_sets.
    Apply {
        /// Path to the manifest YAML file (see docs/manifest-yaml.md).
        file: PathBuf,
        #[clap(long, value_enum, default_value = "table")]
        format: crate::display::DisplayFormat,
        #[clap(long)]
        no_truncate: bool,
    },
}

impl ManifestCommand {
    pub async fn execute(&self, client: &JobworkerpClient, metadata: &HashMap<String, String>) {
        match self {
            Self::Apply {
                file,
                format,
                no_truncate,
            } => {
                apply_manifest(client, metadata, file, *format, *no_truncate).await;
            }
        }
    }
}

async fn apply_manifest(
    client: &JobworkerpClient,
    metadata: &HashMap<String, String>,
    file: &std::path::Path,
    format: crate::display::DisplayFormat,
    no_truncate: bool,
) {
    let wrapper: JobworkerpClientWrapper = client.clone().into();
    let ManifestResult {
        workers,
        function_sets,
    } = match wrapper
        .register_manifest_from_yaml(None, Arc::new(metadata.clone()), file)
        .await
    {
        Ok(r) => r,
        Err(err) => {
            eprintln!("manifest apply failed: {err:#}");
            std::process::exit(1);
        }
    };

    let worker_rows = id_map_to_rows(workers, "worker_id");
    let fs_rows = id_map_to_rows(function_sets, "function_set_id");

    if matches!(format, crate::display::DisplayFormat::Json) {
        let combined = serde_json::json!({
            "workers": worker_rows,
            "function_sets": fs_rows,
        });
        let pretty =
            serde_json::to_string_pretty(&combined).unwrap_or_else(|_| combined.to_string());
        println!("{pretty}");
        return;
    }

    let options = DisplayOptions::new(format)
        .with_color(supports_color())
        .with_no_truncate(no_truncate);
    print_section("workers", &worker_rows, &options);
    println!();
    print_section("function_sets", &fs_rows, &options);
}

fn print_section(label: &str, rows: &[serde_json::Value], options: &DisplayOptions) {
    println!("[{label}]");
    if rows.is_empty() {
        println!("(no {label} registered)");
    } else {
        println!("{}", visualize_rows(rows, options));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Parser, Debug)]
    struct TestRoot {
        #[clap(subcommand)]
        cmd: ManifestCommand,
    }

    #[test]
    fn parses_apply_with_format_and_no_truncate() {
        let parsed = TestRoot::parse_from([
            "test",
            "apply",
            "./manifest.yaml",
            "--format",
            "json",
            "--no-truncate",
        ]);
        match parsed.cmd {
            ManifestCommand::Apply {
                file,
                format,
                no_truncate,
            } => {
                assert_eq!(file, PathBuf::from("./manifest.yaml"));
                assert!(matches!(format, crate::display::DisplayFormat::Json));
                assert!(no_truncate);
            }
        }
    }

    #[test]
    fn apply_defaults_to_table_format() {
        let parsed = TestRoot::parse_from(["test", "apply", "./manifest.yaml"]);
        match parsed.cmd {
            ManifestCommand::Apply {
                format,
                no_truncate,
                ..
            } => {
                assert!(matches!(format, crate::display::DisplayFormat::Table));
                assert!(!no_truncate);
            }
        }
    }

    #[test]
    fn apply_rejects_unknown_format() {
        let err = TestRoot::try_parse_from(["test", "apply", "./manifest.yaml", "--format", "xml"])
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("xml") || msg.contains("invalid value"));
    }
}
