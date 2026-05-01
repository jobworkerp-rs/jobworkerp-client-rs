//! YAML-driven combined registration of workers and function_sets in a
//! single document.
//!
//! Each section (`workers:` / `function_sets:`) is optional. When both are
//! present, workers are registered first so that function_sets referencing
//! them by name resolve cleanly during their own pre-validation phase.
//!
//! See `docs/manifest-yaml.md` for the full user-facing reference.

#![allow(clippy::missing_errors_doc, clippy::doc_markdown)]

use super::helper::UseJobworkerpClientHelper;
use super::{function_set_yaml, worker_yaml, yaml_common};
use crate::error::ClientError;
use crate::jobworkerp::data::WorkerId;
use crate::jobworkerp::function::data::FunctionSetId;
use anyhow::{Context as _, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Combined top-level YAML document. Both sections are optional so a YAML
/// containing only `workers:` or only `function_sets:` parses cleanly.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    #[serde(default)]
    pub(crate) workers: Option<WorkersSection>,
    #[serde(default)]
    pub(crate) function_sets: Option<FunctionSetsSection>,
}

/// Worker section: same shape as the standalone `WorkersYaml` top level,
/// minus its outer `workers:` key (which is the manifest key).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkersSection {
    #[serde(default)]
    pub(crate) defaults: worker_yaml::WorkerDefaults,
    #[serde(default)]
    pub(crate) entries: Vec<worker_yaml::WorkerSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FunctionSetsSection {
    #[serde(default)]
    pub(crate) defaults: function_set_yaml::FunctionSetDefaults,
    #[serde(default)]
    pub(crate) entries: Vec<function_set_yaml::FunctionSetSpec>,
}

/// Result of registering a manifest. Either map may be empty when the
/// corresponding section was absent or empty.
#[derive(Debug, Default)]
pub struct ManifestResult {
    pub workers: HashMap<String, WorkerId>,
    pub function_sets: HashMap<String, FunctionSetId>,
}

/// Read a manifest YAML file and register each present section.
pub async fn register_manifest_from_yaml<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    yaml_path: &Path,
) -> Result<ManifestResult>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    let raw = tokio::fs::read_to_string(yaml_path)
        .await
        .with_context(|| format!("failed to read manifest YAML at {}", yaml_path.display()))?;
    let base_dir = yaml_path
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    register_manifest_from_yaml_str(client, cx, metadata, &raw, &base_dir).await
}

/// Variant of [`register_manifest_from_yaml`] that takes raw YAML text.
/// `base_dir` is forwarded to the workers section for `$file:` resolution.
///
/// Ordering: when both sections are present, workers are upserted first so
/// that any function_set that references a worker by name can resolve it
/// during its own pre-validation phase. Pre-validation is therefore split
/// across the two sections — a worker spec error fails before any RPC, a
/// function_set spec error fails after workers are already upserted.
/// Because both upsert paths are idempotent, retrying the manifest after
/// fixing the function_set side simply reconciles state.
pub async fn register_manifest_from_yaml_str<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    raw_yaml: &str,
    base_dir: &Path,
) -> Result<ManifestResult>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    let expanded = yaml_common::expand_env(raw_yaml)?;
    let manifest: Manifest =
        serde_yaml::from_str(&expanded).with_context(|| "failed to parse manifest YAML")?;

    let mut result = ManifestResult::default();

    if let Some(WorkersSection { defaults, entries }) = manifest.workers
        && !entries.is_empty()
    {
        let prepared = worker_yaml::prepare_workers(
            client,
            cx,
            metadata.clone(),
            entries,
            &defaults,
            base_dir,
        )
        .await?;
        for prep in prepared {
            let worker = client
                .upsert_worker(cx, metadata.clone(), &prep.worker_data)
                .await
                .with_context(|| format!("registering worker '{}' failed", prep.worker_name))?;
            let id = worker.id.ok_or_else(|| {
                ClientError::RuntimeError(format!(
                    "upsert_worker returned no id for {}",
                    prep.worker_name
                ))
            })?;
            result.workers.insert(prep.worker_name, id);
        }
    }

    if let Some(FunctionSetsSection { defaults, entries }) = manifest.function_sets
        && !entries.is_empty()
    {
        let prepared = function_set_yaml::prepare_function_sets(
            client,
            cx,
            metadata.clone(),
            entries,
            &defaults,
        )
        .await?;
        for prep in prepared {
            let id = client
                .upsert_function_set_by_name(cx, metadata.clone(), prep.data)
                .await
                .with_context(|| format!("registering function_set '{}' failed", prep.name))?;
            result.function_sets.insert(prep.name, id);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_workers_only() {
        let yaml = r"
workers:
  defaults:
    channel: high
  entries:
    - name: w1
      runner: COMMAND
";
        let m: Manifest = serde_yaml::from_str(yaml).unwrap();
        assert!(m.function_sets.is_none());
        let w = m.workers.unwrap();
        assert_eq!(w.entries.len(), 1);
        assert_eq!(w.entries[0].name, "w1");
        assert_eq!(w.defaults.channel.as_deref(), Some("high"));
    }

    #[test]
    fn parses_function_sets_only() {
        let yaml = r"
function_sets:
  defaults:
    category: 5
  entries:
    - name: fs1
      targets:
        - type: RUNNER
          name: COMMAND
";
        let m: Manifest = serde_yaml::from_str(yaml).unwrap();
        assert!(m.workers.is_none());
        let f = m.function_sets.unwrap();
        assert_eq!(f.entries.len(), 1);
        assert_eq!(f.defaults.category, Some(5));
    }

    #[test]
    fn parses_both_sections() {
        let yaml = r"
workers:
  entries:
    - name: w1
      runner: COMMAND
function_sets:
  entries:
    - name: fs1
      targets:
        - type: WORKER
          name: w1
";
        let m: Manifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(m.workers.unwrap().entries.len(), 1);
        assert_eq!(m.function_sets.unwrap().entries.len(), 1);
    }

    #[test]
    fn parses_empty_document() {
        // {} is a valid YAML mapping; both sections default to None.
        let m: Manifest = serde_yaml::from_str("{}").unwrap();
        assert!(m.workers.is_none());
        assert!(m.function_sets.is_none());
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let yaml = r"
workers:
  entries: []
extra: oops
";
        let err = serde_yaml::from_str::<Manifest>(yaml).unwrap_err();
        assert!(format!("{err}").contains("extra"));
    }

    #[test]
    #[serial_test::serial]
    fn env_expansion_runs_on_manifest_yaml() {
        // Regression guard: switching from per-section to manifest loader
        // must not silently drop `${VAR}` interpolation.
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe { std::env::set_var("MANIFEST_TEST_NAME", "expanded-name") };
        let yaml = r"
workers:
  entries:
    - name: ${MANIFEST_TEST_NAME}
      runner: COMMAND
";
        let expanded = yaml_common::expand_env(yaml).unwrap();
        let m: Manifest = serde_yaml::from_str(&expanded).unwrap();
        let entries = m.workers.unwrap().entries;
        assert_eq!(entries[0].name, "expanded-name");
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe { std::env::remove_var("MANIFEST_TEST_NAME") };
    }

    #[test]
    fn rejects_legacy_flat_workers_key() {
        // A YAML written for `register_workers_from_yaml` (flat `workers:`
        // listing specs directly) does not parse as a manifest, since
        // `workers:` here is a section object, not a sequence. The serde
        // error makes the schema mismatch obvious to the operator.
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
";
        let err = serde_yaml::from_str::<Manifest>(yaml).unwrap_err();
        let rendered = format!("{err}");
        assert!(
            rendered.contains("expected") || rendered.contains("invalid"),
            "expected schema-mismatch error, got: {rendered}"
        );
    }
}
