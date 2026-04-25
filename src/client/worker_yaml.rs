//! YAML-driven worker registration for jobworkerp.
//!
//! Loads N worker definitions from a single YAML file, expands `${VAR:-default}`
//! environment-variable interpolation and `$file: <path>` includes (constrained
//! to `base_dir`), then upserts each `WorkerData` to the jobworkerp server via
//! [`UseJobworkerpClientHelper::upsert_worker`].
//!
//! See `docs/worker-yaml.md` (in this crate) for the full user-facing
//! reference, including schema, security model around `$file`, and error
//! semantics.

#![allow(clippy::missing_errors_doc, clippy::doc_markdown)]

use super::helper::{DEFAULT_RETRY_POLICY, UseJobworkerpClientHelper};
use crate::error::ClientError;
use crate::jobworkerp::data::{
    QueueType, ResponseType, RetryPolicy, RetryType, WorkerData, WorkerId,
};
use crate::proto::JobworkerpProto;
use anyhow::{Context as _, Result, anyhow};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Top-level YAML document.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkersYaml {
    #[serde(default)]
    pub defaults: WorkerDefaults,
    pub workers: Vec<WorkerSpec>,
}

/// Default values applied to every worker that does not override them.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerDefaults {
    pub channel: Option<String>,
    pub queue_type: Option<String>,
    pub response_type: Option<String>,
    pub store_success: Option<bool>,
    pub store_failure: Option<bool>,
    pub use_static: Option<bool>,
    pub broadcast_results: Option<bool>,
    pub periodic_interval: Option<u32>,
    pub retry_policy: Option<RetryPolicySpec>,
}

/// One worker definition.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerSpec {
    pub name: String,
    pub runner: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Free-form YAML matching the runner's `runner_settings` proto schema.
    /// Converted to JSON, then to proto bytes via `JobworkerpProto`.
    /// Omitting (or writing `settings: ~`) sends empty bytes, matching the
    /// `helper.rs::setup_worker_and_enqueue_with_json` semantics where a
    /// missing settings JSON skips proto encoding entirely.
    #[serde(default)]
    pub settings: Option<serde_yaml::Value>,
    pub channel: Option<String>,
    pub queue_type: Option<String>,
    pub response_type: Option<String>,
    pub store_success: Option<bool>,
    pub store_failure: Option<bool>,
    pub use_static: Option<bool>,
    pub broadcast_results: Option<bool>,
    pub periodic_interval: Option<u32>,
    pub retry_policy: Option<RetryPolicySpec>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicySpec {
    #[serde(rename = "type")]
    pub kind: String,
    pub interval: Option<u32>,
    pub max_interval: Option<u32>,
    pub max_retry: Option<u32>,
    pub basis: Option<f64>,
}

/// Register a single worker. Looks up the runner by name, encodes
/// `settings_json` to proto bytes via the runner's schema, fills in
/// `runner_id` / `runner_settings`, and calls `upsert_worker`.
///
/// `settings_json = None` sends empty `runner_settings` bytes regardless of
/// the runner's schema, mirroring the existing `helper.rs` upsert helpers
/// so that runners with optional-only schemas can be registered with no
/// configuration. `Some(Value::Null)` is treated the same way.
pub async fn register_worker<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    runner_name: &str,
    mut worker_data: WorkerData,
    settings_json: Option<&serde_json::Value>,
) -> Result<WorkerId>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    let (runner_id, rdata) = client
        .find_runner_or_error(cx, metadata.clone(), runner_name)
        .await?;
    let settings_desc = JobworkerpProto::parse_runner_settings_schema_descriptor(&rdata)?;
    let settings_bytes = match (settings_desc, settings_json) {
        (Some(desc), Some(json)) if !json.is_null() => {
            JobworkerpProto::json_value_to_message(desc, json, true, true).with_context(|| {
                format!("encoding runner_settings for runner '{runner_name}' as proto failed")
            })?
        }
        _ => Vec::new(),
    };
    worker_data.runner_id = Some(runner_id);
    worker_data.runner_settings = settings_bytes;

    let worker = client.upsert_worker(cx, metadata, &worker_data).await?;
    worker.id.ok_or_else(|| {
        ClientError::RuntimeError(format!(
            "upsert_worker returned no id for {}",
            &worker_data.name
        ))
        .into()
    })
}

/// Read a worker-definition YAML file and register every worker it contains.
/// Returns a `name -> WorkerId` map so callers can pick up specific worker IDs.
pub async fn register_workers_from_yaml<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    yaml_path: &Path,
) -> Result<HashMap<String, WorkerId>>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    let raw = tokio::fs::read_to_string(yaml_path)
        .await
        .with_context(|| format!("failed to read workers YAML at {}", yaml_path.display()))?;
    let base_dir = yaml_path
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    register_workers_from_yaml_str(client, cx, metadata, &raw, &base_dir).await
}

/// Variant of [`register_workers_from_yaml`] that takes raw YAML text.
/// `base_dir` is used to resolve `$file: <relative-path>` includes.
///
/// All YAML-level validation (env expansion, include resolution, enum
/// parsing in [`build_worker_data`], YAML→JSON conversion) is performed
/// up-front for every spec before any network call. If any spec fails
/// validation, no worker is registered. Once validation passes, workers
/// are upserted sequentially in YAML order; a network failure mid-loop
/// can still leave earlier workers registered (jobworkerp's
/// `UpsertByName` is idempotent, so a retry recovers).
pub async fn register_workers_from_yaml_str<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    raw_yaml: &str,
    base_dir: &Path,
) -> Result<HashMap<String, WorkerId>>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    let expanded = expand_env(raw_yaml)?;
    let doc: WorkersYaml =
        serde_yaml::from_str(&expanded).with_context(|| "failed to parse workers YAML")?;

    let WorkersYaml { defaults, workers } = doc;
    let prepared = prepare_workers(workers, &defaults, base_dir).await?;

    let mut out = HashMap::with_capacity(prepared.len());
    for prep in prepared {
        let id = register_worker(
            client,
            cx,
            metadata.clone(),
            &prep.runner_name,
            prep.worker_data,
            prep.settings_json.as_ref(),
        )
        .await
        .with_context(|| format!("registering worker '{}' failed", prep.worker_name))?;
        out.insert(prep.worker_name, id);
    }
    Ok(out)
}

/// Validated, network-ready worker payload. Built locally before any
/// jobworkerp RPC so per-spec failures never leak partial registrations.
struct PreparedWorker {
    worker_name: String,
    runner_name: String,
    worker_data: WorkerData,
    settings_json: Option<serde_json::Value>,
}

async fn prepare_workers(
    workers: Vec<WorkerSpec>,
    defaults: &WorkerDefaults,
    base_dir: &Path,
) -> Result<Vec<PreparedWorker>> {
    let mut out = Vec::with_capacity(workers.len());
    for mut spec in workers {
        let worker_name = spec.name.clone();
        let runner_name = std::mem::take(&mut spec.runner);
        let settings_yaml = std::mem::take(&mut spec.settings);
        let settings_json = match settings_yaml {
            Some(mut yaml) => {
                resolve_includes(&mut yaml, base_dir).await?;
                Some(serde_json::to_value(&yaml).with_context(|| {
                    format!("converting runner_settings of worker '{worker_name}' to JSON failed")
                })?)
            }
            None => None,
        };
        let worker_data = build_worker_data(&spec, defaults)
            .with_context(|| format!("validating worker '{worker_name}' failed"))?;
        out.push(PreparedWorker {
            worker_name,
            runner_name,
            worker_data,
            settings_json,
        });
    }
    Ok(out)
}

fn build_worker_data(spec: &WorkerSpec, defaults: &WorkerDefaults) -> Result<WorkerData> {
    let response_type = pick(&spec.response_type, &defaults.response_type)
        .map(parse_response_type)
        .transpose()?
        .unwrap_or(ResponseType::Direct as i32);
    let queue_type = pick(&spec.queue_type, &defaults.queue_type)
        .map(parse_queue_type)
        .transpose()?
        .unwrap_or(QueueType::Normal as i32);
    let retry_policy_spec = spec
        .retry_policy
        .as_ref()
        .or(defaults.retry_policy.as_ref());
    let retry_policy = match retry_policy_spec {
        Some(rp) => Some(parse_retry_policy(rp)?),
        None => Some(DEFAULT_RETRY_POLICY),
    };

    Ok(WorkerData {
        name: spec.name.clone(),
        description: spec.description.clone().unwrap_or_default(),
        runner_id: None,
        runner_settings: Vec::new(),
        retry_policy,
        periodic_interval: spec
            .periodic_interval
            .or(defaults.periodic_interval)
            .unwrap_or(0),
        channel: spec.channel.clone().or_else(|| defaults.channel.clone()),
        queue_type,
        response_type,
        store_success: spec
            .store_success
            .or(defaults.store_success)
            .unwrap_or(false),
        store_failure: spec
            .store_failure
            .or(defaults.store_failure)
            .unwrap_or(true),
        use_static: spec.use_static.or(defaults.use_static).unwrap_or(false),
        broadcast_results: spec
            .broadcast_results
            .or(defaults.broadcast_results)
            .unwrap_or(true),
    })
}

fn pick<'a>(per_worker: &'a Option<String>, default: &'a Option<String>) -> Option<&'a str> {
    per_worker.as_deref().or(default.as_deref())
}

fn parse_response_type(s: &str) -> Result<i32> {
    ResponseType::from_str_name(s)
        .map(|e| e as i32)
        .ok_or_else(|| anyhow!("invalid response_type '{s}' (expected DIRECT or NO_RESULT)"))
}

fn parse_queue_type(s: &str) -> Result<i32> {
    QueueType::from_str_name(s)
        .map(|e| e as i32)
        .ok_or_else(|| anyhow!("invalid queue_type '{s}' (expected NORMAL, WITH_BACKUP, DB_ONLY)"))
}

fn parse_retry_type(s: &str) -> Result<i32> {
    RetryType::from_str_name(s)
        .map(|e| e as i32)
        .ok_or_else(|| {
            anyhow!(
                "invalid retry_policy.type '{s}' (expected NONE, EXPONENTIAL, LINEAR, CONSTANT)"
            )
        })
}

fn parse_retry_policy(spec: &RetryPolicySpec) -> Result<RetryPolicy> {
    Ok(RetryPolicy {
        r#type: parse_retry_type(&spec.kind)?,
        interval: spec.interval.unwrap_or(DEFAULT_RETRY_POLICY.interval),
        max_interval: spec
            .max_interval
            .unwrap_or(DEFAULT_RETRY_POLICY.max_interval),
        max_retry: spec.max_retry.unwrap_or(DEFAULT_RETRY_POLICY.max_retry),
        #[allow(clippy::cast_possible_truncation)]
        basis: spec.basis.map_or(DEFAULT_RETRY_POLICY.basis, |b| b as f32),
    })
}

static ENV_RE: Lazy<Regex> = Lazy::new(|| {
    // ${NAME} or ${NAME:-default} where NAME = [A-Z_][A-Z0-9_]*
    Regex::new(r"\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}").expect("hardcoded regex")
});

/// Expand `${VAR}` and `${VAR:-default}` against the process environment.
/// Returns Err if a variable has neither an env value nor a `:-default` segment.
pub(crate) fn expand_env(raw: &str) -> Result<String> {
    // `replace_all` cannot return Result, so we record the first missing
    // variable and bail after the scan completes. Subsequent matches
    // short-circuit to "" to avoid additional env lookups.
    let mut missing: Option<String> = None;
    let out = ENV_RE.replace_all(raw, |caps: &Captures<'_>| -> String {
        if missing.is_some() {
            return String::new();
        }
        let name = &caps[1];
        if let Ok(v) = std::env::var(name) {
            return v;
        }
        match caps.get(2) {
            Some(d) => d.as_str().to_string(),
            None => {
                missing = Some(name.to_string());
                String::new()
            }
        }
    });
    if let Some(name) = missing {
        return Err(anyhow!(
            "environment variable '{name}' is referenced in YAML without a default \
             (use ${{{name}:-some-default}} to allow a fallback)"
        ));
    }
    Ok(out.into_owned())
}

/// Replace each `{ $file: "<path>" }` mapping in `value` with a plain
/// string scalar holding the file's contents. Relative paths resolve
/// against `base_dir`.
///
/// Path traversal is rejected: the canonical resolved path must live
/// under the canonical `base_dir`. Absolute paths and `..` segments
/// that escape `base_dir` are an error rather than a silent file read,
/// so a malicious YAML cannot exfiltrate arbitrary files via the
/// runner_settings the operator forwards to jobworkerp.
///
/// `base_dir` itself must exist and be canonicalize-able. The included
/// content is inserted as-is; nested `$file:` directives inside the
/// content are NOT expanded (the result is a string scalar, which the
/// walk treats as opaque).
///
/// Implemented as a sync-walk-then-async-read loop so the returned future
/// stays `Send` (recursive async fns crossing &mut tree borrows do not).
async fn resolve_includes(value: &mut serde_yaml::Value, base_dir: &Path) -> Result<()> {
    if first_include_path(value).is_none() {
        // Avoid the `base_dir` canonicalize round-trip when nothing to do.
        // (Test fixtures and YAMLs without $file directives skip the I/O entirely.)
        return Ok(());
    }
    let canonical_base = tokio::fs::canonicalize(base_dir).await.with_context(|| {
        format!(
            "$file include base_dir {} does not exist or is not accessible",
            base_dir.display()
        )
    })?;
    while let Some(rel) = first_include_path(value) {
        let raw_path = if Path::new(&rel).is_absolute() {
            PathBuf::from(&rel)
        } else {
            base_dir.join(&rel)
        };
        let canonical = tokio::fs::canonicalize(&raw_path)
            .await
            .with_context(|| format!("$file include failed: {}", raw_path.display()))?;
        if !canonical.starts_with(&canonical_base) {
            return Err(anyhow!(
                "$file include '{}' resolves to {}, which is outside base_dir {}",
                rel,
                canonical.display(),
                canonical_base.display()
            ));
        }
        let content = tokio::fs::read_to_string(&canonical)
            .await
            .with_context(|| format!("$file include read failed: {}", canonical.display()))?;
        let replaced = replace_first_include(value, &content);
        debug_assert!(
            replaced,
            "include marker present in collect pass but not in replace pass"
        );
    }
    Ok(())
}

/// Pre-order walk for the first `{ $file: "..." }` marker. The two halves
/// (read vs. mutate) are kept as separate functions because a generic visitor
/// would have to bridge `&Value` and `&mut Value`, which is more complex than
/// the duplication it would remove.
fn first_include_path(value: &serde_yaml::Value) -> Option<String> {
    match value {
        serde_yaml::Value::Mapping(map) => include_target(map)
            .or_else(|| map.iter().find_map(|(_, child)| first_include_path(child))),
        serde_yaml::Value::Sequence(seq) => seq.iter().find_map(first_include_path),
        _ => None,
    }
}

fn replace_first_include(value: &mut serde_yaml::Value, content: &str) -> bool {
    match value {
        serde_yaml::Value::Mapping(map) => {
            if include_target(map).is_some() {
                *value = serde_yaml::Value::String(content.to_string());
                return true;
            }
            map.iter_mut()
                .any(|(_, child)| replace_first_include(child, content))
        }
        serde_yaml::Value::Sequence(seq) => seq
            .iter_mut()
            .any(|child| replace_first_include(child, content)),
        _ => false,
    }
}

/// If `map` is exactly `{ $file: <string> }`, return that string. Otherwise None.
fn include_target(map: &serde_yaml::Mapping) -> Option<String> {
    if map.len() != 1 {
        return None;
    }
    let (k, v) = map.iter().next()?;
    if k.as_str()? != "$file" {
        return None;
    }
    v.as_str().map(std::string::ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn parses_minimal_yaml() {
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(doc.workers.len(), 1);
        assert_eq!(doc.workers[0].name, "w1");
        assert!(doc.defaults.retry_policy.is_none());
    }

    #[test]
    fn merges_defaults_with_overrides() {
        let yaml = r"
defaults:
  response_type: NO_RESULT
  store_failure: true
  retry_policy: { type: EXPONENTIAL, interval: 100, max_interval: 1000, max_retry: 3, basis: 2.0 }
workers:
  - name: w1
    runner: COMMAND
    response_type: DIRECT
  - name: w2
    runner: COMMAND
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let w1 = build_worker_data(&doc.workers[0], &doc.defaults).unwrap();
        let w2 = build_worker_data(&doc.workers[1], &doc.defaults).unwrap();
        assert_eq!(w1.response_type, ResponseType::Direct as i32);
        assert_eq!(w2.response_type, ResponseType::NoResult as i32);
        assert_eq!(w1.retry_policy.unwrap().interval, 100);
        assert_eq!(w2.retry_policy.unwrap().max_retry, 3);
    }

    #[test]
    #[serial]
    fn interpolates_env_with_default() {
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::remove_var("LLMM_TEST_ENV_X");
            std::env::set_var("LLMM_TEST_ENV_Y", "alpha");
        }
        let raw = "k1: ${LLMM_TEST_ENV_X:-fallback}\nk2: ${LLMM_TEST_ENV_Y}\n";
        let out = expand_env(raw).unwrap();
        assert_eq!(out, "k1: fallback\nk2: alpha\n");
        unsafe {
            std::env::remove_var("LLMM_TEST_ENV_Y");
        }
    }

    #[test]
    #[serial]
    fn errors_on_missing_required_env() {
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::remove_var("LLMM_TEST_NEVER_SET");
        }
        let raw = "k: ${LLMM_TEST_NEVER_SET}\n";
        let err = expand_env(raw).unwrap_err();
        assert!(err.to_string().contains("LLMM_TEST_NEVER_SET"));
    }

    #[tokio::test]
    async fn resolves_file_include_relative() {
        let dir = tempfile::tempdir().unwrap();
        let inc = dir.path().join("inc.yaml");
        tokio::fs::write(&inc, "hello: world\n").await.unwrap();

        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
    settings:
      blob:
        $file: inc.yaml
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let mut settings = doc.workers[0]
            .settings
            .clone()
            .expect("settings present in this fixture");
        resolve_includes(&mut settings, dir.path()).await.unwrap();
        let blob = settings.get("blob").unwrap().as_str().unwrap();
        assert_eq!(blob, "hello: world\n");
        // Confirm round-trip into JSON yields a JSON string (not an object).
        let j = serde_json::to_value(&settings).unwrap();
        assert!(j["blob"].is_string());
    }

    #[tokio::test]
    async fn rejects_file_include_outside_base_dir() {
        let outer = tempfile::tempdir().unwrap();
        // Place "secret.yaml" at the top level; the YAML's base_dir is one
        // directory below, so "../secret.yaml" reaches an existing file
        // *outside* base_dir — exactly the traversal we want to reject.
        let secret = outer.path().join("secret.yaml");
        tokio::fs::write(&secret, "leaked: true\n").await.unwrap();
        let inner = outer.path().join("base");
        tokio::fs::create_dir_all(&inner).await.unwrap();

        let mut settings: serde_yaml::Value =
            serde_yaml::from_str("leak:\n  $file: ../secret.yaml\n").unwrap();
        let err = resolve_includes(&mut settings, &inner).await.unwrap_err();
        assert!(
            err.to_string().contains("outside base_dir"),
            "expected base_dir escape rejection, got: {err}"
        );

        // Absolute path to the same secret is also rejected.
        let mut settings: serde_yaml::Value =
            serde_yaml::from_str(&format!("leak:\n  $file: {}\n", secret.display())).unwrap();
        let err = resolve_includes(&mut settings, &inner).await.unwrap_err();
        assert!(
            err.to_string().contains("outside base_dir"),
            "expected absolute-path escape rejection, got: {err}"
        );
    }

    #[test]
    fn errors_on_unknown_response_type() {
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
    response_type: BOGUS
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let err = build_worker_data(&doc.workers[0], &doc.defaults).unwrap_err();
        assert!(err.to_string().contains("invalid response_type"));
    }

    #[test]
    fn retry_policy_uses_spec_values_then_defaults() {
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
    retry_policy: { type: LINEAR, interval: 50 }
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let wd = build_worker_data(&doc.workers[0], &doc.defaults).unwrap();
        let rp = wd.retry_policy.unwrap();
        assert_eq!(rp.r#type, RetryType::Linear as i32);
        assert_eq!(rp.interval, 50);
        // Unset fields fall back to DEFAULT_RETRY_POLICY values.
        assert_eq!(rp.max_interval, DEFAULT_RETRY_POLICY.max_interval);
        assert_eq!(rp.max_retry, DEFAULT_RETRY_POLICY.max_retry);
    }

    #[test]
    fn defaults_used_when_no_retry_policy_anywhere() {
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let wd = build_worker_data(&doc.workers[0], &doc.defaults).unwrap();
        let rp = wd.retry_policy.unwrap();
        assert_eq!(rp.r#type, DEFAULT_RETRY_POLICY.r#type);
        assert_eq!(rp.interval, DEFAULT_RETRY_POLICY.interval);
    }

    #[test]
    fn rejects_unknown_worker_field() {
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
    response_typo: DIRECT
";
        let err = serde_yaml::from_str::<WorkersYaml>(yaml).unwrap_err();
        assert!(err.to_string().contains("response_typo"));
    }

    #[test]
    fn rejects_unknown_root_field() {
        // A typo of `defaults` (e.g. `defualts`) must error rather than
        // silently leave defaults unset; ditto for any other top-level key.
        let yaml = r"
defualts:
  store_failure: false
workers:
  - name: w1
    runner: COMMAND
";
        let err = serde_yaml::from_str::<WorkersYaml>(yaml).unwrap_err();
        assert!(
            err.to_string().contains("defualts"),
            "expected unknown-field error mentioning 'defualts', got: {err}"
        );
    }
}
