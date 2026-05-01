//! YAML-driven worker registration for jobworkerp.
//!
//! Loads N worker definitions from a single YAML file, expands `%{VAR:-default}`
//! environment-variable interpolation and `$file: <path>` includes (constrained
//! to `base_dir`), then upserts each `WorkerData` to the jobworkerp server via
//! [`UseJobworkerpClientHelper::upsert_worker`].
//!
//! See `docs/worker-yaml.md` (in this crate) for the full user-facing
//! reference, including schema, security model around `$file`, and error
//! semantics.

#![allow(clippy::missing_errors_doc, clippy::doc_markdown)]

use super::helper::{DEFAULT_RETRY_POLICY, UseJobworkerpClientHelper};
use super::yaml_common;
use crate::error::ClientError;
use crate::jobworkerp::data::{
    QueueType, ResponseType, RetryPolicy, RetryType, RunnerData, RunnerId, WorkerData, WorkerId,
};
use crate::proto::JobworkerpProto;
use anyhow::{Context as _, Result, anyhow};
use prost_reflect::MessageDescriptor;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
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
/// Input shapes (see [`finalize_register_worker`] for the full table):
///
/// - `settings_json = None`, `worker_data.runner_settings` empty —
///   register with empty `runner_settings`. Mirrors
///   `helper.rs::setup_worker_and_enqueue_with_json` so runners whose
///   schema is optional can be registered with no configuration.
/// - `settings_json = None`, `runner_settings` non-empty — caller has
///   pre-encoded bytes; they are preserved verbatim, only `runner_id`
///   is filled in from the runner lookup.
/// - `settings_json = Some(Value::Null)`, `runner_settings` empty —
///   explicit "send empty" intent; behaves like the first row.
/// - `settings_json = Some(Value::Null)`, `runner_settings` non-empty —
///   **error**. The two inputs disagree (the explicit null asks for
///   empty bytes, the field already holds non-empty bytes). The caller
///   must pick one: clear the field, or drop the `Some(Value::Null)`.
/// - `settings_json = Some(non-null)`, `runner_settings` empty — JSON
///   is encoded against the runner's `runner_settings_proto` schema
///   and the bytes are written to `runner_settings`.
/// - `settings_json = Some(non-null)`, `runner_settings` non-empty —
///   **error** (ambiguous; the caller has supplied both inputs).
///
/// A non-null `settings_json` for a runner that declares no
/// `runner_settings` schema is also rejected: silently dropping the
/// configuration would be a footgun (see `register_workers_from_yaml_str`
/// for the bulk-registration counterpart).
///
/// `worker_data.periodic_interval > 0` paired with
/// `worker_data.response_type = ResponseType::Direct` is rejected
/// up-front (matches the YAML path's pre-validation, mirroring the
/// proto-level rule in `worker.proto`).
///
/// Encoding here uses the lenient (`ignore_unknown_fields=true`) proto
/// path, matching `setup_worker_and_enqueue_with_json` and other runtime
/// helpers. The strict path is reserved for declarative YAML
/// registration where catching a typo at startup is the point;
/// programmatic callers conversely benefit from forward compatibility
/// when the server adds a new `runner_settings` field ahead of the
/// client.
pub async fn register_worker<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    runner_name: &str,
    worker_data: WorkerData,
    settings_json: Option<&serde_json::Value>,
) -> Result<WorkerId>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    let metadata_for_lookup = metadata.clone();
    let worker_data = finalize_register_worker(runner_name, worker_data, settings_json, |name| {
        let metadata = metadata_for_lookup.clone();
        async move { client.find_runner_or_error(cx, metadata, &name).await }
    })
    .await?;
    let worker_name = worker_data.name.clone();
    let worker = client.upsert_worker(cx, metadata, &worker_data).await?;
    worker.id.ok_or_else(|| {
        ClientError::RuntimeError(format!("upsert_worker returned no id for {worker_name}")).into()
    })
}

/// Pure-ish core of [`register_worker`]: validate the input combination,
/// resolve the runner via `runner_lookup`, and return a `WorkerData`
/// ready to be upserted. Split out so unit tests can drive the
/// preencoded/JSON/strictness logic without standing up
/// `UseJobworkerpClientHelper`.
///
/// Resolution table (`runner_settings` = `worker_data.runner_settings`):
///
/// | `settings_json`   | `runner_settings` | Result                                                |
/// |-------------------|-------------------|-------------------------------------------------------|
/// | `None`            | empty             | empty bytes preserved                                 |
/// | `None`            | non-empty         | bytes preserved verbatim (caller pre-encoded)          |
/// | `Some(Null)`      | empty             | empty bytes (matches the documented "send empty" intent) |
/// | `Some(Null)`      | non-empty         | **error** — ambiguous; reject to avoid silent retention or wipe |
/// | `Some(non-null)`  | empty             | encoded via runner schema, lenient unknown fields     |
/// | `Some(non-null)`  | non-empty         | **error** — both inputs supplied, pick exactly one    |
///
/// `Some(Null)` is treated as an explicit "send empty" intent rather
/// than a no-op. Pairing it with non-empty pre-encoded bytes is
/// rejected because the caller's intent is undecidable: the docstring
/// of [`register_worker`] promises empty bytes for `Some(Null)`, but
/// the pre-encoded path would silently keep the existing bytes — so we
/// surface the conflict instead of letting one win silently.
async fn finalize_register_worker<F, Fut>(
    runner_name: &str,
    mut worker_data: WorkerData,
    settings_json: Option<&serde_json::Value>,
    runner_lookup: F,
) -> Result<WorkerData>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<(RunnerId, RunnerData)>>,
{
    let worker_name = worker_data.name.clone();
    // Cross-field check shared with the YAML path. Run before runner
    // lookup so a misconfigured spec fails locally instead of after a
    // server round-trip.
    validate_periodic_response_combo(
        &worker_name,
        worker_data.periodic_interval,
        worker_data.response_type,
    )?;
    let json_kind = match settings_json {
        None => SettingsJsonKind::Absent,
        Some(v) if v.is_null() => SettingsJsonKind::ExplicitNull,
        Some(_) => SettingsJsonKind::Provided,
    };
    let has_preencoded = !worker_data.runner_settings.is_empty();
    match (json_kind, has_preencoded) {
        (SettingsJsonKind::Provided, true) => {
            return Err(anyhow!(
                "register_worker for '{worker_name}' was given both a non-empty `worker_data.runner_settings` \
                 and a non-null `settings_json`; supply exactly one — either pre-encode the bytes yourself \
                 or pass JSON for the helper to encode"
            ));
        }
        (SettingsJsonKind::ExplicitNull, true) => {
            // `Some(Null)` documents intent to send empty bytes. The
            // caller also has non-empty pre-encoded bytes, so the two
            // disagree: silently honouring the bytes would violate the
            // docstring; silently clearing them would discard work the
            // caller already did. Reject and let the caller pick.
            return Err(anyhow!(
                "register_worker for '{worker_name}' was given an explicit-null `settings_json` (intent: \
                 send empty `runner_settings`) but `worker_data.runner_settings` is non-empty; clear the \
                 field on the caller side or drop the `Some(Value::Null)` argument"
            ));
        }
        _ => {}
    }

    let (runner_id, rdata) = runner_lookup(runner_name.to_owned())
        .await
        .with_context(|| {
            format!("resolving runner '{runner_name}' for worker '{worker_name}' failed")
        })?;

    // Pre-encoded path is taken only when settings_json is Absent and the
    // caller already populated runner_settings. ExplicitNull with empty
    // bytes falls through to encode_settings_into, which short-circuits
    // and leaves runner_settings empty (matching the table's third row).
    let take_preencoded = matches!(json_kind, SettingsJsonKind::Absent) && has_preencoded;
    if !take_preencoded {
        encode_settings_into(
            runner_name,
            &worker_name,
            &rdata,
            settings_json,
            &mut worker_data,
            false,
        )?;
    }
    worker_data.runner_id = Some(runner_id);
    Ok(worker_data)
}

/// Tri-state classification of `settings_json` that the resolution
/// table in [`finalize_register_worker`] keys on. Kept private since
/// the outer API accepts `Option<&serde_json::Value>` directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettingsJsonKind {
    /// `None` — caller said nothing about JSON.
    Absent,
    /// `Some(Value::Null)` — caller explicitly asked for empty bytes.
    ExplicitNull,
    /// `Some(non-null Value)` — caller supplied a JSON document to encode.
    Provided,
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
/// **Pre-validation (no upsert RPCs):** env expansion, YAML parsing,
/// duplicate-name detection, `$file:` resolution, enum parsing,
/// runner lookup (cached per runner_name to avoid redundant RPCs),
/// `runner_settings` schema parsing, and proto encoding all run
/// before the first `upsert_worker`. If any step fails, no worker is
/// registered.
///
/// **Registration:** validated specs are upserted sequentially in
/// YAML order. A network failure mid-loop can still leave earlier
/// workers registered; because `UpsertByName` is idempotent, retrying
/// reconciles the state.
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
    let expanded = yaml_common::expand_env(raw_yaml)?;
    let doc: WorkersYaml =
        serde_yaml::from_str(&expanded).with_context(|| "failed to parse workers YAML")?;

    let WorkersYaml { defaults, workers } = doc;
    let prepared =
        prepare_workers(client, cx, metadata.clone(), workers, &defaults, base_dir).await?;

    let mut out = HashMap::with_capacity(prepared.len());
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
        out.insert(prep.worker_name, id);
    }
    Ok(out)
}

/// Validated, network-ready worker payload. `worker_data.runner_id`
/// and `worker_data.runner_settings` are already filled, so the
/// upsert loop only has to call `upsert_worker`.
#[derive(Debug)]
pub(crate) struct PreparedWorker {
    pub(crate) worker_name: String,
    pub(crate) worker_data: WorkerData,
}

/// Validate every spec and resolve runner-side metadata up-front.
///
/// Splitting this from the upsert loop is deliberate: it lets
/// duplicate-name errors, runner typos, schema mismatches, and proto
/// encoding failures surface before any worker is written to
/// jobworkerp, restoring the "all-or-nothing pre-validation" guarantee
/// the documentation promises.
pub(crate) async fn prepare_workers<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    workers: Vec<WorkerSpec>,
    defaults: &WorkerDefaults,
    base_dir: &Path,
) -> Result<Vec<PreparedWorker>>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    prepare_workers_with_lookup(workers, defaults, base_dir, |name| {
        let metadata = metadata.clone();
        async move { client.find_runner_or_error(cx, metadata, &name).await }
    })
    .await
}

/// Pure-ish core of [`prepare_workers`]: takes a closure for runner
/// resolution so unit tests can inject a fixture without standing up
/// `UseJobworkerpClientHelper`. Production callers go through
/// [`prepare_workers`], which wires the closure to `find_runner_or_error`.
///
/// Lookup caching, duplicate-name detection, and the
/// "settings-without-schema is an error" rule all live here so tests
/// observe the same control flow as production.
async fn prepare_workers_with_lookup<F, Fut>(
    workers: Vec<WorkerSpec>,
    defaults: &WorkerDefaults,
    base_dir: &Path,
    mut runner_lookup: F,
) -> Result<Vec<PreparedWorker>>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<(RunnerId, RunnerData)>>,
{
    let mut seen: HashSet<String> = HashSet::with_capacity(workers.len());
    for spec in &workers {
        if !seen.insert(spec.name.clone()) {
            return Err(anyhow!(
                "duplicate worker name '{}' in YAML — names must be unique within one document",
                spec.name
            ));
        }
    }

    // Cache resolved runners *and* the parsed runner_settings descriptor.
    // Many YAMLs register several workers against the same runner, and
    // re-parsing the proto schema for each of them is wasted work — the
    // cached `MessageDescriptor` is internally Arc-backed so cloning it
    // is cheap. The descriptor is wrapped in `Option` so we still cache
    // "this runner has no schema" (avoiding repeated parses that all
    // produce `None`); the outer `Option` distinguishes "not parsed yet"
    // from "parsed and absent".
    let mut runner_cache: HashMap<String, RunnerCacheEntry> = HashMap::new();
    let mut out = Vec::with_capacity(workers.len());
    for mut spec in workers {
        let worker_name = spec.name.clone();
        let runner_name = std::mem::take(&mut spec.runner);
        let settings_yaml = std::mem::take(&mut spec.settings);
        let settings_json = match settings_yaml {
            Some(mut yaml) => {
                yaml_common::resolve_includes(&mut yaml, base_dir).await?;
                Some(serde_json::to_value(&yaml).with_context(|| {
                    format!("converting runner_settings of worker '{worker_name}' to JSON failed")
                })?)
            }
            None => None,
        };
        let mut worker_data = build_worker_data(&spec, defaults)
            .with_context(|| format!("validating worker '{worker_name}' failed"))?;

        if !runner_cache.contains_key(&runner_name) {
            let (rid, rdata) = runner_lookup(runner_name.clone()).await.with_context(|| {
                format!("resolving runner '{runner_name}' for worker '{worker_name}' failed")
            })?;
            runner_cache.insert(
                runner_name.clone(),
                RunnerCacheEntry {
                    runner_id: rid,
                    runner_data: rdata,
                    settings_descriptor: None,
                },
            );
        }
        // `runner_cache` was just populated for this name; lookup is
        // infallible. Holding a &mut here is fine: every call below is
        // synchronous, so we never await while borrowed.
        let entry = runner_cache
            .get_mut(&runner_name)
            .expect("entry just inserted");

        // Only resolve the descriptor when this spec actually provides
        // settings. Skipping the parse for settings-less workers
        // preserves the contract that a malformed `runner_settings_proto`
        // is irrelevant unless the YAML asks to encode against it (also
        // covered by `settings_omitted_skips_schema_parse_even_when_runner_proto_is_broken`).
        let provides_settings = matches!(&settings_json, Some(json) if !json.is_null());
        if provides_settings {
            let descriptor = ensure_settings_descriptor(entry, &runner_name, &worker_name)?;
            encode_settings_with_descriptor(
                &runner_name,
                &worker_name,
                descriptor,
                settings_json.as_ref(),
                &mut worker_data,
                true,
            )?;
        } else {
            // Empty settings: the worker_data field starts empty
            // already, so nothing to write — but stay explicit.
            worker_data.runner_settings = Vec::new();
        }
        worker_data.runner_id = Some(entry.runner_id);

        out.push(PreparedWorker {
            worker_name,
            worker_data,
        });
    }
    Ok(out)
}

/// Cached per-runner data shared across all workers in one batch:
/// the runner ID/data resolved via the lookup closure, plus a lazily
/// parsed `runner_settings_proto` descriptor so workers that share a
/// runner avoid re-parsing the same proto source.
struct RunnerCacheEntry {
    runner_id: RunnerId,
    runner_data: RunnerData,
    /// `None` until we attempt the parse; `Some(None)` records "the
    /// runner declares no `runner_settings_proto`"; `Some(Some(desc))`
    /// holds a parsed descriptor (cheap to clone — internally Arc-backed).
    settings_descriptor: Option<Option<MessageDescriptor>>,
}

/// Lazy-parse the runner's `runner_settings_proto` once per cache
/// entry and memoise the result. Returns a clone of the descriptor
/// because `prost_reflect::MessageDescriptor` is Arc-backed and clone
/// is cheap; this lets the caller hand the descriptor off to the
/// proto encoder without holding the cache borrow across the call.
fn ensure_settings_descriptor(
    entry: &mut RunnerCacheEntry,
    runner_name: &str,
    worker_name: &str,
) -> Result<Option<MessageDescriptor>> {
    if entry.settings_descriptor.is_none() {
        let parsed = JobworkerpProto::parse_runner_settings_schema_descriptor(&entry.runner_data)
            .with_context(|| {
                format!(
                    "parsing runner_settings_proto for worker '{worker_name}' (runner '{runner_name}') failed"
                )
            })?;
        entry.settings_descriptor = Some(parsed);
    }
    Ok(entry
        .settings_descriptor
        .as_ref()
        .expect("just initialised")
        .clone())
}

/// Encode `settings_json` against the runner's schema and stamp the
/// resulting bytes onto `worker_data.runner_settings`.
///
/// Pulled out so [`finalize_register_worker`] (single-worker path) and
/// [`prepare_workers_with_lookup`] (bulk path) share the same rule for
/// rejecting settings on schema-less runners.
///
/// `strict_unknown_fields` controls whether unknown keys in
/// `settings_json` cause an error (`true`, used by declarative YAML
/// registration) or are silently dropped (`false`, used by programmatic
/// helpers — see [`register_worker`]). Strictness is opt-in because
/// startup-time YAML benefits from catching typos but the programmatic
/// API would otherwise lose forward compatibility whenever the server
/// adds a new field ahead of the client.
fn encode_settings_into(
    runner_name: &str,
    worker_name: &str,
    rdata: &RunnerData,
    settings_json: Option<&serde_json::Value>,
    worker_data: &mut WorkerData,
    strict_unknown_fields: bool,
) -> Result<()> {
    // Short-circuit when the YAML omits `settings:` (or writes
    // `settings: ~`). Skipping schema parsing here matches the documented
    // behaviour ("the runner schema is not consulted") and avoids
    // refusing to register a schema-less worker when the runner happens
    // to ship a malformed `runner_settings_proto`.
    let provides_settings = matches!(settings_json, Some(json) if !json.is_null());
    if !provides_settings {
        worker_data.runner_settings = Vec::new();
        return Ok(());
    }
    let descriptor = JobworkerpProto::parse_runner_settings_schema_descriptor(rdata)?;
    encode_settings_with_descriptor(
        runner_name,
        worker_name,
        descriptor,
        settings_json,
        worker_data,
        strict_unknown_fields,
    )
}

/// Variant of [`encode_settings_into`] that takes a pre-parsed
/// descriptor. Used by [`prepare_workers_with_lookup`] so workers
/// sharing a runner do not re-parse `runner_settings_proto` for every
/// spec; the single-worker path goes through [`encode_settings_into`]
/// which parses on demand.
fn encode_settings_with_descriptor(
    runner_name: &str,
    worker_name: &str,
    descriptor: Option<MessageDescriptor>,
    settings_json: Option<&serde_json::Value>,
    worker_data: &mut WorkerData,
    strict_unknown_fields: bool,
) -> Result<()> {
    // Same omitted/null short-circuit as encode_settings_into so callers
    // that already have a cached descriptor don't pay the encode path
    // when the YAML had no `settings:` block.
    let provides_settings = matches!(settings_json, Some(json) if !json.is_null());
    if !provides_settings {
        worker_data.runner_settings = Vec::new();
        return Ok(());
    }
    let json = settings_json.expect("checked above");
    let settings_bytes = match descriptor {
        Some(desc) => {
            let ignore_unknown = !strict_unknown_fields;
            JobworkerpProto::json_value_to_message(desc, json, true, ignore_unknown).with_context(
                || {
                    format!(
                        "encoding runner_settings for worker '{worker_name}' (runner '{runner_name}') as proto failed"
                    )
                },
            )?
        }
        None => {
            return Err(anyhow!(
                "worker '{worker_name}' has settings but runner '{runner_name}' declares no \
                 runner_settings schema; remove the `settings:` block or pick a runner that \
                 accepts settings"
            ));
        }
    };
    worker_data.runner_settings = settings_bytes;
    Ok(())
}

/// Build a `WorkerData` from a YAML spec.
///
/// Every fallback (used when neither the per-worker spec nor the
/// `defaults:` block sets a key) is the proto3 zero value for the
/// corresponding `WorkerData` field. Specifically: `response_type =
/// NO_RESULT`, `queue_type = NORMAL`, `retry_policy = None` (no retry
/// policy), all four bool flags = `false`, `periodic_interval = 0`,
/// `channel = None`. The CLI `worker create` command and
/// `helper.rs::build_worker_data_default` both apply opinionated
/// non-proto defaults (`response_type = DIRECT`, `retry_policy =
/// DEFAULT_RETRY_POLICY`); the YAML loader deliberately does not, so a
/// declarative YAML behaves identically to a hand-built `WorkerData`
/// passed straight to `upsert_worker`. Callers who want the helper /
/// CLI defaults must set them explicitly under `defaults:`.
fn build_worker_data(spec: &WorkerSpec, defaults: &WorkerDefaults) -> Result<WorkerData> {
    let response_type = pick(&spec.response_type, &defaults.response_type)
        .map(parse_response_type)
        .transpose()?
        .unwrap_or(ResponseType::NoResult as i32);
    let queue_type = pick(&spec.queue_type, &defaults.queue_type)
        .map(parse_queue_type)
        .transpose()?
        .unwrap_or(QueueType::Normal as i32);
    let retry_policy_spec = spec
        .retry_policy
        .as_ref()
        .or(defaults.retry_policy.as_ref());
    let retry_policy = retry_policy_spec.map(parse_retry_policy).transpose()?;

    let periodic_interval = spec
        .periodic_interval
        .or(defaults.periodic_interval)
        .unwrap_or(0);

    validate_periodic_response_combo(&spec.name, periodic_interval, response_type)?;

    Ok(WorkerData {
        name: spec.name.clone(),
        description: spec.description.clone().unwrap_or_default(),
        runner_id: None,
        runner_settings: Vec::new(),
        retry_policy,
        periodic_interval,
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
            .unwrap_or(false),
        use_static: spec.use_static.or(defaults.use_static).unwrap_or(false),
        broadcast_results: spec
            .broadcast_results
            .or(defaults.broadcast_results)
            .unwrap_or(false),
    })
}

fn pick<'a>(per_worker: &'a Option<String>, default: &'a Option<String>) -> Option<&'a str> {
    per_worker.as_deref().or(default.as_deref())
}

/// Reject the `periodic_interval > 0` × `response_type = DIRECT`
/// combination at validation time.
///
/// Rationale: `worker.proto` documents that "if periodic is enabled,
/// direct_response cannot be used" because the scheduler does not
/// return direct results for periodic jobs. Both the YAML batch path
/// (via `build_worker_data`) and the programmatic path (via
/// `finalize_register_worker`) call this so the constraint surfaces at
/// startup rather than after a network round-trip to `upsert_worker`.
fn validate_periodic_response_combo(
    worker_name: &str,
    periodic_interval: u32,
    response_type: i32,
) -> Result<()> {
    if periodic_interval > 0 && response_type == ResponseType::Direct as i32 {
        return Err(anyhow!(
            "worker '{worker_name}' has periodic_interval={periodic_interval} but \
             response_type=DIRECT; periodic workers must use response_type: NO_RESULT \
             (jobworkerp does not return direct results for scheduled jobs)"
        ));
    }
    Ok(())
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

/// Translate a `RetryPolicySpec` into a `RetryPolicy`, filling in any
/// omitted leaf field from `helper::DEFAULT_RETRY_POLICY` rather than
/// from the proto3 zero. The whole-policy fallback (`retry_policy:`
/// missing entirely) goes through `build_worker_data` and stays at the
/// proto3 zero (`None`); these per-leaf defaults only apply once the
/// caller has opted in to *some* retry policy by writing the block.
/// Defaulting individual leaves to `0` would yield a policy that
/// retries instantly and forever, which is rarely the intent behind
/// `retry_policy: { type: EXPONENTIAL }`.
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
    fn implicit_bool_defaults_match_cli() {
        // The four bool flags must default to `false` (the proto3 zero
        // and the CLI `worker create` default). This regression test
        // guards against silently re-introducing the older
        // `helper.rs::build_worker_data_default` behaviour where
        // `store_failure` / `broadcast_results` defaulted to `true`
        // and made YAML-registered workers diverge from CLI-registered
        // ones.
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let wd = build_worker_data(&doc.workers[0], &doc.defaults).unwrap();
        assert!(!wd.store_success, "store_success default must be false");
        assert!(!wd.store_failure, "store_failure default must be false");
        assert!(!wd.use_static, "use_static default must be false");
        assert!(
            !wd.broadcast_results,
            "broadcast_results default must be false"
        );
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
        let raw = "k1: %{LLMM_TEST_ENV_X:-fallback}\nk2: %{LLMM_TEST_ENV_Y}\n";
        let out = yaml_common::expand_env(raw).unwrap();
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
        let raw = "k: %{LLMM_TEST_NEVER_SET}\n";
        let err = yaml_common::expand_env(raw).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("LLMM_TEST_NEVER_SET"));
        // Guards against a brace-escaping regression in the format string
        // that renders the suggested-fallback hint.
        assert!(
            msg.contains("%{LLMM_TEST_NEVER_SET:-"),
            "error message must self-render %{{NAME:-...}} correctly, got: {msg}"
        );
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
        yaml_common::resolve_includes(&mut settings, dir.path())
            .await
            .unwrap();
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
        let err = yaml_common::resolve_includes(&mut settings, &inner)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("outside base_dir"),
            "expected base_dir escape rejection, got: {err}"
        );

        // Absolute path to the same secret is also rejected.
        let mut settings: serde_yaml::Value =
            serde_yaml::from_str(&format!("leak:\n  $file: {}\n", secret.display())).unwrap();
        let err = yaml_common::resolve_includes(&mut settings, &inner)
            .await
            .unwrap_err();
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
    fn no_retry_policy_yields_unset_field() {
        // When neither the spec nor the defaults block declares a
        // retry_policy, the field stays None — i.e. the proto3 zero
        // value. Earlier revisions silently injected
        // `helper::DEFAULT_RETRY_POLICY`, which made YAML-registered
        // workers behave differently from a hand-built `WorkerData`.
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let wd = build_worker_data(&doc.workers[0], &doc.defaults).unwrap();
        assert!(
            wd.retry_policy.is_none(),
            "missing retry_policy must stay unset, got: {:?}",
            wd.retry_policy
        );
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

    // --- prepare_workers_with_lookup tests ----------------------------------
    //
    // These cover the four review findings the recent refactor addresses:
    //   #1 settings-without-schema must be rejected (not silently dropped)
    //   #2 runner lookup / proto encoding must run before any upsert RPC
    //   #3 duplicate worker names within one YAML must error
    //   #4 runner lookup must be cached per runner_name
    //
    // We inject a closure for runner resolution so the tests don't need to
    // implement the full `UseJobworkerpClientHelper` trait.

    use std::cell::RefCell;
    use std::rc::Rc;

    fn schema_less_runner(name: &str, id: i64) -> (RunnerId, RunnerData) {
        (
            RunnerId { value: id },
            RunnerData {
                name: name.to_string(),
                description: String::new(),
                runner_type: 0,
                runner_settings_proto: String::new(),
                definition: String::new(),
                method_proto_map: None,
            },
        )
    }

    /// Build a runner with a minimal `runner_settings_proto` containing a
    /// single message `TestSettings { string host = 1; uint32 port = 2; }`.
    /// Used to exercise the proto-encoding path including unknown-field
    /// rejection.
    fn proto_runner(name: &str, id: i64) -> (RunnerId, RunnerData) {
        let proto = r#"syntax = "proto3";
message TestSettings {
  string host = 1;
  uint32 port = 2;
}
"#;
        (
            RunnerId { value: id },
            RunnerData {
                name: name.to_string(),
                description: String::new(),
                runner_type: 0,
                runner_settings_proto: proto.to_string(),
                definition: String::new(),
                method_proto_map: None,
            },
        )
    }

    fn parse_yaml(raw: &str) -> WorkersYaml {
        serde_yaml::from_str(raw).expect("test fixture YAML must parse")
    }

    fn empty_base_dir() -> PathBuf {
        // No `$file:` directives in these fixtures, so resolve_includes
        // never touches base_dir. Use the current directory to avoid the
        // need for tempdir setup.
        std::env::current_dir().expect("cwd must be readable")
    }

    #[tokio::test]
    async fn errors_when_settings_present_but_no_schema() {
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
    settings:
      something: 1
";
        let WorkersYaml { defaults, workers } = parse_yaml(yaml);
        let runners = vec![schema_less_runner("COMMAND", 1)];
        let err = prepare_workers_with_lookup(workers, &defaults, &empty_base_dir(), |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found: {name}"))
            }
        })
        .await
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("declares no runner_settings schema"),
            "error must explain the schema-less rejection, got: {msg}"
        );
        assert!(
            msg.contains("'w1'"),
            "error must name the offending worker, got: {msg}"
        );
    }

    #[tokio::test]
    async fn all_or_nothing_before_upsert_fails_on_unknown_runner() {
        // Two specs; the second references an unknown runner. Both lookups
        // run during pre-validation, but the batch must fail before any
        // worker is upserted — that is the all-or-nothing guarantee.
        let yaml = r"
workers:
  - name: ok
    runner: KNOWN
  - name: bad
    runner: MISSING
";
        let WorkersYaml { defaults, workers } = parse_yaml(yaml);
        let lookup_calls: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
        let calls_for_closure = lookup_calls.clone();
        let result = prepare_workers_with_lookup(workers, &defaults, &empty_base_dir(), |name| {
            let calls = calls_for_closure.clone();
            async move {
                calls.borrow_mut().push(name.clone());
                if name == "KNOWN" {
                    Ok(schema_less_runner("KNOWN", 7))
                } else {
                    Err(anyhow!("runner not found: {name}"))
                }
            }
        })
        .await;
        assert!(result.is_err(), "MISSING runner must abort the batch");
        // The lookup is invoked for both specs (the first succeeds, the
        // second fails), but no PreparedWorker is produced — so callers
        // never reach the upsert stage with a partial batch.
        let calls = lookup_calls.borrow();
        assert!(calls.contains(&"KNOWN".to_string()));
        assert!(calls.contains(&"MISSING".to_string()));
    }

    #[tokio::test]
    async fn errors_on_duplicate_worker_name() {
        // Same `name` twice: must fail before any runner lookup, so the
        // closure is never called at all (proves the duplicate check
        // runs first).
        let yaml = r"
workers:
  - name: dup
    runner: A
  - name: dup
    runner: B
";
        let WorkersYaml { defaults, workers } = parse_yaml(yaml);
        let lookup_calls: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));
        let calls = lookup_calls.clone();
        let err = prepare_workers_with_lookup(workers, &defaults, &empty_base_dir(), |_name| {
            let calls = calls.clone();
            async move {
                *calls.borrow_mut() += 1;
                Ok(schema_less_runner("dummy", 0))
            }
        })
        .await
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("duplicate worker name"), "got: {msg}");
        assert!(msg.contains("'dup'"), "got: {msg}");
        assert_eq!(
            *lookup_calls.borrow(),
            0,
            "duplicate-name check must short-circuit before any lookup"
        );
    }

    #[tokio::test]
    async fn runner_lookup_is_cached_per_name() {
        // Three specs share runner "COMMAND" plus one references "HTTP".
        // The closure must be invoked exactly twice (once per distinct
        // runner name), proving the per-name cache short-circuits later
        // hits.
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
  - name: w2
    runner: COMMAND
  - name: w3
    runner: HTTP
  - name: w4
    runner: COMMAND
";
        let WorkersYaml { defaults, workers } = parse_yaml(yaml);
        let lookup_calls: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
        let calls_for_closure = lookup_calls.clone();
        let prepared = prepare_workers_with_lookup(workers, &defaults, &empty_base_dir(), |name| {
            let calls = calls_for_closure.clone();
            async move {
                calls.borrow_mut().push(name.clone());
                Ok(schema_less_runner(&name, name.len() as i64))
            }
        })
        .await
        .expect("all four specs must succeed");
        assert_eq!(prepared.len(), 4);
        let calls = lookup_calls.borrow();
        assert_eq!(
            calls.len(),
            2,
            "expected two lookups (COMMAND, HTTP), got: {calls:?}"
        );
        assert!(calls.contains(&"COMMAND".to_string()));
        assert!(calls.contains(&"HTTP".to_string()));
    }

    #[tokio::test]
    async fn settings_null_or_omitted_yields_empty_bytes() {
        // Both `settings:` omitted and `settings: ~` must succeed against
        // a schema-less runner and result in empty `runner_settings`
        // bytes — preserving the existing parity with
        // `setup_worker_and_enqueue_with_json`.
        let yaml = r"
workers:
  - name: omitted
    runner: COMMAND
  - name: explicit_null
    runner: COMMAND
    settings: ~
";
        let WorkersYaml { defaults, workers } = parse_yaml(yaml);
        let runners = vec![schema_less_runner("COMMAND", 1)];
        let prepared = prepare_workers_with_lookup(workers, &defaults, &empty_base_dir(), |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .expect("both specs must validate");
        assert_eq!(prepared.len(), 2);
        for prep in &prepared {
            assert!(
                prep.worker_data.runner_settings.is_empty(),
                "{} should have empty runner_settings",
                prep.worker_name
            );
            assert_eq!(prep.worker_data.runner_id, Some(RunnerId { value: 1 }));
        }
    }

    #[test]
    fn rejects_periodic_with_direct_response() {
        // proto-level rule: a periodic worker cannot return a direct
        // response. The startup validator must catch this rather than
        // letting upsert_worker fail mid-batch.
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
    periodic_interval: 1000
    response_type: DIRECT
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let err = build_worker_data(&doc.workers[0], &doc.defaults).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("periodic_interval=1000"),
            "error must include the offending interval, got: {msg}"
        );
        assert!(
            msg.contains("DIRECT"),
            "error must reference the conflicting response_type, got: {msg}"
        );
    }

    #[test]
    fn allows_periodic_with_no_result() {
        // Sanity: the same periodic_interval combined with NO_RESULT
        // must validate.
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
    periodic_interval: 1000
    response_type: NO_RESULT
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let wd = build_worker_data(&doc.workers[0], &doc.defaults).unwrap();
        assert_eq!(wd.periodic_interval, 1000);
        assert_eq!(wd.response_type, ResponseType::NoResult as i32);
    }

    #[test]
    fn implicit_response_type_is_no_result() {
        // The fallback for response_type is the proto3 zero
        // (`NO_RESULT`), not the helper-style `DIRECT`. A worker with
        // `periodic_interval` and no explicit response_type therefore
        // validates cleanly — the proto rule that periodic workers
        // must not return direct results is satisfied automatically.
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
    periodic_interval: 1000
";
        let doc: WorkersYaml = serde_yaml::from_str(yaml).unwrap();
        let wd = build_worker_data(&doc.workers[0], &doc.defaults).unwrap();
        assert_eq!(wd.response_type, ResponseType::NoResult as i32);
        assert_eq!(wd.periodic_interval, 1000);
    }

    #[test]
    #[serial]
    fn rejects_env_value_with_linebreak() {
        // Multi-line env values would silently re-shape the document
        // (turning a single scalar into multiple YAML lines / a new
        // mapping). expand_env must reject them up-front.
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("LLMM_TEST_INJECT_NEWLINE", "innocent\nfoo: gotcha");
        }
        let raw = "value: %{LLMM_TEST_INJECT_NEWLINE}\n";
        let err = yaml_common::expand_env(raw).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("line break"),
            "error must explain the rejected character class, got: {msg}"
        );
        assert!(
            msg.contains("LLMM_TEST_INJECT_NEWLINE"),
            "error must name the offending variable, got: {msg}"
        );
        unsafe {
            std::env::remove_var("LLMM_TEST_INJECT_NEWLINE");
        }
    }

    #[test]
    #[serial]
    fn allows_url_with_internal_special_chars() {
        // Real-world DSNs and URLs contain `&`, `?`, `=`, `:`, `@`, `/`
        // mid-string. These must pass through — the previous overly
        // strict guard refused them and made env-templated DSNs
        // unusable.
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var(
                "LLMM_TEST_DSN",
                "postgres://user:pw@host:5432/db?sslmode=require&app=foo",
            );
        }
        let raw = "url: %{LLMM_TEST_DSN}\n";
        let out = yaml_common::expand_env(raw).unwrap();
        assert_eq!(
            out,
            "url: postgres://user:pw@host:5432/db?sslmode=require&app=foo\n"
        );
        unsafe {
            std::env::remove_var("LLMM_TEST_DSN");
        }
    }

    #[test]
    #[serial]
    fn allows_value_with_internal_anchor_like_ampersand() {
        // `&anchor` at the start would set up a YAML anchor; the same
        // character mid-string is just a literal `&`. Allow the latter.
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("LLMM_TEST_AMP_MID", "a&b");
        }
        let raw = "v: %{LLMM_TEST_AMP_MID}\n";
        let out = yaml_common::expand_env(raw).unwrap();
        assert_eq!(out, "v: a&b\n");
        unsafe {
            std::env::remove_var("LLMM_TEST_AMP_MID");
        }
    }

    #[test]
    #[serial]
    fn allows_env_value_that_is_boolean_literal() {
        // Bool literals must flow through unchanged so that env
        // interpolation works for bool fields like store_failure /
        // use_static / broadcast_results — `store_failure: %{X:-true}`
        // is a documented use case.
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("LLMM_TEST_INJECT_BOOL", "true");
        }
        let raw = "store_failure: %{LLMM_TEST_INJECT_BOOL}\n";
        let out = yaml_common::expand_env(raw).expect("bool literal must pass through");
        assert_eq!(out, "store_failure: true\n");
        unsafe {
            std::env::remove_var("LLMM_TEST_INJECT_BOOL");
        }
    }

    #[test]
    #[serial]
    fn allows_env_value_that_is_null_literal() {
        // null/~ literals likewise pass through — the previous blanket
        // rejection broke env-driven defaulting for nullable fields.
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("LLMM_TEST_INJECT_NULL", "null");
        }
        let raw = "value: %{LLMM_TEST_INJECT_NULL}\n";
        let out = yaml_common::expand_env(raw).expect("null literal must pass through");
        assert_eq!(out, "value: null\n");
        unsafe {
            std::env::remove_var("LLMM_TEST_INJECT_NULL");
        }
    }

    #[test]
    #[serial]
    fn bool_env_default_drives_worker_data_field() {
        // End-to-end: a `%{X:-true}` placeholder in a bool field must
        // survive expansion AND the WorkerSpec → WorkerData translation,
        // landing as the expected bool.
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::remove_var("LLMM_TEST_STORE_FAILURE");
        }
        let yaml = r"
workers:
  - name: w1
    runner: COMMAND
    store_failure: %{LLMM_TEST_STORE_FAILURE:-false}
    use_static: %{LLMM_TEST_USE_STATIC:-true}
";
        let expanded =
            yaml_common::expand_env(yaml).expect("env expansion must succeed for bool defaults");
        let WorkersYaml { defaults, workers } = serde_yaml::from_str::<WorkersYaml>(&expanded)
            .expect("YAML must parse after expansion");
        let wd = build_worker_data(&workers[0], &defaults).expect("WorkerData must build");
        assert!(
            !wd.store_failure,
            "store_failure must reflect the :-false default"
        );
        assert!(wd.use_static, "use_static must reflect the :-true default");
    }

    #[test]
    #[serial]
    fn allows_numeric_env_value() {
        // Plain numeric values must still flow through — the documented
        // common case is `port: %{MEMORY_GRPC_PORT:-9010}` resolving to a
        // YAML integer.
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("LLMM_TEST_NUMERIC", "9010");
        }
        let raw = "port: %{LLMM_TEST_NUMERIC}\n";
        let out = yaml_common::expand_env(raw).unwrap();
        assert_eq!(out, "port: 9010\n");
        unsafe {
            std::env::remove_var("LLMM_TEST_NUMERIC");
        }
    }

    #[test]
    #[serial]
    fn allows_env_value_without_structural_chars() {
        // Plain alphanumeric values (the overwhelming common case) must
        // still pass through untouched.
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("LLMM_TEST_SAFE_VALUE", "abc-123_XYZ.local");
        }
        let raw = "host: %{LLMM_TEST_SAFE_VALUE}\n";
        let out = yaml_common::expand_env(raw).unwrap();
        assert_eq!(out, "host: abc-123_XYZ.local\n");
        unsafe {
            std::env::remove_var("LLMM_TEST_SAFE_VALUE");
        }
    }

    #[tokio::test]
    async fn settings_omitted_skips_schema_parse_even_when_runner_proto_is_broken() {
        // The documentation guarantees that omitting `settings:` makes
        // the runner schema irrelevant. Validate that promise by giving
        // the closure a runner whose `runner_settings_proto` is
        // syntactically broken: registration must still succeed because
        // the schema parse is never reached.
        let yaml = r"
workers:
  - name: w1
    runner: BROKEN_PROTO
";
        let WorkersYaml { defaults, workers } = parse_yaml(yaml);
        let broken = (
            RunnerId { value: 99 },
            RunnerData {
                name: "BROKEN_PROTO".to_string(),
                description: String::new(),
                runner_type: 0,
                // Deliberately invalid proto source — would fail
                // `ProtobufDescriptor::new` if parsed.
                runner_settings_proto: "this is not valid proto3 syntax".to_string(),
                definition: String::new(),
                method_proto_map: None,
            },
        );
        let prepared = prepare_workers_with_lookup(workers, &defaults, &empty_base_dir(), |name| {
            let broken = broken.clone();
            async move {
                if name == "BROKEN_PROTO" {
                    Ok(broken)
                } else {
                    Err(anyhow!("runner not found"))
                }
            }
        })
        .await
        .expect("settings-less registration must not parse the schema");
        assert_eq!(prepared.len(), 1);
        assert!(prepared[0].worker_data.runner_settings.is_empty());
    }

    #[tokio::test]
    async fn rejects_unknown_settings_field() {
        // Pre-validation must fail fast on a settings typo. The runner
        // declares `host` / `port`, but the YAML uses `prot` (typo).
        // With ignore_unknown_fields=false this is rejected at startup,
        // catching the misconfiguration before any worker is upserted.
        let yaml = r"
workers:
  - name: w1
    runner: TESTRUNNER
    settings:
      host: localhost
      prot: 1234
";
        let WorkersYaml { defaults, workers } = parse_yaml(yaml);
        let runners = vec![proto_runner("TESTRUNNER", 1)];
        let err = prepare_workers_with_lookup(workers, &defaults, &empty_base_dir(), |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("encoding runner_settings"),
            "error must surface the encode failure context, got: {msg}"
        );
        // The underlying prost-reflect error mentions the offending field
        // name; verify it bubbles up so users can spot the typo.
        assert!(
            msg.to_lowercase().contains("prot") || msg.to_lowercase().contains("unknown"),
            "error should pinpoint the unknown field, got: {msg}"
        );
    }

    #[tokio::test]
    async fn accepts_settings_matching_schema() {
        // Sanity check that a well-formed settings block still encodes
        // successfully under the strict ignore_unknown_fields=false rule.
        let yaml = r"
workers:
  - name: w1
    runner: TESTRUNNER
    settings:
      host: localhost
      port: 1234
";
        let WorkersYaml { defaults, workers } = parse_yaml(yaml);
        let runners = vec![proto_runner("TESTRUNNER", 1)];
        let prepared = prepare_workers_with_lookup(workers, &defaults, &empty_base_dir(), |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .expect("well-formed settings must validate");
        assert_eq!(prepared.len(), 1);
        assert!(
            !prepared[0].worker_data.runner_settings.is_empty(),
            "encoded bytes must be non-empty"
        );
    }

    fn empty_worker_data(name: &str) -> WorkerData {
        WorkerData {
            name: name.to_string(),
            description: String::new(),
            runner_id: None,
            runner_settings: Vec::new(),
            retry_policy: None,
            periodic_interval: 0,
            channel: None,
            queue_type: 0,
            response_type: 0,
            store_success: false,
            store_failure: true,
            use_static: false,
            broadcast_results: true,
        }
    }

    #[tokio::test]
    async fn finalize_register_worker_preserves_preencoded_runner_settings() {
        // Regression: when the caller has already encoded runner_settings
        // bytes, finalize_register_worker must keep those bytes verbatim
        // even when settings_json is None. Previously the helper would
        // silently overwrite them with Vec::new().
        let mut wd = empty_worker_data("w1");
        let original = vec![1u8, 2, 3, 4];
        wd.runner_settings = original.clone();
        let runners = vec![proto_runner("TESTRUNNER", 1)];
        let out = finalize_register_worker("TESTRUNNER", wd, None, |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .expect("preencoded bytes path must succeed");
        assert_eq!(
            out.runner_settings, original,
            "runner_settings bytes must be preserved verbatim"
        );
        assert_eq!(out.runner_id, Some(RunnerId { value: 1 }));
    }

    #[tokio::test]
    async fn finalize_register_worker_rejects_both_inputs() {
        // Mixing both inputs (preencoded bytes AND settings_json) is the
        // ambiguous case we now reject up-front, rather than silently
        // letting one win.
        let mut wd = empty_worker_data("w1");
        wd.runner_settings = vec![9, 9, 9];
        let runners = vec![proto_runner("TESTRUNNER", 1)];
        let json = serde_json::json!({"host": "h", "port": 1});
        let err = finalize_register_worker("TESTRUNNER", wd, Some(&json), |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("supply exactly one"),
            "error must explain the ambiguous-input rejection, got: {msg}"
        );
        assert!(
            msg.contains("'w1'"),
            "error must name the worker, got: {msg}"
        );
    }

    #[tokio::test]
    async fn finalize_register_worker_uses_lenient_encoding_for_unknown_fields() {
        // The programmatic register_worker path encodes settings_json
        // with ignore_unknown_fields=true so that a server-side schema
        // expansion (new field added on the server before the client
        // catches up) does not break callers. The bulk YAML path keeps
        // strict=true (see rejects_unknown_settings_field).
        let wd = empty_worker_data("w1");
        let runners = vec![proto_runner("TESTRUNNER", 1)];
        // `extra_field` is not in the runner schema. Strict encoding
        // would error; lenient encoding must drop it silently.
        let json = serde_json::json!({"host": "h", "port": 1, "extra_field": "ignored"});
        let out = finalize_register_worker("TESTRUNNER", wd, Some(&json), |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .expect("lenient path must drop unknown fields silently");
        assert!(!out.runner_settings.is_empty());
        assert_eq!(out.runner_id, Some(RunnerId { value: 1 }));
    }

    #[tokio::test]
    async fn finalize_register_worker_encodes_when_no_preencoded_bytes() {
        // Sanity: with empty runner_settings and Some(settings_json),
        // the helper encodes the JSON via the runner schema as before.
        let wd = empty_worker_data("w1");
        let runners = vec![proto_runner("TESTRUNNER", 1)];
        let json = serde_json::json!({"host": "h", "port": 1});
        let out = finalize_register_worker("TESTRUNNER", wd, Some(&json), |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .expect("normal JSON encoding must succeed");
        assert!(!out.runner_settings.is_empty());
        assert_eq!(out.runner_id, Some(RunnerId { value: 1 }));
    }

    #[tokio::test]
    async fn finalize_register_worker_settings_none_zeroes_runner_settings_when_empty() {
        // When neither preencoded bytes nor settings_json are provided,
        // runner_settings stays empty and only runner_id is filled in
        // (matches the documented "settings: omitted" semantics).
        let wd = empty_worker_data("w1");
        let runners = vec![schema_less_runner("COMMAND", 7)];
        let out = finalize_register_worker("COMMAND", wd, None, |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .expect("settings-less registration must succeed");
        assert!(out.runner_settings.is_empty());
        assert_eq!(out.runner_id, Some(RunnerId { value: 7 }));
    }

    #[tokio::test]
    async fn finalize_register_worker_explicit_null_with_empty_bytes_keeps_empty() {
        // Some(Value::Null) is the documented "send empty bytes" intent.
        // With an empty pre-encoded field this must succeed and produce
        // empty runner_settings — matching the docstring's first row.
        let wd = empty_worker_data("w1");
        let runners = vec![proto_runner("TESTRUNNER", 1)];
        let null_json = serde_json::Value::Null;
        let out = finalize_register_worker("TESTRUNNER", wd, Some(&null_json), |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .expect("Some(Null) with empty bytes must succeed");
        assert!(
            out.runner_settings.is_empty(),
            "Some(Null) intent is to send empty bytes"
        );
        assert_eq!(out.runner_id, Some(RunnerId { value: 1 }));
    }

    #[tokio::test]
    async fn finalize_register_worker_explicit_null_with_preencoded_bytes_errors() {
        // Some(Value::Null) asks for empty bytes but the caller also
        // supplied non-empty pre-encoded bytes. The two intents disagree;
        // surface the conflict instead of silently retaining the bytes
        // (which would contradict the docstring) or silently wiping
        // them (which would discard the caller's work).
        let mut wd = empty_worker_data("w1");
        wd.runner_settings = vec![1, 2, 3];
        let runners = vec![proto_runner("TESTRUNNER", 1)];
        let null_json = serde_json::Value::Null;
        let err = finalize_register_worker("TESTRUNNER", wd, Some(&null_json), |name| {
            let runners = runners.clone();
            async move {
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("explicit-null") || msg.contains("explicit null"),
            "error must explain the explicit-null + bytes conflict, got: {msg}"
        );
        assert!(
            msg.contains("'w1'"),
            "error must name the worker, got: {msg}"
        );
    }

    #[test]
    fn ensure_settings_descriptor_memoises_after_first_parse() {
        // The cached descriptor must be `Some(Some(_))` after the first
        // call so subsequent workers sharing the runner do not re-parse
        // `runner_settings_proto`. A second call must observe the cache
        // and return the same descriptor without flipping the cache
        // state back to `None`.
        let (_id, rdata) = proto_runner("TESTRUNNER", 1);
        let mut entry = RunnerCacheEntry {
            runner_id: RunnerId { value: 1 },
            runner_data: rdata,
            settings_descriptor: None,
        };
        assert!(
            entry.settings_descriptor.is_none(),
            "cache must start unparsed"
        );
        let first = ensure_settings_descriptor(&mut entry, "TESTRUNNER", "w1")
            .expect("parse must succeed for valid proto");
        assert!(
            first.is_some(),
            "TESTRUNNER declares a runner_settings_proto so the descriptor must be Some"
        );
        assert!(
            entry.settings_descriptor.is_some(),
            "cache must record the parsed descriptor"
        );

        let second = ensure_settings_descriptor(&mut entry, "TESTRUNNER", "w2")
            .expect("cached path must succeed");
        // Both calls return the same underlying descriptor (Arc-shared
        // inside prost_reflect). Comparing by full_name is enough since
        // there is only one message in the test proto.
        assert_eq!(
            first.as_ref().unwrap().full_name(),
            second.as_ref().unwrap().full_name(),
            "second call must return the cached descriptor"
        );
    }

    #[test]
    fn ensure_settings_descriptor_memoises_absent_schema() {
        // Schema-less runners must also be cached so repeated
        // settings-less workers don't reparse the empty proto string.
        let (_id, rdata) = schema_less_runner("COMMAND", 7);
        let mut entry = RunnerCacheEntry {
            runner_id: RunnerId { value: 7 },
            runner_data: rdata,
            settings_descriptor: None,
        };
        let first =
            ensure_settings_descriptor(&mut entry, "COMMAND", "w1").expect("absent proto is OK");
        assert!(first.is_none(), "schema-less runner must yield None");
        assert!(
            matches!(entry.settings_descriptor, Some(None)),
            "cache must record `parsed and absent` so the second call short-circuits"
        );
        let second =
            ensure_settings_descriptor(&mut entry, "COMMAND", "w2").expect("cached lookup");
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn shared_runner_does_not_reparse_descriptor() {
        // End-to-end: three workers share TESTRUNNER, all with non-empty
        // settings. The bulk path must succeed (proving every worker
        // reaches the encoder) and the runner cache must end with
        // exactly one entry whose descriptor is parsed once.
        let yaml = r"
workers:
  - name: w1
    runner: TESTRUNNER
    settings: { host: a, port: 1 }
  - name: w2
    runner: TESTRUNNER
    settings: { host: b, port: 2 }
  - name: w3
    runner: TESTRUNNER
    settings: { host: c, port: 3 }
";
        let WorkersYaml { defaults, workers } = parse_yaml(yaml);
        let runners = vec![proto_runner("TESTRUNNER", 1)];
        let lookup_calls: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));
        let calls = lookup_calls.clone();
        let prepared = prepare_workers_with_lookup(workers, &defaults, &empty_base_dir(), |name| {
            let runners = runners.clone();
            let calls = calls.clone();
            async move {
                *calls.borrow_mut() += 1;
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .expect("all three specs must validate");
        assert_eq!(prepared.len(), 3);
        // RPC-level cache: a single lookup for the shared runner. The
        // descriptor cache is exercised within the same loop and is
        // unit-tested separately by `ensure_settings_descriptor_*`.
        assert_eq!(
            *lookup_calls.borrow(),
            1,
            "shared runner must be resolved exactly once"
        );
        for prep in prepared {
            assert!(
                !prep.worker_data.runner_settings.is_empty(),
                "{} must have encoded settings",
                prep.worker_name
            );
        }
    }

    #[tokio::test]
    async fn finalize_register_worker_rejects_periodic_with_direct_response() {
        // The proto-level constraint must be enforced on the
        // programmatic path too, not just the YAML path. Otherwise the
        // failure only surfaces server-side after upsert_worker is
        // already in flight.
        let mut wd = empty_worker_data("w1");
        wd.periodic_interval = 1000;
        wd.response_type = ResponseType::Direct as i32;
        let runners = vec![schema_less_runner("COMMAND", 1)];
        // The lookup closure is wired up but must NOT be invoked: the
        // periodic/response check has to short-circuit before any RPC.
        let lookup_calls: Rc<RefCell<usize>> = Rc::new(RefCell::new(0));
        let calls = lookup_calls.clone();
        let err = finalize_register_worker("COMMAND", wd, None, |name| {
            let calls = calls.clone();
            let runners = runners.clone();
            async move {
                *calls.borrow_mut() += 1;
                runners
                    .iter()
                    .find(|(_, d)| d.name == name)
                    .cloned()
                    .ok_or_else(|| anyhow!("runner not found"))
            }
        })
        .await
        .unwrap_err();
        let msg = format!("{err:?}");
        assert!(
            msg.contains("periodic_interval=1000") && msg.contains("DIRECT"),
            "error must explain the proto rule, got: {msg}"
        );
        assert_eq!(
            *lookup_calls.borrow(),
            0,
            "periodic/response check must run before any runner lookup"
        );
    }
}
