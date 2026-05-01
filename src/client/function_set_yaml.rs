//! YAML-driven function_set registration for jobworkerp.
//!
//! Loads N function_set definitions from a single YAML file, expands
//! `${VAR:-default}` interpolation via [`super::yaml_common::expand_env`],
//! resolves runner/worker names to IDs, and upserts each `FunctionSetData`
//! to the jobworkerp server via
//! [`UseJobworkerpClientHelper::upsert_function_set_by_name`].
//!
//! `$file:` includes from `worker_yaml` are intentionally not supported here:
//! `FunctionSetData` has no free-form YAML field where embedding an external
//! document is meaningful (settings/payloads do not exist on this proto).
//!
//! See `docs/function-set-yaml.md` for the full user-facing reference.

#![allow(clippy::missing_errors_doc, clippy::doc_markdown)]

use super::helper::UseJobworkerpClientHelper;
use super::yaml_common;
use crate::jobworkerp::data::{RunnerId, WorkerId};
use crate::jobworkerp::function::data::{
    FunctionId, FunctionSetData, FunctionSetId, FunctionUsing, function_id,
};
use anyhow::{Context as _, Result, anyhow};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FunctionSetsYaml {
    #[serde(default)]
    pub(crate) defaults: FunctionSetDefaults,
    pub(crate) function_sets: Vec<FunctionSetSpec>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FunctionSetDefaults {
    pub(crate) category: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FunctionSetSpec {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) description: Option<String>,
    pub(crate) category: Option<i32>,
    pub(crate) targets: Vec<TargetSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetSpec {
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) name: Option<String>,
    pub(crate) id: Option<i64>,
    pub(crate) using: Option<String>,
}

/// Read a function_set YAML file and register every set it contains.
/// Returns a `name -> FunctionSetId` map.
pub async fn register_function_sets_from_yaml<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    yaml_path: &Path,
) -> Result<HashMap<String, FunctionSetId>>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    let raw = tokio::fs::read_to_string(yaml_path)
        .await
        .with_context(|| {
            format!(
                "failed to read function_sets YAML at {}",
                yaml_path.display()
            )
        })?;
    register_function_sets_from_yaml_str(client, cx, metadata, &raw).await
}

/// Variant of [`register_function_sets_from_yaml`] that takes raw YAML text.
///
/// Two phases:
/// 1. **Pre-validation:** env expansion, YAML parsing, duplicate-name
///    detection, target-shape checks (`name` xor `id`, `type` enum,
///    non-empty `targets`), and runner/worker name resolution. No
///    server-mutating RPC has fired yet; failure here registers zero sets.
/// 2. **Registration:** validated specs are upserted sequentially in YAML
///    order via [`UseJobworkerpClientHelper::upsert_function_set_by_name`],
///    which is idempotent — a mid-batch network failure leaves earlier
///    sets registered, but retrying with the same YAML reconciles state.
pub async fn register_function_sets_from_yaml_str<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    raw_yaml: &str,
) -> Result<HashMap<String, FunctionSetId>>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    let expanded = yaml_common::expand_env(raw_yaml)?;
    let doc: FunctionSetsYaml =
        serde_yaml::from_str(&expanded).with_context(|| "failed to parse function_sets YAML")?;
    let FunctionSetsYaml {
        defaults,
        function_sets,
    } = doc;

    let prepared =
        prepare_function_sets(client, cx, metadata.clone(), function_sets, &defaults).await?;

    let mut out = HashMap::with_capacity(prepared.len());
    for prep in prepared {
        let id = client
            .upsert_function_set_by_name(cx, metadata.clone(), prep.data)
            .await
            .with_context(|| format!("registering function_set '{}' failed", prep.name))?;
        out.insert(prep.name, id);
    }
    Ok(out)
}

/// Validated, network-ready function_set payload.
#[derive(Debug, Clone)]
struct PreparedFunctionSet {
    name: String,
    data: FunctionSetData,
}

async fn prepare_function_sets<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    sets: Vec<FunctionSetSpec>,
    defaults: &FunctionSetDefaults,
) -> Result<Vec<PreparedFunctionSet>>
where
    C: UseJobworkerpClientHelper + Send + Sync,
{
    // Closures over the live client form the test seam: unit tests pass
    // hand-crafted lookups to `prepare_function_sets_with_lookups` without
    // standing up a full gRPC client.
    let runner_metadata = metadata.clone();
    let runner_lookup = |name: String| {
        let metadata = runner_metadata.clone();
        async move {
            let runner = client.find_runner_by_name(cx, metadata, &name).await?;
            runner
                .and_then(|r| r.id)
                .ok_or_else(|| anyhow!("runner '{name}' is not registered on jobworkerp"))
        }
    };
    let worker_metadata = metadata;
    let worker_lookup = |name: String| {
        let metadata = worker_metadata.clone();
        async move {
            let found = client.find_worker_by_name(cx, metadata, &name).await?;
            found
                .map(|(wid, _)| wid)
                .ok_or_else(|| anyhow!("worker '{name}' is not registered on jobworkerp"))
        }
    };
    prepare_function_sets_with_lookups(sets, defaults, runner_lookup, worker_lookup).await
}

async fn prepare_function_sets_with_lookups<RFut, WFut, RF, WF>(
    sets: Vec<FunctionSetSpec>,
    defaults: &FunctionSetDefaults,
    mut runner_lookup: RF,
    mut worker_lookup: WF,
) -> Result<Vec<PreparedFunctionSet>>
where
    RF: FnMut(String) -> RFut,
    WF: FnMut(String) -> WFut,
    RFut: std::future::Future<Output = Result<RunnerId>>,
    WFut: std::future::Future<Output = Result<WorkerId>>,
{
    let mut seen: HashSet<String> = HashSet::with_capacity(sets.len());
    for spec in &sets {
        if !seen.insert(spec.name.clone()) {
            return Err(anyhow!(
                "duplicate function_set name '{}' in YAML — names must be unique within one document",
                spec.name
            ));
        }
    }

    // Per-batch cache: a typo'd name should round-trip the server once,
    // not once per occurrence. Targets routinely repeat across sets.
    let mut runner_cache: HashMap<String, RunnerId> = HashMap::new();
    let mut worker_cache: HashMap<String, WorkerId> = HashMap::new();

    let mut out = Vec::with_capacity(sets.len());
    for spec in sets {
        if spec.targets.is_empty() {
            return Err(anyhow!(
                "function_set '{}' has no targets; an empty targets list is not allowed",
                spec.name
            ));
        }
        let category = spec.category.or(defaults.category).unwrap_or(0);

        let mut targets = Vec::with_capacity(spec.targets.len());
        for (idx, t) in spec.targets.into_iter().enumerate() {
            let kind = parse_target_kind(&t.kind).with_context(|| {
                format!(
                    "validating function_set '{}' target #{idx} failed",
                    spec.name
                )
            })?;
            let locator = Locator::from_spec(t.name, t.id).with_context(|| {
                format!(
                    "validating function_set '{}' target #{idx} failed",
                    spec.name
                )
            })?;
            let id = match (kind, locator) {
                (TargetKind::Runner, Locator::ByName(name)) => function_id::Id::RunnerId(
                    resolve_cached(&mut runner_cache, name, &mut runner_lookup)
                        .await
                        .with_context(|| {
                            format!(
                                "resolving RUNNER for function_set '{}' target #{idx} failed",
                                spec.name
                            )
                        })?,
                ),
                (TargetKind::Worker, Locator::ByName(name)) => function_id::Id::WorkerId(
                    resolve_cached(&mut worker_cache, name, &mut worker_lookup)
                        .await
                        .with_context(|| {
                            format!(
                                "resolving WORKER for function_set '{}' target #{idx} failed",
                                spec.name
                            )
                        })?,
                ),
                (TargetKind::Runner, Locator::ById(value)) => {
                    function_id::Id::RunnerId(RunnerId { value })
                }
                (TargetKind::Worker, Locator::ById(value)) => {
                    function_id::Id::WorkerId(WorkerId { value })
                }
            };
            targets.push(FunctionUsing {
                function_id: Some(FunctionId { id: Some(id) }),
                using: t.using,
            });
        }

        out.push(PreparedFunctionSet {
            name: spec.name.clone(),
            data: FunctionSetData {
                name: spec.name,
                description: spec.description.unwrap_or_default(),
                category,
                targets,
            },
        });
    }
    Ok(out)
}

/// Memoise `lookup(name)` in `cache`. Only one lookup per unique name fires,
/// even when the name appears in multiple targets across multiple sets.
async fn resolve_cached<V, Fut, F>(
    cache: &mut HashMap<String, V>,
    name: String,
    lookup: &mut F,
) -> Result<V>
where
    V: Copy,
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<V>>,
{
    if let Some(v) = cache.get(&name) {
        return Ok(*v);
    }
    let v = lookup(name.clone()).await?;
    cache.insert(name, v);
    Ok(v)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetKind {
    Runner,
    Worker,
}

enum Locator {
    ByName(String),
    ById(i64),
}

impl Locator {
    fn from_spec(name: Option<String>, id: Option<i64>) -> Result<Self> {
        match (name, id) {
            (Some(_), Some(_)) => Err(anyhow!(
                "target sets both `name` and `id`; pick exactly one"
            )),
            (Some(name), None) => Ok(Self::ByName(name)),
            (None, Some(id)) => Ok(Self::ById(id)),
            (None, None) => Err(anyhow!("target has neither `name` nor `id`")),
        }
    }
}

fn parse_target_kind(s: &str) -> Result<TargetKind> {
    match s {
        "RUNNER" => Ok(TargetKind::Runner),
        "WORKER" => Ok(TargetKind::Worker),
        other => Err(anyhow!(
            "invalid target type '{other}' (expected RUNNER or WORKER)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test seam: closures are `impl FnMut(String) -> impl Future<...>` already
    // satisfied by an `async move {}` block, so call sites embed the closure
    // inline. The macro keeps the call sites readable without the boxing
    // gymnastics that a `fn`-typed return signature would force.
    macro_rules! ok_lookup {
        ($id_ty:ident, $id:expr) => {
            move |_name: String| async move { Ok($id_ty { value: $id }) }
        };
    }
    macro_rules! err_lookup {
        ($what:literal) => {
            |name: String| async move { Err(anyhow!(concat!($what, " '{}' not found"), name)) }
        };
    }

    #[test]
    fn parses_minimal_yaml() {
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: RUNNER
        name: COMMAND
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(doc.function_sets.len(), 1);
        assert_eq!(doc.function_sets[0].name, "fs1");
        assert!(doc.defaults.category.is_none());
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let yaml = r"
function_sets: []
extra: oops
";
        let err = serde_yaml::from_str::<FunctionSetsYaml>(yaml).unwrap_err();
        assert!(format!("{err}").contains("extra"));
    }

    #[test]
    fn rejects_unknown_target_field() {
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: RUNNER
        name: COMMAND
        bogus: 1
";
        let err = serde_yaml::from_str::<FunctionSetsYaml>(yaml).unwrap_err();
        assert!(format!("{err}").contains("bogus"));
    }

    #[tokio::test]
    async fn rejects_empty_targets() {
        let yaml = r"
function_sets:
  - name: fs1
    targets: []
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let err = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            ok_lookup!(RunnerId, 1),
            ok_lookup!(WorkerId, 2),
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("empty targets"));
    }

    #[tokio::test]
    async fn rejects_duplicate_names() {
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: RUNNER
        name: A
  - name: fs1
    targets:
      - type: RUNNER
        name: B
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let err = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            ok_lookup!(RunnerId, 1),
            ok_lookup!(WorkerId, 2),
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("duplicate"));
    }

    #[tokio::test]
    async fn rejects_target_with_both_name_and_id() {
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: RUNNER
        name: COMMAND
        id: 5
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let err = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            ok_lookup!(RunnerId, 1),
            ok_lookup!(WorkerId, 2),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("both"));
    }

    #[tokio::test]
    async fn rejects_target_with_neither_name_nor_id() {
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: RUNNER
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let err = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            ok_lookup!(RunnerId, 1),
            ok_lookup!(WorkerId, 2),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("neither"));
    }

    #[tokio::test]
    async fn rejects_invalid_target_type() {
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: BOGUS
        name: x
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let err = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            ok_lookup!(RunnerId, 1),
            ok_lookup!(WorkerId, 2),
        )
        .await
        .unwrap_err();
        // anyhow's `{}` only shows the outermost context; `{:#}` walks the chain.
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("invalid target type"),
            "expected error to mention invalid target type, got: {rendered}"
        );
    }

    #[tokio::test]
    async fn defaults_category_applies_when_unset() {
        let yaml = r"
defaults:
  category: 7
function_sets:
  - name: fs1
    targets:
      - type: RUNNER
        name: A
  - name: fs2
    category: 3
    targets:
      - type: RUNNER
        name: A
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let prepared = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            ok_lookup!(RunnerId, 1),
            ok_lookup!(WorkerId, 2),
        )
        .await
        .unwrap();
        assert_eq!(prepared.len(), 2);
        assert_eq!(prepared[0].data.category, 7);
        assert_eq!(prepared[1].data.category, 3);
    }

    #[tokio::test]
    async fn id_path_skips_lookup() {
        // Always-erroring closures prove the `id:` branch never invokes them.
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: RUNNER
        id: 42
      - type: WORKER
        id: 100
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let prepared = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            err_lookup!("runner"),
            err_lookup!("worker"),
        )
        .await
        .unwrap();
        assert_eq!(prepared.len(), 1);
        let targets = &prepared[0].data.targets;
        assert_eq!(targets.len(), 2);
        match &targets[0].function_id.as_ref().unwrap().id {
            Some(function_id::Id::RunnerId(rid)) => assert_eq!(rid.value, 42),
            other => panic!("expected RunnerId(42), got {other:?}"),
        }
        match &targets[1].function_id.as_ref().unwrap().id {
            Some(function_id::Id::WorkerId(wid)) => assert_eq!(wid.value, 100),
            other => panic!("expected WorkerId(100), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn name_path_resolves_via_lookup() {
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: RUNNER
        name: COMMAND
      - type: WORKER
        name: my-worker
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let prepared = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            ok_lookup!(RunnerId, 11),
            ok_lookup!(WorkerId, 22),
        )
        .await
        .unwrap();
        let targets = &prepared[0].data.targets;
        match &targets[0].function_id.as_ref().unwrap().id {
            Some(function_id::Id::RunnerId(rid)) => assert_eq!(rid.value, 11),
            other => panic!("expected RunnerId(11), got {other:?}"),
        }
        match &targets[1].function_id.as_ref().unwrap().id {
            Some(function_id::Id::WorkerId(wid)) => assert_eq!(wid.value, 22),
            other => panic!("expected WorkerId(22), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn worker_target_with_using_is_allowed() {
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: WORKER
        name: w1
        using: run
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let prepared = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            ok_lookup!(RunnerId, 0),
            ok_lookup!(WorkerId, 99),
        )
        .await
        .unwrap();
        assert_eq!(prepared[0].data.targets[0].using.as_deref(), Some("run"));
    }

    #[tokio::test]
    async fn lookup_is_cached_within_batch() {
        let yaml = r"
function_sets:
  - name: fs1
    targets:
      - type: RUNNER
        name: SHARED
  - name: fs2
    targets:
      - type: RUNNER
        name: SHARED
";
        let doc: FunctionSetsYaml = serde_yaml::from_str(yaml).unwrap();
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_for_closure = calls.clone();
        let runner_lookup = move |_name: String| {
            calls_for_closure.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move { Ok(RunnerId { value: 1 }) }
        };
        let prepared = prepare_function_sets_with_lookups(
            doc.function_sets,
            &doc.defaults,
            runner_lookup,
            err_lookup!("worker"),
        )
        .await
        .unwrap();
        assert_eq!(prepared.len(), 2);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
