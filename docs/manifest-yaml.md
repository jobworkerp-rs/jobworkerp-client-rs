# `manifest_yaml` — Combined Worker + FunctionSet YAML Registration

A single YAML document containing both `workers:` and `function_sets:` sections, registered in one call. Layered on top of `worker_yaml` and `function_set_yaml` (see [`worker-yaml.md`](./worker-yaml.md) and [`function-set-yaml.md`](./function-set-yaml.md)) — the manifest loader does not redefine field semantics, only the outer container.

Use this when a single deployment unit ships both workers and the function_set that groups them. The standalone loaders remain available for cases where the lifecycle differs.

## API

```rust
pub async fn register_manifest_from_yaml<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    yaml_path: &Path,
) -> Result<ManifestResult>
where C: UseJobworkerpClientHelper + Send + Sync;

pub async fn register_manifest_from_yaml_str<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    raw_yaml: &str,
    base_dir: &Path,
) -> Result<ManifestResult>;

pub struct ManifestResult {
    pub workers: HashMap<String, WorkerId>,
    pub function_sets: HashMap<String, FunctionSetId>,
}
```

`UseJobworkerpClientHelper::register_manifest_from_yaml` is also exposed as a trait facade (bound by `Self: Sized`, same constraint as the other registration helpers).

## YAML format

```yaml
workers:                            # optional — section may be omitted entirely
  defaults:                         # forwarded to worker_yaml WorkerDefaults
    channel: high-priority
  entries:                          # WorkerSpec list (same as worker-yaml.md)
    - name: rss-summarizer
      runner: LLM_CHAT

function_sets:                      # optional — section may be omitted entirely
  defaults:
    category: 0
  entries:                          # FunctionSetSpec list (same as function-set-yaml.md)
    - name: rss-pipeline
      targets:
        - type: WORKER
          name: rss-summarizer      # references the worker declared above
```

`#[serde(deny_unknown_fields)]` is applied at the manifest top level and at every section level, so typos like `worker:` (missing `s`) or `entires:` are surfaced as parse errors.

### Schema differences from the standalone loaders

The manifest does **not** accept the YAML shapes that `register_workers_from_yaml` and `register_function_sets_from_yaml` consume directly. Specifically:

| Standalone (`worker-yaml.md`) | Manifest |
|----|----|
| `defaults:` and `workers:` at top level | nested under `workers:` as `defaults:` and `entries:` |
| top-level `workers:` is a sequence | manifest's `workers:` is an object with `entries:` (sequence) |

Same for `function_sets:`. This is intentional — it keeps the two top-level keys as homogeneous objects (each owns its own `defaults`), and the `deny_unknown_fields` on the manifest catches anyone who mistakenly feeds a standalone YAML to the manifest loader.

If you need to keep using a standalone YAML, call `register_workers_from_yaml` / `register_function_sets_from_yaml` directly.

### Sections are independent and optional

Either section may be omitted entirely; the corresponding `ManifestResult` field is then an empty map. A manifest containing only `workers:` (no `function_sets:`) behaves like `register_workers_from_yaml` operating on the section's `entries`. Likewise for `function_sets:` only. An empty `entries:` list within a present section is also a no-op for that section — but note the validation is split across two layers: field-name typos in `defaults:` are still rejected at parse time (both `WorkerDefaults` and `FunctionSetDefaults` are `#[serde(deny_unknown_fields)]`), while value-level checks that only run when defaults are applied to a spec — enum string parsing for `response_type` / `queue_type` / `retry_policy.type`, and the periodic-vs-DIRECT cross-field rule — are skipped when `entries:` is empty and surface only once you add at least one entry.

The two sections do not share `defaults:` — each maintains its own (`workers.defaults` is a `WorkerDefaults`; `function_sets.defaults` is a `FunctionSetDefaults`). There is no global `defaults:` block at the manifest top level.

## Registration order and pre-validation semantics

When both sections are present, **workers are registered before function_sets**. This matters because a function_set may reference a worker by `type: WORKER, name: <worker-name>`; the function_set's pre-validation phase resolves that name against the live server, so the worker must already exist.

Consequences:

- A worker spec error (typo, missing runner, schema mismatch) fails before any RPC fires — no workers and no function_sets are registered.
- A function_set spec error fails **after** workers have been upserted — workers are already registered, function_sets are not.
- This is asymmetric compared to the all-or-nothing guarantee that each standalone loader makes for its own scope. Both upsert paths are idempotent (`upsert_by_name` for workers, `find_by_name` + `Update`/`Create` for function_sets), so retrying the same manifest after fixing the function_set side simply reconciles state.

If you need true all-or-nothing semantics across both sections, do dry validation with the standalone loaders first (or split your YAML into two files registered separately and check the failure output before continuing).

## Environment-variable interpolation

Same machinery as `worker_yaml`, applied to the entire manifest before YAML parsing — both sections benefit equally. See [`worker-yaml.md`](./worker-yaml.md#environment-variable-interpolation) for syntax and rationale.

## `$file:` includes

Forwarded to the workers section only (it is the only section whose specs accept free-form embedded YAML, via `runner_settings`). The function_sets section has no field where a `$file:` directive would land, matching the behaviour of `function_set_yaml` standalone — see [`function-set-yaml.md`](./function-set-yaml.md) for the rationale.

`base_dir` for `$file:` resolution is the parent directory of `yaml_path` (file entry point) or the explicit argument (string entry point).

## Failure-mode summary

| Failure case | Behaviour |
|----|----|
| Env variable unset and no default | Startup fails (pre-validation) |
| Env value violates structure-safety guard | Startup fails |
| Unknown top-level / section field (typo) | Startup fails (serde parse) |
| Section content shape mismatch (e.g. legacy flat `workers:` sequence) | Startup fails (serde parse) |
| Worker spec invalid (per `worker-yaml.md`) | Startup fails — nothing registered |
| Worker registration network error | Earlier workers may be registered; function_sets section not started |
| Function_set spec invalid (per `function-set-yaml.md`) | Workers already registered; function_sets not |
| Function_set registration network error | Earlier function_sets may be registered |

All failures except concurrent network errors are recoverable by fixing the YAML and re-running the manifest. Idempotent retries reconcile state.

## Related files

- Implementation: `src/client/manifest_yaml.rs`
- Section loaders: `src/client/worker_yaml.rs`, `src/client/function_set_yaml.rs`
- Trait facade: `src/client/helper.rs::UseJobworkerpClientHelper::register_manifest_from_yaml`
- Section docs: [`worker-yaml.md`](./worker-yaml.md), [`function-set-yaml.md`](./function-set-yaml.md)
