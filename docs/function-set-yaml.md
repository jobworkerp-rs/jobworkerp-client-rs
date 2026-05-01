# `function_set_yaml` — YAML-driven jobworkerp Function Set Registration

The `jobworkerp_client::client::function_set_yaml` module provides a generic library facility for loading jobworkerp `FunctionSet` definitions from YAML files and upserting them to a jobworkerp server through `UseJobworkerpClientHelper::upsert_function_set_by_name`.

By consolidating each `FunctionSetData` field into a single YAML file, function set registration logic that used to be hard-coded in Rust (or hand-built JSON for the CLI `function_set create`) can be expressed declaratively.

This reuses the env-var interpolation machinery from [`worker-yaml.md`](./worker-yaml.md) via the shared `client::yaml_common` module — both loaders therefore have the same security guarantees around `${VAR}` substitution.

`$file:` includes are intentionally **not** supported here. Worker YAMLs use them to embed an external document inside `runner_settings`; `FunctionSetData` has no equivalent free-form YAML field, so the directive would have nowhere meaningful to land.

## Differences from `worker_yaml`

| | `worker_yaml` | `function_set_yaml` |
|---|---|---|
| Server-side `UpsertByName` RPC | yes (`WorkerService::UpsertByName`) | **no** — emulated client-side as `FindByName` → `Update`/`Create` |
| Dynamic proto fields | `runner_settings` (per-runner schema) | none (fixed schema) |
| External references resolved | runner name → `RunnerId` | runner name → `RunnerId`, worker name → `WorkerId` |
| Idempotent retry | proto-level | client-level (with one race fallback; see below) |

The client-side upsert means a concurrent delete of the same set between our `FindByName` and `Update` is detected and falls back to `Create` exactly once. Any other error from `Update` is surfaced as-is; idempotent retries with the same YAML still reconcile state.

## API

```rust
// Load from a file (the production entry point).
pub async fn register_function_sets_from_yaml<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    yaml_path: &Path,
) -> Result<HashMap<String, FunctionSetId>>
where C: UseJobworkerpClientHelper + Send + Sync;

// Load from a string (for tests and dynamic generation).
pub async fn register_function_sets_from_yaml_str<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    raw_yaml: &str,
) -> Result<HashMap<String, FunctionSetId>>;
```

For programmatic single-set registration, use the trait methods directly: `client.upsert_function_set_by_name(cx, metadata, data).await`.

The `UseJobworkerpClientHelper` trait also exposes `register_function_sets_from_yaml`, `find_function_set_by_name`, and `upsert_function_set_by_name`, so callers can invoke them alongside the worker-yaml helpers; the file-path entry point is bound by `where Self: Sized` (same constraint as `register_worker`), so callers that need dynamic dispatch should call the free function `function_set_yaml::register_function_sets_from_yaml(&*client, ...)` directly.

The return value is a `HashMap` of `function_set_name -> FunctionSetId`. Because jobworkerp keys function sets by name, an existing set with the same name has its data overwritten via the client-side upsert (idempotent across retries).

## YAML format

```yaml
defaults:               # optional — fallback values for every set
  category: 0

function_sets:          # required — N function set definitions per file
  - name: rss-pipeline
    description: "RSS fetch + summarisation pipeline"
    category: 10
    targets:
      - type: RUNNER
        name: HTTP_REQUEST          # resolve runner by name
      - type: RUNNER
        name: MCP_CLIENT
        using: fetch_html           # MCP sub-tool selector
      - type: WORKER
        name: rss-summarizer        # resolve worker by name
      - type: RUNNER
        id: 5                       # specify ID directly (bypasses lookup)
```

`#[serde(deny_unknown_fields)]` is applied at every level (top-level, `defaults`, each function set, each target), so unknown fields cause a parse error. Typos like `defualts:` or `targest:` are surfaced rather than silently ignored.

### `defaults` block

Values applied to every function set that does not override them. The whole block is optional. Only one key is supported on purpose:

| Key | Type | Example |
|----|----|----|
| `category` | `i32` | `0` |

`description` is intentionally not part of `defaults` — descriptions are set-specific by nature, and a shared default would mostly invite accidental copy-paste descriptions across unrelated sets. If your sets do share a description prefix, write it out per set or generate the YAML.

#### Implicit fallback values

When neither the per-set spec nor the `defaults` block sets `category`, the field falls back to the proto3 zero value (`0`). This matches the `worker_yaml` philosophy of staying at the proto baseline so a declarative YAML behaves identically to a hand-built `FunctionSetData` passed straight to the helper.

### Per-function_set entry

| Key | Required | Type | Description |
|----|----|----|----|
| `name` | yes | `string` | Function set name on jobworkerp |
| `description` | no | `string` | Free-form description; defaults to `""` |
| `category` | no | `i32` | Per-set category override |
| `targets` | yes | sequence (1+) | Functions belonging to this set; **empty list is rejected** |

Empty `targets` is rejected at validation time: a function set with nothing in it cannot be invoked, so the only ways to get an empty set are typos or generation bugs we want to surface early.

### `targets` entry

Each target maps to one `FunctionUsing` proto entry.

```yaml
- type: RUNNER | WORKER     # required, uppercase enum
  name: <string>            # exactly one of `name` or `id` required
  id:   <int64>             # bypasses name lookup
  using: <string>           # optional, see below
```

| Key | Required | Notes |
|---|---|---|
| `type` | yes | `RUNNER` or `WORKER`. Any other value is rejected. |
| `name` | one-of | Resolved via `find_runner_by_name` / `find_worker_by_name` during pre-validation. Lookups are cached per name within a batch so the same runner referenced from N targets only round-trips the server once. |
| `id` | one-of | Directly inserted as `RunnerId` / `WorkerId`. Bypasses name resolution — useful for hermetic tests, but reduces portability across environments because IDs are not stable. Specifying both `name` and `id`, or neither, is a startup error. |
| `using` | no | MCP/Plugin sub-tool selector for runners; corresponds to a method name in the runner's proto service for normal runners. **Allowed for both `RUNNER` and `WORKER` targets** — see "Why `using` is allowed for WORKER" below. |

### Why `using` is allowed for WORKER targets

The proto comment on `FunctionUsing` documents `using` as "ignored for workers", but in practice the field is not type-gated by the server: workers internally wrap a runner, and downstream call sites (e.g. `command::job::find_runner_descriptors_by_worker` in this crate) honour `using` for worker-backed jobs to select which method on the underlying runner to execute. Refusing `using` on `WORKER` targets at the YAML layer would impose a stricter contract than the proto and lose expressive power that real call sites already rely on.

The trade-off: a `using` value on a `WORKER` target may be a no-op against some servers. That is the proto's promise, and we surface no extra warning — operators who do not need the field should simply omit it.

## Pre-validation rules

Run **before any server-mutating RPC** (the same all-or-nothing guarantee `worker_yaml` makes):

| Failure case | Behaviour |
|----|----|
| Env variable unset and no default | Startup fails (pre-validation) |
| Env value violates the structure-safety guard | Startup fails (see `worker-yaml.md` for the precise rules — same module, same ban list) |
| Top-level / set / target field typo | Startup fails (serde parse stage) |
| Duplicate `name` within one YAML | Startup fails |
| `targets` empty | Startup fails |
| `type` not `RUNNER` / `WORKER` | Startup fails |
| Both `name` and `id` set on a target | Startup fails |
| Neither `name` nor `id` set on a target | Startup fails |
| `name` for a runner/worker not registered on jobworkerp | Startup fails (network: name lookup) |
| Network failure mid-upsert (registration phase) | Earlier sets may already be registered; idempotent retry reconciles |
| Update target deleted between FindByName and Update | One automatic fallback to `Create` |

## Environment-variable interpolation

Inherited verbatim from [`worker-yaml.md`](./worker-yaml.md): same `${VAR}` / `${VAR:-default}` syntax, same structure-safety guard. The implementation lives in `client::yaml_common` and is shared between the two loaders, so a divergence in security posture is impossible. See `worker-yaml.md#structure-safety-guard` for the precise ban list and rationale.

`$file:` includes are not processed here — see "Differences from `worker_yaml`" above.

## Two-phase registration

`register_function_sets_from_yaml*` runs in **two phases** (matching `worker_yaml`):

1. **Pre-validation:** env expansion, YAML parsing, duplicate-name detection, `$file:` resolution, target shape checks, and runner/worker name resolution. Lookups are cached per name within the batch. **No `Create` or `Update` RPC is issued.** If any step fails, no function set is registered.
2. **Registration:** validated specs are upserted sequentially in YAML order via the helper's `upsert_function_set_by_name`. A failure here can leave earlier sets registered; because the upsert is idempotent (find→update/create), retrying with the same YAML reconciles the state.

Library-side errors (YAML syntax, target typos, name lookup failures, …) therefore never leave partial registrations behind. Only mid-batch network failures during phase 2 are best-effort, and idempotent retries recover them.

## Related files

- Implementation: `src/client/function_set_yaml.rs`
- Shared YAML preprocessing: `src/client/yaml_common.rs` (also used by `worker_yaml`)
- Trait method facades: `src/client/helper.rs::UseJobworkerpClientHelper::{find_function_set_by_name, upsert_function_set_by_name, register_function_sets_from_yaml}`
- Worker counterpart: `docs/worker-yaml.md`
