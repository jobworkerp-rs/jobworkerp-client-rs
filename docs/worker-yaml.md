# `worker_yaml` — YAML-driven jobworkerp Worker Registration

The `jobworkerp_client::client::worker_yaml` module provides a generic library facility for loading jobworkerp Worker definitions from YAML files and upserting them to a jobworkerp server through `UseJobworkerpClientHelper::upsert_worker`.

By consolidating each `WorkerData` field and the (arbitrary-proto) `runner_settings` into a single YAML file, worker registration logic that used to be hard-coded in Rust can be expressed declaratively. For an end-to-end usage example, see `memories/docs/yaml-workers.md` in the parent repository.

## API

```rust
// Load from a file (the production entry point).
pub async fn register_workers_from_yaml<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    yaml_path: &Path,
) -> Result<HashMap<String, WorkerId>>
where C: UseJobworkerpClientHelper + Send + Sync;

// Load from a string (for tests and dynamic generation).
pub async fn register_workers_from_yaml_str<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    raw_yaml: &str,
    base_dir: &Path,         // used to resolve `$file:` includes
) -> Result<HashMap<String, WorkerId>>;

// Register a single worker without going through YAML.
pub async fn register_worker<C>(
    client: &C,
    cx: Option<&opentelemetry::Context>,
    metadata: Arc<HashMap<String, String>>,
    runner_name: &str,
    worker_data: WorkerData,
    settings_json: Option<&serde_json::Value>,
) -> Result<WorkerId>;
```

The `UseJobworkerpClientHelper` trait also exposes a `register_worker` method, so callers can invoke `client.register_worker(...)` alongside the other helpers; the implementation is a thin facade over the free function above.

The return value is a `HashMap` of `worker_name -> WorkerId`. Because jobworkerp keys workers by name, an existing worker with the same name has its settings overwritten via `UpsertByName` (idempotent).

## YAML format

```yaml
defaults:        # optional — fallback values for every worker
  ...
workers:         # required — N worker definitions per file
  - name: ...
    runner: ...
    settings: ...
    ...
```

`#[serde(deny_unknown_fields)]` is applied at every level (top-level, `defaults`, each worker, `retry_policy`), so unknown fields cause a parse error. Typos like `defualts:` are surfaced rather than silently ignored.

### `defaults` block

Values applied to every worker that does not override them. The whole block is optional.

| Key | Type | Example |
|----|----|----|
| `channel` | `string` | `"high-priority"` |
| `queue_type` | `string` (enum) | `NORMAL` / `WITH_BACKUP` / `DB_ONLY` |
| `response_type` | `string` (enum) | `DIRECT` / `NO_RESULT` |
| `store_success` / `store_failure` / `use_static` / `broadcast_results` | `bool` | — |
| `periodic_interval` | `u32` (msec) | `0` |
| `retry_policy` | `RetryPolicy` | see below |

`RetryPolicy`:

```yaml
retry_policy:
  type: EXPONENTIAL    # NONE / EXPONENTIAL / LINEAR / CONSTANT
  interval: 800        # base retry interval (ms)
  max_interval: 60000  # cap on the retry interval (ms)
  max_retry: 1         # maximum retry attempts (0 = unlimited)
  basis: 2.0           # exponential backoff base
```

Only `type` is required; omitted fields fall back to `helper::DEFAULT_RETRY_POLICY`.

### Per-worker entry

| Key | Required | Type | Description |
|----|----|----|----|
| `name` | yes | `string` | Worker name on jobworkerp |
| `runner` | yes | `string` | Runner name (resolved by `find_runner_by_name`) |
| `description` | no | `string` | Free-form description |
| `settings` | no | YAML tree or omitted | Maps to `runner_settings`. Optional; see below |
| `channel`, etc. | no | (same as `defaults`) | Per-worker overrides |

### Enum values

Enums are written as the proto enum name. They are converted to `i32` via `from_str_name`; an unknown value produces an explicit error.

- `response_type`: `DIRECT`, `NO_RESULT`
- `queue_type`: `NORMAL`, `WITH_BACKUP`, `DB_ONLY`
- `retry_policy.type`: `NONE`, `EXPONENTIAL`, `LINEAR`, `CONSTANT`

### Behaviour of `settings` (important)

| YAML form | Behaviour |
|----|----|
| `settings:` omitted, or `settings: ~` (Null) | Empty `runner_settings` bytes are sent; the runner schema is not consulted |
| `settings: { ... }` | YAML→JSON conversion, then proto encoding via the runner's `runner_settings_proto` schema |

This matches the existing semantics of `helper.rs::setup_worker_and_enqueue_with_json`, allowing runners that have no required fields to be registered with no configuration at all.

## Environment-variable interpolation

Within scalars, mapping keys, or quoted values, both `${VAR}` and `${VAR:-default}` are recognised:

```yaml
host: ${MEMORY_GRPC_HOST:-127.0.0.1}
port: ${MEMORY_GRPC_PORT:-9010}
api_key: ${SECRET_KEY}        # no default — error if SECRET_KEY is unset
```

- Variable names must match `[A-Z_][A-Z0-9_]*` (POSIX convention)
- `${X:-fb}` expands to `fb` when X is unset
- `${X}` errors out if X is unset (`environment variable 'X' is referenced in YAML without a default`)
- Values are bound at YAML-load time. Because the resulting `WorkerData` is upserted to jobworkerp, env-var changes after registration are not picked up until the process restarts.

## `$file: <path>` file includes

A mapping of the form `{ $file: <relative-or-absolute-path> }` is replaced with a **string scalar** holding the file's contents. This is intended for cases like the WORKFLOW runner's `workflow_data` field, where you want to embed a separate YAML document as the value:

```yaml
- name: my-workflow-worker
  runner: WORKFLOW
  settings:
    workflow_data:
      $file: my-workflow.yaml
```

### Path-traversal restrictions

For security, the canonicalised include path must be a prefix of the canonicalised `base_dir`:

- `$file: ../../etc/passwd` (relative path escaping `base_dir`) → **error**
- `$file: /etc/shadow` (absolute path outside `base_dir`) → **error**
- `$file: ./../base/sub.yaml` (re-enters `base_dir` after a detour) → ok, because the canonical destination still lives under `base_dir`

`base_dir` itself must exist and be canonicalisable; otherwise the include resolution fails. This pins the trust boundary to the `base_dir` directory tree, so the API stays safe even when the YAML it loads originates from a less-trusted source.

### No nested expansion

The substitution result is a `Value::String` (opaque text), so `$file:` directives written inside the included content are **not** re-expanded. The intent is to embed a separate YAML document verbatim as a string (e.g. for `workflow_data`).

## Error semantics

`register_workers_from_yaml*` runs in **two phases**:

1. **Pre-validation (no network I/O)**: env expansion, YAML parsing, `$file:` resolution, `build_worker_data` (enum string parsing, etc.), and YAML→JSON conversion are performed for every worker spec. **If any of these fails, `Err` is returned and not a single RPC is sent to jobworkerp.**
2. **Registration (network)**: validated specs are upserted sequentially in YAML order. A failure here can leave earlier workers registered; because `UpsertByName` is idempotent, retrying with the same YAML reconciles the state.

Library-side errors (YAML syntax, enum typos, …) therefore never leave partial registrations behind. Server-side errors (unknown runner, network failure, etc.) are best-effort.

## Failure-mode summary

| Failure case | Behaviour |
|----|----|
| Env variable unset and no default | Startup fails (pre-validation) |
| `$file:` target unreadable | Startup fails (pre-validation) |
| `$file:` resolves outside `base_dir` | Startup fails (pre-validation) |
| Top-level / worker / retry_policy field typo | Startup fails (serde parse stage) |
| Invalid enum string | Startup fails (pre-validation) |
| Runner name not registered on jobworkerp | RPC fails — earlier workers are already registered |
| `settings` does not match the runner schema | Startup fails (proto encoding fails during registration) |

## Dependencies

- `serde_yaml = "0.9"`
- `regex = "1"`
- `once_cell = "1"` (already used)
- `tokio` (already used; the `fs` module)

## Related files

- Implementation: `src/client/worker_yaml.rs`
- Trait method facade: `src/client/helper.rs::UseJobworkerpClientHelper::register_worker`
- Usage example: `memories/docs/yaml-workers.md` and `memories/workflows/auto-embedding-workers.yaml`
