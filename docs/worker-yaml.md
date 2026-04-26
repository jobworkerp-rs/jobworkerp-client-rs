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

The trait method is bound by `where Self: Sized`, so it cannot be called through a `dyn UseJobworkerpClientHelper` trait object. Callers that need dynamic dispatch (e.g. holding a `Box<dyn UseJobworkerpClientHelper>`) should call the free function `worker_yaml::register_worker(&*client, ...)` directly — it has no `Sized` bound and accepts any concrete client by reference.

`register_worker` resolves the runner by name and writes the result back into `worker_data` before upserting. Behaviour depends on the combination of `settings_json` and `worker_data.runner_settings`:

| `settings_json`   | `worker_data.runner_settings` | Result |
|-------------------|-------------------------------|--------|
| `None`            | empty                         | empty bytes registered |
| `None`            | non-empty                     | bytes registered as-is (caller pre-encoded them) |
| `Some(Value::Null)` | empty                       | empty bytes registered (explicit "send empty" intent) |
| `Some(Value::Null)` | non-empty                   | **error** — the explicit-null asks for empty bytes but the field is non-empty; resolve the conflict on the caller side |
| `Some(non-null)`  | empty                         | JSON encoded against the runner's `runner_settings_proto` and stored |
| `Some(non-null)`  | non-empty                     | **error** — both inputs supplied; pick exactly one |

Encoding here uses `ignore_unknown_fields=true` (lenient) so the programmatic API keeps forward compatibility when the server adds a new `runner_settings` field. The strict (`ignore_unknown_fields=false`) path is reserved for declarative YAML registration.

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

#### Implicit fallback values

When neither the per-worker spec nor the `defaults` block sets a key, every field falls back to the proto3 zero value for `WorkerData`: `bool` → `false`, `uint32` → `0`, `optional` message (`channel`, `retry_policy`) → unset, enum → variant `0` (`response_type = NO_RESULT`, `queue_type = NORMAL`). This contrasts with the CLI `worker create` command and `helper.rs::build_worker_data_default`, which apply opinionated non-proto defaults (`response_type = DIRECT`, `retry_policy = DEFAULT_RETRY_POLICY`, etc.). The YAML loader deliberately stays at the proto-zero baseline so a declarative YAML behaves the same as a hand-built `WorkerData` passed straight to `upsert_worker`; YAMLs that want the helper-style behaviour must set the values explicitly under `defaults:` or per worker.

Inside an explicit `retry_policy:` block, omitted leaf fields (`interval`, `max_interval`, `max_retry`, `basis`) fall back to the corresponding fields of `helper::DEFAULT_RETRY_POLICY` rather than to `0`. A retry policy with all-zero leaves would loop instantaneously and forever, which is rarely what an operator means when they write `retry_policy: { type: EXPONENTIAL }`; the helper-derived per-leaf defaults give a safer baseline for partially specified policies. (To opt out entirely, omit the whole `retry_policy:` block — the field then stays unset.)

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

### Cross-field constraints

The proto contract enforces that periodic workers cannot return a direct response (`worker.proto`: "If enabled, direct_response cannot be used"). The pre-validator therefore rejects any spec where `periodic_interval > 0` while `response_type` resolves to `DIRECT`, so the conflict surfaces at startup instead of mid-batch during `upsert_worker`. The implicit fallback for `response_type` is `NO_RESULT`, so a periodic worker that does not set `response_type:` validates cleanly; the rejection only fires when `DIRECT` is set explicitly (per worker, or under `defaults:`).

The same pre-validation runs in the programmatic `register_worker` path: a `WorkerData` with `periodic_interval > 0` and `response_type = ResponseType::Direct` is rejected before the runner lookup RPC, so the constraint behaves identically whether the worker is registered via YAML or via the helper trait.

### Behaviour of `settings` (important)

| YAML form | Behaviour |
|----|----|
| `settings:` omitted, or `settings: ~` (Null) | Empty `runner_settings` bytes are sent; the runner schema is not consulted |
| `settings: { ... }` (runner declares a `runner_settings` schema) | YAML→JSON conversion, then proto encoding via the runner's `runner_settings_proto` schema |
| `settings: { ... }` (runner declares **no** `runner_settings` schema) | Pre-validation error — the configuration is not silently dropped |

The "no settings + optional-only schema" case keeps parity with `helper.rs::setup_worker_and_enqueue_with_json`, allowing runners with no required fields to be registered with no configuration at all. Conversely, providing a `settings:` block to a runner that declares no schema is rejected at startup so a typo in `runner:` (e.g. wrong runner name) cannot cause configuration to disappear silently.

Proto encoding for the second row uses `ignore_unknown_fields=false`: a key in `settings` that does not match any field in the runner's `runner_settings` proto schema (typically a typo such as `prot` instead of `port`) is a startup error. Runtime helpers like `setup_worker_and_enqueue_with_json` and the programmatic `register_worker` entry point keep the lenient default — the strict check only applies to declarative YAML registration where catching misconfigurations at startup is the entire point. (Forward compatibility: when the server schema gains a new field ahead of a client release, programmatic callers continue to work; declarative YAML callers still trip the strict check until they update.)

Note: the CLI `worker create` command (in `command/worker.rs`) accepts a runner with **no** `runner_settings` schema and stores the raw `--settings` string as bytes. The YAML registration path deliberately does not replicate that behaviour: silently treating arbitrary text as `runner_settings` bytes is an error-prone path that we want operators to opt into explicitly via the CLI rather than via a declarative manifest. Workers that need the raw-bytes path should be created through the CLI; the YAML path is reserved for runners with a declared schema (or with no settings at all).

## Environment-variable interpolation

Substitution is plain string replacement that runs *before* YAML parsing. `${VAR}` and `${VAR:-default}` are recognised wherever they appear in the raw text:

```yaml
host: ${MEMORY_GRPC_HOST:-127.0.0.1}
port: ${MEMORY_GRPC_PORT:-9010}
api_key: ${SECRET_KEY}        # no default — error if SECRET_KEY is unset
url: ${DATABASE_URL}          # DSN with `:`, `@`, `?`, `&`, `=`, `/` is fine — see guard below
store_failure: ${STORE_FAILURE:-true}  # opt in to result storage; bool fields accept bool literals (true/false/yes/no/null/~)
```

- Variable names must match `[A-Z_][A-Z0-9_]*` (POSIX convention)
- `${X:-fb}` expands to `fb` when X is unset
- `${X}` errors out if X is unset (`environment variable 'X' is referenced in YAML without a default`)
- The default-value segment after `:-` cannot contain `}`; the expander stops capturing at the first `}` it sees. Pre-encode such defaults (URL-encode, etc.) or move them into a `$file:` include.
- Values are bound at YAML-load time. Because the resulting `WorkerData` is upserted to jobworkerp, env-var changes after registration are not picked up until the process restarts.

### Structure-safety guard

Because substitution runs before YAML parsing, the resolved value is pasted into the document verbatim and **the expander cannot tell whether the placeholder sits inside YAML quotes**. The guard therefore applies the same rules to every placeholder; YAML-quoting it does not relax them. The intent is to refuse values that would change the field's *type* or *shape* while letting practical strings (DSNs, URLs, paths, tokens) through.

Two failure classes:

1. **Whole-value structural breakers** — characters or substrings that re-shape the document anywhere they appear:
   - line break (`\n`, `\r`)
   - tab character
   - `": "` (key/value separator)
   - `" #"` (inline comment marker)
   - `,`, `]`, `}` (these split or close a flow collection if the host happens to be in a flow context)
   - `"`, `'`, `\` — these break a YAML quoted scalar from the inside; e.g. `api_key: "${SECRET}"` with `SECRET=abc"def` would expand to `api_key: "abc"def"` and corrupt the document. Because the expander cannot see the surrounding quotes, the only safe option is to refuse the value outright.
2. **Leading-position indicators** — characters that only re-interpret the value when they appear at the *very start* of the resolved scalar; mid-string occurrences are allowed:
   - `#` (turns the entire value into a comment, leaving the field null)
   - `&` (anchor), `*` (alias), `!` (tag)
   - `|`, `>` (block-scalar indicators)
   - `[`, `{` (flow collection openers)
   - `` ` `` (YAML reserved)
   - `?` (complex-key indicator)
   - `"- "` (sequence entry, when followed by space)
   - `"% "` (directive, when followed by space)

**Bool/null literals are deliberately allowed.** Values like `true`, `false`, `yes`, `no`, `null`, `~` flow through unchanged so that env interpolation works for bool fields such as `store_success`, `store_failure`, `use_static`, and `broadcast_results` — `store_failure: ${STORE_FAILURE:-true}` is a supported pattern. Plain numerics like `9010` likewise pass through, so `port: ${PORT:-9010}` resolves to a YAML integer. If a bool/null literal lands at a string-typed position the type mismatch surfaces during downstream YAML→proto encoding, which is a more precise error than a blanket pre-parse rejection.

Real-world strings such as `postgres://user:pw@host:5432/db?sslmode=require&app=foo` pass cleanly: the `&`, `?`, `=`, `:`, `@`, `/` are mid-string, none of the leading-position bans apply, and there is no `": "` substring (the `:` after `host` is followed by `5432`, not a space).

Values that contain literal `"`, `'`, or `\` (Windows paths, JSON fragments, passwords with embedded quotes) cannot be passed through env interpolation safely. Move them into an external file referenced via `$file:`, or pre-encode them (e.g. base64 / URL-encode) and decode at the consumer.

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

1. **Pre-validation**: env expansion, YAML parsing, duplicate-name detection, `$file:` resolution, `build_worker_data` (enum string parsing, etc.), YAML→JSON conversion, **runner lookup (cached per `runner_name` to avoid redundant RPCs), `runner_settings` schema parsing, and proto encoding** are all performed for every worker spec. Runner lookup is the only step in this phase that touches the network, but no `upsert_worker` is issued. **If any of these fails, `Err` is returned and not a single worker is registered.**
2. **Registration**: validated specs are upserted sequentially in YAML order via `upsert_worker`. A failure here can leave earlier workers registered; because `UpsertByName` is idempotent, retrying with the same YAML reconciles the state.

Library-side errors (YAML syntax, enum typos, runner typos, schema mismatches, …) therefore never leave partial registrations behind. Only mid-batch network failures during phase 2 are best-effort, and idempotent retries recover them.

When several workers share the same `runner:`, the lookup is cached per name so the runner is fetched only once per batch.

## Failure-mode summary

| Failure case | Behaviour |
|----|----|
| Env variable unset and no default | Startup fails (pre-validation) |
| `$file:` target unreadable | Startup fails (pre-validation) |
| `$file:` resolves outside `base_dir` | Startup fails (pre-validation) |
| Top-level / worker / retry_policy field typo | Startup fails (serde parse stage) |
| Invalid enum string | Startup fails (pre-validation) |
| Duplicate `name` within one YAML | Startup fails (pre-validation) |
| Runner name not registered on jobworkerp | Startup fails (pre-validation) |
| `settings` does not match the runner schema | Startup fails (pre-validation) |
| Worker has `settings` but the runner declares no `runner_settings` schema | Startup fails (pre-validation) |
| `settings` contains a key that is not in the runner's `runner_settings` proto schema (typo) | Startup fails (pre-validation — `ignore_unknown_fields=false` for declarative registration) |
| Env value violates the structure-safety guard (line break, tab, `": "`, `" #"`, `,`, `]`, `}`, `"`, `'`, `\` anywhere; or starts with `#`, `&`, `*`, `!`, `|`, `>`, `[`, `{`, `` ` ``, `?`, `"- "`, `"% "`) | Startup fails (pre-validation — see "Structure-safety guard") |
| Worker has `periodic_interval > 0` and `response_type: DIRECT` | Startup fails (pre-validation — proto rule: periodic workers must use `NO_RESULT`) |
| Network failure mid-upsert (phase 2) | Earlier workers may already be registered; idempotent retry reconciles |

## Dependencies

- `serde_yaml = "0.9"`
- `regex = "1"`
- `once_cell = "1"` (already used)
- `tokio` (already used; the `fs` module)

## Related files

- Implementation: `src/client/worker_yaml.rs`
- Trait method facade: `src/client/helper.rs::UseJobworkerpClientHelper::register_worker`
- Usage example: `memories/docs/yaml-workers.md` and `memories/workflows/auto-embedding-workers.yaml`
