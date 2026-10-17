//! Shared YAML primitives for declarative jobworkerp registration.
//!
//! Both [`super::worker_yaml`] and [`super::function_set_yaml`] resolve a YAML
//! document the same way before serde parsing: `%{VAR}` / `%{VAR:-default}`
//! interpolation against the process environment, then `{ $file: <path> }`
//! mappings replaced with the file's contents (constrained to a base
//! directory). Keeping these in one module guarantees both loaders get the
//! same security guarantees — a divergence between worker and function_set
//! YAMLs would mean one path could exfiltrate files the other rejects.
//!
//! See `docs/worker-yaml.md` for the rationale behind the `%{...}` delimiter
//! and the structure-safety envelope.

#![allow(clippy::missing_errors_doc, clippy::doc_markdown)]

use anyhow::{Context as _, Result, anyhow};
use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Limits for callers that opt in to bounded YAML preparation.
///
/// `max_bytes` covers each source's raw contents, expanded root/include text,
/// and the logical strings retained in prepared worker definitions.
/// `max_strings` includes expanded document text and prepared worker/settings
/// strings. These are payload budgets, not estimates of temporary parser or
/// allocator memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BoundedYamlSourceLimits {
    pub max_sources: usize,
    pub max_strings: usize,
    pub max_bytes: usize,
}

/// Immutable contents of a root YAML source and the include files captured by
/// one bounded preparation. Cloning before preparation creates another
/// root-only preparation token; callers own accounting across deliberate
/// independent loads. Cloning a returned prepared snapshot retains its
/// processed state and cannot trigger another include read.
#[derive(Clone, Debug)]
pub struct BoundedYamlSourceSnapshot {
    source_path: PathBuf,
    base_dir: PathBuf,
    contents: Arc<str>,
    charged_bytes: usize,
    included_sources: Vec<IncludedYamlSourceSnapshot>,
    preparation_state: SnapshotPreparationState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotPreparationState {
    RootOnly,
    Prepared,
}

/// Typed failure returned when a captured result snapshot is submitted to the
/// bounded preparation loader more than once.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum BoundedYamlSnapshotError {
    #[error("bounded YAML source snapshot was already prepared")]
    AlreadyPrepared,
}

/// Immutable contents of a `$file` source read during bounded preparation.
#[derive(Clone, Debug)]
pub struct IncludedYamlSourceSnapshot {
    source_path: PathBuf,
    contents: Arc<str>,
}

impl BoundedYamlSourceSnapshot {
    /// Read a root YAML file once, rejecting a source larger than `max_bytes`.
    /// The resulting snapshot retains the exact bytes used by later parsing.
    pub async fn read(source_path: &Path, max_bytes: usize) -> Result<Self> {
        let contents = read_to_string_bounded(source_path, max_bytes)
            .await
            .with_context(|| {
                format!(
                    "reading bounded YAML source {} failed",
                    source_path.display()
                )
            })?;
        let charged_bytes = contents.len();
        Ok(Self {
            source_path: source_path.to_path_buf(),
            base_dir: yaml_base_dir(source_path),
            contents: Arc::from(contents),
            charged_bytes,
            included_sources: Vec::new(),
            preparation_state: SnapshotPreparationState::RootOnly,
        })
    }

    pub(crate) fn begin_preparation(&mut self) -> Result<()> {
        if self.preparation_state == SnapshotPreparationState::Prepared {
            return Err(BoundedYamlSnapshotError::AlreadyPrepared.into());
        }
        self.preparation_state = SnapshotPreparationState::Prepared;
        Ok(())
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    pub fn contents(&self) -> &str {
        &self.contents
    }

    /// Raw root source bytes. Expanded contents and include payloads are
    /// reported by the bounded preparation result instead.
    pub fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }

    pub fn included_sources(&self) -> &[IncludedYamlSourceSnapshot] {
        &self.included_sources
    }
}

impl IncludedYamlSourceSnapshot {
    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub fn contents(&self) -> &str {
        &self.contents
    }
}

/// Read at most `max_bytes + 1` bytes so an oversized file is rejected
/// without an unbounded `read_to_string` allocation.
async fn read_to_string_bounded(source_path: &Path, max_bytes: usize) -> Result<String> {
    use tokio::io::AsyncReadExt as _;

    let read_limit = u64::try_from(max_bytes)
        .context("bounded YAML byte limit does not fit a file read bound")?
        .checked_add(1)
        .ok_or_else(|| anyhow!("bounded YAML file read bound overflow"))?;
    let file = tokio::fs::File::open(source_path)
        .await
        .with_context(|| format!("opening {} failed", source_path.display()))?;
    let mut contents = String::new();
    file.take(read_limit)
        .read_to_string(&mut contents)
        .await
        .with_context(|| format!("reading {} failed", source_path.display()))?;
    if contents.len() > max_bytes {
        return Err(anyhow!(
            "YAML source {} exceeds the {max_bytes} byte limit",
            source_path.display()
        ));
    }
    Ok(contents)
}

#[derive(Debug)]
pub(crate) struct BoundedYamlBudget {
    limits: BoundedYamlSourceLimits,
    source_count: usize,
    string_count: usize,
    charged_bytes: usize,
}

impl BoundedYamlBudget {
    pub(crate) fn new(limits: BoundedYamlSourceLimits, root_source_bytes: usize) -> Result<Self> {
        let mut budget = Self {
            limits,
            source_count: 0,
            string_count: 0,
            charged_bytes: 0,
        };
        budget.charge_source_count()?;
        budget.charge_bytes(root_source_bytes, "root YAML source")?;
        Ok(budget)
    }

    pub(crate) fn source_count(&self) -> usize {
        self.source_count
    }

    pub(crate) fn string_count(&self) -> usize {
        self.string_count
    }

    pub(crate) fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }

    pub(crate) fn remaining_bytes(&self) -> usize {
        self.limits.max_bytes.saturating_sub(self.charged_bytes)
    }

    pub(crate) fn charge_source_count(&mut self) -> Result<()> {
        let next = self
            .source_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("YAML source count overflow"))?;
        if next > self.limits.max_sources {
            return Err(anyhow!(
                "YAML source count exceeds the {} source limit",
                self.limits.max_sources
            ));
        }
        self.source_count = next;
        Ok(())
    }

    pub(crate) fn charge_string(&mut self, bytes: usize) -> Result<()> {
        let next = self
            .string_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("YAML string count overflow"))?;
        if next > self.limits.max_strings {
            return Err(anyhow!(
                "bounded YAML strings exceed the {} string limit",
                self.limits.max_strings
            ));
        }
        self.string_count = next;
        self.charge_bytes(bytes, "bounded YAML strings")
    }

    pub(crate) fn charge_bytes(&mut self, bytes: usize, kind: &str) -> Result<()> {
        let next = self
            .charged_bytes
            .checked_add(bytes)
            .ok_or_else(|| anyhow!("bounded YAML byte budget overflow"))?;
        if next > self.limits.max_bytes {
            return Err(anyhow!(
                "{kind} exceed the {} byte bounded YAML limit",
                self.limits.max_bytes
            ));
        }
        self.charged_bytes = next;
        Ok(())
    }
}

static ENV_RE: Lazy<Regex> = Lazy::new(|| {
    // %{NAME} or %{NAME:-default} where NAME = [A-Z_][A-Z0-9_]*
    Regex::new(r"%\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}").expect("hardcoded regex")
});

/// Expand `%{VAR}` and `%{VAR:-default}` against the process environment.
///
/// Returns Err if a variable has neither an env value nor a `:-default`
/// segment, or if the resolved value violates [`check_yaml_safe_scalar`].
///
/// **This is raw textual substitution, not a typed template engine.** The
/// resolved value is pasted into the document before YAML parsing; the YAML
/// parser then interprets the resulting document, so a value like `true`,
/// `null`, `9010`, `#hidden`, `&anchor`, or `{a: 1}` will be parsed as the
/// corresponding YAML form rather than as a string. Callers that require a
/// string can wrap the placeholder in YAML quotes at the call site
/// (e.g. `description: "%{TEXT}"` or `'%{TEXT}'`); quoting steers the YAML
/// parser toward a string interpretation but does **not** escape the value
/// — a literal `"` / `\` inside a double-quoted form, or `'` inside a
/// single-quoted form, is still the operator's responsibility. See
/// `docs/worker-yaml.md` for the full substitution model.
pub fn expand_env(raw: &str) -> Result<String> {
    expand_env_with_overrides(raw, &HashMap::new())
}

/// Expand placeholders with explicit values taking precedence over the
/// process environment. Use this when a caller must carry configuration into
/// a registered YAML payload without mutating process-global state.
pub fn expand_env_with_overrides(raw: &str, overrides: &HashMap<String, String>) -> Result<String> {
    expand_env_impl(raw, overrides, None)
}

/// Bounded variant of [`expand_env_with_overrides`]. The same placeholder
/// parser and scalar-safety rules are used, but output growth is checked as
/// each literal and replacement is appended.
pub fn expand_env_with_overrides_bounded(
    raw: &str,
    overrides: &HashMap<String, String>,
    max_output_bytes: usize,
) -> Result<String> {
    expand_env_impl(raw, overrides, Some(max_output_bytes))
}

fn expand_env_impl(
    raw: &str,
    overrides: &HashMap<String, String>,
    max_output_bytes: Option<usize>,
) -> Result<String> {
    let mut error: Option<String> = None;
    let initial_capacity = max_output_bytes.map_or(raw.len(), |limit| raw.len().min(limit));
    let mut out = String::with_capacity(initial_capacity);
    let mut cursor = 0;
    for caps in ENV_RE.captures_iter(raw) {
        let whole = caps
            .get(0)
            .expect("the regex always captures its full match");
        append_bounded(&mut out, &raw[cursor..whole.start()], max_output_bytes)?;
        if error.is_some() {
            cursor = whole.end();
            continue;
        }
        let name = &caps[1];
        let resolved = if let Some(v) = overrides.get(name) {
            v.clone()
        } else if let Ok(v) = std::env::var(name) {
            v
        } else {
            match caps.get(2) {
                Some(d) => d.as_str().to_string(),
                None => {
                    error = Some(format!(
                        "environment variable '{name}' is referenced in YAML without a default \
                         (use %{{{name}:-some-default}} to allow a fallback)"
                    ));
                    cursor = whole.end();
                    continue;
                }
            }
        };
        if let Err(reason) = check_yaml_safe_scalar(&resolved) {
            error = Some(format!(
                "environment variable '{name}' resolves to a value that would break the surrounding \
                 YAML structure: {reason}. Sanitize the value before exporting it"
            ));
        } else if let Err(err) = append_bounded(&mut out, &resolved, max_output_bytes) {
            error = Some(err.to_string());
        }
        cursor = whole.end();
    }
    append_bounded(&mut out, &raw[cursor..], max_output_bytes)?;
    if let Some(msg) = error {
        return Err(anyhow!(msg));
    }
    Ok(out)
}

fn append_bounded(out: &mut String, part: &str, max_bytes: Option<usize>) -> Result<()> {
    let needed = out
        .len()
        .checked_add(part.len())
        .ok_or_else(|| anyhow!("expanded YAML byte count overflow"))?;
    if let Some(limit) = max_bytes
        && needed > limit
    {
        return Err(anyhow!("expanded YAML exceeds the {limit} byte limit"));
    }
    out.push_str(part);
    Ok(())
}

/// Reject env values containing characters that re-shape the surrounding
/// YAML document regardless of context: line breaks (`\n` / `\r`) and tab.
/// A line break splits a single-value scalar across YAML lines; a tab is
/// rejected at scalar boundaries by YAML 1.2. Both produce silent
/// corruption rather than a parse error, so the guard catches them
/// before they reach the parser.
///
/// This is the irreducible minimum. Other YAML-significant characters
/// (`#`, `&`, `*`, `{`, `[`, `:`, quotes, ...) are deliberately allowed
/// because the call site is responsible for quoting placeholders that
/// must be strings — see [`expand_env`] and `docs/worker-yaml.md`.
pub fn check_yaml_safe_scalar(value: &str) -> std::result::Result<(), String> {
    if value.contains('\n') || value.contains('\r') {
        return Err("contains a line break".to_string());
    }
    if value.contains('\t') {
        return Err("contains a tab character".to_string());
    }
    Ok(())
}

/// Resolve the directory used as `base_dir` for `$file:` includes from
/// the YAML file's path.
///
/// `Path::parent()` returns `Some("")` (an empty `PathBuf`) when the
/// argument has no directory component (e.g. `foo.yaml` passed directly
/// from a shell `cd`'d into the file's directory). A bare `map_or_else`
/// on `None` therefore lets the empty path through and the subsequent
/// `canonicalize` fails with a misleading "does not exist" error. Treat
/// an empty parent the same as no parent so the fallback to `.` kicks in.
pub fn yaml_base_dir(yaml_path: &Path) -> PathBuf {
    match yaml_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Replace each `{ $file: "<path>" }` mapping in `value` with a plain
/// string scalar holding the file's contents. Relative paths resolve
/// against `base_dir`.
///
/// Path traversal is rejected: the canonical resolved path must live
/// under the canonical `base_dir`. Absolute paths and `..` segments
/// that escape `base_dir` are an error rather than a silent file read,
/// so a malicious YAML cannot exfiltrate arbitrary files via the
/// content the operator forwards to jobworkerp.
///
/// `base_dir` itself must exist and be canonicalize-able. Nested `$file:`
/// directives inside the content are NOT expanded (the result is a string
/// scalar, which the walk treats as opaque), but `%{VAR}` /
/// `%{VAR:-default}` placeholders in the content ARE expanded via
/// [`expand_env`]. The top-level [`expand_env`] runs before YAML parsing
/// and therefore never sees `$file` content (it is read here, afterwards),
/// so without this second pass an embedded workflow document could not use
/// env placeholders — its `%{...}` would survive verbatim into the
/// registered `workflow_data` and reach the runtime as a literal value.
///
/// Implemented as a sync-walk-then-async-read loop so the returned future
/// stays `Send` (recursive async fns crossing &mut tree borrows do not).
pub async fn resolve_includes(value: &mut serde_yaml::Value, base_dir: &Path) -> Result<()> {
    resolve_includes_with_overrides(value, base_dir, &HashMap::new()).await
}

/// Resolve `$file` includes while applying explicit placeholder overrides to
/// included content. This mirrors [`resolve_includes`] without requiring a
/// caller to modify the process environment.
pub async fn resolve_includes_with_overrides(
    value: &mut serde_yaml::Value,
    base_dir: &Path,
    overrides: &HashMap<String, String>,
) -> Result<()> {
    resolve_includes_impl(value, base_dir, overrides, None).await
}

/// Resolve includes through the standard include walk while recording and
/// bounding each file source. Included files are captured when this loader
/// reads them; later changes to those paths do not change the retained source.
pub(crate) async fn resolve_includes_bounded(
    value: &mut serde_yaml::Value,
    source_snapshot: &mut BoundedYamlSourceSnapshot,
    budget: &mut BoundedYamlBudget,
) -> Result<()> {
    let base_dir = source_snapshot.base_dir.clone();
    resolve_includes_impl(
        value,
        &base_dir,
        &HashMap::new(),
        Some((source_snapshot, budget)),
    )
    .await
}

async fn resolve_includes_impl(
    value: &mut serde_yaml::Value,
    base_dir: &Path,
    overrides: &HashMap<String, String>,
    mut bounded: Option<(&mut BoundedYamlSourceSnapshot, &mut BoundedYamlBudget)>,
) -> Result<()> {
    if first_include_path(value).is_none() {
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
        let content = if let Some((source_snapshot, budget)) = bounded.as_mut() {
            budget.charge_source_count()?;
            let content = read_to_string_bounded(&canonical, budget.remaining_bytes())
                .await
                .with_context(|| format!("$file include read failed: {}", canonical.display()))?;
            budget.charge_bytes(content.len(), "$file include source")?;
            source_snapshot
                .included_sources
                .push(IncludedYamlSourceSnapshot {
                    source_path: canonical.clone(),
                    contents: Arc::from(content.as_str()),
                });
            content
        } else {
            tokio::fs::read_to_string(&canonical)
                .await
                .with_context(|| format!("$file include read failed: {}", canonical.display()))?
        };
        // Expand `%{VAR}` in the included content too: the top-level
        // expand_env ran before this file was read, so its placeholders
        // would otherwise survive verbatim into the registered payload.
        let content = if let Some((_, budget)) = bounded.as_mut() {
            let expanded =
                expand_env_with_overrides_bounded(&content, overrides, budget.remaining_bytes())
                    .with_context(|| {
                        format!("expanding env in $file include {}", canonical.display())
                    })?;
            budget.charge_string(expanded.len())?;
            expanded
        } else {
            expand_env_with_overrides(&content, overrides).with_context(|| {
                format!("expanding env in $file include {}", canonical.display())
            })?
        };
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
    fn explicit_overrides_take_precedence_without_mutating_process_environment() {
        let overrides = std::collections::HashMap::from([(
            "YAML_COMMON_TEST_PREFIX".to_string(),
            "\"escaped\\nvalue\"".to_string(),
        )]);
        let expanded =
            expand_env_with_overrides("text: %{YAML_COMMON_TEST_PREFIX:-\"\"}", &overrides)
                .unwrap();

        assert_eq!(expanded, "text: \"escaped\\nvalue\"");
    }

    #[test]
    fn bounded_env_expansion_accepts_exact_output_and_rejects_one_byte_over() {
        let overrides =
            std::collections::HashMap::from([("VALUE".to_owned(), "expanded".to_owned())]);
        let raw = "value: %{VALUE}";
        let expanded = expand_env_with_overrides_bounded(raw, &overrides, 15).unwrap();
        assert_eq!(expanded, "value: expanded");

        let error = expand_env_with_overrides_bounded(raw, &overrides, 14).unwrap_err();
        assert!(error.to_string().contains("limit"));
    }

    #[test]
    fn yaml_base_dir_handles_bare_filename() {
        // `Path::parent()` returns `Some("")` for a bare filename; without
        // the explicit empty-check, canonicalize fails with a misleading
        // "$file include base_dir  does not exist".
        assert_eq!(yaml_base_dir(Path::new("workers.yaml")), PathBuf::from("."));
        assert_eq!(
            yaml_base_dir(Path::new("./workers.yaml")),
            PathBuf::from(".")
        );
        assert_eq!(
            yaml_base_dir(Path::new("/abs/workers.yaml")),
            PathBuf::from("/abs")
        );
        assert_eq!(
            yaml_base_dir(Path::new("dir/workers.yaml")),
            PathBuf::from("dir")
        );
    }

    #[test]
    #[serial]
    fn jq_and_liquid_expressions_pass_through_unchanged() {
        // Verifies %{...} does not consume Workflow-DSL ${...} jq filters,
        // $${...} Liquid templates, or ${{...}} jq object literals.
        // SAFETY: `#[serial]` guards against concurrent env access.
        unsafe {
            std::env::set_var("YAML_COMMON_TEST_HOST", "localhost");
        }
        let raw = r#"
host: %{YAML_COMMON_TEST_HOST}
filter: ${.input.key}
filter_obj: ${{a: 1, b: 2}}
template: $${{ user.name | upcase }}
"#;
        let out = expand_env(raw).unwrap();
        assert!(out.contains("host: localhost"));
        assert!(out.contains("filter: ${.input.key}"));
        assert!(out.contains("filter_obj: ${{a: 1, b: 2}}"));
        assert!(out.contains("template: $${{ user.name | upcase }}"));
        // SAFETY: see above.
        unsafe {
            std::env::remove_var("YAML_COMMON_TEST_HOST");
        }
    }

    /// `$file` content is read AFTER the top-level expand_env, so
    /// `resolve_includes` must run its own env expansion on the included
    /// text — otherwise an embedded workflow document's `%{VAR:-default}`
    /// would reach the registered payload verbatim. The included document
    /// also carries jq `${...}` filters, which must survive untouched.
    #[tokio::test]
    #[serial]
    async fn resolve_includes_expands_env_in_included_content() {
        let dir = tempfile::tempdir().unwrap();
        let inc = dir.path().join("workflow.yaml");
        // `%{...:-default}` resolves to the default when unset; the jq
        // `${...}` filter must pass through unchanged.
        tokio::fs::write(
            &inc,
            "worker: %{YAML_COMMON_TEST_WORKER:-default-worker}\nfilter: ${.input.key}\n",
        )
        .await
        .unwrap();

        // SAFETY: serialized via `#[serial]`.
        unsafe {
            std::env::remove_var("YAML_COMMON_TEST_WORKER");
        }
        let mut value: serde_yaml::Value =
            serde_yaml::from_str("data:\n  $file: workflow.yaml\n").unwrap();
        resolve_includes(&mut value, dir.path()).await.unwrap();

        let included = value["data"]
            .as_str()
            .expect("included content is a scalar");
        assert!(
            included.contains("worker: default-worker"),
            "%{{...}} in included content must be expanded; got:\n{included}"
        );
        assert!(
            included.contains("filter: ${.input.key}"),
            "jq ${{...}} in included content must survive; got:\n{included}"
        );

        // With the env set, the override wins.
        // SAFETY: serialized via `#[serial]`.
        unsafe {
            std::env::set_var("YAML_COMMON_TEST_WORKER", "override-worker");
        }
        let mut value: serde_yaml::Value =
            serde_yaml::from_str("data:\n  $file: workflow.yaml\n").unwrap();
        resolve_includes(&mut value, dir.path()).await.unwrap();
        let included = value["data"]
            .as_str()
            .expect("included content is a scalar");
        assert!(
            included.contains("worker: override-worker"),
            "the env override must win over the default; got:\n{included}"
        );
        // SAFETY: see above.
        unsafe {
            std::env::remove_var("YAML_COMMON_TEST_WORKER");
        }
    }

    #[tokio::test]
    #[serial]
    async fn bounded_include_capture_keeps_raw_source_and_expands_standard_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().join("workers.yaml");
        let include_path = dir.path().join("settings.yaml");
        let root = "settings:\n  $file: settings.yaml\n";
        let included = "host: %{YAML_COMMON_BOUNDED_HOST:-default-host}\n";
        tokio::fs::write(&root_path, root).await.unwrap();
        tokio::fs::write(&include_path, included).await.unwrap();
        // SAFETY: this variable is serialized by `#[serial]`.
        unsafe {
            std::env::remove_var("YAML_COMMON_BOUNDED_HOST");
        }

        let mut snapshot = BoundedYamlSourceSnapshot::read(&root_path, 4096)
            .await
            .unwrap();
        let mut budget = BoundedYamlBudget::new(
            BoundedYamlSourceLimits {
                max_sources: 4,
                max_strings: 8,
                max_bytes: 4096,
            },
            snapshot.charged_bytes(),
        )
        .unwrap();
        budget
            .charge_bytes(root.len(), "expanded root YAML source")
            .unwrap();
        let mut value: serde_yaml::Value = serde_yaml::from_str(root).unwrap();
        resolve_includes_bounded(&mut value, &mut snapshot, &mut budget)
            .await
            .unwrap();

        assert_eq!(value["settings"].as_str().unwrap(), "host: default-host\n");
        assert_eq!(snapshot.included_sources().len(), 1);
        assert_eq!(snapshot.included_sources()[0].contents(), included);
        assert_eq!(budget.source_count(), 2);
        assert!(budget.charged_bytes() >= root.len() + included.len());
    }
}
