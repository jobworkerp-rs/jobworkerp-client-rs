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
use regex::{Captures, Regex};
use std::path::{Path, PathBuf};

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
    // `replace_all` cannot return Result, so we record the first error
    // and bail after the scan completes. Subsequent matches
    // short-circuit to "" to avoid additional env lookups.
    let mut error: Option<String> = None;
    let out = ENV_RE.replace_all(raw, |caps: &Captures<'_>| -> String {
        if error.is_some() {
            return String::new();
        }
        let name = &caps[1];
        let resolved = if let Ok(v) = std::env::var(name) {
            v
        } else {
            match caps.get(2) {
                Some(d) => d.as_str().to_string(),
                None => {
                    error = Some(format!(
                        "environment variable '{name}' is referenced in YAML without a default \
                         (use %{{{name}:-some-default}} to allow a fallback)"
                    ));
                    return String::new();
                }
            }
        };
        if let Err(reason) = check_yaml_safe_scalar(&resolved) {
            error = Some(format!(
                "environment variable '{name}' resolves to a value that would break the surrounding \
                 YAML structure: {reason}. Sanitize the value before exporting it"
            ));
            return String::new();
        }
        resolved
    });
    if let Some(msg) = error {
        return Err(anyhow!(msg));
    }
    Ok(out.into_owned())
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
/// `base_dir` itself must exist and be canonicalize-able. The included
/// content is inserted as-is; nested `$file:` directives inside the
/// content are NOT expanded (the result is a string scalar, which the
/// walk treats as opaque).
///
/// Implemented as a sync-walk-then-async-read loop so the returned future
/// stays `Send` (recursive async fns crossing &mut tree borrows do not).
pub async fn resolve_includes(value: &mut serde_yaml::Value, base_dir: &Path) -> Result<()> {
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
}
