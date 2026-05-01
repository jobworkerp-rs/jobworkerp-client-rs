//! Shared YAML primitives for declarative jobworkerp registration.
//!
//! Both [`super::worker_yaml`] and [`super::function_set_yaml`] resolve a YAML
//! document the same way before serde parsing: `${VAR}` / `${VAR:-default}`
//! interpolation against the process environment, then `{ $file: <path> }`
//! mappings replaced with the file's contents (constrained to a base
//! directory). Keeping these in one module guarantees both loaders get the
//! same security guarantees — a divergence between worker and function_set
//! YAMLs would mean one path could exfiltrate files the other rejects.

#![allow(clippy::missing_errors_doc, clippy::doc_markdown)]

use anyhow::{Context as _, Result, anyhow};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use std::path::{Path, PathBuf};

static ENV_RE: Lazy<Regex> = Lazy::new(|| {
    // ${NAME} or ${NAME:-default} where NAME = [A-Z_][A-Z0-9_]*
    Regex::new(r"\$\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?\}").expect("hardcoded regex")
});

/// Expand `${VAR}` and `${VAR:-default}` against the process environment.
///
/// Returns Err if (a) a variable has neither an env value nor a `:-default`
/// segment, or (b) the resolved value contains characters that would break
/// the surrounding YAML structure when inlined as a raw scalar.
///
/// The expander runs *before* YAML parsing, so the substituted value is
/// pasted into the document verbatim. A multi-line value, or one containing
/// `:` followed by whitespace, would silently re-shape the document — a
/// `port: ${SECRET}` where `SECRET="a: b"` would parse as a mapping rather
/// than the intended scalar. The guard therefore rejects characters and
/// substrings that carry structure-changing meaning at the substitution
/// site (see [`check_yaml_safe_scalar`] for the precise rules).
///
/// **Wrapping the placeholder in YAML quotes does not relax these
/// checks.** Expansion happens before YAML parsing, so the surrounding
/// quotes are not visible to the expander; a value containing `"`, `'`,
/// or `\` will break a `field: "${VAR}"` host context just as readily
/// as a bare one. Operators must sanitize the value at the source (or
/// move it out of YAML, e.g. via `$file:`) — there is no escape-at-the-
/// YAML-level workaround.
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
                         (use ${{{name}:-some-default}} to allow a fallback)"
                    ));
                    return String::new();
                }
            }
        };
        if let Err(reason) = check_yaml_safe_scalar(&resolved) {
            error = Some(format!(
                "environment variable '{name}' resolves to a value that would break the surrounding \
                 YAML structure: {reason}. Sanitize the value before exporting it; wrapping the \
                 placeholder in YAML quotes does NOT relax this check because expansion runs before \
                 YAML parsing and the surrounding quotes are not visible to the expander"
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

/// Reject env values that, when pasted as a raw scalar at the
/// substitution site, would re-shape the YAML document into something
/// the template did not intend (a new mapping, an open flow collection,
/// a comment, a closed quoted scalar mid-string, etc.).
///
/// The check is conservative but tries not to refuse practical
/// strings. Two failure classes:
///
/// 1. **Whole-value structural breakers** — characters or substrings
///    that re-shape the document anywhere they appear: line breaks,
///    tabs, `": "` (key/value separator), `" #"` (inline comment),
///    flow-collection terminators `,` `]` `}` (these split scalars
///    even mid-string when the host is in a flow context, which we
///    cannot detect from raw text), and quote / escape characters
///    `"` `'` `\` (these break a quoted host context such as
///    `field: "${VAR}"` mid-string — e.g. `SECRET=abc"def` would close
///    the surrounding `"..."` early and corrupt the document).
/// 2. **Leading-position indicators** — characters that only change
///    parsing when they appear at the *start* of the scalar: `#`
///    (entire value parsed as a comment, leaving the field null),
///    `&` `*` `!` `|` `>` (YAML anchor / alias / tag / block-scalar
///    indicators), `[` `{` (flow collection openers), `` ` `` (YAML
///    reserved), `?` (complex-key indicator), and the two-char
///    prefixes `"- "` (sequence entry) and `"% "` (directive).
///    Mid-string occurrences of these (e.g. `postgres://u:p@h?a=1&b=2`
///    which contains `&`) are left alone because real-world DSNs and
///    URLs routinely include them.
///
/// **Bool/null literals are deliberately allowed.** Values like `true`,
/// `false`, `yes`, `no`, `null`, `~` flow through unchecked because
/// they are required for legitimate env interpolation against bool
/// fields. Pasting them at a string-typed position produces a type
/// error at YAML→proto encoding time, which is a more precise error
/// than a blanket pre-parse rejection.
pub fn check_yaml_safe_scalar(value: &str) -> std::result::Result<(), String> {
    if value.contains('\n') || value.contains('\r') {
        return Err("contains a line break".to_string());
    }
    if value.contains('\t') {
        return Err("contains a tab character".to_string());
    }
    if value.contains(": ") {
        return Err("contains ': ' which YAML treats as a key/value separator".to_string());
    }
    if value.contains(" #") {
        return Err("contains ' #' which YAML treats as a comment marker".to_string());
    }
    const ALWAYS_BANNED: &[char] = &[',', ']', '}', '"', '\'', '\\'];
    if let Some(c) = value.chars().find(|c| ALWAYS_BANNED.contains(c)) {
        let reason = match c {
            ',' | ']' | '}' => format!(
                "contains '{c}' which can terminate a YAML flow collection regardless of context"
            ),
            '"' | '\'' => format!(
                "contains {c:?} which would close or escape a YAML quoted scalar; the surrounding \
                 quoted form (e.g. `field: \"${{VAR}}\"`) cannot be made safe by escaping at \
                 expansion time"
            ),
            '\\' => format!(
                "contains '{c}' which is the escape character of YAML double-quoted scalars and \
                 cannot be safely passed through raw substitution"
            ),
            _ => unreachable!(),
        };
        return Err(reason);
    }
    if let Some(first) = value.chars().next() {
        const LEADING_BANNED: &[char] = &[
            '#', // turns the value into a comment, field becomes null
            '&', // anchor
            '*', // alias
            '!', // tag
            '|', // literal block scalar
            '>', // folded block scalar
            '[', // flow sequence opener
            '{', // flow mapping opener
            '`', // YAML reserved
            '?', // complex-key indicator
        ];
        if LEADING_BANNED.contains(&first) {
            return Err(format!(
                "starts with '{first}' which YAML treats as an indicator at the beginning of a scalar"
            ));
        }
        if value.starts_with("- ") {
            return Err(
                "starts with '- ' which YAML treats as a sequence entry marker".to_string(),
            );
        }
        if value.starts_with("% ") {
            return Err("starts with '% ' which YAML treats as a directive marker".to_string());
        }
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
