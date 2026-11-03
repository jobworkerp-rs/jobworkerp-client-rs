//! The stream error contract: how a result stream's End trailer reports an in-band failure, and
//! how such a classified failure (also carried by a failed `JobResultData.error`) is described.
//!
//! `Trailer.error` is authoritative. Older servers (or a proxy that drops the typed field) report
//! failures only through deprecated metadata keys, which are read as fallbacks. Public so plugin
//! authors overriding `collect_stream` apply the same rules as the default implementation.

use crate::jobworkerp::data::{
    JobResultData, ProblemDetails, ResultStatus, StreamError, StreamErrorCode, Trailer,
};

/// Deprecated plain-text failure key written by plugin runners and the worker idle timeout.
pub const PLUGIN_RUN_STREAM_ERROR_KEY: &str = "jobworkerp.plugin.run_stream_error";
/// Deprecated JSON mirror of `Trailer.error` (`{"version":1,"code":"TIMEOUT",...}`).
pub const COMMON_STREAM_ERROR_KEY: &str = "stream_error";
const STREAM_ERROR_CODE_PREFIX: &str = "STREAM_ERROR_CODE_";
const UNKNOWN_ORIGIN: &str = "UNKNOWN";
const LEGACY_PLUGIN_ORIGIN: &str = "PLUGIN";

#[derive(serde::Deserialize)]
struct CommonStreamError {
    code: Option<String>,
    message: String,
    origin: Option<String>,
}

fn stream_error(code: StreamErrorCode, message: String, origin: String) -> StreamError {
    StreamError {
        code: code as i32,
        message,
        origin,
        problem: None,
    }
}

/// Extract the failure reported in an End trailer. `Trailer.error` is authoritative; the JSON
/// mirror and then the legacy plugin key are read only when it is absent. A malformed JSON value
/// is still a failure, reported with its raw value. An End with none of these is a success.
pub fn stream_error_from_trailer(trailer: &Trailer) -> Option<StreamError> {
    if let Some(error) = &trailer.error {
        return Some(error.clone());
    }
    if let Some(raw) = trailer.metadata.get(COMMON_STREAM_ERROR_KEY) {
        return Some(match serde_json::from_str::<CommonStreamError>(raw) {
            Ok(CommonStreamError {
                code,
                message,
                origin,
            }) => stream_error(
                code.and_then(|code| {
                    StreamErrorCode::from_str_name(&format!("{STREAM_ERROR_CODE_PREFIX}{code}"))
                })
                .unwrap_or(StreamErrorCode::Unspecified),
                message,
                origin.unwrap_or_else(|| UNKNOWN_ORIGIN.to_string()),
            ),
            Err(_) => stream_error(
                StreamErrorCode::Unspecified,
                raw.clone(),
                UNKNOWN_ORIGIN.to_string(),
            ),
        });
    }
    trailer
        .metadata
        .get(PLUGIN_RUN_STREAM_ERROR_KEY)
        .map(|message| {
            stream_error(
                StreamErrorCode::ExecutionFailed,
                message.clone(),
                LEGACY_PLUGIN_ORIGIN.to_string(),
            )
        })
}

/// Code name without the `STREAM_ERROR_CODE_` prefix, matching the server's wording. Unknown
/// codes from a newer server are shown by number.
pub(crate) fn code_name(code: i32) -> String {
    StreamErrorCode::try_from(code).map_or_else(
        |_| code.to_string(),
        |code| {
            code.as_str_name()
                .trim_start_matches(STREAM_ERROR_CODE_PREFIX)
                .to_string()
        },
    )
}

/// The present fields of a typed problem, in RFC 9457 order. Empty when the problem has no type,
/// since it then identifies nothing; a zero status means the producer gave none and is omitted.
pub(crate) fn problem_fields(problem: &ProblemDetails) -> Vec<(&'static str, serde_json::Value)> {
    if problem.r#type.is_empty() {
        return Vec::new();
    }
    let mut fields = vec![("type", serde_json::json!(problem.r#type))];
    if problem.status != 0 {
        fields.push(("status", serde_json::json!(problem.status)));
    }
    for (name, value) in [
        ("title", &problem.title),
        ("detail", &problem.detail),
        ("instance", &problem.instance),
    ] {
        if let Some(value) = value {
            fields.push((name, serde_json::json!(value)));
        }
    }
    fields
}

/// `" (problem: type=.., status=..)"` for a typed problem, or empty when there is none.
fn problem_suffix(error: &StreamError) -> String {
    let fields = error
        .problem
        .as_ref()
        .map(problem_fields)
        .unwrap_or_default();
    if fields.is_empty() {
        return String::new();
    }
    let fields: Vec<String> = fields
        .into_iter()
        .map(|(name, value)| {
            let value = value
                .as_str()
                .map_or_else(|| value.to_string(), str::to_owned);
            format!("{name}={value}")
        })
        .collect();
    format!(" (problem: {})", fields.join(", "))
}

/// Human-readable form `"{origin} {CODE}: {message}"`, matching the server's wording, followed by
/// the structured problem when the failed component reported one.
pub fn describe_stream_error(error: &StreamError) -> String {
    format!(
        "{} {}: {}{}",
        error.origin,
        code_name(error.code),
        error.message,
        problem_suffix(error)
    )
}

/// The cause of a failed job result, `"{origin} {CODE}"` plus the problem when present. The
/// message is left out because the result's output already carries the same diagnostic.
pub(crate) fn describe_classification(error: &StreamError) -> String {
    format!(
        "{} {}{}",
        error.origin,
        code_name(error.code),
        problem_suffix(error)
    )
}

/// Diagnostic of a failed (non-streaming) job result: the output text, followed by the failure
/// classification when the worker reported one. Results without it (older workers, results read
/// back from the RDB) keep the bare diagnostic.
pub(crate) fn describe_job_failure(data: &JobResultData) -> String {
    let mut diagnostic = data
        .output
        .as_ref()
        .map(|output| String::from_utf8_lossy(&output.items).into_owned())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| format!("no output (status: {})", data.status().as_str_name()));
    if let Some(error) = &data.error {
        diagnostic.push_str(" [");
        diagnostic.push_str(&describe_classification(error));
        diagnostic.push(']');
    }
    diagnostic
}

/// The error a collector returns for a failed stream.
pub fn stream_failure_error(error: &StreamError) -> anyhow::Error {
    anyhow::anyhow!(
        "stream terminated with error: {}",
        describe_stream_error(error)
    )
}

/// The job status a failed stream maps to. Only cancellation and invalid input are
/// distinguished; every other failure (including unknown codes) is a non-retried failure.
pub fn result_status_for_stream_error(error: &StreamError) -> ResultStatus {
    match StreamErrorCode::try_from(error.code) {
        Ok(StreamErrorCode::Cancelled) => ResultStatus::Cancelled,
        Ok(StreamErrorCode::InvalidArgument) => ResultStatus::OtherError,
        _ => ResultStatus::MaxRetry,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobworkerp::data::ResultOutput;
    use std::collections::HashMap;

    fn trailer_with(key: &str, value: &str) -> Trailer {
        Trailer {
            metadata: HashMap::from([(key.to_string(), value.to_string())]),
            ..Default::default()
        }
    }

    fn typed_error(code: i32, message: &str, origin: &str) -> StreamError {
        StreamError {
            code,
            message: message.to_string(),
            origin: origin.to_string(),
            ..Default::default()
        }
    }

    // The typed field wins over the deprecated JSON mirror when both are present.
    #[test]
    fn stream_error_from_trailer_prefers_the_typed_error_over_json() {
        let error = typed_error(StreamErrorCode::Cancelled as i32, "cancelled", "WORKER");
        let mut trailer = trailer_with(
            COMMON_STREAM_ERROR_KEY,
            r#"{"version":1,"code":"TIMEOUT","message":"idle","origin":"SANDBOX"}"#,
        );
        trailer.error = Some(error.clone());
        assert_eq!(stream_error_from_trailer(&trailer), Some(error));
    }

    // An unknown code from a newer server is still a failure.
    #[test]
    fn stream_error_from_trailer_keeps_unknown_typed_codes() {
        let error = typed_error(99, "future", "X");
        let trailer = Trailer {
            error: Some(error.clone()),
            ..Default::default()
        };
        assert_eq!(stream_error_from_trailer(&trailer), Some(error));
    }

    // A server (or grpc-front) that drops the typed field still reports failures via JSON.
    #[test]
    fn stream_error_from_trailer_reads_the_json_mirror() {
        let trailer = trailer_with(
            COMMON_STREAM_ERROR_KEY,
            r#"{"version":1,"code":"TIMEOUT","message":"Sandbox command was idle","origin":"SANDBOX"}"#,
        );
        assert_eq!(
            stream_error_from_trailer(&trailer),
            Some(typed_error(
                StreamErrorCode::Timeout as i32,
                "Sandbox command was idle",
                "SANDBOX"
            ))
        );
    }

    #[test]
    fn stream_error_from_trailer_maps_unknown_json_codes_to_unspecified() {
        let trailer = trailer_with(
            COMMON_STREAM_ERROR_KEY,
            r#"{"version":1,"code":"SOMETHING_NEW","message":"m","origin":"O"}"#,
        );
        assert_eq!(
            stream_error_from_trailer(&trailer),
            Some(typed_error(StreamErrorCode::Unspecified as i32, "m", "O"))
        );
    }

    // A malformed JSON value is still a failure signal: keep the raw value as the message.
    #[test]
    fn stream_error_from_trailer_keeps_malformed_json_raw() {
        let trailer = trailer_with(COMMON_STREAM_ERROR_KEY, "not json");
        assert_eq!(
            stream_error_from_trailer(&trailer),
            Some(typed_error(
                StreamErrorCode::Unspecified as i32,
                "not json",
                "UNKNOWN"
            ))
        );
    }

    // Plugin/idle-timeout failures from older workers carry only the plain-text plugin key.
    #[test]
    fn stream_error_from_trailer_reads_the_legacy_plugin_key() {
        let trailer = trailer_with(
            PLUGIN_RUN_STREAM_ERROR_KEY,
            "stream idle timeout: no chunk emitted within 1000ms",
        );
        assert_eq!(
            stream_error_from_trailer(&trailer),
            Some(typed_error(
                StreamErrorCode::ExecutionFailed as i32,
                "stream idle timeout: no chunk emitted within 1000ms",
                "PLUGIN"
            ))
        );
    }

    #[test]
    fn stream_error_from_trailer_is_none_for_clean_end() {
        assert_eq!(stream_error_from_trailer(&Trailer::default()), None);
        assert_eq!(
            stream_error_from_trailer(&trailer_with("other", "value")),
            None
        );
    }

    #[test]
    fn result_status_follows_the_stream_error_code() {
        let status = |code: i32| result_status_for_stream_error(&typed_error(code, "", ""));
        assert_eq!(
            status(StreamErrorCode::Cancelled as i32),
            ResultStatus::Cancelled
        );
        assert_eq!(
            status(StreamErrorCode::InvalidArgument as i32),
            ResultStatus::OtherError
        );
        assert_eq!(
            status(StreamErrorCode::Timeout as i32),
            ResultStatus::MaxRetry
        );
        assert_eq!(
            status(StreamErrorCode::ExecutionFailed as i32),
            ResultStatus::MaxRetry
        );
        assert_eq!(
            status(StreamErrorCode::Unspecified as i32),
            ResultStatus::MaxRetry
        );
        assert_eq!(status(99), ResultStatus::MaxRetry);
    }

    #[test]
    fn describe_stream_error_names_origin_and_code() {
        assert_eq!(
            describe_stream_error(&typed_error(
                StreamErrorCode::ExecutionFailed as i32,
                "exit 3",
                "COMMAND"
            )),
            "COMMAND EXECUTION_FAILED: exit 3"
        );
        assert_eq!(describe_stream_error(&typed_error(99, "m", "X")), "X 99: m");
    }

    fn full_problem() -> ProblemDetails {
        ProblemDetails {
            r#type: "https://open-workflow-specification.org/spec/1.0.0/errors/timeout".to_string(),
            status: 408,
            title: Some("Task timed out".to_string()),
            detail: Some("no response in 30s".to_string()),
            instance: Some("/do/0/fetch".to_string()),
        }
    }

    fn with_problem(mut error: StreamError, problem: ProblemDetails) -> StreamError {
        error.problem = Some(problem);
        error
    }

    fn typed_problem(problem: ProblemDetails) -> StreamError {
        with_problem(typed_error(0, "", ""), problem)
    }

    #[test]
    fn problem_suffix_lists_every_present_field() {
        assert_eq!(
            problem_suffix(&typed_problem(full_problem())),
            " (problem: type=https://open-workflow-specification.org/spec/1.0.0/errors/timeout, \
             status=408, title=Task timed out, detail=no response in 30s, instance=/do/0/fetch)"
        );
    }

    // Producers may omit the optional fields and report status 0 for "none given".
    #[test]
    fn problem_suffix_skips_absent_fields_and_zero_status() {
        let problem = ProblemDetails {
            r#type: "urn:custom".to_string(),
            ..Default::default()
        };
        assert_eq!(
            problem_suffix(&typed_problem(problem)),
            " (problem: type=urn:custom)"
        );
    }

    // A problem without a type identifies nothing, so it adds no noise.
    #[test]
    fn problem_suffix_is_empty_without_a_typed_problem() {
        let problem = ProblemDetails {
            status: 500,
            title: Some("t".to_string()),
            ..Default::default()
        };
        assert_eq!(problem_suffix(&typed_problem(problem)), "");
        assert_eq!(problem_suffix(&typed_error(0, "", "")), "");
    }

    #[test]
    fn describe_stream_error_appends_the_problem() {
        let error = with_problem(
            typed_error(StreamErrorCode::Timeout as i32, "timed out", "WORKFLOW"),
            ProblemDetails {
                r#type: "urn:t".to_string(),
                instance: Some("/do/1".to_string()),
                ..Default::default()
            },
        );
        assert_eq!(
            describe_stream_error(&error),
            "WORKFLOW TIMEOUT: timed out (problem: type=urn:t, instance=/do/1)"
        );
    }

    // The message is left out: for a job result it duplicates the diagnostic in the output.
    #[test]
    fn describe_classification_omits_the_message() {
        assert_eq!(
            describe_classification(&typed_error(
                StreamErrorCode::Timeout as i32,
                "connect timeout",
                "HTTP_REQUEST"
            )),
            "HTTP_REQUEST TIMEOUT"
        );
        let error = with_problem(
            typed_error(StreamErrorCode::ExecutionFailed as i32, "m", "WORKFLOW"),
            ProblemDetails {
                r#type: "urn:t".to_string(),
                ..Default::default()
            },
        );
        assert_eq!(
            describe_classification(&error),
            "WORKFLOW EXECUTION_FAILED (problem: type=urn:t)"
        );
    }

    fn failed_result(output: Option<&str>, error: Option<StreamError>) -> JobResultData {
        JobResultData {
            status: ResultStatus::MaxRetry as i32,
            output: output.map(|o| ResultOutput {
                items: o.as_bytes().to_vec(),
            }),
            error,
            ..Default::default()
        }
    }

    // Results from older workers or read back from the RDB carry no classification.
    #[test]
    fn describe_job_failure_without_classification_is_the_diagnostic() {
        assert_eq!(
            describe_job_failure(&failed_result(Some("exit 1"), None)),
            "exit 1"
        );
    }

    #[test]
    fn describe_job_failure_appends_the_classification() {
        let error = typed_error(StreamErrorCode::Timeout as i32, "x", "HTTP_REQUEST");
        assert_eq!(
            describe_job_failure(&failed_result(Some("connect timeout"), Some(error))),
            "connect timeout [HTTP_REQUEST TIMEOUT]"
        );
    }

    #[test]
    fn describe_job_failure_includes_the_workflow_problem() {
        let error = with_problem(
            typed_error(StreamErrorCode::Timeout as i32, "x", "WORKFLOW"),
            full_problem(),
        );
        let described = describe_job_failure(&failed_result(Some("runtime error"), Some(error)));
        assert!(
            described.starts_with("runtime error [WORKFLOW TIMEOUT (problem: type="),
            "got: {described}"
        );
        assert!(
            described.contains("instance=/do/0/fetch"),
            "got: {described}"
        );
    }

    #[test]
    fn describe_job_failure_names_the_status_when_output_is_missing_or_empty() {
        assert_eq!(
            describe_job_failure(&failed_result(None, None)),
            "no output (status: MAX_RETRY)"
        );
        assert_eq!(
            describe_job_failure(&failed_result(Some(""), None)),
            "no output (status: MAX_RETRY)"
        );
    }
}
