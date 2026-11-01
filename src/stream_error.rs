//! The stream error contract: how a result stream's End trailer reports an in-band failure.
//!
//! `Trailer.error` is authoritative. Older servers (or a proxy that drops the typed field) report
//! failures only through deprecated metadata keys, which are read as fallbacks. Public so plugin
//! authors overriding `collect_stream` apply the same rules as the default implementation.

use crate::jobworkerp::data::{ResultStatus, StreamError, StreamErrorCode, Trailer};

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

/// Human-readable form `"{origin} {CODE}: {message}"`, matching the server's wording. Unknown
/// codes from a newer server are shown by number.
pub fn describe_stream_error(error: &StreamError) -> String {
    let code = StreamErrorCode::try_from(error.code).map_or_else(
        |_| error.code.to_string(),
        |code| {
            code.as_str_name()
                .trim_start_matches(STREAM_ERROR_CODE_PREFIX)
                .to_string()
        },
    );
    format!("{} {code}: {}", error.origin, error.message)
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
}
