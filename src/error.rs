use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("NotFound({0})")]
    NotFound(String),
    #[error("Conflict({0})")]
    Conflict(String),
    #[error("ParseError({0})")]
    ParseError(String),
    #[error("InvalidParameter({0})")]
    InvalidParameter(String),
    #[error("TimeoutError({0})")]
    TimeoutError(String),
    #[error("ExternalServiceError({0})")]
    ExternalServiceError(String),
    #[error("RuntimeError({0})")]
    RuntimeError(String),
    #[error("UnimplementedError({0})")]
    UnimplementedError(String),
    #[error("UnknownError({0})")]
    UnknownError(String),
}

impl ClientError {
    /// Convert tonic::Status to ClientError based on gRPC status code
    pub fn from_tonic_status(status: tonic::Status) -> Self {
        let message = status.message().to_string();
        match status.code() {
            tonic::Code::NotFound => ClientError::NotFound(message),
            tonic::Code::InvalidArgument | tonic::Code::OutOfRange => {
                ClientError::InvalidParameter(message)
            }
            tonic::Code::AlreadyExists | tonic::Code::Aborted => ClientError::Conflict(message),
            tonic::Code::DeadlineExceeded => ClientError::TimeoutError(message),
            tonic::Code::Unauthenticated
            | tonic::Code::PermissionDenied
            | tonic::Code::Unavailable
            | tonic::Code::ResourceExhausted => ClientError::ExternalServiceError(message),
            tonic::Code::FailedPrecondition
            | tonic::Code::Internal
            | tonic::Code::Cancelled
            | tonic::Code::DataLoss => ClientError::RuntimeError(message),
            tonic::Code::Unimplemented => ClientError::UnimplementedError(message),
            _ => ClientError::UnknownError(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_tonic_status_not_found() {
        let status = tonic::Status::not_found("resource not found");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::NotFound(msg) if msg == "resource not found"));
    }

    #[test]
    fn test_from_tonic_status_invalid_argument() {
        let status = tonic::Status::invalid_argument("bad input");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::InvalidParameter(msg) if msg == "bad input"));
    }

    #[test]
    fn test_from_tonic_status_out_of_range() {
        let status = tonic::Status::out_of_range("value out of range");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::InvalidParameter(_)));
    }

    #[test]
    fn test_from_tonic_status_already_exists() {
        let status = tonic::Status::already_exists("resource exists");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::Conflict(_)));
    }

    #[test]
    fn test_from_tonic_status_aborted() {
        let status = tonic::Status::aborted("transaction aborted");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::Conflict(_)));
    }

    #[test]
    fn test_from_tonic_status_deadline_exceeded() {
        let status = tonic::Status::deadline_exceeded("timeout");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::TimeoutError(_)));
    }

    #[test]
    fn test_from_tonic_status_unauthenticated() {
        let status = tonic::Status::unauthenticated("not authenticated");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::ExternalServiceError(_)));
    }

    #[test]
    fn test_from_tonic_status_permission_denied() {
        let status = tonic::Status::permission_denied("access denied");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::ExternalServiceError(_)));
    }

    #[test]
    fn test_from_tonic_status_unavailable() {
        let status = tonic::Status::unavailable("service unavailable");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::ExternalServiceError(_)));
    }

    #[test]
    fn test_from_tonic_status_resource_exhausted() {
        let status = tonic::Status::resource_exhausted("rate limited");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::ExternalServiceError(_)));
    }

    #[test]
    fn test_from_tonic_status_failed_precondition() {
        let status = tonic::Status::failed_precondition("precondition failed");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::RuntimeError(_)));
    }

    #[test]
    fn test_from_tonic_status_internal() {
        let status = tonic::Status::internal("internal error");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::RuntimeError(_)));
    }

    #[test]
    fn test_from_tonic_status_cancelled() {
        let status = tonic::Status::cancelled("operation cancelled");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::RuntimeError(_)));
    }

    #[test]
    fn test_from_tonic_status_data_loss() {
        let status = tonic::Status::data_loss("data corrupted");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::RuntimeError(_)));
    }

    #[test]
    fn test_from_tonic_status_unimplemented() {
        let status = tonic::Status::unimplemented("method not implemented");
        let error = ClientError::from_tonic_status(status);
        assert!(
            matches!(error, ClientError::UnimplementedError(msg) if msg == "method not implemented")
        );
    }

    #[test]
    fn test_from_tonic_status_unknown() {
        let status = tonic::Status::unknown("unknown error");
        let error = ClientError::from_tonic_status(status);
        assert!(matches!(error, ClientError::UnknownError(_)));
    }
}
