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
    #[error("UnknownError({0})")]
    UnknownError(String),
}

impl ClientError {
    /// Convert tonic::Status to ClientError based on gRPC status code
    pub fn from_tonic_status(status: tonic::Status) -> Self {
        let message = status.message().to_string();
        match status.code() {
            tonic::Code::NotFound => ClientError::NotFound(message),
            tonic::Code::InvalidArgument => ClientError::InvalidParameter(message),
            tonic::Code::AlreadyExists => ClientError::Conflict(message),
            tonic::Code::DeadlineExceeded => ClientError::TimeoutError(message),
            tonic::Code::Unauthenticated
            | tonic::Code::PermissionDenied
            | tonic::Code::Unavailable => ClientError::ExternalServiceError(message),
            tonic::Code::FailedPrecondition | tonic::Code::Internal => {
                ClientError::RuntimeError(message)
            }
            _ => ClientError::UnknownError(message),
        }
    }
}
