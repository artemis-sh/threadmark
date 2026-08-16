use axum::{
    Json,
    http::{StatusCode, header},
    response::IntoResponse,
};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Authentication required.")]
    Unauthorized,
    #[error("Permission denied.")]
    Forbidden,
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{message}")]
    CodedConflict { code: &'static str, message: String },
    #[error("{0}")]
    PayloadTooLarge(String),
    #[error("object storage operation failed")]
    ObjectStore(#[source] anyhow::Error),
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, code) = match &self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::BadRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            Self::CodedConflict { code, .. } => (StatusCode::CONFLICT, *code),
            Self::PayloadTooLarge(_) => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
            Self::ObjectStore(error) => {
                tracing::error!(?error, "object storage request failed");
                (StatusCode::BAD_GATEWAY, "object_store_error")
            }
            Self::Database(error) => {
                tracing::error!(?error, "database request failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
            }
        };
        let message = match self {
            Self::Database(_) | Self::ObjectStore(_) => "Storage operation failed.".to_owned(),
            other => other.to_string(),
        };
        let mut response = (
            status,
            Json(json!({ "error": { "code": code, "message": message } })),
        )
            .into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                "Bearer".parse().expect("static header value"),
            );
        }
        response
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
