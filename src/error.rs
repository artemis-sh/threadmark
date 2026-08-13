use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, code) = match &self {
            Self::BadRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            Self::Database(error) => {
                tracing::error!(?error, "database request failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
            }
        };
        let message = match self {
            Self::Database(_) => "Internal server error.".to_owned(),
            other => other.to_string(),
        };
        (
            status,
            Json(json!({ "error": { "code": code, "message": message } })),
        )
            .into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
