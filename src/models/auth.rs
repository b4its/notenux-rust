use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AuthUser {
    pub id: u64,
}

#[derive(Debug, FromRow)]
pub struct TokenRow {
    pub tokenable_id: u64,
    pub token: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, FromRow)]
pub struct SessionRow {
    pub user_id: Option<u64>,
}

#[derive(Debug)]
pub enum AuthError {
    MissingCredentials,
    InvalidToken,
    DatabaseError,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            AuthError::MissingCredentials => (
                StatusCode::UNAUTHORIZED,
                "Authentication required. Please provide a Sanctum bearer token.",
            ),
            AuthError::InvalidToken => (
                StatusCode::UNAUTHORIZED,
                "Invalid or expired Sanctum credentials.",
            ),
            AuthError::DatabaseError => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Database error while validating authentication token.",
            ),
        };

        (
            status,
            Json(serde_json::json!({
                "status": "error",
                "message": msg
            })),
        )
            .into_response()
    }
}
