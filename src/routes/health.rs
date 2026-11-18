use axum::{extract::State, response::IntoResponse, Json};
use crate::AppState;

pub async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let ping = sqlx::query("SELECT 1").execute(&state.pool).await.is_ok();
    Json(serde_json::json!({
        "status": if ping { "healthy" } else { "degraded" },
        "service": "be-notenux",
        "database_connected": ping,
        "engine": "rust-heavy-lifter",
        "cache": "moka-bounded-lru",
    }))
}
