pub mod chunks;
pub mod health;
pub mod notes;
pub mod search;

use axum::{
    extract::DefaultBodyLimit,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use crate::AppState;

pub struct ApiError(pub StatusCode, pub String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(serde_json::json!({
                "status": "error",
                "message": self.1
            })),
        )
            .into_response()
    }
}

pub fn create_routes() -> Router<AppState> {
    Router::new()
        // Health
        .route("/health", get(health::health_handler))
        .route("/api/health", get(health_handler_alias))
        // Notes CRUD & Search
        .route("/api/notes", get(notes::list_notes).post(notes::create_note))
        .route("/api/notes/search", get(search::search_notes))
        .route(
            "/api/notes/{id}",
            get(notes::get_note)
                .put(notes::update_note)
                .delete(notes::delete_note),
        )
        .route("/api/notes/{id}/pin", post(notes::toggle_pin))
        .route("/api/notes/{id}/favorite", post(notes::toggle_favorite))
        .route("/api/notes/{id}/duplicate", post(notes::duplicate_note))
        // Chunks
        .route(
            "/api/notes/{id}/chunks",
            get(chunks::get_chunks).put(chunks::update_chunks),
        )
        .route("/api/notes/{id}/chunks/verify", get(chunks::verify_chunks))
        .route("/api/notes/{id}/chunks/reorder", post(chunks::reorder_chunks))
        .route(
            "/api/notes/{id}/chunks/{chunk_id}",
            put(chunks::update_single_chunk).delete(chunks::delete_chunk)
                .layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
        )
}

async fn health_handler_alias(
    state: axum::extract::State<AppState>,
) -> impl IntoResponse {
    health::health_handler(state).await
}
