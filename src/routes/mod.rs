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
use tower::limit::GlobalConcurrencyLimitLayer;

const LARGE_NOTE_BODY_LIMIT: usize = 8 * 1024 * 1024;

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
    // Share the limit across all large writes while keeping GET requests independent.
    let large_writes = GlobalConcurrencyLimitLayer::new(2);
    Router::new()
        // Health
        .route("/health", get(health::health_handler))
        .route("/api/health", get(health_handler_alias))
        // Notes CRUD & Search
        .route("/api/notes", get(notes::list_notes).merge(
            post(notes::create_note)
                .layer::<_, std::convert::Infallible>(DefaultBodyLimit::max(LARGE_NOTE_BODY_LIMIT))
                .layer(large_writes.clone())
        ))
        .route("/api/notes/search", get(search::search_notes))
        .route(
            "/api/notes/{id}",
            get(notes::get_note).delete(notes::delete_note).merge(
                put(notes::update_note)
                    .layer::<_, std::convert::Infallible>(DefaultBodyLimit::max(LARGE_NOTE_BODY_LIMIT))
                    .layer(large_writes.clone())
            ),
        )
        .route("/api/notes/{id}/pin", post(notes::toggle_pin))
        .route("/api/notes/{id}/favorite", post(notes::toggle_favorite))
        .route("/api/notes/{id}/duplicate", post(notes::duplicate_note))
        // Chunks
        .route(
            "/api/notes/{id}/chunks",
            get(chunks::get_chunks).merge(
                put(chunks::update_chunks)
                    .layer::<_, std::convert::Infallible>(DefaultBodyLimit::max(LARGE_NOTE_BODY_LIMIT))
                    .layer(large_writes.clone())
            ),
        )
        .route("/api/notes/{id}/chunks/verify", get(chunks::verify_chunks))
        .route("/api/notes/{id}/chunks/reorder", post(chunks::reorder_chunks))
        .route(
            "/api/notes/{id}/chunks/{chunk_id}",
            put(chunks::update_single_chunk)
                .layer::<_, std::convert::Infallible>(DefaultBodyLimit::max(16 * 1024 * 1024))
                .layer(large_writes)
                .delete(chunks::delete_chunk),
        )
}

async fn health_handler_alias(
    state: axum::extract::State<AppState>,
) -> impl IntoResponse {
    health::health_handler(state).await
}
