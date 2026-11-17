pub mod auth;
pub mod cache;
pub mod chunker;
pub mod config;
pub mod handlers;
pub mod models;

use axum::{
    http::{header, HeaderValue, Method},
    routing::{get, post, put},
    Router,
};
use cache::AppCache;
use config::Config;
use handlers::*;
use sqlx::mysql::MySqlPoolOptions;
use std::time::Duration;
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    trace::TraceLayer,
};

pub async fn create_app(config: &Config) -> Result<Router, Box<dyn std::error::Error>> {
    // Limit pool strictly to max_db_connections (5) for shared hosting safety
    let pool = MySqlPoolOptions::new()
        .max_connections(config.max_db_connections)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .idle_timeout(Duration::from_secs(300))
        .connect(&config.database_url)
        .await?;

    tracing::info!("Connected to MySQL database successfully.");

    // Bounded In-Memory LRU Cache with Moka
    let cache = AppCache::new(config.cache_limit_bytes);

    let state = AppState { pool, cache };

    // Setup CORS for Svelte frontend (http://localhost:5999)
    let cors = CorsLayer::new()
        .allow_origin([
            config.cors_origin.parse::<HeaderValue>()?,
            "http://127.0.0.1:5999".parse::<HeaderValue>()?,
            "http://localhost:5999".parse::<HeaderValue>()?,
        ])
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::ACCEPT,
            header::CONTENT_TYPE,
            header::COOKIE,
            header::HeaderName::from_static("x-dev-user-id"),
            header::HeaderName::from_static("x-session-id"),
            header::HeaderName::from_static("x-csrf-token"),
            header::HeaderName::from_static("x-requested-with"),
        ])
        .allow_credentials(true);

    // HTTP Compression with Gzip and Brotli
    let compression = CompressionLayer::new().gzip(true).br(true);

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/api/health", get(health_handler))
        .route("/api/notes", get(list_notes).post(create_note))
        .route(
            "/api/notes/{id}",
            get(get_note).put(update_note).delete(delete_note),
        )
        .route("/api/notes/{id}/pin", post(toggle_pin))
        .route("/api/notes/{id}/favorite", post(toggle_favorite))
        .route("/api/notes/{id}/chunks", get(get_chunks))
        .route("/api/notes/{id}/chunks/verify", get(verify_chunks))
        .route("/api/notes/{id}/chunks/reorder", post(reorder_chunks))
        .route(
            "/api/notes/{id}/chunks/{chunk_id}",
            put(update_chunk).delete(delete_chunk),
        )
        .layer(cors)
        .layer(compression)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    Ok(app)
}
