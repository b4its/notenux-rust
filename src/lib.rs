pub mod auth;
pub mod cache;
pub mod chunker;
pub mod config;
pub mod db;
pub mod models;
pub mod routes;

use axum::{
    http::{header, HeaderValue, Method},
    Router,
};
use cache::AppCache;
use config::Config;
use sqlx::MySqlPool;
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer,
    trace::TraceLayer,
};

#[derive(Clone)]
pub struct AppState {
    pub pool: MySqlPool,
    pub cache: AppCache,
}

pub async fn create_app(config: &Config) -> Result<Router, Box<dyn std::error::Error>> {
    // 1. Initialize DB Pool via modular db module (max 5 conns, 5s timeout)
    let pool = db::init_pool(config).await?;

    // 2. Bounded In-Memory Cache with Moka (45 MB limit, 30 min TTL, initial chunk priority)
    let cache = AppCache::new(config.cache_limit_bytes);

    let state = AppState { pool, cache };

    // 3. Network Middleware: CorsLayer (http://localhost:5173, https://app.notenux.oryphem.com)
    let mut allowed_origins = Vec::new();
    for orig in [
        "http://localhost:5173",
        "https://app.notenux.oryphem.com",
        "http://127.0.0.1:5173",
        "http://localhost:5999",
        "http://127.0.0.1:5999",
        config.cors_origin.as_str(),
    ] {
        if let Ok(hv) = orig.parse::<HeaderValue>() {
            if !allowed_origins.contains(&hv) {
                allowed_origins.push(hv);
            }
        }
    }

    let cors = CorsLayer::new()
        .allow_origin(allowed_origins)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::ACCEPT,
            header::COOKIE,
            header::HeaderName::from_static("x-dev-user-id"),
            header::HeaderName::from_static("x-session-id"),
            header::HeaderName::from_static("x-csrf-token"),
            header::HeaderName::from_static("x-requested-with"),
        ])
        .allow_credentials(true);

    // 4. Network Middleware: CompressionLayer (Gzip & Brotli)
    let compression = CompressionLayer::new().gzip(true).br(true);

    let app = routes::create_routes()
        .layer(cors)
        .layer(compression)
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    Ok(app)
}
