use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub host: String,
    pub port: u16,
    pub cors_origin: String,
    pub cache_limit_bytes: u64,
    pub max_db_connections: u32,
    pub min_db_connections: u32,
    pub acquire_timeout_secs: u64,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "mysql://notenux:secret@127.0.0.1:39285/notenux".to_string());
        let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
        let port = env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8999);
        let cors_origin = env::var("CORS_ORIGIN")
            .unwrap_or_else(|_| "http://localhost:5173".to_string());
        let cache_limit_mb: u64 = env::var("CACHE_LIMIT_MB")
            .ok()
            .and_then(|m| m.parse().ok())
            .unwrap_or(45);
        // Strictly cap cache between 30MB and 50MB (default 45MB)
        let cache_limit_bytes = cache_limit_mb.clamp(30, 50) * 1024 * 1024;

        // Strictly cap DB connections to 5 max for shared hosting safety
        let max_db_connections = env::var("MAX_DB_CONNECTIONS")
            .ok()
            .and_then(|c| c.parse().ok())
            .unwrap_or(5)
            .min(5);

        let min_db_connections = env::var("MIN_DB_CONNECTIONS")
            .ok()
            .and_then(|c| c.parse().ok())
            .unwrap_or(1)
            .min(max_db_connections);

        let acquire_timeout_secs = env::var("ACQUIRE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);

        Self {
            database_url,
            host,
            port,
            cors_origin,
            cache_limit_bytes,
            max_db_connections,
            min_db_connections,
            acquire_timeout_secs,
        }
    }
}
