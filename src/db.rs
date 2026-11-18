use crate::config::Config;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use std::time::Duration;

/// Initialize MySQL connection pool strictly configured for shared hosting safety:
/// - max_connections: 5 (prevent hitting shared hosting limits)
/// - min_connections: 1
/// - acquire_timeout: 5 seconds
pub async fn init_pool(config: &Config) -> Result<MySqlPool, sqlx::Error> {
    tracing::info!(
        "Initializing MySQL connection pool (max: {}, min: {}, acquire timeout: {}s)...",
        config.max_db_connections,
        config.min_db_connections,
        config.acquire_timeout_secs
    );

    let pool = MySqlPoolOptions::new()
        .max_connections(config.max_db_connections)
        .min_connections(config.min_db_connections)
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(300))
        .connect(&config.database_url)
        .await?;

    tracing::info!("MySQL connection pool initialized successfully.");
    Ok(pool)
}
