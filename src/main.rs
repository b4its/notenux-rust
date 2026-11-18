use be_notenux::{config::Config, create_app};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,be_notenux=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::from_env();
    tracing::info!("Starting Notenux Rust Backend API...");
    tracing::info!(
        "Database pool: max {} conns, min {} conns, acquire timeout {}s",
        config.max_db_connections,
        config.min_db_connections,
        config.acquire_timeout_secs
    );
    tracing::info!(
        "Bounded Moka cache limit: {} MB",
        config.cache_limit_bytes / (1024 * 1024)
    );

    let app = create_app(&config).await?;

    let addr = format!("{}:{}", config.host, config.port);
    tracing::info!("Notenux Backend API running at http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
