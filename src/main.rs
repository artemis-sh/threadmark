mod api;
mod auth;
mod capability;
mod config;
mod error;
mod files;
mod ids;
mod model;
mod object_store;
mod store;
mod uploads;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "threadmark=info,tower_http=info".into()),
        )
        .init();

    let config = config::Config::from_env()?;
    let auth = auth::Authenticator::from_config(&config)
        .await
        .context("initialize authentication")?;
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.database_url)
        .await
        .context("connect to Postgres")?;

    sqlx::migrate!()
        .run(&pool)
        .await
        .context("run migrations")?;

    let object_store = object_store::ObjectStore::new(&config);
    object_store.ping().await.context("access S3 bucket")?;
    if !object_store.versioning_enabled().await? {
        anyhow::bail!("S3 bucket versioning must be Enabled");
    }
    files::cleanup_deletions(&pool, &object_store)
        .await
        .context("clean up pending file deletions")?;
    uploads::cleanup_expired(&pool, &object_store).await?;
    let cleanup_pool = pool.clone();
    let cleanup_store = object_store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(error) = files::cleanup_deletions(&cleanup_pool, &cleanup_store).await {
                tracing::error!(?error, "file deletion outbox pass failed");
            }
            if let Err(error) = uploads::cleanup_expired(&cleanup_pool, &cleanup_store).await {
                tracing::error!(?error, "expired direct-upload cleanup pass failed");
            }
        }
    });

    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .with_context(|| format!("bind {}", config.listen_addr))?;
    info!(address = %config.listen_addr, "Threadmark listening");
    axum::serve(
        listener,
        api::router(api::AppState {
            pool,
            object_store,
            config,
            auth,
        }),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .context("serve HTTP")?;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
