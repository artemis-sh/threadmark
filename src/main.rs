mod api;
mod auth;
mod blob;
mod capability;
mod config;
mod db;
mod error;
mod files;
mod ids;
mod model;
mod store;
mod uploads;

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use tokio::net::TcpListener;
use tracing::info;

use crate::store::{SqlStore, Stores};

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
    let store = connect_store(&config).await?;

    blob::validate(&config)?;
    let object_store = blob::ObjectStore::new(&config);
    object_store.ensure_ready().await?;
    store
        .cleanup_deletions(&object_store)
        .await
        .context("clean up pending file deletions")?;
    store.cleanup_expired(&object_store).await?;
    let cleanup_store = store.clone();
    let cleanup_objects = object_store.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(error) = cleanup_store.cleanup_deletions(&cleanup_objects).await {
                tracing::error!(?error, "file deletion outbox pass failed");
            }
            if let Err(error) = cleanup_store.cleanup_expired(&cleanup_objects).await {
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
            store,
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

/// Connect the store named by `DATABASE_URL` and bring its schema up to date.
///
/// The URL scheme selects the engine, so there is no separate backend setting to
/// keep consistent with it.
async fn connect_store(config: &config::Config) -> anyhow::Result<Stores> {
    let url = &config.database_url;
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(url)
            .await
            .context("connect to Postgres")?;
        sqlx::migrate!("./migrations/postgres")
            .run(&pool)
            .await
            .context("run Postgres migrations")?;
        info!(backend = "postgres", "store ready");
        return Ok(Stores::Postgres(Arc::new(SqlStore::new(pool))));
    }
    if url.starts_with("sqlite:") {
        let options = SqliteConnectOptions::from_str(url)
            .context("DATABASE_URL is not a valid SQLite URL")?
            .create_if_missing(true)
            // WAL lets readers proceed during a write, which matters because
            // every write transaction here reads first.
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            .busy_timeout(std::time::Duration::from_millis(
                config.sqlite_busy_timeout_ms,
            ))
            .synchronous(if config.sqlite_synchronous_full {
                SqliteSynchronous::Full
            } else {
                SqliteSynchronous::Normal
            });
        // SQLite permits one writer at a time. A larger pool would not add write
        // throughput, and would only convert lock contention into idle
        // connections.
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .context("open SQLite database")?;
        sqlx::migrate!("./migrations/sqlite")
            .run(&pool)
            .await
            .context("run SQLite migrations")?;
        info!(
            backend = "sqlite",
            synchronous = if config.sqlite_synchronous_full {
                "full"
            } else {
                "normal"
            },
            "store ready"
        );
        return Ok(Stores::Sqlite(Arc::new(SqlStore::new(pool))));
    }
    anyhow::bail!("DATABASE_URL must start with postgres://, postgresql://, or sqlite:")
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
