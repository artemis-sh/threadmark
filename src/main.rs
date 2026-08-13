mod api;
mod capability;
mod config;
mod error;
mod files;
mod ids;
mod model;
mod object_store;
mod store;

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
