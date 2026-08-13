mod api;
mod error;
mod ids;
mod model;
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

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL is required")?;
    let listen_addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8090".into());
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await
        .context("connect to Postgres")?;

    sqlx::migrate!()
        .run(&pool)
        .await
        .context("run migrations")?;

    let listener = TcpListener::bind(&listen_addr)
        .await
        .with_context(|| format!("bind {listen_addr}"))?;
    info!(address = %listen_addr, "Threadmark listening");
    axum::serve(listener, api::router(pool))
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
