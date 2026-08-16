//! Assumptions the shared SQL layer depends on.
//!
//! Threadmark writes each query once and runs it on both PostgreSQL and SQLite.
//! That is only sound because SQLite treats `$N` as a numbered parameter with
//! the same positional binding semantics Postgres uses. These tests pin that
//! behaviour so a dependency bump cannot silently break every query.

use chrono::{Duration, Utc};
use sqlx::SqlitePool;

async fn pool() -> SqlitePool {
    SqlitePool::connect("sqlite::memory:").await.unwrap()
}

#[tokio::test]
async fn dollar_placeholders_bind_positionally() {
    let pool = pool().await;
    sqlx::query("CREATE TABLE t (a text, b text)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t (a, b) VALUES ($1, $2)")
        .bind("one")
        .bind("two")
        .execute(&pool)
        .await
        .unwrap();
    let row: (String, String) = sqlx::query_as("SELECT a, b FROM t WHERE a = $1 AND b = $2")
        .bind("one")
        .bind("two")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row, ("one".into(), "two".into()));
}

#[tokio::test]
async fn repeated_placeholder_consumes_one_bind() {
    // Postgres reuses `$1` without a second bind. SQLite must agree, or every
    // query that references a parameter twice would bind off by one.
    let pool = pool().await;
    sqlx::query("CREATE TABLE t (a text, b text)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t (a, b) VALUES ($1, $2)")
        .bind("one")
        .bind("two")
        .execute(&pool)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM t WHERE a = $1 OR b = $1")
        .bind("one")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 1);
}

#[tokio::test]
async fn timestamps_compare_and_order_correctly() {
    // Upload expiry and outbox scheduling compare timestamps in SQL. SQLite
    // stores them as text, so the encoding must sort chronologically.
    let pool = pool().await;
    sqlx::query("CREATE TABLE t (id text, at timestamptz)")
        .execute(&pool)
        .await
        .unwrap();
    let now = Utc::now();
    for (id, at) in [
        ("past", now - Duration::hours(1)),
        ("future", now + Duration::hours(1)),
        ("far", now + Duration::days(400)),
    ] {
        sqlx::query("INSERT INTO t (id, at) VALUES ($1, $2)")
            .bind(id)
            .bind(at)
            .execute(&pool)
            .await
            .unwrap();
    }
    let expired: Vec<String> = sqlx::query_scalar("SELECT id FROM t WHERE at <= $1 ORDER BY at")
        .bind(now)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(expired, vec!["past".to_string()]);

    let ordered: Vec<String> = sqlx::query_scalar("SELECT id FROM t ORDER BY at DESC")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(ordered, vec!["far", "future", "past"]);
}

#[tokio::test]
async fn returning_and_on_conflict_are_supported() {
    let pool = pool().await;
    sqlx::query("CREATE TABLE t (id text PRIMARY KEY, n bigint NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    let id: String = sqlx::query_scalar("INSERT INTO t (id, n) VALUES ($1, $2) RETURNING id")
        .bind("a")
        .bind(1i64)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(id, "a");
    let affected = sqlx::query("INSERT INTO t (id, n) VALUES ($1, $2) ON CONFLICT DO NOTHING")
        .bind("a")
        .bind(2i64)
        .execute(&pool)
        .await
        .unwrap()
        .rows_affected();
    assert_eq!(affected, 0);
}

/// The single-active-turn conflict is reported to clients as `active_turn_exists`
/// rather than a 500. PostgreSQL identifies the violated index by name; SQLite
/// exposes only the column list. These pin SQLite's side of that contract.
#[tokio::test]
async fn unique_violation_names_columns_not_the_index() {
    let pool = pool().await;
    sqlx::query("CREATE TABLE turns (id text PRIMARY KEY, conversation_id text, idempotency_key text, status text)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE UNIQUE INDEX turns_one_active_per_conversation_idx
         ON turns (conversation_id) WHERE status IN ('pending', 'streaming')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO turns VALUES ('t1', 'c1', 'k1', 'pending')")
        .execute(&pool)
        .await
        .unwrap();

    let error = sqlx::query("INSERT INTO turns VALUES ('t2', 'c1', 'k2', 'pending')")
        .execute(&pool)
        .await
        .unwrap_err();
    let db = error.as_database_error().unwrap();

    assert_eq!(db.constraint(), None, "SQLite exposes no constraint name");
    assert_eq!(db.code().as_deref(), Some("2067"));
    assert_eq!(
        db.message(),
        "UNIQUE constraint failed: turns.conversation_id"
    );
}

#[tokio::test]
async fn a_wider_index_on_the_same_column_reports_distinctly() {
    // Guards the exact-match discriminator: this must not be mistaken for the
    // single-active-turn index, which covers conversation_id alone.
    let pool = pool().await;
    sqlx::query(
        "CREATE TABLE turns (id text PRIMARY KEY, conversation_id text, idempotency_key text)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE UNIQUE INDEX turns_conversation_id_idempotency_key_idx
         ON turns (conversation_id, idempotency_key)",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO turns VALUES ('t1', 'c1', 'k1')")
        .execute(&pool)
        .await
        .unwrap();

    let error = sqlx::query("INSERT INTO turns VALUES ('t2', 'c1', 'k1')")
        .execute(&pool)
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().message(),
        "UNIQUE constraint failed: turns.conversation_id, turns.idempotency_key"
    );
}
