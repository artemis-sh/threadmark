//! Database backend abstraction.
//!
//! Threadmark supports PostgreSQL for production self-hosting and SQLite for
//! single-node deployments. Rather than maintaining two copies of every query,
//! the store is written once and generic over [`Backend`].
//!
//! This is only sound because SQLite treats `$N` as a numbered parameter with
//! the same positional binding semantics PostgreSQL uses, so the overwhelming
//! majority of queries are byte-identical on both engines. `tests/sqlite_dialect.rs`
//! pins that assumption. The handful of genuine differences — row locking,
//! transaction start, and advisory locking — are the associated constants and
//! items below.
//!
//! PostgreSQL is the governing tier: where the two engines cannot express the
//! same thing, the PostgreSQL behaviour is the specification and SQLite
//! conforms to it.

use sqlx::error::DatabaseError;
use sqlx::{Postgres, Sqlite};

/// A unique index whose violation Threadmark reports as a specific API conflict
/// rather than an internal error.
///
/// The two engines identify a violated index differently, so callers ask
/// [`Backend::violated`] rather than inspecting the error themselves.
/// PostgreSQL reports the index name through `DatabaseError::constraint()`.
/// SQLite returns `None` there and names only the columns in its message, for
/// example `UNIQUE constraint failed: turns.conversation_id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UniqueIndex {
    /// At most one `pending` or `streaming` turn per conversation.
    ActiveTurnPerConversation,
}

impl UniqueIndex {
    const fn postgres_constraint(self) -> &'static str {
        match self {
            Self::ActiveTurnPerConversation => "turns_one_active_per_conversation_idx",
        }
    }

    /// The exact column list SQLite names for this index.
    ///
    /// Matched exactly rather than by substring so that a wider index over the
    /// same leading column, such as
    /// `turns (conversation_id, idempotency_key)`, is not misreported.
    const fn sqlite_columns(self) -> &'static str {
        match self {
            Self::ActiveTurnPerConversation => "turns.conversation_id",
        }
    }
}

/// SQLite extended result code for `SQLITE_CONSTRAINT_UNIQUE`.
const SQLITE_CONSTRAINT_UNIQUE: &str = "2067";

/// A SQL engine Threadmark can run its store against.
pub trait Backend: sqlx::Database {
    /// Row-lock clause for a read that will be written later in the same
    /// transaction.
    ///
    /// Empty on SQLite: a write transaction begun with `BEGIN IMMEDIATE` already
    /// holds the database write lock, so no per-row escalation exists or is
    /// needed.
    const FOR_UPDATE: &'static str;

    /// Shared row-lock clause, used to pin file rows against concurrent deletion
    /// while a turn that references them is being recorded.
    ///
    /// Empty on SQLite for the same reason as [`Self::FOR_UPDATE`].
    const FOR_KEY_SHARE: &'static str;

    /// Statement used to open a write transaction.
    ///
    /// SQLite must use `BEGIN IMMEDIATE`. A bare `BEGIN` is `DEFERRED`, which
    /// starts as a reader and can fail with `SQLITE_BUSY_SNAPSHOT` when it later
    /// upgrades to a writer — a failure mode SQLite deliberately does not route
    /// through the busy handler, so `busy_timeout` never applies. Every
    /// Threadmark write transaction reads before it writes, so every one is
    /// exposed without this.
    const BEGIN_WRITE: Option<&'static str>;

    /// Statement acquiring a transaction-scoped advisory lock on a caller-supplied
    /// key, or `None` when the engine has no such concept.
    ///
    /// PostgreSQL allows concurrent writers, so turn-start idempotency needs an
    /// explicit lock to serialize same-key requests. SQLite's write lock already
    /// serializes all writers, making the advisory lock redundant there.
    const ADVISORY_XACT_LOCK: Option<&'static str>;

    /// Whether `error` is a unique violation of `index`.
    fn violated(error: &dyn DatabaseError, index: UniqueIndex) -> bool;

    /// Rows changed by a statement.
    ///
    /// Both engines expose this, but `Database::QueryResult` is only bound by
    /// `Default + Extend`, so there is no shared method to call generically.
    fn rows_affected(result: &Self::QueryResult) -> u64;
}

impl Backend for Postgres {
    const FOR_UPDATE: &'static str = " FOR UPDATE";
    const FOR_KEY_SHARE: &'static str = " FOR KEY SHARE";
    const BEGIN_WRITE: Option<&'static str> = None;
    const ADVISORY_XACT_LOCK: Option<&'static str> = Some("SELECT pg_advisory_xact_lock($1)");

    fn violated(error: &dyn DatabaseError, index: UniqueIndex) -> bool {
        error.constraint() == Some(index.postgres_constraint())
    }

    fn rows_affected(result: &Self::QueryResult) -> u64 {
        result.rows_affected()
    }
}

impl Backend for Sqlite {
    const FOR_UPDATE: &'static str = "";
    const FOR_KEY_SHARE: &'static str = "";
    const BEGIN_WRITE: Option<&'static str> = Some("BEGIN IMMEDIATE");
    const ADVISORY_XACT_LOCK: Option<&'static str> = None;

    fn violated(error: &dyn DatabaseError, index: UniqueIndex) -> bool {
        // SQLite exposes neither the index nor a constraint name, so the
        // extended result code and the column list in the message are the only
        // discriminators available.
        error.code().as_deref() == Some(SQLITE_CONSTRAINT_UNIQUE)
            && error
                .message()
                .strip_prefix("UNIQUE constraint failed: ")
                .is_some_and(|columns| columns == index.sqlite_columns())
    }

    fn rows_affected(result: &Self::QueryResult) -> u64 {
        result.rows_affected()
    }
}

/// Render a `column IN (...)` fragment with `count` numbered placeholders
/// starting at `start`.
///
/// Replaces PostgreSQL's `unnest($n::text[])`, which has no SQLite equivalent.
/// An empty set renders a predicate that is false rather than the syntactically
/// invalid `IN ()`.
pub fn in_list(count: usize, start: usize) -> String {
    if count == 0 {
        return "SELECT NULL WHERE 1 = 0".into();
    }
    let mut sql = String::from("VALUES ");
    for index in 0..count {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("(${})", start + index));
    }
    sql
}

#[cfg(test)]
mod tests {
    use super::{Backend, UniqueIndex, in_list};
    use sqlx::{Postgres, Sqlite};

    #[test]
    fn sqlite_uses_immediate_transactions_and_no_row_locks() {
        assert_eq!(Sqlite::BEGIN_WRITE, Some("BEGIN IMMEDIATE"));
        assert_eq!(Sqlite::FOR_UPDATE, "");
        assert_eq!(Sqlite::FOR_KEY_SHARE, "");
        assert_eq!(Sqlite::ADVISORY_XACT_LOCK, None);
    }

    #[test]
    fn postgres_keeps_row_locks_and_advisory_locking() {
        assert_eq!(Postgres::BEGIN_WRITE, None);
        assert_eq!(Postgres::FOR_UPDATE, " FOR UPDATE");
        assert_eq!(Postgres::FOR_KEY_SHARE, " FOR KEY SHARE");
        assert!(Postgres::ADVISORY_XACT_LOCK.is_some());
    }

    async fn turns_table() -> sqlx::SqlitePool {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        for statement in [
            "CREATE TABLE turns (id text PRIMARY KEY, conversation_id text, idempotency_key text, status text)",
            "CREATE UNIQUE INDEX turns_one_active_per_conversation_idx
             ON turns (conversation_id) WHERE status IN ('pending', 'streaming')",
            "CREATE UNIQUE INDEX turns_conversation_id_idempotency_key_idx
             ON turns (conversation_id, idempotency_key)",
            "INSERT INTO turns VALUES ('t1', 'c1', 'k1', 'pending')",
        ] {
            sqlx::query(statement).execute(&pool).await.unwrap();
        }
        pool
    }

    #[tokio::test]
    async fn sqlite_recognizes_the_active_turn_index() {
        let pool = turns_table().await;
        let error = sqlx::query("INSERT INTO turns VALUES ('t2', 'c1', 'k2', 'pending')")
            .execute(&pool)
            .await
            .unwrap_err();
        assert!(Sqlite::violated(
            error.as_database_error().unwrap(),
            UniqueIndex::ActiveTurnPerConversation
        ));
    }

    #[tokio::test]
    async fn sqlite_does_not_confuse_a_wider_index_with_the_active_turn_index() {
        // Both indexes lead with conversation_id. A substring match would
        // misreport this idempotency-key collision as an active-turn conflict.
        let pool = turns_table().await;
        let error = sqlx::query("INSERT INTO turns VALUES ('t2', 'c1', 'k1', 'completed')")
            .execute(&pool)
            .await
            .unwrap_err();
        assert!(!Sqlite::violated(
            error.as_database_error().unwrap(),
            UniqueIndex::ActiveTurnPerConversation
        ));
    }

    #[tokio::test]
    async fn sqlite_ignores_unrelated_errors() {
        let pool = turns_table().await;
        let error = sqlx::query("INSERT INTO nonexistent VALUES (1)")
            .execute(&pool)
            .await
            .unwrap_err();
        assert!(!Sqlite::violated(
            error.as_database_error().unwrap(),
            UniqueIndex::ActiveTurnPerConversation
        ));
    }

    #[test]
    fn in_list_numbers_placeholders_from_the_given_offset() {
        assert_eq!(in_list(3, 2), "VALUES ($2), ($3), ($4)");
        assert_eq!(in_list(1, 1), "VALUES ($1)");
    }

    #[test]
    fn empty_in_list_is_valid_sql_matching_nothing() {
        assert_eq!(in_list(0, 1), "SELECT NULL WHERE 1 = 0");
    }
}
