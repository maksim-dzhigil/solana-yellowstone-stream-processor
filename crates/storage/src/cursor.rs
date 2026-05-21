use crate::CursorStore;
use async_trait::async_trait;
use sqlx::PgPool;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamCursor {
    pub stream_name: String,
    pub last_persisted_slot: u64,
}

impl StreamCursor {
    pub fn new(stream_name: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            last_persisted_slot: 0,
        }
    }

    pub fn advance_to(&mut self, slot: u64) {
        self.last_persisted_slot = self.last_persisted_slot.max(slot);
    }
}

#[derive(Debug, Clone)]
pub struct PostgresCursorStore {
    pool: PgPool,
}

impl PostgresCursorStore {
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CursorStore for PostgresCursorStore {
    type Error = PostgresCursorError;

    async fn update_after_batch(
        &self,
        stream_name: &str,
        last_persisted_slot: u64,
    ) -> Result<(), Self::Error> {
        let slot = i64::try_from(last_persisted_slot).map_err(|_| {
            PostgresCursorError::SlotOutOfRange {
                slot: last_persisted_slot,
            }
        })?;

        sqlx::query(
            r#"
            INSERT INTO stream_cursors (stream_name, last_persisted_slot, metadata)
            VALUES ($1, $2, '{}'::jsonb)
            ON CONFLICT (stream_name) DO UPDATE
            SET last_persisted_slot = GREATEST(
                    stream_cursors.last_persisted_slot,
                    EXCLUDED.last_persisted_slot
                ),
                updated_at = now()
            "#,
        )
        .bind(stream_name)
        .bind(slot)
        .execute(&self.pool)
        .await
        .map_err(PostgresCursorError::Sqlx)?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum PostgresCursorError {
    SlotOutOfRange { slot: u64 },
    Sqlx(sqlx::Error),
}

impl fmt::Display for PostgresCursorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SlotOutOfRange { slot } => {
                write!(f, "cursor slot {slot} does not fit into postgres BIGINT")
            }
            Self::Sqlx(err) => write!(f, "postgres cursor update failed: {err}"),
        }
    }
}

impl std::error::Error for PostgresCursorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SlotOutOfRange { .. } => None,
            Self::Sqlx(err) => Some(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresCursorError, PostgresCursorStore, StreamCursor};
    use crate::CursorStore;
    use crate::postgres::PostgresEventWriter;
    use std::sync::atomic::{AtomicU64, Ordering};

    static STREAM_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn stream_cursor_advances_monotonically() {
        let mut cursor = StreamCursor::new("replay");

        cursor.advance_to(42);
        cursor.advance_to(10);

        assert_eq!(cursor.stream_name, "replay");
        assert_eq!(cursor.last_persisted_slot, 42);
    }

    #[tokio::test]
    async fn rejects_slots_that_do_not_fit_postgres_bigint() {
        let database_url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@localhost:5433/solana_stream".to_owned()
        });
        let pool = sqlx::PgPool::connect_lazy(&database_url).expect("database url should be valid");
        let cursor_store = PostgresCursorStore::from_pool(pool);

        let err = cursor_store
            .update_after_batch("replay", u64::MAX)
            .await
            .expect_err("slot should be out of range");

        assert!(matches!(
            err,
            PostgresCursorError::SlotOutOfRange { slot: u64::MAX }
        ));
    }

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn upserts_cursor_without_moving_backwards() {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set for postgres integration tests");
        let writer = PostgresEventWriter::connect(&database_url)
            .await
            .expect("postgres writer should connect");
        let cursor_store = PostgresCursorStore::from_pool(writer.pool().clone());
        let stream_name = unique_stream_name();

        cursor_store
            .update_after_batch(&stream_name, 100)
            .await
            .expect("cursor should update");
        cursor_store
            .update_after_batch(&stream_name, 50)
            .await
            .expect("cursor should not move backwards");
        cursor_store
            .update_after_batch(&stream_name, 150)
            .await
            .expect("cursor should advance");

        let persisted_slot: i64 = sqlx::query_scalar(
            "SELECT last_persisted_slot FROM stream_cursors WHERE stream_name = $1",
        )
        .bind(&stream_name)
        .fetch_one(writer.pool())
        .await
        .expect("cursor query should succeed");

        assert_eq!(persisted_slot, 150);
    }

    fn unique_stream_name() -> String {
        let id = STREAM_ID.fetch_add(1, Ordering::Relaxed);
        format!("postgres-cursor-test-{}-{id}", std::process::id())
    }
}
