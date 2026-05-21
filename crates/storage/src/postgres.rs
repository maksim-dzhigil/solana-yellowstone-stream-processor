use crate::{EventWriter, WriteSummary};
use async_trait::async_trait;
use serde_json::Value;
use solana_yellowstone_domain::event::NormalizedEvent;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::fmt;

#[derive(Debug, Clone)]
pub struct PostgresEventWriter {
    pool: PgPool,
}

impl PostgresEventWriter {
    pub async fn connect(database_url: &str) -> Result<Self, PostgresInitError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(PostgresInitError::Connect)?;

        sqlx::migrate!("../../migrations")
            .run(&pool)
            .await
            .map_err(PostgresInitError::Migrate)?;

        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventWriter for PostgresEventWriter {
    type Error = PostgresWriteError;

    async fn write_batch(&self, events: &[NormalizedEvent]) -> Result<WriteSummary, Self::Error> {
        if events.is_empty() {
            return Ok(WriteSummary::default());
        }

        let rows = events
            .iter()
            .map(EventRow::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO events (event_id, slot, signature, program_id, account, event_type, payload) ",
        );

        query.push_values(rows.iter(), |mut builder, row| {
            builder
                .push_bind(&row.event_id)
                .push_bind(row.slot)
                .push_bind(&row.signature)
                .push_bind(&row.program_id)
                .push_bind(&row.account)
                .push_bind(&row.event_type)
                .push_bind(&row.payload);
        });
        query.push(" ON CONFLICT (event_id) DO NOTHING");

        let result = query
            .build()
            .execute(&self.pool)
            .await
            .map_err(PostgresWriteError::Sqlx)?;

        let attempted = events.len();
        let inserted = usize::try_from(result.rows_affected()).unwrap_or(usize::MAX);

        Ok(WriteSummary {
            attempted,
            inserted,
            deduplicated: attempted.saturating_sub(inserted),
        })
    }
}

#[derive(Debug)]
pub enum PostgresInitError {
    Connect(sqlx::Error),
    Migrate(sqlx::migrate::MigrateError),
}

impl fmt::Display for PostgresInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(err) => write!(f, "failed to connect to postgres: {err}"),
            Self::Migrate(err) => write!(f, "failed to run postgres migrations: {err}"),
        }
    }
}

impl std::error::Error for PostgresInitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(err) => Some(err),
            Self::Migrate(err) => Some(err),
        }
    }
}

#[derive(Debug)]
pub enum PostgresWriteError {
    SlotOutOfRange { slot: u64 },
    Sqlx(sqlx::Error),
}

impl fmt::Display for PostgresWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SlotOutOfRange { slot } => {
                write!(f, "slot {slot} does not fit into postgres BIGINT")
            }
            Self::Sqlx(err) => write!(f, "postgres write failed: {err}"),
        }
    }
}

impl std::error::Error for PostgresWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SlotOutOfRange { .. } => None,
            Self::Sqlx(err) => Some(err),
        }
    }
}

#[derive(Debug)]
struct EventRow {
    event_id: String,
    slot: i64,
    signature: Option<String>,
    program_id: Option<String>,
    account: Option<String>,
    event_type: String,
    payload: Value,
}

impl TryFrom<&NormalizedEvent> for EventRow {
    type Error = PostgresWriteError;

    fn try_from(event: &NormalizedEvent) -> Result<Self, Self::Error> {
        let slot = i64::try_from(event.slot)
            .map_err(|_| PostgresWriteError::SlotOutOfRange { slot: event.slot })?;

        Ok(Self {
            event_id: event.event_id(),
            slot,
            signature: event.signature.clone(),
            program_id: event.program_id.clone(),
            account: event.account.clone(),
            event_type: event.event_type.as_str().to_owned(),
            payload: event.payload.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{EventRow, PostgresEventWriter, PostgresWriteError};
    use crate::EventWriter;
    use serde_json::json;
    use solana_yellowstone_domain::event::{EventType, NormalizedEvent};
    use std::sync::atomic::{AtomicU64, Ordering};

    static EVENT_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn writes_and_deduplicates_events_in_postgres() {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set for postgres integration tests");
        let writer = PostgresEventWriter::connect(&database_url)
            .await
            .expect("postgres writer should connect");

        let first = unique_event(10_001);
        let duplicate = first.clone();
        let second = unique_event(10_002);

        let first_summary = writer
            .write_batch(&[first.clone(), duplicate, second.clone()])
            .await
            .expect("first batch should write");

        assert_eq!(first_summary.attempted, 3);
        assert_eq!(first_summary.inserted, 2);
        assert_eq!(first_summary.deduplicated, 1);

        let second_summary = writer
            .write_batch(&[first.clone(), second.clone()])
            .await
            .expect("second batch should deduplicate");

        assert_eq!(second_summary.attempted, 2);
        assert_eq!(second_summary.inserted, 0);
        assert_eq!(second_summary.deduplicated, 2);

        let persisted_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE signature = ANY($1)")
                .bind(vec![
                    first.signature.as_deref(),
                    second.signature.as_deref(),
                ])
                .fetch_one(&writer.pool)
                .await
                .expect("count query should succeed");

        assert_eq!(persisted_count, 2);
    }

    fn unique_event(slot: u64) -> NormalizedEvent {
        let id = EVENT_ID.fetch_add(1, Ordering::Relaxed);
        let signature = format!("postgres-test-{}-{id}", std::process::id());

        NormalizedEvent::new(
            slot,
            Some(signature),
            Some("program-1".to_owned()),
            None,
            EventType::new(EventType::TRANSACTION).expect("event type should be valid"),
            json!({ "source": "postgres-test", "id": id }),
        )
    }

    #[test]
    fn converts_normalized_event_to_postgres_row() {
        let event = NormalizedEvent::new(
            42,
            Some("sig-1".to_owned()),
            Some("program-1".to_owned()),
            None,
            EventType::new(EventType::TRANSACTION).expect("event type should be valid"),
            json!({ "source": "test" }),
        );

        let row = EventRow::try_from(&event).expect("event should convert");

        assert_eq!(row.event_id, event.event_id());
        assert_eq!(row.slot, 42);
        assert_eq!(row.signature.as_deref(), Some("sig-1"));
        assert_eq!(row.program_id.as_deref(), Some("program-1"));
        assert_eq!(row.event_type, EventType::TRANSACTION);
        assert_eq!(row.payload, json!({ "source": "test" }));
    }

    #[test]
    fn rejects_slots_that_do_not_fit_postgres_bigint() {
        let event = NormalizedEvent::new(
            u64::MAX,
            None,
            None,
            None,
            EventType::new(EventType::SLOT).expect("event type should be valid"),
            json!({}),
        );

        let err = EventRow::try_from(&event).expect_err("slot should be out of range");

        assert!(matches!(
            err,
            PostgresWriteError::SlotOutOfRange { slot: u64::MAX }
        ));
    }
}
