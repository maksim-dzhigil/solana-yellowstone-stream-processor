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

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl EventWriter for PostgresEventWriter {
    type Error = PostgresWriteError;

    async fn write_batch(&self, events: &[NormalizedEvent]) -> Result<WriteSummary, Self::Error> {
        if events.is_empty() {
            return Ok(WriteSummary::default());
        }

        let mut rows = Vec::with_capacity(events.len());
        let mut skipped = 0usize;

        for event in events {
            match EventRow::try_from(event) {
                Ok(row) => rows.push(row),
                Err(ref e) => {
                    skipped += 1;
                    tracing::warn!(
                        event_id = %event.event_id(),
                        slot = event.slot(),
                        error = %e,
                        "skipping event that failed to convert to postgres row"
                    );
                }
            }
        }

        if rows.is_empty() {
            return Ok(WriteSummary {
                attempted: events.len(),
                inserted: 0,
                deduplicated: 0,
                skipped,
            });
        }

        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO events (event_id, identity_version, identity, slot, signature, program_id, account, event_type, payload) ",
        );

        query.push_values(rows.iter(), |mut builder, row| {
            builder
                .push_bind(&row.event_id)
                .push_bind(row.identity_version)
                .push_bind(&row.identity)
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

        let attempted = rows.len();
        let inserted = usize::try_from(result.rows_affected()).unwrap_or(usize::MAX);

        Ok(WriteSummary {
            attempted,
            inserted,
            deduplicated: attempted.saturating_sub(inserted),
            skipped,
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
    IdentityVersionOutOfRange { version: u16 },
    SerializeIdentity(serde_json::Error),
    Sqlx(sqlx::Error),
}

impl fmt::Display for PostgresWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SlotOutOfRange { slot } => {
                write!(f, "slot {slot} does not fit into postgres BIGINT")
            }
            Self::IdentityVersionOutOfRange { version } => {
                write!(
                    f,
                    "identity version {version} does not fit into postgres SMALLINT"
                )
            }
            Self::SerializeIdentity(err) => {
                write!(f, "failed to serialize event identity: {err}")
            }
            Self::Sqlx(err) => write!(f, "postgres write failed: {err}"),
        }
    }
}

impl std::error::Error for PostgresWriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SlotOutOfRange { .. } | Self::IdentityVersionOutOfRange { .. } => None,
            Self::SerializeIdentity(err) => Some(err),
            Self::Sqlx(err) => Some(err),
        }
    }
}

#[derive(Debug)]
struct EventRow {
    event_id: String,
    identity_version: i16,
    identity: Value,
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
        let event_slot = event.slot();
        let slot = i64::try_from(event_slot)
            .map_err(|_| PostgresWriteError::SlotOutOfRange { slot: event_slot })?;

        let identity_version = i16::try_from(event.identity_version()).map_err(|_| {
            PostgresWriteError::IdentityVersionOutOfRange {
                version: event.identity_version(),
            }
        })?;

        let identity =
            serde_json::to_value(&event.identity).map_err(PostgresWriteError::SerializeIdentity)?;

        Ok(Self {
            event_id: event.event_id(),
            identity_version,
            identity,
            slot,
            signature: event.signature().map(str::to_owned),
            program_id: event.program_id().map(str::to_owned),
            account: event.account().map(str::to_owned),
            event_type: event.kind().as_str().to_owned(),
            payload: event.payload.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{EventRow, PostgresEventWriter, PostgresWriteError};
    use crate::EventWriter;
    use serde_json::json;
    use solana_yellowstone_domain::event::{EventIdentity, EventKind, NormalizedEvent};
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
        assert_eq!(first_summary.skipped, 0);

        let second_summary = writer
            .write_batch(&[first.clone(), second.clone()])
            .await
            .expect("second batch should deduplicate");

        assert_eq!(second_summary.attempted, 2);
        assert_eq!(second_summary.inserted, 0);
        assert_eq!(second_summary.deduplicated, 2);
        assert_eq!(second_summary.skipped, 0);

        let persisted_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE signature = ANY($1)")
                .bind(vec![first.signature(), second.signature()])
                .fetch_one(&writer.pool)
                .await
                .expect("count query should succeed");

        assert_eq!(persisted_count, 2);
    }

    fn unique_event(slot: u64) -> NormalizedEvent {
        let id = EVENT_ID.fetch_add(1, Ordering::Relaxed);
        let signature = format!("postgres-test-{}-{id}", std::process::id());

        NormalizedEvent::new(
            EventIdentity::Transaction {
                cluster: "localnet".to_owned(),
                slot,
                signature,
                index: id,
            },
            json!({ "source": "postgres-test", "id": id }),
        )
    }

    #[test]
    fn converts_normalized_event_to_postgres_row() {
        let event = NormalizedEvent::new(
            EventIdentity::Instruction {
                cluster: "localnet".to_owned(),
                slot: 42,
                signature: "sig-1".to_owned(),
                transaction_index: 7,
                instruction_index: 0,
                inner_instruction_index: None,
                program_id: "program-1".to_owned(),
            },
            json!({ "source": "test" }),
        );

        let row = EventRow::try_from(&event).expect("event should convert");

        assert_eq!(row.event_id, event.event_id());
        assert_eq!(row.identity_version, 1);
        assert_eq!(
            row.identity,
            serde_json::to_value(&event.identity).expect("serialize identity")
        );
        assert_eq!(row.slot, 42);
        assert_eq!(row.signature.as_deref(), Some("sig-1"));
        assert_eq!(row.program_id.as_deref(), Some("program-1"));
        assert_eq!(row.event_type, EventKind::Instruction.as_str());
        assert_eq!(row.payload, json!({ "source": "test" }));
    }

    #[test]
    fn rejects_slots_that_do_not_fit_postgres_bigint() {
        let event = NormalizedEvent::new(
            EventIdentity::Slot {
                cluster: "localnet".to_owned(),
                slot: u64::MAX,
                status: solana_yellowstone_domain::event::SlotStatus::Processed,
            },
            json!({}),
        );

        let err = EventRow::try_from(&event).expect_err("slot should be out of range");

        assert!(matches!(
            err,
            PostgresWriteError::SlotOutOfRange { slot: u64::MAX }
        ));
    }

    #[test]
    fn identity_version_out_of_range_returns_error() {
        // Note: NormalizedEvent does not expose a way to set an arbitrary identity_version,
        // so we test the error variant directly.
        let err = PostgresWriteError::IdentityVersionOutOfRange { version: u16::MAX };
        assert!(err.to_string().contains("identity version"));
    }
}
