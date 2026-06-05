use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct RecentEvent {
    pub event_id: String,
    pub slot: i64,
    pub event_type: String,
    pub signature: Option<String>,
    pub program_id: Option<String>,
    pub payload: serde_json::Value,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
}

/// Query recent events ordered by slot descending, then signature, then event_id.
///
/// `limit` is clamped to a maximum of 1_000 rows.
pub async fn recent_events(
    pool: &sqlx::PgPool,
    event_type: Option<&str>,
    limit: i64,
) -> Result<Vec<RecentEvent>, sqlx::Error> {
    let limit = limit.clamp(1, 1_000);
    let sql = r#"
        SELECT event_id, slot, event_type, signature, program_id, payload, inserted_at
        FROM events
        WHERE ($1::text IS NULL OR event_type = $1)
        ORDER BY slot DESC, signature DESC NULLS LAST, event_id DESC
        LIMIT $2
    "#;
    sqlx::query_as::<_, RecentEvent>(sql)
        .bind(event_type)
        .bind(limit)
        .fetch_all(pool)
        .await
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct RecentSwap {
    pub slot: i64,
    pub signature: String,
    pub program_id: String,
    pub token_in: String,
    pub token_in_amount: i64,
    pub token_out: String,
    pub token_out_amount: i64,
    pub inferred_at: chrono::DateTime<chrono::Utc>,
}

/// Query recent inferred swaps ordered by slot descending, then signature.
///
/// `limit` is clamped to a maximum of 1_000 rows.
pub async fn recent_swaps(
    pool: &sqlx::PgPool,
    program_id: Option<&str>,
    limit: i64,
) -> Result<Vec<RecentSwap>, sqlx::Error> {
    let limit = limit.clamp(1, 1_000);
    let sql = r#"
        SELECT slot, signature, program_id, token_in, token_in_amount, token_out, token_out_amount, inferred_at
        FROM swaps
        WHERE ($1::text IS NULL OR program_id = $1)
        ORDER BY slot DESC, signature DESC
        LIMIT $2
    "#;
    sqlx::query_as::<_, RecentSwap>(sql)
        .bind(program_id)
        .bind(limit)
        .fetch_all(pool)
        .await
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct StreamSlotStatus {
    pub slot: i64,
    pub parent_slot: Option<i64>,
    pub finalized: bool,
    pub dead: bool,
    pub first_seen_at: chrono::DateTime<chrono::Utc>,
}

/// Query recent slot states for a stream, ordered by slot descending.
///
/// `limit` is clamped to a maximum of 1_000 rows.
pub async fn recent_slot_states(
    pool: &sqlx::PgPool,
    stream_name: &str,
    limit: i64,
) -> Result<Vec<StreamSlotStatus>, sqlx::Error> {
    let limit = limit.clamp(1, 1_000);
    let sql = r#"
        SELECT slot, parent_slot, finalized, dead, first_seen_at
        FROM stream_slots
        WHERE stream_name = $1
        ORDER BY slot DESC
        LIMIT $2
    "#;
    sqlx::query_as::<_, StreamSlotStatus>(sql)
        .bind(stream_name)
        .bind(limit)
        .fetch_all(pool)
        .await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamLag {
    pub stream_name: String,
    pub last_persisted_slot: Option<i64>,
    pub last_contiguous_finalized_slot: Option<i64>,
    pub last_finalized_slot: Option<i64>,
}

/// Return cursor progress for a stream.
pub async fn stream_lag(pool: &sqlx::PgPool, stream_name: &str) -> Result<StreamLag, sqlx::Error> {
    let row: (Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(
        r#"
        SELECT last_persisted_slot, last_contiguous_finalized_slot, last_finalized_slot
        FROM stream_cursors
        WHERE stream_name = $1
        "#,
    )
    .bind(stream_name)
    .fetch_optional(pool)
    .await?
    .unwrap_or((Some(0), None, None));

    Ok(StreamLag {
        stream_name: stream_name.to_owned(),
        last_persisted_slot: row.0,
        last_contiguous_finalized_slot: row.1,
        last_finalized_slot: row.2,
    })
}

#[cfg(test)]
mod tests {
    use super::{recent_events, recent_swaps, stream_lag};
    use crate::{
        EventWriter, postgres::PostgresEventWriter, swaps::PostgresSwapWriter, swaps::SwapWriter,
    };
    use serde_json::json;
    use solana_yellowstone_domain::decoded::DexSwap;
    use solana_yellowstone_domain::event::{EventIdentity, NormalizedEvent};

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn api_queries_return_recent_events_and_swaps() {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set for postgres integration tests");

        let writer = PostgresEventWriter::connect(&database_url)
            .await
            .expect("connect");
        let pool = writer.pool().clone();

        // Seed events
        let event = NormalizedEvent::new(
            EventIdentity::Transaction {
                cluster: "localnet".to_owned(),
                slot: 50_001,
                signature: "api-test-sig".to_owned(),
                index: 0,
            },
            json!({"token_balances": []}),
        );
        writer.write_batch(&[event]).await.expect("write event");

        let events = recent_events(&pool, Some("transaction"), 10)
            .await
            .expect("query events");
        assert!(!events.is_empty());
        assert_eq!(events[0].slot, 50_001);

        // Seed swap
        let swap_writer = PostgresSwapWriter::from_pool(pool.clone());
        let swap = DexSwap {
            slot: 50_002,
            signature: "api-swap-sig".to_owned(),
            program_id: "program-api".to_owned(),
            token_in: "mint-a".to_owned(),
            token_in_amount: 100,
            token_out: "mint-b".to_owned(),
            token_out_amount: 200,
        };
        swap_writer.write_swaps(&[swap]).await.expect("write swap");

        let swaps = recent_swaps(&pool, Some("program-api"), 10)
            .await
            .expect("query swaps");
        assert!(!swaps.is_empty());
        assert_eq!(swaps[0].slot, 50_002);
        assert_eq!(swaps[0].program_id, "program-api");

        let lag = stream_lag(&pool, "api-test-stream")
            .await
            .expect("query lag");
        assert_eq!(lag.stream_name, "api-test-stream");
    }
}
