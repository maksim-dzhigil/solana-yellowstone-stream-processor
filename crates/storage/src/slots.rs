use async_trait::async_trait;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotStateUpdate {
    pub slot: u64,
    pub parent_slot: Option<u64>,
    pub finalized: bool,
    pub dead: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedFrontier {
    pub last_contiguous_finalized_slot: Option<u64>,
    pub last_finalized_slot: Option<u64>,
}

#[async_trait]
pub trait SlotStateStore {
    type Error;

    async fn record_slot_states(
        &self,
        stream_name: &str,
        updates: &[SlotStateUpdate],
    ) -> Result<(), Self::Error>;

    async fn get_finalized_frontier(
        &self,
        stream_name: &str,
    ) -> Result<FinalizedFrontier, Self::Error>;

    async fn advance_contiguous_finalized(
        &self,
        stream_name: &str,
    ) -> Result<FinalizedFrontier, Self::Error>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSlotStateStore;

#[async_trait]
impl SlotStateStore for NoopSlotStateStore {
    type Error = Infallible;

    async fn record_slot_states(
        &self,
        _stream_name: &str,
        _updates: &[SlotStateUpdate],
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn get_finalized_frontier(
        &self,
        _stream_name: &str,
    ) -> Result<FinalizedFrontier, Self::Error> {
        Ok(FinalizedFrontier::default())
    }

    async fn advance_contiguous_finalized(
        &self,
        _stream_name: &str,
    ) -> Result<FinalizedFrontier, Self::Error> {
        Ok(FinalizedFrontier::default())
    }
}

#[derive(Debug, Clone)]
pub struct PostgresSlotStateStore {
    pool: PgPool,
}

impl PostgresSlotStateStore {
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl SlotStateStore for PostgresSlotStateStore {
    type Error = PostgresSlotStateError;

    async fn record_slot_states(
        &self,
        stream_name: &str,
        updates: &[SlotStateUpdate],
    ) -> Result<(), Self::Error> {
        if updates.is_empty() {
            return Ok(());
        }

        let mut merged: HashMap<u64, SlotStateUpdate> = HashMap::new();
        for update in updates {
            merged
                .entry(update.slot)
                .and_modify(|existing| {
                    existing.finalized |= update.finalized;
                    existing.dead |= update.dead;
                    if update.parent_slot.is_some() {
                        existing.parent_slot = update.parent_slot;
                    }
                })
                .or_insert(*update);
        }

        let rows = merged
            .into_values()
            .map(SlotStateRow::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO stream_slots (stream_name, slot, parent_slot, finalized, dead) ",
        );
        query.push_values(rows.iter(), |mut builder, row| {
            builder
                .push_bind(stream_name)
                .push_bind(row.slot)
                .push_bind(row.parent_slot)
                .push_bind(row.finalized)
                .push_bind(row.dead);
        });
        query.push(
            r#"
            ON CONFLICT (stream_name, slot) DO UPDATE SET
                parent_slot = COALESCE(EXCLUDED.parent_slot, stream_slots.parent_slot),
                finalized   = stream_slots.finalized OR EXCLUDED.finalized,
                dead        = stream_slots.dead OR EXCLUDED.dead
            "#,
        );

        query
            .build()
            .execute(&self.pool)
            .await
            .map_err(PostgresSlotStateError::Sqlx)?;

        Ok(())
    }

    async fn get_finalized_frontier(
        &self,
        stream_name: &str,
    ) -> Result<FinalizedFrontier, Self::Error> {
        let row: Option<(Option<i64>, Option<i64>)> = sqlx::query_as(
            "SELECT last_contiguous_finalized_slot, last_finalized_slot \
             FROM stream_cursors WHERE stream_name = $1",
        )
        .bind(stream_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(PostgresSlotStateError::Sqlx)?;

        match row {
            Some((contiguous, head)) => Ok(FinalizedFrontier {
                last_contiguous_finalized_slot: decode_slot(contiguous)?,
                last_finalized_slot: decode_slot(head)?,
            }),
            None => Ok(FinalizedFrontier::default()),
        }
    }

    async fn advance_contiguous_finalized(
        &self,
        stream_name: &str,
    ) -> Result<FinalizedFrontier, Self::Error> {
        let (watermark, head): (Option<i64>, Option<i64>) = sqlx::query_as(
            r#"
            WITH RECURSIVE seed AS (
                SELECT COALESCE(
                    (SELECT last_contiguous_finalized_slot FROM stream_cursors WHERE stream_name = $1),
                    (SELECT min(slot) FROM stream_slots WHERE stream_name = $1 AND finalized)
                ) AS slot
            ),
            chain AS (
                SELECT slot FROM seed WHERE slot IS NOT NULL
              UNION ALL
                SELECT s.slot
                FROM stream_slots s
                JOIN chain c ON s.parent_slot = c.slot
                WHERE s.stream_name = $1 AND s.finalized
            )
            SELECT
                (SELECT max(slot) FROM chain) AS watermark,
                (SELECT max(slot) FROM stream_slots WHERE stream_name = $1 AND finalized) AS head
            "#,
        )
        .bind(stream_name)
        .fetch_one(&self.pool)
        .await
        .map_err(PostgresSlotStateError::Sqlx)?;

        let (contiguous, head): (Option<i64>, Option<i64>) = sqlx::query_as(
            r#"
            INSERT INTO stream_cursors
                (stream_name, last_persisted_slot, last_contiguous_finalized_slot, last_finalized_slot)
            VALUES ($1, 0, $2, $3)
            ON CONFLICT (stream_name) DO UPDATE SET
                last_contiguous_finalized_slot = GREATEST(
                    stream_cursors.last_contiguous_finalized_slot,
                    EXCLUDED.last_contiguous_finalized_slot
                ),
                last_finalized_slot = GREATEST(
                    stream_cursors.last_finalized_slot,
                    EXCLUDED.last_finalized_slot
                ),
                updated_at = now()
            RETURNING last_contiguous_finalized_slot, last_finalized_slot
            "#,
        )
        .bind(stream_name)
        .bind(watermark)
        .bind(head)
        .fetch_one(&self.pool)
        .await
        .map_err(PostgresSlotStateError::Sqlx)?;

        Ok(FinalizedFrontier {
            last_contiguous_finalized_slot: decode_slot(contiguous)?,
            last_finalized_slot: decode_slot(head)?,
        })
    }
}

#[derive(Debug)]
struct SlotStateRow {
    slot: i64,
    parent_slot: Option<i64>,
    finalized: bool,
    dead: bool,
}

impl TryFrom<SlotStateUpdate> for SlotStateRow {
    type Error = PostgresSlotStateError;

    fn try_from(update: SlotStateUpdate) -> Result<Self, Self::Error> {
        let slot = i64::try_from(update.slot)
            .map_err(|_| PostgresSlotStateError::SlotOutOfRange { slot: update.slot })?;
        let parent_slot = update
            .parent_slot
            .map(|parent| {
                i64::try_from(parent)
                    .map_err(|_| PostgresSlotStateError::SlotOutOfRange { slot: parent })
            })
            .transpose()?;

        Ok(Self {
            slot,
            parent_slot,
            finalized: update.finalized,
            dead: update.dead,
        })
    }
}

fn decode_slot(slot: Option<i64>) -> Result<Option<u64>, PostgresSlotStateError> {
    slot.map(|slot| {
        u64::try_from(slot).map_err(|_| PostgresSlotStateError::PersistedSlotOutOfRange { slot })
    })
    .transpose()
}

#[derive(Debug)]
pub enum PostgresSlotStateError {
    SlotOutOfRange { slot: u64 },
    PersistedSlotOutOfRange { slot: i64 },
    Sqlx(sqlx::Error),
}

impl fmt::Display for PostgresSlotStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SlotOutOfRange { slot } => {
                write!(f, "slot {slot} does not fit into postgres BIGINT")
            }
            Self::PersistedSlotOutOfRange { slot } => {
                write!(f, "persisted slot {slot} cannot be converted to u64")
            }
            Self::Sqlx(err) => write!(f, "postgres slot-state operation failed: {err}"),
        }
    }
}

impl std::error::Error for PostgresSlotStateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SlotOutOfRange { .. } | Self::PersistedSlotOutOfRange { .. } => None,
            Self::Sqlx(err) => Some(err),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct FinalizedMap {
    parents: HashMap<u64, Option<u64>>,
}

impl FinalizedMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, slot: u64, parent_slot: Option<u64>) {
        self.parents
            .entry(slot)
            .and_modify(|existing| {
                if parent_slot.is_some() {
                    *existing = parent_slot;
                }
            })
            .or_insert(parent_slot);
    }

    pub fn contains(&self, slot: u64) -> bool {
        self.parents.contains_key(&slot)
    }

    pub fn min_finalized(&self) -> Option<u64> {
        self.parents.keys().copied().min()
    }

    pub fn max_finalized(&self) -> Option<u64> {
        self.parents.keys().copied().max()
    }
}

impl FromIterator<(u64, Option<u64>)> for FinalizedMap {
    fn from_iter<I: IntoIterator<Item = (u64, Option<u64>)>>(iter: I) -> Self {
        let mut map = Self::new();
        for (slot, parent) in iter {
            map.insert(slot, parent);
        }
        map
    }
}

pub fn advance(anchor: Option<u64>, finalized: &FinalizedMap) -> Option<u64> {
    let mut children: HashMap<u64, Vec<u64>> = HashMap::new();
    for (&slot, &parent) in &finalized.parents {
        if let Some(parent) = parent {
            children.entry(parent).or_default().push(slot);
        }
    }

    let seed = match anchor {
        Some(anchor) => anchor,
        None => finalized.min_finalized()?,
    };

    let mut head = seed;
    while let Some(next) = children.get(&head) {
        let Some(&child) = next.iter().min() else {
            break;
        };
        if child <= head {
            break;
        }
        head = child;
    }

    Some(head)
}

#[cfg(test)]
mod tests {
    use super::{FinalizedMap, advance};

    fn map(entries: &[(u64, Option<u64>)]) -> FinalizedMap {
        entries.iter().copied().collect()
    }

    #[test]
    fn advances_in_order_with_parent_links() {
        let finalized = map(&[(10, Some(9)), (11, Some(10)), (12, Some(11))]);

        assert_eq!(advance(Some(10), &finalized), Some(12));
    }

    #[test]
    fn advances_across_skipped_slots() {
        let finalized = map(&[(10, Some(9)), (12, Some(10)), (15, Some(12))]);

        assert_eq!(advance(Some(10), &finalized), Some(15));
    }

    #[test]
    fn missing_finalized_link_holds_watermark() {
        let finalized = map(&[
            (10, Some(9)),
            (11, Some(10)),
            (13, Some(12)),
            (14, Some(13)),
        ]);

        assert_eq!(advance(Some(10), &finalized), Some(11));
    }

    #[test]
    fn out_of_order_arrival_bridges_on_later_advance() {
        let partial = map(&[(10, Some(9)), (12, Some(11))]);
        assert_eq!(advance(Some(10), &partial), Some(10));

        let bridged = map(&[(10, Some(9)), (11, Some(10)), (12, Some(11))]);
        assert_eq!(advance(Some(10), &bridged), Some(12));
    }

    #[test]
    fn gap_below_watermark_does_not_regress() {
        let finalized = map(&[(5, Some(4))]);

        assert_eq!(advance(Some(20), &finalized), Some(20));
    }

    #[test]
    fn dead_slot_is_not_a_chain_link() {
        let finalized = map(&[(10, Some(9)), (12, Some(11))]);

        assert_eq!(advance(Some(10), &finalized), Some(10));
    }

    #[test]
    fn bootstrap_from_lowest_finalized_when_anchor_null() {
        let finalized = map(&[(7, Some(6)), (8, Some(7)), (9, Some(8))]);

        assert_eq!(advance(None, &finalized), Some(9));
    }

    #[test]
    fn bootstrap_returns_none_without_finalized_slots() {
        assert_eq!(advance(None, &FinalizedMap::new()), None);
    }

    #[test]
    fn anchor_without_extension_returns_anchor() {
        let finalized = map(&[(10, Some(9))]);

        assert_eq!(advance(Some(10), &finalized), Some(10));
    }
}

#[cfg(test)]
mod postgres_tests {
    use super::{PostgresSlotStateStore, SlotStateStore, SlotStateUpdate};
    use crate::postgres::PostgresEventWriter;
    use std::sync::atomic::{AtomicU64, Ordering};

    static STREAM_ID: AtomicU64 = AtomicU64::new(0);

    fn unique_stream_name() -> String {
        let id = STREAM_ID.fetch_add(1, Ordering::Relaxed);
        format!("postgres-slot-state-test-{}-{id}", std::process::id())
    }

    fn finalized(slot: u64, parent: u64) -> SlotStateUpdate {
        SlotStateUpdate {
            slot,
            parent_slot: Some(parent),
            finalized: true,
            dead: false,
        }
    }

    async fn store() -> PostgresSlotStateStore {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set for postgres integration tests");
        let writer = PostgresEventWriter::connect(&database_url)
            .await
            .expect("postgres writer should connect");
        PostgresSlotStateStore::from_pool(writer.pool().clone())
    }

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn records_finalized_slots_and_advances_contiguous_watermark() {
        let store = store().await;
        let stream = unique_stream_name();

        store
            .record_slot_states(
                &stream,
                &[finalized(10, 9), finalized(11, 10), finalized(12, 11)],
            )
            .await
            .expect("slot states should record");

        let frontier = store
            .advance_contiguous_finalized(&stream)
            .await
            .expect("watermark should advance");

        assert_eq!(frontier.last_contiguous_finalized_slot, Some(12));
        assert_eq!(frontier.last_finalized_slot, Some(12));

        let read_back = store
            .get_finalized_frontier(&stream)
            .await
            .expect("frontier should read back");
        assert_eq!(read_back, frontier);
    }

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn watermark_holds_at_gap_and_resumes_after_fill() {
        let store = store().await;
        let stream = unique_stream_name();

        store
            .record_slot_states(
                &stream,
                &[finalized(10, 9), finalized(11, 10), finalized(13, 12)],
            )
            .await
            .expect("slot states should record");

        let held = store
            .advance_contiguous_finalized(&stream)
            .await
            .expect("watermark should advance");
        assert_eq!(held.last_contiguous_finalized_slot, Some(11));
        assert_eq!(held.last_finalized_slot, Some(13));

        store
            .record_slot_states(&stream, &[finalized(12, 11)])
            .await
            .expect("gap fill should record");

        let resumed = store
            .advance_contiguous_finalized(&stream)
            .await
            .expect("watermark should resume");
        assert_eq!(resumed.last_contiguous_finalized_slot, Some(13));
        assert_eq!(resumed.last_finalized_slot, Some(13));
    }

    #[tokio::test]
    #[ignore = "requires local postgres; run `make compose-up test-postgres`"]
    async fn watermark_upsert_is_idempotent_and_monotonic() {
        let store = store().await;
        let stream = unique_stream_name();

        store
            .record_slot_states(&stream, &[finalized(10, 9), finalized(11, 10)])
            .await
            .expect("slot states should record");
        let first = store
            .advance_contiguous_finalized(&stream)
            .await
            .expect("watermark should advance");
        assert_eq!(first.last_contiguous_finalized_slot, Some(11));

        store
            .record_slot_states(&stream, &[finalized(10, 9), finalized(11, 10)])
            .await
            .expect("re-record should be idempotent");
        let second = store
            .advance_contiguous_finalized(&stream)
            .await
            .expect("watermark should not regress");
        assert_eq!(second.last_contiguous_finalized_slot, Some(11));

        store
            .record_slot_states(&stream, &[finalized(5, 4)])
            .await
            .expect("late slot should record");
        let third = store
            .advance_contiguous_finalized(&stream)
            .await
            .expect("watermark should hold");
        assert_eq!(third.last_contiguous_finalized_slot, Some(11));
    }
}
