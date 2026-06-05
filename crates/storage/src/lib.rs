pub mod cursor;
pub mod postgres;
pub mod slots;

use crate::cursor::StreamCursor;
use async_trait::async_trait;
use solana_yellowstone_domain::event::NormalizedEvent;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WriteSummary {
    pub attempted: usize,
    pub inserted: usize,
    pub deduplicated: usize,
    pub skipped: usize,
}

#[async_trait]
pub trait EventWriter {
    type Error;

    async fn write_batch(&self, events: &[NormalizedEvent]) -> Result<WriteSummary, Self::Error>;
}

#[async_trait]
pub trait CursorStore {
    type Error;

    async fn get_cursor(&self, stream_name: &str) -> Result<Option<StreamCursor>, Self::Error>;

    async fn update_after_batch(
        &self,
        stream_name: &str,
        last_persisted_slot: u64,
    ) -> Result<(), Self::Error>;
}

impl std::ops::AddAssign for WriteSummary {
    fn add_assign(&mut self, rhs: Self) {
        self.attempted += rhs.attempted;
        self.inserted += rhs.inserted;
        self.deduplicated += rhs.deduplicated;
        self.skipped += rhs.skipped;
    }
}

#[cfg(test)]
mod tests {
    use super::WriteSummary;

    #[test]
    fn write_summary_tracks_deduplicated_count() {
        let summary = WriteSummary {
            attempted: 10,
            inserted: 7,
            deduplicated: 3,
            skipped: 0,
        };

        assert_eq!(summary.attempted - summary.inserted, summary.deduplicated);
    }
}
