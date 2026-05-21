pub mod cursor;
pub mod postgres;

use solana_yellowstone_domain::event::NormalizedEvent;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WriteSummary {
    pub attempted: usize,
    pub inserted: usize,
    pub deduplicated: usize,
}

pub trait EventWriter {
    type Error;

    fn write_batch(&self, events: &[NormalizedEvent]) -> Result<WriteSummary, Self::Error>;
}

impl std::ops::AddAssign for WriteSummary {
    fn add_assign(&mut self, rhs: Self) {
        self.attempted += rhs.attempted;
        self.inserted += rhs.inserted;
        self.deduplicated += rhs.deduplicated;
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
        };

        assert_eq!(summary.attempted - summary.inserted, summary.deduplicated);
    }
}
