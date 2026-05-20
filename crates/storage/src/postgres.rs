use crate::{EventWriter, WriteSummary};
use solana_yellowstone_domain::event::NormalizedEvent;

#[derive(Debug, Default)]
pub struct PostgresEventWriter;

impl EventWriter for PostgresEventWriter {
    type Error = String;

    fn write_batch(&self, events: &[NormalizedEvent]) -> Result<WriteSummary, Self::Error> {
        Ok(WriteSummary {
            attempted: events.len(),
            inserted: events.len(),
            deduplicated: 0,
        })
    }
}
