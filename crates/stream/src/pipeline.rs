use crate::batcher::Batcher;
use solana_yellowstone_domain::event::NormalizedEvent;
use solana_yellowstone_storage::{CursorStore, EventWriter, WriteSummary};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineConfig {
    pub batch_size: usize,
    pub channel_capacity: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            channel_capacity: 10_000,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PipelineSummary {
    pub events_seen: usize,
    pub batches_written: usize,
    pub write_summary: WriteSummary,
    pub last_persisted_slot: Option<u64>,
}

#[derive(Debug)]
pub enum PipelineError<W, C> {
    Write(W),
    Cursor(C),
}

impl<W, C> fmt::Display for PipelineError<W, C>
where
    W: fmt::Display,
    C: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write(err) => write!(f, "failed to write event batch: {err}"),
            Self::Cursor(err) => write!(f, "failed to update stream cursor: {err}"),
        }
    }
}

impl<W, C> std::error::Error for PipelineError<W, C>
where
    W: std::error::Error + 'static,
    C: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Write(err) => Some(err),
            Self::Cursor(err) => Some(err),
        }
    }
}

pub async fn run_replay_pipeline<W, C>(
    events: impl IntoIterator<Item = NormalizedEvent>,
    writer: &W,
    cursor_store: &C,
    stream_name: &str,
    config: PipelineConfig,
) -> Result<PipelineSummary, PipelineError<W::Error, C::Error>>
where
    W: EventWriter + Sync,
    C: CursorStore + Sync,
{
    let mut batcher = Batcher::new(config.batch_size);
    let mut summary = PipelineSummary::default();

    for event in events {
        summary.events_seen += 1;

        if let Some(batch) = batcher.push(event) {
            write_batch(writer, cursor_store, stream_name, &batch, &mut summary).await?;
        }
    }

    if let Some(batch) = batcher.flush() {
        write_batch(writer, cursor_store, stream_name, &batch, &mut summary).await?;
    }

    Ok(summary)
}

async fn write_batch<W, C>(
    writer: &W,
    cursor_store: &C,
    stream_name: &str,
    batch: &[NormalizedEvent],
    summary: &mut PipelineSummary,
) -> Result<(), PipelineError<W::Error, C::Error>>
where
    W: EventWriter + Sync,
    C: CursorStore + Sync,
{
    let last_slot = batch.iter().map(|event| event.slot).max();
    let write_summary = writer
        .write_batch(batch)
        .await
        .map_err(PipelineError::Write)?;

    if let Some(slot) = last_slot {
        cursor_store
            .update_after_batch(stream_name, slot)
            .await
            .map_err(PipelineError::Cursor)?;
        summary.last_persisted_slot = Some(
            summary
                .last_persisted_slot
                .map_or(slot, |current| current.max(slot)),
        );
    }

    summary.batches_written += 1;
    summary.write_summary += write_summary;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PipelineConfig, PipelineError, run_replay_pipeline};
    use async_trait::async_trait;
    use serde_json::json;
    use solana_yellowstone_domain::event::{EventType, NormalizedEvent};
    use solana_yellowstone_storage::{CursorStore, EventWriter, WriteSummary};
    use std::fmt;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct FakeWriter {
        batch_sizes: Mutex<Vec<usize>>,
    }

    #[async_trait]
    impl EventWriter for FakeWriter {
        type Error = FakeError;

        async fn write_batch(
            &self,
            events: &[NormalizedEvent],
        ) -> Result<WriteSummary, Self::Error> {
            self.batch_sizes
                .lock()
                .expect("batch sizes lock")
                .push(events.len());
            Ok(WriteSummary {
                attempted: events.len(),
                inserted: events.len(),
                deduplicated: 0,
            })
        }
    }

    #[derive(Debug, Default)]
    struct FakeCursorStore {
        updates: Mutex<Vec<(String, u64)>>,
    }

    #[async_trait]
    impl CursorStore for FakeCursorStore {
        type Error = FakeError;

        async fn update_after_batch(
            &self,
            stream_name: &str,
            last_persisted_slot: u64,
        ) -> Result<(), Self::Error> {
            self.updates
                .lock()
                .expect("cursor updates lock")
                .push((stream_name.to_owned(), last_persisted_slot));
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FakeError;

    impl fmt::Display for FakeError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("fake pipeline error")
        }
    }

    impl std::error::Error for FakeError {}

    #[derive(Debug)]
    struct FailingWriter;

    #[async_trait]
    impl EventWriter for FailingWriter {
        type Error = FakeError;

        async fn write_batch(
            &self,
            _events: &[NormalizedEvent],
        ) -> Result<WriteSummary, Self::Error> {
            Err(FakeError)
        }
    }

    #[derive(Debug)]
    struct FailingCursorStore;

    #[async_trait]
    impl CursorStore for FailingCursorStore {
        type Error = FakeError;

        async fn update_after_batch(
            &self,
            _stream_name: &str,
            _last_persisted_slot: u64,
        ) -> Result<(), Self::Error> {
            Err(FakeError)
        }
    }

    fn event(slot: u64) -> NormalizedEvent {
        NormalizedEvent::new(
            slot,
            Some(format!("sig-{slot}")),
            Some("program-1".to_owned()),
            None,
            EventType::new(EventType::TRANSACTION).expect("static event type should be valid"),
            json!({ "slot": slot }),
        )
    }

    #[tokio::test]
    async fn writes_full_and_partial_batches() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let events = [event(1), event(2), event(3), event(4), event(5)];
        let config = PipelineConfig {
            batch_size: 2,
            channel_capacity: 10,
        };

        let summary = run_replay_pipeline(events, &writer, &cursor_store, "replay", config)
            .await
            .expect("pipeline should run");

        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![2, 2, 1]
        );
        assert_eq!(
            *cursor_store.updates.lock().expect("cursor updates lock"),
            vec![
                ("replay".to_owned(), 2),
                ("replay".to_owned(), 4),
                ("replay".to_owned(), 5)
            ]
        );
        assert_eq!(summary.events_seen, 5);
        assert_eq!(summary.batches_written, 3);
        assert_eq!(summary.write_summary.attempted, 5);
        assert_eq!(summary.write_summary.inserted, 5);
        assert_eq!(summary.write_summary.deduplicated, 0);
        assert_eq!(summary.last_persisted_slot, Some(5));
    }

    #[tokio::test]
    async fn does_not_write_empty_batch_for_empty_input() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let events: Vec<NormalizedEvent> = Vec::new();
        let config = PipelineConfig {
            batch_size: 2,
            channel_capacity: 10,
        };

        let summary = run_replay_pipeline(events, &writer, &cursor_store, "replay", config)
            .await
            .expect("pipeline should run");

        assert!(
            writer
                .batch_sizes
                .lock()
                .expect("batch sizes lock")
                .is_empty()
        );
        assert!(
            cursor_store
                .updates
                .lock()
                .expect("cursor updates lock")
                .is_empty()
        );
        assert_eq!(summary.events_seen, 0);
        assert_eq!(summary.batches_written, 0);
        assert_eq!(summary.write_summary.attempted, 0);
        assert_eq!(summary.last_persisted_slot, None);
    }

    #[tokio::test]
    async fn returns_writer_errors() {
        let cursor_store = FakeCursorStore::default();
        let events = [event(1)];
        let config = PipelineConfig {
            batch_size: 1,
            channel_capacity: 10,
        };

        let err = run_replay_pipeline(events, &FailingWriter, &cursor_store, "replay", config)
            .await
            .expect_err("writer error should fail pipeline");

        assert!(matches!(err, PipelineError::Write(_)));
        assert!(
            cursor_store
                .updates
                .lock()
                .expect("cursor updates lock")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn returns_cursor_errors_after_successful_write() {
        let writer = FakeWriter::default();
        let events = [event(1)];
        let config = PipelineConfig {
            batch_size: 1,
            channel_capacity: 10,
        };

        let err = run_replay_pipeline(events, &writer, &FailingCursorStore, "replay", config)
            .await
            .expect_err("cursor error should fail pipeline");

        assert!(matches!(err, PipelineError::Cursor(_)));
        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![1]
        );
    }
}
