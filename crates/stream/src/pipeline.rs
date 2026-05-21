use crate::batcher::Batcher;
use solana_yellowstone_domain::event::NormalizedEvent;
use solana_yellowstone_storage::{EventWriter, WriteSummary};
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
}

#[derive(Debug)]
pub enum PipelineError<E> {
    Write(E),
}

impl<E> fmt::Display for PipelineError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write(err) => write!(f, "failed to write event batch: {err}"),
        }
    }
}

impl<E> std::error::Error for PipelineError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Write(err) => Some(err),
        }
    }
}

pub fn run_replay_pipeline<W>(
    events: impl IntoIterator<Item = NormalizedEvent>,
    writer: &W,
    config: PipelineConfig,
) -> Result<PipelineSummary, PipelineError<W::Error>>
where
    W: EventWriter,
{
    let mut batcher = Batcher::new(config.batch_size);
    let mut summary = PipelineSummary::default();

    for event in events {
        summary.events_seen += 1;

        if let Some(batch) = batcher.push(event) {
            write_batch(writer, &batch, &mut summary)?;
        }
    }

    if let Some(batch) = batcher.flush() {
        write_batch(writer, &batch, &mut summary)?;
    }

    Ok(summary)
}

fn write_batch<W>(
    writer: &W,
    batch: &[NormalizedEvent],
    summary: &mut PipelineSummary,
) -> Result<(), PipelineError<W::Error>>
where
    W: EventWriter,
{
    let write_summary = writer.write_batch(batch).map_err(PipelineError::Write)?;
    summary.batches_written += 1;
    summary.write_summary += write_summary;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{PipelineConfig, PipelineError, run_replay_pipeline};
    use serde_json::json;
    use solana_yellowstone_domain::event::{EventType, NormalizedEvent};
    use solana_yellowstone_storage::{EventWriter, WriteSummary};
    use std::cell::RefCell;
    use std::fmt;

    #[derive(Debug, Default)]
    struct FakeWriter {
        batch_sizes: RefCell<Vec<usize>>,
    }

    impl EventWriter for FakeWriter {
        type Error = FakeWriterError;

        fn write_batch(&self, events: &[NormalizedEvent]) -> Result<WriteSummary, Self::Error> {
            self.batch_sizes.borrow_mut().push(events.len());
            Ok(WriteSummary {
                attempted: events.len(),
                inserted: events.len(),
                deduplicated: 0,
            })
        }
    }

    #[derive(Debug)]
    struct FakeWriterError;

    impl fmt::Display for FakeWriterError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("fake writer error")
        }
    }

    impl std::error::Error for FakeWriterError {}

    #[derive(Debug)]
    struct FailingWriter;

    impl EventWriter for FailingWriter {
        type Error = FakeWriterError;

        fn write_batch(&self, _events: &[NormalizedEvent]) -> Result<WriteSummary, Self::Error> {
            Err(FakeWriterError)
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

    #[test]
    fn writes_full_and_partial_batches() {
        let writer = FakeWriter::default();
        let events = [event(1), event(2), event(3), event(4), event(5)];
        let config = PipelineConfig {
            batch_size: 2,
            channel_capacity: 10,
        };

        let summary = run_replay_pipeline(events, &writer, config).expect("pipeline should run");

        assert_eq!(*writer.batch_sizes.borrow(), vec![2, 2, 1]);
        assert_eq!(summary.events_seen, 5);
        assert_eq!(summary.batches_written, 3);
        assert_eq!(summary.write_summary.attempted, 5);
        assert_eq!(summary.write_summary.inserted, 5);
        assert_eq!(summary.write_summary.deduplicated, 0);
    }

    #[test]
    fn does_not_write_empty_batch_for_empty_input() {
        let writer = FakeWriter::default();
        let events: Vec<NormalizedEvent> = Vec::new();
        let config = PipelineConfig {
            batch_size: 2,
            channel_capacity: 10,
        };

        let summary = run_replay_pipeline(events, &writer, config).expect("pipeline should run");

        assert!(writer.batch_sizes.borrow().is_empty());
        assert_eq!(summary.events_seen, 0);
        assert_eq!(summary.batches_written, 0);
        assert_eq!(summary.write_summary.attempted, 0);
    }

    #[test]
    fn returns_writer_errors() {
        let events = [event(1)];
        let config = PipelineConfig {
            batch_size: 1,
            channel_capacity: 10,
        };

        let err = run_replay_pipeline(events, &FailingWriter, config)
            .expect_err("writer error should fail pipeline");

        assert!(matches!(err, PipelineError::Write(_)));
    }
}
