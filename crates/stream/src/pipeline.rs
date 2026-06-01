use crate::batcher::Batcher;
use solana_yellowstone_domain::event::NormalizedEvent;
use solana_yellowstone_storage::{CursorStore, EventWriter, WriteSummary};
use std::fmt;
use std::future::Future;
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineConfig {
    pub batch_size: usize,
    pub channel_capacity: usize,
    pub resume_after_slot: Option<u64>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            channel_capacity: 10_000,
            resume_after_slot: None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PipelineSummary {
    pub events_seen: usize,
    pub events_skipped: usize,
    pub batches_written: usize,
    pub write_summary: WriteSummary,
    pub last_persisted_slot: Option<u64>,
}

#[derive(Debug)]
pub enum PipelineError<W, C> {
    Write(W),
    Cursor(C),
    ProducerJoin(tokio::task::JoinError),
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
            Self::ProducerJoin(err) => write!(f, "failed to join event producer task: {err}"),
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
            Self::ProducerJoin(err) => Some(err),
        }
    }
}

#[derive(Debug)]
pub enum ProducerPipelineError<W, C, P> {
    Pipeline(PipelineError<W, C>),
    Producer(P),
    ProducerJoin(tokio::task::JoinError),
}

struct AbortOnDrop<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDrop<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn abort(&self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }

    async fn join(&mut self) -> Result<T, tokio::task::JoinError> {
        self.handle
            .take()
            .expect("producer task should not be joined twice")
            .await
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

impl<W, C, P> fmt::Display for ProducerPipelineError<W, C, P>
where
    W: fmt::Display,
    C: fmt::Display,
    P: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pipeline(err) => write!(f, "{err}"),
            Self::Producer(err) => write!(f, "event producer failed: {err}"),
            Self::ProducerJoin(err) => write!(f, "failed to join event producer task: {err}"),
        }
    }
}

impl<W, C, P> std::error::Error for ProducerPipelineError<W, C, P>
where
    W: std::error::Error + 'static,
    C: std::error::Error + 'static,
    P: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Pipeline(err) => Some(err),
            Self::Producer(err) => Some(err),
            Self::ProducerJoin(err) => Some(err),
        }
    }
}

pub async fn run_replay_pipeline<I, W, C>(
    events: I,
    writer: &W,
    cursor_store: &C,
    stream_name: &str,
    config: PipelineConfig,
) -> Result<PipelineSummary, PipelineError<W::Error, C::Error>>
where
    I: IntoIterator<Item = NormalizedEvent> + Send + 'static,
    I::IntoIter: Send,
    W: EventWriter + Sync,
    C: CursorStore + Sync,
{
    let (sender, receiver) = mpsc::channel(config.channel_capacity);
    let producer = tokio::spawn(send_events(events, sender));

    let result =
        run_event_receiver_pipeline(receiver, writer, cursor_store, stream_name, config).await;
    let producer_result = producer.await.map_err(PipelineError::ProducerJoin);

    match (result, producer_result) {
        (Err(err), _) => Err(err),
        (Ok(_), Err(err)) => Err(err),
        (Ok(summary), Ok(())) => Ok(summary),
    }
}

pub async fn run_event_producer_pipeline<P, F, E, W, C>(
    producer: P,
    writer: &W,
    cursor_store: &C,
    stream_name: &str,
    config: PipelineConfig,
) -> Result<PipelineSummary, ProducerPipelineError<W::Error, C::Error, E>>
where
    P: FnOnce(mpsc::Sender<NormalizedEvent>) -> F + Send + 'static,
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: Send + 'static,
    W: EventWriter + Sync,
    C: CursorStore + Sync,
{
    run_event_producer_pipeline_with_progress(
        producer,
        writer,
        cursor_store,
        stream_name,
        config,
        |_| {},
    )
    .await
}

pub async fn run_event_producer_pipeline_with_progress<P, F, E, W, C, O>(
    producer: P,
    writer: &W,
    cursor_store: &C,
    stream_name: &str,
    config: PipelineConfig,
    on_progress: O,
) -> Result<PipelineSummary, ProducerPipelineError<W::Error, C::Error, E>>
where
    P: FnOnce(mpsc::Sender<NormalizedEvent>) -> F + Send + 'static,
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: Send + 'static,
    W: EventWriter + Sync,
    C: CursorStore + Sync,
    O: FnMut(PipelineSummary) + Send,
{
    let (sender, receiver) = mpsc::channel(config.channel_capacity);
    let mut producer = AbortOnDrop::new(tokio::spawn(producer(sender)));

    let result = run_event_receiver_pipeline_with_progress(
        receiver,
        writer,
        cursor_store,
        stream_name,
        config,
        on_progress,
    )
    .await;

    match result {
        Err(err) => {
            producer.abort();
            let _ = producer.join().await;
            Err(ProducerPipelineError::Pipeline(err))
        }
        Ok(summary) => match producer.join().await {
            Ok(Ok(())) => Ok(summary),
            Ok(Err(err)) => Err(ProducerPipelineError::Producer(err)),
            Err(err) => Err(ProducerPipelineError::ProducerJoin(err)),
        },
    }
}

pub async fn run_event_receiver_pipeline<W, C>(
    events: mpsc::Receiver<NormalizedEvent>,
    writer: &W,
    cursor_store: &C,
    stream_name: &str,
    config: PipelineConfig,
) -> Result<PipelineSummary, PipelineError<W::Error, C::Error>>
where
    W: EventWriter + Sync,
    C: CursorStore + Sync,
{
    run_event_receiver_pipeline_with_progress(
        events,
        writer,
        cursor_store,
        stream_name,
        config,
        |_| {},
    )
    .await
}

async fn run_event_receiver_pipeline_with_progress<W, C, O>(
    mut events: mpsc::Receiver<NormalizedEvent>,
    writer: &W,
    cursor_store: &C,
    stream_name: &str,
    config: PipelineConfig,
    mut on_progress: O,
) -> Result<PipelineSummary, PipelineError<W::Error, C::Error>>
where
    W: EventWriter + Sync,
    C: CursorStore + Sync,
    O: FnMut(PipelineSummary) + Send,
{
    let mut batcher = Batcher::new(config.batch_size);
    let mut summary = PipelineSummary {
        last_persisted_slot: config.resume_after_slot,
        ..PipelineSummary::default()
    };
    on_progress(summary);

    while let Some(event) = events.recv().await {
        summary.events_seen += 1;

        if should_skip_event(&event, config.resume_after_slot) {
            summary.events_skipped += 1;
            continue;
        }

        if let Some(batch) = batcher.push(event) {
            write_batch(writer, cursor_store, stream_name, &batch, &mut summary).await?;
            on_progress(summary);
        }
    }

    if let Some(batch) = batcher.flush() {
        write_batch(writer, cursor_store, stream_name, &batch, &mut summary).await?;
    }

    on_progress(summary);
    Ok(summary)
}

async fn send_events<I>(events: I, sender: mpsc::Sender<NormalizedEvent>)
where
    I: IntoIterator<Item = NormalizedEvent>,
{
    for event in events {
        if sender.send(event).await.is_err() {
            break;
        }
    }
}

fn should_skip_event(event: &NormalizedEvent, resume_after_slot: Option<u64>) -> bool {
    resume_after_slot.is_some_and(|slot| event.slot() <= slot)
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
    let last_slot = batch.iter().map(|event| event.slot()).max();
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
    use super::{
        PipelineConfig, PipelineError, PipelineSummary, ProducerPipelineError,
        run_event_producer_pipeline, run_event_producer_pipeline_with_progress,
        run_event_receiver_pipeline, run_replay_pipeline, send_events,
    };
    use async_trait::async_trait;
    use serde_json::json;
    use solana_yellowstone_domain::event::{EventIdentity, NormalizedEvent};
    use solana_yellowstone_storage::{
        CursorStore, EventWriter, WriteSummary, cursor::StreamCursor,
    };
    use std::fmt;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

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

        async fn get_cursor(&self, stream_name: &str) -> Result<Option<StreamCursor>, Self::Error> {
            Ok(Some(StreamCursor {
                stream_name: stream_name.to_owned(),
                last_persisted_slot: 0,
            }))
        }

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

        async fn get_cursor(
            &self,
            _stream_name: &str,
        ) -> Result<Option<StreamCursor>, Self::Error> {
            Err(FakeError)
        }

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
            EventIdentity::Transaction {
                cluster: "localnet".to_owned(),
                slot,
                signature: format!("sig-{slot}"),
                index: slot,
            },
            json!({ "slot": slot }),
        )
    }

    fn config(batch_size: usize) -> PipelineConfig {
        PipelineConfig {
            batch_size,
            channel_capacity: 10,
            resume_after_slot: None,
        }
    }

    #[tokio::test]
    async fn writes_full_and_partial_batches() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let events = [event(1), event(2), event(3), event(4), event(5)];

        let summary = run_replay_pipeline(events, &writer, &cursor_store, "replay", config(2))
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
        assert_eq!(summary.events_skipped, 0);
        assert_eq!(summary.batches_written, 3);
        assert_eq!(summary.write_summary.attempted, 5);
        assert_eq!(summary.write_summary.inserted, 5);
        assert_eq!(summary.write_summary.deduplicated, 0);
        assert_eq!(summary.last_persisted_slot, Some(5));
    }

    #[tokio::test]
    async fn producer_pipeline_drives_receiver_pipeline() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();

        let summary = run_event_producer_pipeline(
            |sender| async move {
                sender.send(event(1)).await.expect("send first event");
                sender.send(event(2)).await.expect("send second event");
                Ok::<(), FakeError>(())
            },
            &writer,
            &cursor_store,
            "producer",
            config(2),
        )
        .await
        .expect("producer pipeline should run");

        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![2]
        );
        assert_eq!(
            *cursor_store.updates.lock().expect("cursor updates lock"),
            vec![("producer".to_owned(), 2)]
        );
        assert_eq!(summary.events_seen, 2);
        assert_eq!(summary.batches_written, 1);
        assert_eq!(summary.last_persisted_slot, Some(2));
    }

    #[tokio::test]
    async fn producer_pipeline_reports_progress_after_successful_batch_writes() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let progress = std::sync::Arc::new(std::sync::Mutex::new(Vec::<PipelineSummary>::new()));
        let progress_for_callback = progress.clone();

        let summary = run_event_producer_pipeline_with_progress(
            |sender| async move {
                sender.send(event(1)).await.expect("send first event");
                sender.send(event(2)).await.expect("send second event");
                sender.send(event(3)).await.expect("send third event");
                Ok::<(), FakeError>(())
            },
            &writer,
            &cursor_store,
            "producer",
            config(2),
            move |summary| {
                progress_for_callback
                    .lock()
                    .expect("progress lock")
                    .push(summary);
            },
        )
        .await
        .expect("producer pipeline should run");

        assert_eq!(summary.last_persisted_slot, Some(3));
        let progress = progress.lock().expect("progress lock");
        assert_eq!(progress.len(), 3);
        assert_eq!(progress[0].last_persisted_slot, None);
        assert_eq!(progress[1].last_persisted_slot, Some(2));
        assert_eq!(progress[1].batches_written, 1);
        assert_eq!(progress[2].last_persisted_slot, Some(3));
        assert_eq!(progress[2].batches_written, 2);
    }

    #[tokio::test]
    async fn producer_pipeline_returns_producer_errors_after_flush() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();

        let err = run_event_producer_pipeline(
            |sender| async move {
                sender.send(event(1)).await.expect("send event");
                Err::<(), FakeError>(FakeError)
            },
            &writer,
            &cursor_store,
            "producer",
            config(2),
        )
        .await
        .expect_err("producer error should fail pipeline");

        assert!(matches!(err, ProducerPipelineError::Producer(_)));
        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![1]
        );
        assert_eq!(
            *cursor_store.updates.lock().expect("cursor updates lock"),
            vec![("producer".to_owned(), 1)]
        );
    }

    #[tokio::test]
    async fn consumes_events_from_bounded_receiver() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let (sender, receiver) = mpsc::channel(2);

        sender.send(event(1)).await.expect("send first event");
        sender.send(event(2)).await.expect("send second event");
        drop(sender);

        let summary =
            run_event_receiver_pipeline(receiver, &writer, &cursor_store, "receiver", config(2))
                .await
                .expect("pipeline should run");

        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![2]
        );
        assert_eq!(
            *cursor_store.updates.lock().expect("cursor updates lock"),
            vec![("receiver".to_owned(), 2)]
        );
        assert_eq!(summary.events_seen, 2);
        assert_eq!(summary.batches_written, 1);
        assert_eq!(summary.last_persisted_slot, Some(2));
    }

    #[tokio::test]
    async fn flushes_partial_batch_when_receiver_closes() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let (sender, receiver) = mpsc::channel(2);

        sender.send(event(1)).await.expect("send event");
        drop(sender);

        let summary =
            run_event_receiver_pipeline(receiver, &writer, &cursor_store, "receiver", config(2))
                .await
                .expect("pipeline should run");

        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![1]
        );
        assert_eq!(summary.events_seen, 1);
        assert_eq!(summary.batches_written, 1);
        assert_eq!(summary.write_summary.attempted, 1);
        assert_eq!(summary.last_persisted_slot, Some(1));
    }

    #[tokio::test]
    async fn receiver_pipeline_skips_events_at_or_before_resume_slot() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let (sender, receiver) = mpsc::channel(3);
        let config = PipelineConfig {
            resume_after_slot: Some(2),
            ..config(2)
        };

        sender.send(event(1)).await.expect("send first event");
        sender.send(event(2)).await.expect("send second event");
        sender.send(event(3)).await.expect("send third event");
        drop(sender);

        let summary =
            run_event_receiver_pipeline(receiver, &writer, &cursor_store, "receiver", config)
                .await
                .expect("pipeline should run");

        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![1]
        );
        assert_eq!(summary.events_seen, 3);
        assert_eq!(summary.events_skipped, 2);
        assert_eq!(summary.write_summary.attempted, 1);
        assert_eq!(summary.last_persisted_slot, Some(3));
    }

    #[tokio::test]
    async fn receiver_pipeline_returns_writer_errors() {
        let cursor_store = FakeCursorStore::default();
        let (sender, receiver) = mpsc::channel(1);

        sender.send(event(1)).await.expect("send event");
        drop(sender);

        let err = run_event_receiver_pipeline(
            receiver,
            &FailingWriter,
            &cursor_store,
            "receiver",
            config(1),
        )
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
    async fn receiver_pipeline_returns_cursor_errors_after_successful_write() {
        let writer = FakeWriter::default();
        let (sender, receiver) = mpsc::channel(1);

        sender.send(event(1)).await.expect("send event");
        drop(sender);

        let err = run_event_receiver_pipeline(
            receiver,
            &writer,
            &FailingCursorStore,
            "receiver",
            config(1),
        )
        .await
        .expect_err("cursor error should fail pipeline");

        assert!(matches!(err, PipelineError::Cursor(_)));
        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![1]
        );
    }

    #[tokio::test]
    async fn producer_stops_when_receiver_is_closed() {
        struct PanicAfterFirstEvent {
            next_slot: u64,
        }

        impl Iterator for PanicAfterFirstEvent {
            type Item = NormalizedEvent;

            fn next(&mut self) -> Option<Self::Item> {
                match self.next_slot {
                    1 => {
                        self.next_slot = 2;
                        Some(event(1))
                    }
                    _ => panic!("producer should stop after the first failed send"),
                }
            }
        }

        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);

        send_events(PanicAfterFirstEvent { next_slot: 1 }, sender).await;
    }

    #[tokio::test]
    async fn replay_pipeline_uses_bounded_channel_capacity() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let events = [event(1), event(2), event(3)];
        let config = PipelineConfig {
            channel_capacity: 1,
            ..config(2)
        };

        let summary = run_replay_pipeline(events, &writer, &cursor_store, "replay", config)
            .await
            .expect("pipeline should run");

        assert_eq!(summary.events_seen, 3);
        assert_eq!(summary.batches_written, 2);
        assert_eq!(summary.last_persisted_slot, Some(3));
    }

    #[tokio::test]
    async fn skips_events_at_or_before_resume_slot() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let events = [event(1), event(2), event(3), event(4), event(5)];
        let config = PipelineConfig {
            resume_after_slot: Some(2),
            ..config(2)
        };

        let summary = run_replay_pipeline(events, &writer, &cursor_store, "replay", config)
            .await
            .expect("pipeline should run");

        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![2, 1]
        );
        assert_eq!(
            *cursor_store.updates.lock().expect("cursor updates lock"),
            vec![("replay".to_owned(), 4), ("replay".to_owned(), 5)]
        );
        assert_eq!(summary.events_seen, 5);
        assert_eq!(summary.events_skipped, 2);
        assert_eq!(summary.batches_written, 2);
        assert_eq!(summary.write_summary.attempted, 3);
        assert_eq!(summary.last_persisted_slot, Some(5));
    }

    #[tokio::test]
    async fn keeps_existing_cursor_when_all_events_are_skipped() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let events = [event(1), event(2)];
        let config = PipelineConfig {
            resume_after_slot: Some(5),
            ..config(2)
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
        assert_eq!(summary.events_seen, 2);
        assert_eq!(summary.events_skipped, 2);
        assert_eq!(summary.batches_written, 0);
        assert_eq!(summary.write_summary.attempted, 0);
        assert_eq!(summary.last_persisted_slot, Some(5));
    }

    #[tokio::test]
    async fn does_not_write_empty_batch_for_empty_input() {
        let writer = FakeWriter::default();
        let cursor_store = FakeCursorStore::default();
        let events: Vec<NormalizedEvent> = Vec::new();

        let summary = run_replay_pipeline(events, &writer, &cursor_store, "replay", config(2))
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
        assert_eq!(summary.events_skipped, 0);
        assert_eq!(summary.batches_written, 0);
        assert_eq!(summary.write_summary.attempted, 0);
        assert_eq!(summary.last_persisted_slot, None);
    }

    #[tokio::test]
    async fn returns_writer_errors() {
        let cursor_store = FakeCursorStore::default();
        let events = [event(1)];

        let err = run_replay_pipeline(events, &FailingWriter, &cursor_store, "replay", config(1))
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

        let err = run_replay_pipeline(events, &writer, &FailingCursorStore, "replay", config(1))
            .await
            .expect_err("cursor error should fail pipeline");

        assert!(matches!(err, PipelineError::Cursor(_)));
        assert_eq!(
            *writer.batch_sizes.lock().expect("batch sizes lock"),
            vec![1]
        );
    }
}
