use solana_yellowstone_storage::{
    CursorStore, cursor::PostgresCursorStore, postgres::PostgresEventWriter,
};
use solana_yellowstone_stream::{
    pipeline::{PipelineConfig, run_replay_pipeline},
    replay::ReplaySource,
    source::EventSource,
};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

#[tokio::test]
#[ignore = "requires local postgres; run `make test-postgres`"]
async fn replay_fixture_persists_deduplicates_and_updates_cursor() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must be set for postgres integration tests");
    let writer = PostgresEventWriter::connect(&database_url)
        .await
        .expect("postgres writer should connect");
    let cursor_store = PostgresCursorStore::from_pool(writer.pool().clone());
    let fixture = write_replay_fixture();
    let source = ReplaySource::new(&fixture.path);
    let events = EventSource::read_events(&source).expect("fixture events should read");

    let first_summary = run_replay_pipeline(
        events.clone(),
        &writer,
        &cursor_store,
        &fixture.primary_stream_name,
        pipeline_config(),
    )
    .await
    .expect("first replay should run");

    assert_eq!(first_summary.events_seen, 3);
    assert_eq!(first_summary.events_skipped, 0);
    assert_eq!(first_summary.batches_written, 1);
    assert_eq!(first_summary.write_summary.attempted, 3);
    assert_eq!(first_summary.write_summary.inserted, 2);
    assert_eq!(first_summary.write_summary.deduplicated, 1);
    assert_eq!(first_summary.last_persisted_slot, Some(2));

    let first_cursor = cursor_store
        .get_cursor(&fixture.primary_stream_name)
        .await
        .expect("cursor should read")
        .expect("cursor should exist");
    assert_eq!(first_cursor.last_persisted_slot, 2);

    let second_summary = run_replay_pipeline(
        events,
        &writer,
        &cursor_store,
        &fixture.secondary_stream_name,
        pipeline_config(),
    )
    .await
    .expect("second replay should run");

    assert_eq!(second_summary.events_seen, 3);
    assert_eq!(second_summary.events_skipped, 0);
    assert_eq!(second_summary.batches_written, 1);
    assert_eq!(second_summary.write_summary.attempted, 3);
    assert_eq!(second_summary.write_summary.inserted, 0);
    assert_eq!(second_summary.write_summary.deduplicated, 3);
    assert_eq!(second_summary.last_persisted_slot, Some(2));

    let second_cursor = cursor_store
        .get_cursor(&fixture.secondary_stream_name)
        .await
        .expect("cursor should read")
        .expect("cursor should exist");
    assert_eq!(second_cursor.last_persisted_slot, 2);

    fs::remove_file(fixture.path).expect("remove fixture");
}

fn pipeline_config() -> PipelineConfig {
    PipelineConfig {
        batch_size: 10,
        channel_capacity: 2,
        resume_after_slot: None,
    }
}

struct ReplayFixture {
    path: PathBuf,
    primary_stream_name: String,
    secondary_stream_name: String,
}

fn write_replay_fixture() -> ReplayFixture {
    let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    let process_id = std::process::id();
    let unique_prefix = format!("e2e-replay-{process_id}-{id}");
    let path = std::env::temp_dir().join(format!("{unique_prefix}.jsonl"));
    let first = format!(
        r#"{{"slot":1,"signature":"{unique_prefix}-sig-1","program_id":"program-1","account":null,"event_type":"transaction","payload":{{"index":1}}}}"#
    );
    let second = format!(
        r#"{{"slot":2,"signature":"{unique_prefix}-sig-2","program_id":"program-1","account":null,"event_type":"transaction","payload":{{"index":2}}}}"#
    );
    let contents = format!("{first}\n{second}\n{second}\n");

    fs::write(&path, contents).expect("write replay fixture");

    ReplayFixture {
        path,
        primary_stream_name: format!("{unique_prefix}-primary"),
        secondary_stream_name: format!("{unique_prefix}-secondary"),
    }
}
