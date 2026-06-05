#![allow(clippy::unwrap_used, clippy::expect_used)]

use serde_json::json;
use solana_yellowstone_domain::event::{EventIdentity, NormalizedEvent, SlotStatus};
use solana_yellowstone_storage::{
    CursorStore,
    cursor::PostgresCursorStore,
    postgres::PostgresEventWriter,
    slots::{PostgresSlotStateStore, SlotStateStore},
};
use solana_yellowstone_stream::pipeline::{
    PipelineConfig, run_event_producer_pipeline_with_progress_and_activity,
};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

fn gap_fixture() -> PathBuf {
    let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("gap-replay-{}-{id}.jsonl", std::process::id()));

    let lines = [
        r#"{"identity":{"kind":"slot","cluster":"localnet","slot":100,"status":"finalized"},"payload":{"parent":99}}"#,
        r#"{"identity":{"kind":"slot","cluster":"localnet","slot":101,"status":"finalized"},"payload":{"parent":100}}"#,
        r#"{"identity":{"kind":"slot","cluster":"localnet","slot":103,"status":"finalized"},"payload":{"parent":102}}"#,
    ];
    fs::write(&path, lines.join("\n") + "\n").expect("write gap fixture");
    path
}

#[tokio::test]
#[ignore = "requires local postgres; run `make compose-up test-postgres`"]
async fn gap_replay_leaves_contiguous_cursor_at_gap_boundary() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must be set for postgres integration tests");
    let writer = PostgresEventWriter::connect(&database_url)
        .await
        .expect("postgres writer should connect");
    let cursor_store = PostgresCursorStore::from_pool(writer.pool().clone());
    let slot_state_store = PostgresSlotStateStore::from_pool(writer.pool().clone());

    let path = gap_fixture();
    let source = solana_yellowstone_stream::replay::ReplaySource::new(&path);
    let events = solana_yellowstone_stream::source::EventSource::read_events(&source)
        .expect("fixture events should read");

    let stream_name = format!("gap-replay-{}", std::process::id());

    let summary = run_event_producer_pipeline_with_progress_and_activity(
        move |sender| async move {
            for event in events {
                if sender.send(event).await.is_err() {
                    break;
                }
            }
            Ok::<(), std::convert::Infallible>(())
        },
        &writer,
        &cursor_store,
        &slot_state_store,
        &stream_name,
        PipelineConfig {
            batch_size: 10,
            channel_capacity: 10,
            resume_after_slot: None,
            advance_finalized_watermark: true,
            use_slot_resume: false,
        },
        |_| {},
        |_| {},
    )
    .await
    .expect("gap replay pipeline should run");

    assert_eq!(summary.events_seen, 3);
    assert_eq!(summary.batches_written, 1);
    assert_eq!(summary.last_persisted_slot, Some(103));
    assert_eq!(summary.last_contiguous_finalized_slot, Some(101));
    assert_eq!(summary.last_finalized_slot, Some(103));

    let cursor = cursor_store
        .get_cursor(&stream_name)
        .await
        .expect("cursor should read")
        .expect("cursor should exist");
    assert_eq!(cursor.last_persisted_slot, 103);

    let frontier = slot_state_store
        .get_finalized_frontier(&stream_name)
        .await
        .expect("frontier should read");
    assert_eq!(frontier.last_contiguous_finalized_slot, Some(101));
    assert_eq!(frontier.last_finalized_slot, Some(103));

    fs::remove_file(path).expect("remove fixture");
}

#[tokio::test]
#[ignore = "requires local postgres; run `make compose-up test-postgres`"]
async fn gap_replay_resumes_after_gap_is_filled() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must be set for postgres integration tests");
    let writer = PostgresEventWriter::connect(&database_url)
        .await
        .expect("postgres writer should connect");
    let cursor_store = PostgresCursorStore::from_pool(writer.pool().clone());
    let slot_state_store = PostgresSlotStateStore::from_pool(writer.pool().clone());

    let stream_name = format!("gap-fill-replay-{}", std::process::id());

    let first_events = vec![
        slot_event(100, 99),
        slot_event(101, 100),
        slot_event(103, 102),
    ];

    let first_summary = run_event_producer_pipeline_with_progress_and_activity(
        move |sender| async move {
            for event in first_events {
                if sender.send(event).await.is_err() {
                    break;
                }
            }
            Ok::<(), std::convert::Infallible>(())
        },
        &writer,
        &cursor_store,
        &slot_state_store,
        &stream_name,
        PipelineConfig {
            batch_size: 10,
            channel_capacity: 10,
            resume_after_slot: None,
            advance_finalized_watermark: true,
            use_slot_resume: false,
        },
        |_| {},
        |_| {},
    )
    .await
    .expect("first gap replay should run");

    assert_eq!(first_summary.last_contiguous_finalized_slot, Some(101));

    let gap_fill_events = vec![slot_event(102, 101)];

    let second_summary = run_event_producer_pipeline_with_progress_and_activity(
        move |sender| async move {
            for event in gap_fill_events {
                if sender.send(event).await.is_err() {
                    break;
                }
            }
            Ok::<(), std::convert::Infallible>(())
        },
        &writer,
        &cursor_store,
        &slot_state_store,
        &stream_name,
        PipelineConfig {
            batch_size: 10,
            channel_capacity: 10,
            resume_after_slot: None,
            advance_finalized_watermark: true,
            use_slot_resume: false,
        },
        |_| {},
        |_| {},
    )
    .await
    .expect("gap fill replay should run");

    assert_eq!(second_summary.last_contiguous_finalized_slot, Some(103));
    assert_eq!(second_summary.last_finalized_slot, Some(103));

    let frontier = slot_state_store
        .get_finalized_frontier(&stream_name)
        .await
        .expect("frontier should read");
    assert_eq!(frontier.last_contiguous_finalized_slot, Some(103));
    assert_eq!(frontier.last_finalized_slot, Some(103));
}

fn slot_event(slot: u64, parent: u64) -> NormalizedEvent {
    NormalizedEvent::new(
        EventIdentity::Slot {
            cluster: "localnet".to_owned(),
            slot,
            status: SlotStatus::Finalized,
        },
        json!({"parent": parent}),
    )
}
