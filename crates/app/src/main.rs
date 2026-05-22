mod config;
mod error;
mod http;
mod telemetry;

use config::Config;
use solana_yellowstone_storage::{
    CursorStore, cursor::PostgresCursorStore, postgres::PostgresEventWriter,
};
use solana_yellowstone_stream::pipeline::{PipelineConfig, run_replay_pipeline};
use solana_yellowstone_stream::replay::ReplaySource;

#[tokio::main]
async fn main() {
    let config = Config::from_env().unwrap_or_else(|err| {
        eprintln!("configuration error: {err}");
        std::process::exit(2);
    });
    telemetry::init(&config);

    let replay = ReplaySource::new(config.replay_path.clone());
    let events = replay.read_events().unwrap_or_else(|err| {
        eprintln!("replay error: {err}");
        std::process::exit(3);
    });

    let writer = PostgresEventWriter::connect(&config.database_url)
        .await
        .unwrap_or_else(|err| {
            eprintln!("postgres error: {err}");
            std::process::exit(4);
        });
    let cursor_store = PostgresCursorStore::from_pool(writer.pool().clone());
    let cursor = cursor_store
        .get_cursor(&config.stream_name)
        .await
        .unwrap_or_else(|err| {
            eprintln!("cursor error: {err}");
            std::process::exit(5);
        });
    let pipeline_config = PipelineConfig {
        batch_size: config.batch_size,
        channel_capacity: config.channel_capacity,
        resume_after_slot: cursor.as_ref().map(|cursor| cursor.last_persisted_slot),
    };
    let summary = run_replay_pipeline(
        events,
        &writer,
        &cursor_store,
        &config.stream_name,
        pipeline_config,
    )
    .await
    .unwrap_or_else(|err| {
        eprintln!("pipeline error: {err}");
        std::process::exit(5);
    });

    println!(
        "solana-yellowstone-stream-processor completed replay pipeline; {}; events_seen={}; events_skipped={}; batches_written={}; events_attempted={}; events_inserted={}; events_deduplicated={}; last_persisted_slot={:?}",
        config.redacted_summary(),
        summary.events_seen,
        summary.events_skipped,
        summary.batches_written,
        summary.write_summary.attempted,
        summary.write_summary.inserted,
        summary.write_summary.deduplicated,
        summary.last_persisted_slot,
    );
}
