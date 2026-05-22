mod config;
mod error;
mod http;
mod telemetry;

use config::Config;
use http::StatusSnapshot;
use solana_yellowstone_storage::{
    CursorStore, cursor::PostgresCursorStore, postgres::PostgresEventWriter,
};
use solana_yellowstone_stream::pipeline::{PipelineConfig, run_replay_pipeline};
use solana_yellowstone_stream::replay::ReplaySource;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    let config = Config::from_env().unwrap_or_else(|err| {
        eprintln!("configuration error: {err}");
        std::process::exit(2);
    });
    telemetry::init(&config).unwrap_or_else(|err| {
        eprintln!("telemetry error: {err}");
        std::process::exit(2);
    });
    info!(config = %config.redacted_summary(), "configuration loaded");

    let replay = ReplaySource::new(config.replay_path.clone());
    info!(replay_path = %config.replay_path, "reading replay events");
    let events = replay.read_events().unwrap_or_else(|err| {
        error!(error = %err, "replay read failed");
        std::process::exit(3);
    });
    info!(events_loaded = events.len(), "replay events loaded");

    info!("connecting to postgres");
    let writer = PostgresEventWriter::connect(&config.database_url)
        .await
        .unwrap_or_else(|err| {
            error!(error = %err, "postgres initialization failed");
            std::process::exit(4);
        });
    info!("postgres initialized");
    let cursor_store = PostgresCursorStore::from_pool(writer.pool().clone());
    let cursor = cursor_store
        .get_cursor(&config.stream_name)
        .await
        .unwrap_or_else(|err| {
            error!(error = %err, stream_name = %config.stream_name, "cursor read failed");
            std::process::exit(5);
        });
    let resume_after_slot = cursor.as_ref().map(|cursor| cursor.last_persisted_slot);
    if let Some(slot) = resume_after_slot {
        info!(
            stream_name = %config.stream_name,
            last_persisted_slot = slot,
            "loaded stream cursor"
        );
    } else {
        info!(stream_name = %config.stream_name, "stream cursor not found");
    }

    let pipeline_config = PipelineConfig {
        batch_size: config.batch_size,
        channel_capacity: config.channel_capacity,
        resume_after_slot,
    };
    info!(
        stream_name = %config.stream_name,
        batch_size = config.batch_size,
        channel_capacity = config.channel_capacity,
        resume_after_slot = ?pipeline_config.resume_after_slot,
        "running replay pipeline"
    );
    let summary = run_replay_pipeline(
        events,
        &writer,
        &cursor_store,
        &config.stream_name,
        pipeline_config,
    )
    .await
    .unwrap_or_else(|err| {
        error!(error = %err, "pipeline failed");
        std::process::exit(5);
    });

    info!(
        events_seen = summary.events_seen,
        events_skipped = summary.events_skipped,
        batches_written = summary.batches_written,
        events_attempted = summary.write_summary.attempted,
        events_inserted = summary.write_summary.inserted,
        events_deduplicated = summary.write_summary.deduplicated,
        last_persisted_slot = ?summary.last_persisted_slot,
        "replay pipeline completed"
    );

    let status = StatusSnapshot::from_pipeline(config.stream_name.clone(), summary);
    info!(http_addr = %config.http_addr, "serving http endpoints");
    http::serve(&config.http_addr, status)
        .await
        .unwrap_or_else(|err| {
            error!(error = %err, "http server failed");
            std::process::exit(6);
        });
}
