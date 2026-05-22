use crate::config::Config;
use crate::error::AppRunError;
use crate::http::{self, StatusSnapshot};
use solana_yellowstone_storage::{
    CursorStore, cursor::PostgresCursorStore, postgres::PostgresEventWriter,
};
use solana_yellowstone_stream::pipeline::{PipelineConfig, run_replay_pipeline};
use solana_yellowstone_stream::replay::ReplaySource;
use tracing::info;

pub async fn run(config: Config) -> Result<(), AppRunError> {
    info!(config = %config.redacted_summary(), "configuration loaded");

    let replay = ReplaySource::new(config.replay_path.clone());
    info!(replay_path = %config.replay_path, "reading replay events");
    let events = replay.read_events()?;
    info!(events_loaded = events.len(), "replay events loaded");

    info!("connecting to postgres");
    let writer = PostgresEventWriter::connect(&config.database_url).await?;
    info!("postgres initialized");

    let cursor_store = PostgresCursorStore::from_pool(writer.pool().clone());
    let cursor = cursor_store.get_cursor(&config.stream_name).await?;
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
    .await?;

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

    if config.exit_after_replay {
        info!(exit_after_replay = true, "exit after replay requested");
        return Ok(());
    }

    let status = StatusSnapshot::from_pipeline(config.stream_name.clone(), summary);
    info!(http_addr = %config.http_addr, "serving http endpoints");
    http::serve(&config.http_addr, status).await?;
    info!("http server stopped");

    Ok(())
}
