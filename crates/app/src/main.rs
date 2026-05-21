mod config;
mod error;
mod http;
mod telemetry;

use config::Config;
use solana_yellowstone_storage::postgres::PostgresEventWriter;
use solana_yellowstone_stream::pipeline::{PipelineConfig, run_replay_pipeline};
use solana_yellowstone_stream::replay::ReplaySource;

fn main() {
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

    let pipeline_config = PipelineConfig {
        batch_size: config.batch_size,
        channel_capacity: config.channel_capacity,
    };
    let writer = PostgresEventWriter;
    let summary = run_replay_pipeline(events, &writer, pipeline_config).unwrap_or_else(|err| {
        eprintln!("pipeline error: {err}");
        std::process::exit(4);
    });

    println!(
        "solana-yellowstone-stream-processor completed replay pipeline; {}; events_seen={}; batches_written={}; events_attempted={}; events_inserted={}; events_deduplicated={}",
        config.redacted_summary(),
        summary.events_seen,
        summary.batches_written,
        summary.write_summary.attempted,
        summary.write_summary.inserted,
        summary.write_summary.deduplicated,
    );
}
