mod config;
mod error;
mod http;
mod telemetry;

use config::Config;
use solana_yellowstone_stream::replay::ReplaySource;

fn main() {
    let config = Config::from_env();
    telemetry::init(&config);

    let replay = ReplaySource::new(config.replay_path.clone());
    let sample = replay.sample_event();

    println!(
        "solana-yellowstone-stream-processor starting in replay mode; {}; sample_event_id={}",
        config.redacted_summary(),
        sample.event_id()
    );
}
