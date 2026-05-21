mod config;
mod error;
mod http;
mod telemetry;

use config::Config;
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

    println!(
        "solana-yellowstone-stream-processor loaded replay events; {}; events={}",
        config.redacted_summary(),
        events.len()
    );
}
