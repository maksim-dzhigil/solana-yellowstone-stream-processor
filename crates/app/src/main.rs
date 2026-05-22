mod app;
mod config;
mod error;
mod http;
mod telemetry;

use config::Config;
use tracing::error;

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

    if let Err(err) = app::run(config).await {
        error!(error = %err, "application failed");
        std::process::exit(err.exit_code());
    }
}
