use crate::config::Config;
use std::fmt;
use tracing_subscriber::EnvFilter;

pub fn init(config: &Config) -> Result<(), TelemetryError> {
    let filter = EnvFilter::try_new(&config.rust_log).map_err(TelemetryError::InvalidFilter)?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(TelemetryError::Init)
}

#[derive(Debug)]
pub enum TelemetryError {
    InvalidFilter(tracing_subscriber::filter::ParseError),
    Init(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for TelemetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFilter(err) => write!(f, "invalid RUST_LOG filter: {err}"),
            Self::Init(err) => write!(f, "failed to initialize telemetry: {err}"),
        }
    }
}

impl std::error::Error for TelemetryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidFilter(err) => Some(err),
            Self::Init(err) => Some(err.as_ref()),
        }
    }
}
