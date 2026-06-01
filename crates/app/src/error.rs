use crate::http::HttpError;
use solana_yellowstone_storage::cursor::PostgresCursorError;
use solana_yellowstone_storage::postgres::{PostgresInitError, PostgresWriteError};
use solana_yellowstone_stream::pipeline::PipelineError;
#[cfg(feature = "yellowstone-live")]
use solana_yellowstone_stream::pipeline::ProducerPipelineError;
use solana_yellowstone_stream::replay::ReplayReadError;
#[cfg(feature = "yellowstone-live")]
use solana_yellowstone_stream::yellowstone_live::YellowstoneGrpcError;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Empty {
        key: &'static str,
    },
    InvalidRunMode {
        key: &'static str,
        value: String,
    },
    InvalidYellowstoneSubscription {
        key: &'static str,
        value: String,
    },
    InvalidUsize {
        key: &'static str,
        value: String,
    },
    MissingRequired {
        key: &'static str,
        context: &'static str,
    },
    NotUnicode {
        key: &'static str,
    },
    NonPositive {
        key: &'static str,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { key } => write!(f, "{key} must not be empty"),
            Self::InvalidRunMode { key, value } => {
                write!(f, "{key} must be replay or yellowstone, got {value:?}")
            }
            Self::InvalidYellowstoneSubscription { key, value } => write!(
                f,
                "{key} must be a comma-separated list containing slots, transactions, blocks, or entries; got {value:?}"
            ),
            Self::InvalidUsize { key, value } => {
                write!(f, "{key} must be a positive integer, got {value:?}")
            }
            Self::MissingRequired { key, context } => {
                write!(f, "{key} is required when {context}")
            }
            Self::NotUnicode { key } => write!(f, "{key} contains non-unicode data"),
            Self::NonPositive { key } => write!(f, "{key} must be greater than zero"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug)]
pub enum AppRunError {
    Replay(ReplayReadError),
    Postgres(PostgresInitError),
    Cursor(PostgresCursorError),
    Pipeline(PipelineError<PostgresWriteError, PostgresCursorError>),
    #[cfg(feature = "yellowstone-live")]
    YellowstonePipeline(
        ProducerPipelineError<PostgresWriteError, PostgresCursorError, YellowstoneGrpcError>,
    ),
    Http(HttpError),
    #[cfg(not(feature = "yellowstone-live"))]
    YellowstoneRuntimeNotImplemented,
}

impl AppRunError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Replay(_) => 3,
            Self::Postgres(_) => 4,
            Self::Cursor(_) | Self::Pipeline(_) => 5,
            #[cfg(feature = "yellowstone-live")]
            Self::YellowstonePipeline(_) => 5,
            Self::Http(_) => 6,
            #[cfg(not(feature = "yellowstone-live"))]
            Self::YellowstoneRuntimeNotImplemented => 7,
        }
    }
}

impl fmt::Display for AppRunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Replay(err) => write!(f, "replay error: {err}"),
            Self::Postgres(err) => write!(f, "postgres error: {err}"),
            Self::Cursor(err) => write!(f, "cursor error: {err}"),
            Self::Pipeline(err) => write!(f, "pipeline error: {err}"),
            #[cfg(feature = "yellowstone-live")]
            Self::YellowstonePipeline(err) => write!(f, "yellowstone pipeline error: {err}"),
            Self::Http(err) => write!(f, "http error: {err}"),
            #[cfg(not(feature = "yellowstone-live"))]
            Self::YellowstoneRuntimeNotImplemented => {
                f.write_str("yellowstone live runtime is not implemented yet")
            }
        }
    }
}

impl std::error::Error for AppRunError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Replay(err) => Some(err),
            Self::Postgres(err) => Some(err),
            Self::Cursor(err) => Some(err),
            Self::Pipeline(err) => Some(err),
            #[cfg(feature = "yellowstone-live")]
            Self::YellowstonePipeline(err) => Some(err),
            Self::Http(err) => Some(err),
            #[cfg(not(feature = "yellowstone-live"))]
            Self::YellowstoneRuntimeNotImplemented => None,
        }
    }
}

impl From<ReplayReadError> for AppRunError {
    fn from(err: ReplayReadError) -> Self {
        Self::Replay(err)
    }
}

impl From<PostgresInitError> for AppRunError {
    fn from(err: PostgresInitError) -> Self {
        Self::Postgres(err)
    }
}

impl From<PostgresCursorError> for AppRunError {
    fn from(err: PostgresCursorError) -> Self {
        Self::Cursor(err)
    }
}

impl From<PipelineError<PostgresWriteError, PostgresCursorError>> for AppRunError {
    fn from(err: PipelineError<PostgresWriteError, PostgresCursorError>) -> Self {
        Self::Pipeline(err)
    }
}

#[cfg(feature = "yellowstone-live")]
impl From<ProducerPipelineError<PostgresWriteError, PostgresCursorError, YellowstoneGrpcError>>
    for AppRunError
{
    fn from(
        err: ProducerPipelineError<PostgresWriteError, PostgresCursorError, YellowstoneGrpcError>,
    ) -> Self {
        Self::YellowstonePipeline(err)
    }
}

impl From<HttpError> for AppRunError {
    fn from(err: HttpError) -> Self {
        Self::Http(err)
    }
}

#[cfg(test)]
mod tests {
    use super::{AppRunError, ConfigError};
    use solana_yellowstone_stream::replay::ReplayReadError;
    use std::io;
    use std::path::PathBuf;

    #[test]
    fn config_error_formats_env_key() {
        let err = ConfigError::Empty { key: "HTTP_ADDR" };

        assert_eq!(err.to_string(), "HTTP_ADDR must not be empty");
    }

    #[cfg(not(feature = "yellowstone-live"))]
    #[test]
    fn app_run_error_maps_yellowstone_runtime_to_exit_code_seven() {
        let err = AppRunError::YellowstoneRuntimeNotImplemented;

        assert_eq!(err.exit_code(), 7);
        assert_eq!(
            err.to_string(),
            "yellowstone live runtime is not implemented yet"
        );
    }

    #[test]
    fn app_run_error_maps_replay_errors_to_exit_code_three() {
        let err = AppRunError::Replay(ReplayReadError::Open {
            path: PathBuf::from("missing.jsonl"),
            source: io::Error::new(io::ErrorKind::NotFound, "missing"),
        });

        assert_eq!(err.exit_code(), 3);
    }
}
