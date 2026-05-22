use crate::http::HttpError;
use solana_yellowstone_storage::cursor::PostgresCursorError;
use solana_yellowstone_storage::postgres::{PostgresInitError, PostgresWriteError};
use solana_yellowstone_stream::pipeline::PipelineError;
use solana_yellowstone_stream::replay::ReplayReadError;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    Empty { key: &'static str },
    InvalidUsize { key: &'static str, value: String },
    NotUnicode { key: &'static str },
    NonPositive { key: &'static str },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { key } => write!(f, "{key} must not be empty"),
            Self::InvalidUsize { key, value } => {
                write!(f, "{key} must be a positive integer, got {value:?}")
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
    Http(HttpError),
}

impl AppRunError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::Replay(_) => 3,
            Self::Postgres(_) => 4,
            Self::Cursor(_) | Self::Pipeline(_) => 5,
            Self::Http(_) => 6,
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
            Self::Http(err) => write!(f, "http error: {err}"),
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
            Self::Http(err) => Some(err),
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

    #[test]
    fn app_run_error_maps_replay_errors_to_exit_code_three() {
        let err = AppRunError::Replay(ReplayReadError::Open {
            path: PathBuf::from("missing.jsonl"),
            source: io::Error::new(io::ErrorKind::NotFound, "missing"),
        });

        assert_eq!(err.exit_code(), 3);
    }
}
