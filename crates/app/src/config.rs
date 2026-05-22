use crate::error::ConfigError;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub http_addr: String,
    pub rust_log: String,
    pub replay_path: String,
    pub stream_name: String,
    pub batch_size: usize,
    pub channel_capacity: usize,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_source(&SystemEnv)
    }

    fn from_source(source: &impl ConfigSource) -> Result<Self, ConfigError> {
        Ok(Self {
            database_url: env_or_default(
                source,
                "DATABASE_URL",
                "postgres://postgres:postgres@localhost:5433/solana_stream",
            )?,
            http_addr: env_or_default(source, "HTTP_ADDR", "127.0.0.1:8080")?,
            rust_log: env_or_default(
                source,
                "RUST_LOG",
                "solana_yellowstone_stream_processor=info",
            )?,
            replay_path: env_or_default(source, "REPLAY_PATH", "fixtures/sample_stream.jsonl")?,
            stream_name: env_or_default(source, "STREAM_NAME", "replay")?,
            batch_size: env_positive_usize_or_default(source, "STREAM_BATCH_SIZE", 500)?,
            channel_capacity: env_positive_usize_or_default(
                source,
                "STREAM_CHANNEL_CAPACITY",
                10_000,
            )?,
        })
    }

    pub fn redacted_summary(&self) -> String {
        format!(
            "http_addr={}; replay_path={}; stream_name={}; batch_size={}; channel_capacity={}; database_url_configured={}",
            self.http_addr,
            self.replay_path,
            self.stream_name,
            self.batch_size,
            self.channel_capacity,
            !self.database_url.is_empty()
        )
    }
}

trait ConfigSource {
    fn get(&self, key: &'static str) -> Result<Option<String>, ConfigError>;
}

struct SystemEnv;

impl ConfigSource for SystemEnv {
    fn get(&self, key: &'static str) -> Result<Option<String>, ConfigError> {
        match std::env::var(key) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::NotUnicode { key }),
        }
    }
}

fn env_or_default(
    source: &impl ConfigSource,
    key: &'static str,
    default: &str,
) -> Result<String, ConfigError> {
    match source.get(key)? {
        Some(value) if value.trim().is_empty() => Err(ConfigError::Empty { key }),
        Some(value) => Ok(value),
        None => Ok(default.to_owned()),
    }
}

fn env_positive_usize_or_default(
    source: &impl ConfigSource,
    key: &'static str,
    default: usize,
) -> Result<usize, ConfigError> {
    let raw = env_or_default(source, key, &default.to_string())?;
    let value = raw
        .parse::<usize>()
        .map_err(|_| ConfigError::InvalidUsize {
            key,
            value: raw.clone(),
        })?;

    if value == 0 {
        Err(ConfigError::NonPositive { key })
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, ConfigSource};
    use crate::error::ConfigError;
    use std::collections::HashMap;

    #[derive(Default)]
    struct FakeEnv {
        values: HashMap<&'static str, String>,
    }

    impl FakeEnv {
        fn with(mut self, key: &'static str, value: impl Into<String>) -> Self {
            self.values.insert(key, value.into());
            self
        }
    }

    impl ConfigSource for FakeEnv {
        fn get(&self, key: &'static str) -> Result<Option<String>, ConfigError> {
            Ok(self.values.get(key).cloned())
        }
    }

    #[test]
    fn uses_defaults_when_env_is_missing() {
        let config = Config::from_source(&FakeEnv::default()).expect("config should load");

        assert_eq!(config.http_addr, "127.0.0.1:8080");
        assert_eq!(config.rust_log, "solana_yellowstone_stream_processor=info");
        assert_eq!(config.replay_path, "fixtures/sample_stream.jsonl");
        assert_eq!(config.stream_name, "replay");
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.channel_capacity, 10_000);
    }

    #[test]
    fn reads_overrides_from_source() {
        let source = FakeEnv::default()
            .with("HTTP_ADDR", "0.0.0.0:9000")
            .with("RUST_LOG", "debug")
            .with("REPLAY_PATH", "fixtures/custom.jsonl")
            .with("STREAM_NAME", "custom-replay")
            .with("STREAM_BATCH_SIZE", "42")
            .with("STREAM_CHANNEL_CAPACITY", "2048");

        let config = Config::from_source(&source).expect("config should load");

        assert_eq!(config.http_addr, "0.0.0.0:9000");
        assert_eq!(config.rust_log, "debug");
        assert_eq!(config.replay_path, "fixtures/custom.jsonl");
        assert_eq!(config.stream_name, "custom-replay");
        assert_eq!(config.batch_size, 42);
        assert_eq!(config.channel_capacity, 2048);
    }

    #[test]
    fn rejects_empty_values() {
        let source = FakeEnv::default().with("HTTP_ADDR", " ");

        let err = Config::from_source(&source).expect_err("empty value should fail");

        assert_eq!(err, ConfigError::Empty { key: "HTTP_ADDR" });
    }

    #[test]
    fn rejects_invalid_usize_values() {
        let source = FakeEnv::default().with("STREAM_BATCH_SIZE", "abc");

        let err = Config::from_source(&source).expect_err("invalid usize should fail");

        assert_eq!(
            err,
            ConfigError::InvalidUsize {
                key: "STREAM_BATCH_SIZE",
                value: "abc".to_owned()
            }
        );
    }

    #[test]
    fn rejects_zero_capacity_values() {
        let source = FakeEnv::default().with("STREAM_CHANNEL_CAPACITY", "0");

        let err = Config::from_source(&source).expect_err("zero capacity should fail");

        assert_eq!(
            err,
            ConfigError::NonPositive {
                key: "STREAM_CHANNEL_CAPACITY"
            }
        );
    }
}
