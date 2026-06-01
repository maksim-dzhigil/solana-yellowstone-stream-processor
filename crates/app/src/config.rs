use crate::cli::{CliArgs, CliRunMode};
use crate::error::ConfigError;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Replay,
    Yellowstone,
}

impl RunMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::Yellowstone => "yellowstone",
        }
    }
}

impl fmt::Display for RunMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<CliRunMode> for RunMode {
    fn from(value: CliRunMode) -> Self {
        match value {
            CliRunMode::Replay => Self::Replay,
            CliRunMode::Yellowstone => Self::Yellowstone,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct YellowstoneSubscriptions {
    pub slots: bool,
    pub transactions: bool,
    pub blocks: bool,
    pub entries: bool,
}

impl YellowstoneSubscriptions {
    pub fn slots_only() -> Self {
        Self {
            slots: true,
            transactions: false,
            blocks: false,
            entries: false,
        }
    }

    pub fn as_csv(&self) -> String {
        let mut values = Vec::new();
        if self.slots {
            values.push("slots");
        }
        if self.transactions {
            values.push("transactions");
        }
        if self.blocks {
            values.push("blocks");
        }
        if self.entries {
            values.push("entries");
        }
        values.join(",")
    }
}

#[derive(Clone)]
pub struct Config {
    pub run_mode: RunMode,
    pub database_url: String,
    pub http_addr: String,
    pub rust_log: String,
    pub replay_path: String,
    pub stream_name: String,
    pub exit_after_replay: bool,
    pub batch_size: usize,
    pub channel_capacity: usize,
    pub yellowstone_endpoint: Option<String>,
    pub yellowstone_x_token: Option<String>,
    pub yellowstone_cluster: String,
    pub yellowstone_subscriptions: YellowstoneSubscriptions,
    pub yellowstone_transaction_account_include: Vec<String>,
    pub yellowstone_transaction_account_exclude: Vec<String>,
    pub yellowstone_transaction_account_required: Vec<String>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("run_mode", &self.run_mode)
            .field("http_addr", &self.http_addr)
            .field("rust_log", &self.rust_log)
            .field("replay_path", &self.replay_path)
            .field("stream_name", &self.stream_name)
            .field("exit_after_replay", &self.exit_after_replay)
            .field("batch_size", &self.batch_size)
            .field("channel_capacity", &self.channel_capacity)
            .field("database_url_configured", &!self.database_url.is_empty())
            .field(
                "yellowstone_endpoint_configured",
                &self.yellowstone_endpoint.is_some(),
            )
            .field(
                "yellowstone_x_token_configured",
                &self.yellowstone_x_token.is_some(),
            )
            .field("yellowstone_cluster", &self.yellowstone_cluster)
            .field(
                "yellowstone_subscriptions",
                &self.yellowstone_subscriptions.as_csv(),
            )
            .field(
                "yellowstone_transaction_account_include_count",
                &self.yellowstone_transaction_account_include.len(),
            )
            .field(
                "yellowstone_transaction_account_exclude_count",
                &self.yellowstone_transaction_account_exclude.len(),
            )
            .field(
                "yellowstone_transaction_account_required_count",
                &self.yellowstone_transaction_account_required.len(),
            )
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_source(&SystemEnv)
    }

    fn from_source(source: &impl ConfigSource) -> Result<Self, ConfigError> {
        Ok(Self {
            run_mode: parse_run_mode(&env_or_default(source, "RUN_MODE", "replay")?)?,
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
            exit_after_replay: false,
            batch_size: env_positive_usize_or_default(source, "STREAM_BATCH_SIZE", 500)?,
            channel_capacity: env_positive_usize_or_default(
                source,
                "STREAM_CHANNEL_CAPACITY",
                10_000,
            )?,
            yellowstone_endpoint: env_optional_non_empty(source, "YELLOWSTONE_ENDPOINT")?,
            yellowstone_x_token: env_optional_non_empty(source, "YELLOWSTONE_X_TOKEN")?,
            yellowstone_cluster: env_or_default(source, "YELLOWSTONE_CLUSTER", "mainnet-beta")?,
            yellowstone_subscriptions: env_yellowstone_subscriptions_or_default(source)?,
            yellowstone_transaction_account_include: env_account_filter_list(
                source,
                "YELLOWSTONE_TRANSACTION_ACCOUNT_INCLUDE",
            )?,
            yellowstone_transaction_account_exclude: env_account_filter_list(
                source,
                "YELLOWSTONE_TRANSACTION_ACCOUNT_EXCLUDE",
            )?,
            yellowstone_transaction_account_required: env_account_filter_list(
                source,
                "YELLOWSTONE_TRANSACTION_ACCOUNT_REQUIRED",
            )?,
        })
    }

    pub fn apply_overrides(mut self, args: &CliArgs) -> Result<Self, ConfigError> {
        if let Some(mode) = args.mode {
            self.run_mode = mode.into();
        }
        if let Some(value) = non_empty_cli_value("--http-addr", args.http_addr.as_deref())? {
            self.http_addr = value.to_owned();
        }
        if let Some(value) = non_empty_cli_value("--replay", args.replay.as_deref())? {
            self.replay_path = value.to_owned();
        }
        if let Some(value) = non_empty_cli_value("--stream-name", args.stream_name.as_deref())? {
            self.stream_name = value.to_owned();
        }
        if let Some(value) = non_empty_cli_value(
            "--yellowstone-endpoint",
            args.yellowstone_endpoint.as_deref(),
        )? {
            self.yellowstone_endpoint = Some(value.to_owned());
        }
        if let Some(value) =
            non_empty_cli_value("--yellowstone-cluster", args.yellowstone_cluster.as_deref())?
        {
            self.yellowstone_cluster = value.to_owned();
        }
        if let Some(value) = non_empty_cli_value(
            "--yellowstone-subscriptions",
            args.yellowstone_subscriptions.as_deref(),
        )? {
            self.yellowstone_subscriptions =
                parse_yellowstone_subscriptions("--yellowstone-subscriptions", value)?;
        }
        if let Some(value) = args.yellowstone_transaction_account_include.as_deref() {
            self.yellowstone_transaction_account_include =
                parse_account_filter_list("--yellowstone-transaction-account-include", value)?;
        }
        if let Some(value) = args.yellowstone_transaction_account_exclude.as_deref() {
            self.yellowstone_transaction_account_exclude =
                parse_account_filter_list("--yellowstone-transaction-account-exclude", value)?;
        }
        if let Some(value) = args.yellowstone_transaction_account_required.as_deref() {
            self.yellowstone_transaction_account_required =
                parse_account_filter_list("--yellowstone-transaction-account-required", value)?;
        }
        if args.exit_after_replay {
            self.exit_after_replay = true;
        }

        self.validate_runtime_requirements()
    }

    pub fn redacted_summary(&self) -> String {
        format!(
            "run_mode={}; http_addr={}; replay_path={}; stream_name={}; exit_after_replay={}; batch_size={}; channel_capacity={}; database_url_configured={}; yellowstone_endpoint_configured={}; yellowstone_x_token_configured={}; yellowstone_cluster={}; yellowstone_subscriptions={}; yellowstone_transaction_account_include_count={}; yellowstone_transaction_account_exclude_count={}; yellowstone_transaction_account_required_count={}",
            self.run_mode,
            self.http_addr,
            self.replay_path,
            self.stream_name,
            self.exit_after_replay,
            self.batch_size,
            self.channel_capacity,
            !self.database_url.is_empty(),
            self.yellowstone_endpoint.is_some(),
            self.yellowstone_x_token.is_some(),
            self.yellowstone_cluster,
            self.yellowstone_subscriptions.as_csv(),
            self.yellowstone_transaction_account_include.len(),
            self.yellowstone_transaction_account_exclude.len(),
            self.yellowstone_transaction_account_required.len(),
        )
    }

    fn validate_runtime_requirements(self) -> Result<Self, ConfigError> {
        if self.run_mode == RunMode::Yellowstone && self.yellowstone_endpoint.is_none() {
            return Err(ConfigError::MissingRequired {
                key: "YELLOWSTONE_ENDPOINT",
                context: "RUN_MODE=yellowstone",
            });
        }

        Ok(self)
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

fn parse_run_mode(value: &str) -> Result<RunMode, ConfigError> {
    match value {
        "replay" => Ok(RunMode::Replay),
        "yellowstone" => Ok(RunMode::Yellowstone),
        value => Err(ConfigError::InvalidRunMode {
            key: "RUN_MODE",
            value: value.to_owned(),
        }),
    }
}

fn non_empty_cli_value<'a>(
    key: &'static str,
    value: Option<&'a str>,
) -> Result<Option<&'a str>, ConfigError> {
    match value {
        Some(value) if value.trim().is_empty() => Err(ConfigError::Empty { key }),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
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

fn env_account_filter_list(
    source: &impl ConfigSource,
    key: &'static str,
) -> Result<Vec<String>, ConfigError> {
    match source.get(key)? {
        Some(value) => parse_account_filter_list(key, &value),
        None => Ok(Vec::new()),
    }
}

fn parse_account_filter_list(key: &'static str, value: &str) -> Result<Vec<String>, ConfigError> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut values = Vec::new();
    for raw_item in value.split(',') {
        let item = raw_item.trim();
        if item.is_empty() {
            return Err(ConfigError::Empty { key });
        }
        if !values.iter().any(|value| value == item) {
            values.push(item.to_owned());
        }
    }

    Ok(values)
}

fn env_yellowstone_subscriptions_or_default(
    source: &impl ConfigSource,
) -> Result<YellowstoneSubscriptions, ConfigError> {
    match source.get("YELLOWSTONE_SUBSCRIPTIONS")? {
        Some(value) if value.trim().is_empty() => Err(ConfigError::Empty {
            key: "YELLOWSTONE_SUBSCRIPTIONS",
        }),
        Some(value) => parse_yellowstone_subscriptions("YELLOWSTONE_SUBSCRIPTIONS", &value),
        None => Ok(YellowstoneSubscriptions::slots_only()),
    }
}

fn parse_yellowstone_subscriptions(
    key: &'static str,
    value: &str,
) -> Result<YellowstoneSubscriptions, ConfigError> {
    let mut subscriptions = YellowstoneSubscriptions {
        slots: false,
        transactions: false,
        blocks: false,
        entries: false,
    };

    for raw_item in value.split(',') {
        let item = raw_item.trim();
        match item {
            "slots" => subscriptions.slots = true,
            "transactions" => subscriptions.transactions = true,
            "blocks" => subscriptions.blocks = true,
            "entries" => subscriptions.entries = true,
            _ => {
                return Err(ConfigError::InvalidYellowstoneSubscription {
                    key,
                    value: value.to_owned(),
                });
            }
        }
    }

    Ok(subscriptions)
}

fn env_optional_non_empty(
    source: &impl ConfigSource,
    key: &'static str,
) -> Result<Option<String>, ConfigError> {
    match source.get(key)? {
        Some(value) if value.trim().is_empty() => Err(ConfigError::Empty { key }),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
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
    use super::{Config, ConfigSource, RunMode, YellowstoneSubscriptions};
    use crate::cli::{CliArgs, CliRunMode};
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

    fn cli_args() -> CliArgs {
        CliArgs::default()
    }

    #[test]
    fn uses_defaults_when_env_is_missing() {
        let config = Config::from_source(&FakeEnv::default()).expect("config should load");

        assert_eq!(config.run_mode, RunMode::Replay);
        assert_eq!(config.http_addr, "127.0.0.1:8080");
        assert_eq!(config.rust_log, "solana_yellowstone_stream_processor=info");
        assert_eq!(config.replay_path, "fixtures/sample_stream.jsonl");
        assert_eq!(config.stream_name, "replay");
        assert!(!config.exit_after_replay);
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.channel_capacity, 10_000);
        assert_eq!(config.yellowstone_endpoint, None);
        assert_eq!(config.yellowstone_x_token, None);
        assert_eq!(config.yellowstone_cluster, "mainnet-beta");
        assert_eq!(
            config.yellowstone_subscriptions,
            YellowstoneSubscriptions::slots_only()
        );
        assert!(config.yellowstone_transaction_account_include.is_empty());
        assert!(config.yellowstone_transaction_account_exclude.is_empty());
        assert!(config.yellowstone_transaction_account_required.is_empty());
    }

    #[test]
    fn reads_overrides_from_source() {
        let source = FakeEnv::default()
            .with("RUN_MODE", "yellowstone")
            .with("HTTP_ADDR", "0.0.0.0:9000")
            .with("RUST_LOG", "debug")
            .with("REPLAY_PATH", "fixtures/custom.jsonl")
            .with("STREAM_NAME", "custom-replay")
            .with("STREAM_BATCH_SIZE", "42")
            .with("STREAM_CHANNEL_CAPACITY", "2048")
            .with("YELLOWSTONE_ENDPOINT", "https://example.test")
            .with("YELLOWSTONE_X_TOKEN", "secret-token")
            .with("YELLOWSTONE_CLUSTER", "devnet")
            .with(
                "YELLOWSTONE_SUBSCRIPTIONS",
                "slots,transactions,blocks,entries",
            )
            .with(
                "YELLOWSTONE_TRANSACTION_ACCOUNT_INCLUDE",
                "include-1, include-2, include-1",
            )
            .with("YELLOWSTONE_TRANSACTION_ACCOUNT_EXCLUDE", "exclude-1")
            .with("YELLOWSTONE_TRANSACTION_ACCOUNT_REQUIRED", "required-1");

        let config = Config::from_source(&source)
            .expect("config should load")
            .apply_overrides(&cli_args())
            .expect("runtime requirements should pass");

        assert_eq!(config.run_mode, RunMode::Yellowstone);
        assert_eq!(config.http_addr, "0.0.0.0:9000");
        assert_eq!(config.rust_log, "debug");
        assert_eq!(config.replay_path, "fixtures/custom.jsonl");
        assert_eq!(config.stream_name, "custom-replay");
        assert_eq!(config.batch_size, 42);
        assert_eq!(config.channel_capacity, 2048);
        assert_eq!(
            config.yellowstone_endpoint.as_deref(),
            Some("https://example.test")
        );
        assert_eq!(config.yellowstone_x_token.as_deref(), Some("secret-token"));
        assert_eq!(config.yellowstone_cluster, "devnet");
        assert_eq!(
            config.yellowstone_subscriptions,
            YellowstoneSubscriptions {
                slots: true,
                transactions: true,
                blocks: true,
                entries: true,
            }
        );
        assert_eq!(
            config.yellowstone_transaction_account_include,
            vec!["include-1".to_owned(), "include-2".to_owned()]
        );
        assert_eq!(
            config.yellowstone_transaction_account_exclude,
            vec!["exclude-1".to_owned()]
        );
        assert_eq!(
            config.yellowstone_transaction_account_required,
            vec!["required-1".to_owned()]
        );
    }

    #[test]
    fn redacted_summary_does_not_include_secret_config_contents() {
        let config = secret_config();

        let summary = config.redacted_summary();

        assert!(summary.contains("database_url_configured=true"));
        assert!(summary.contains("yellowstone_endpoint_configured=true"));
        assert!(summary.contains("yellowstone_x_token_configured=true"));
        assert_no_secret_config_contents(&summary);
    }

    #[test]
    fn debug_does_not_include_secret_config_contents() {
        let config = secret_config();

        let debug = format!("{config:?}");

        assert!(debug.contains("database_url_configured"));
        assert!(debug.contains("yellowstone_endpoint_configured"));
        assert!(debug.contains("yellowstone_x_token_configured"));
        assert_no_secret_config_contents(&debug);
    }

    fn secret_config() -> Config {
        let source = FakeEnv::default()
            .with(
                "DATABASE_URL",
                "postgres://user:secret-password@db.example:5432/private_db",
            )
            .with(
                "YELLOWSTONE_ENDPOINT",
                "https://provider.example/secret-path?api_key=endpoint-secret",
            )
            .with("YELLOWSTONE_X_TOKEN", "yellowstone-secret-token")
            .with(
                "YELLOWSTONE_TRANSACTION_ACCOUNT_INCLUDE",
                "sensitive-account-filter",
            );

        Config::from_source(&source).expect("config should load")
    }

    fn assert_no_secret_config_contents(value: &str) {
        assert!(!value.contains("postgres://"));
        assert!(!value.contains("secret-password"));
        assert!(!value.contains("db.example"));
        assert!(!value.contains("private_db"));
        assert!(!value.contains("provider.example"));
        assert!(!value.contains("endpoint-secret"));
        assert!(!value.contains("yellowstone-secret-token"));
        assert!(!value.contains("sensitive-account-filter"));
    }

    #[test]
    fn rejects_empty_values() {
        let source = FakeEnv::default().with("HTTP_ADDR", " ");

        let err = Config::from_source(&source).expect_err("empty value should fail");

        assert_eq!(err, ConfigError::Empty { key: "HTTP_ADDR" });
    }

    #[test]
    fn applies_cli_overrides_after_env_config() {
        let config = Config::from_source(&FakeEnv::default())
            .expect("config should load")
            .apply_overrides(&CliArgs {
                mode: Some(CliRunMode::Yellowstone),
                replay: Some("fixtures/custom.jsonl".to_owned()),
                stream_name: Some("custom-stream".to_owned()),
                http_addr: Some("127.0.0.1:9000".to_owned()),
                yellowstone_endpoint: Some("https://example.test".to_owned()),
                yellowstone_cluster: Some("devnet".to_owned()),
                yellowstone_subscriptions: Some("transactions,blocks".to_owned()),
                yellowstone_transaction_account_include: Some(
                    "include-cli-1,include-cli-2".to_owned(),
                ),
                yellowstone_transaction_account_exclude: Some("exclude-cli".to_owned()),
                yellowstone_transaction_account_required: Some("required-cli".to_owned()),
                exit_after_replay: true,
            })
            .expect("cli overrides should apply");

        assert_eq!(config.run_mode, RunMode::Yellowstone);
        assert_eq!(config.replay_path, "fixtures/custom.jsonl");
        assert_eq!(config.stream_name, "custom-stream");
        assert_eq!(config.http_addr, "127.0.0.1:9000");
        assert_eq!(
            config.yellowstone_endpoint.as_deref(),
            Some("https://example.test")
        );
        assert_eq!(config.yellowstone_cluster, "devnet");
        assert_eq!(
            config.yellowstone_subscriptions,
            YellowstoneSubscriptions {
                slots: false,
                transactions: true,
                blocks: true,
                entries: false,
            }
        );
        assert_eq!(
            config.yellowstone_transaction_account_include,
            vec!["include-cli-1".to_owned(), "include-cli-2".to_owned()]
        );
        assert_eq!(
            config.yellowstone_transaction_account_exclude,
            vec!["exclude-cli".to_owned()]
        );
        assert_eq!(
            config.yellowstone_transaction_account_required,
            vec!["required-cli".to_owned()]
        );
        assert!(config.exit_after_replay);
        assert_eq!(
            config.database_url,
            "postgres://postgres:postgres@localhost:5433/solana_stream"
        );
    }

    #[test]
    fn rejects_empty_cli_overrides() {
        let config = Config::from_source(&FakeEnv::default()).expect("config should load");

        let err = config
            .apply_overrides(&CliArgs {
                replay: Some(" ".to_owned()),
                ..cli_args()
            })
            .expect_err("empty cli override should fail");

        assert_eq!(err, ConfigError::Empty { key: "--replay" });
    }

    #[test]
    fn rejects_invalid_run_mode_values() {
        let source = FakeEnv::default().with("RUN_MODE", "live");

        let err = Config::from_source(&source).expect_err("invalid mode should fail");

        assert_eq!(
            err,
            ConfigError::InvalidRunMode {
                key: "RUN_MODE",
                value: "live".to_owned()
            }
        );
    }

    #[test]
    fn requires_yellowstone_endpoint_for_yellowstone_mode() {
        let config = Config::from_source(&FakeEnv::default()).expect("config should load");

        let err = config
            .apply_overrides(&CliArgs {
                mode: Some(CliRunMode::Yellowstone),
                ..cli_args()
            })
            .expect_err("missing endpoint should fail");

        assert_eq!(
            err,
            ConfigError::MissingRequired {
                key: "YELLOWSTONE_ENDPOINT",
                context: "RUN_MODE=yellowstone"
            }
        );
    }

    #[test]
    fn rejects_invalid_yellowstone_subscription_values() {
        let source = FakeEnv::default().with("YELLOWSTONE_SUBSCRIPTIONS", "slots,accounts");

        let err = Config::from_source(&source).expect_err("invalid subscriptions should fail");

        assert_eq!(
            err,
            ConfigError::InvalidYellowstoneSubscription {
                key: "YELLOWSTONE_SUBSCRIPTIONS",
                value: "slots,accounts".to_owned()
            }
        );
    }

    #[test]
    fn rejects_empty_yellowstone_account_filter_items() {
        let source = FakeEnv::default().with("YELLOWSTONE_TRANSACTION_ACCOUNT_INCLUDE", "one,,two");

        let err = Config::from_source(&source).expect_err("empty filter item should fail");

        assert_eq!(
            err,
            ConfigError::Empty {
                key: "YELLOWSTONE_TRANSACTION_ACCOUNT_INCLUDE"
            }
        );
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
