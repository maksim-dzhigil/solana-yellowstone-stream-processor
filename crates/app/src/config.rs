#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub http_addr: String,
    pub rust_log: String,
    pub replay_path: String,
    pub batch_size: usize,
    pub channel_capacity: usize,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            database_url: read_env(
                "DATABASE_URL",
                "postgres://postgres:postgres@localhost:5432/solana_stream",
            ),
            http_addr: read_env("HTTP_ADDR", "127.0.0.1:8080"),
            rust_log: read_env("RUST_LOG", "info"),
            replay_path: read_env("REPLAY_PATH", "fixtures/sample_stream.jsonl"),
            batch_size: read_env("STREAM_BATCH_SIZE", "500").parse().unwrap_or(500),
            channel_capacity: read_env("STREAM_CHANNEL_CAPACITY", "10000")
                .parse()
                .unwrap_or(10_000),
        }
    }

    pub fn redacted_summary(&self) -> String {
        format!(
            "http_addr={}; replay_path={}; batch_size={}; channel_capacity={}; database_url_configured={}",
            self.http_addr,
            self.replay_path,
            self.batch_size,
            self.channel_capacity,
            !self.database_url.is_empty()
        )
    }
}

fn read_env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}
