use clap::{Parser, ValueEnum};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CliRunMode {
    #[default]
    Replay,
    Yellowstone,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Parser)]
#[command(name = "solana-yellowstone-stream-processor")]
#[command(about = "Replay and process Solana Yellowstone stream events.")]
pub struct CliArgs {
    #[arg(long, value_enum)]
    pub mode: Option<CliRunMode>,

    #[arg(long, value_name = "PATH")]
    pub replay: Option<String>,

    #[arg(long, value_name = "NAME")]
    pub stream_name: Option<String>,

    #[arg(long, value_name = "ADDR")]
    pub http_addr: Option<String>,

    #[arg(long, value_name = "URL")]
    pub yellowstone_endpoint: Option<String>,

    #[arg(long, value_name = "NAME")]
    pub yellowstone_cluster: Option<String>,

    #[arg(long, value_name = "LIST")]
    pub yellowstone_subscriptions: Option<String>,

    #[arg(long)]
    pub exit_after_replay: bool,
}
