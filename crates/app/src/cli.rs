use clap::Parser;

#[derive(Debug, Default, Clone, PartialEq, Eq, Parser)]
#[command(name = "solana-yellowstone-stream-processor")]
#[command(about = "Replay and process Solana Yellowstone stream events.")]
pub struct CliArgs {
    #[arg(long, value_name = "PATH")]
    pub replay: Option<String>,

    #[arg(long, value_name = "NAME")]
    pub stream_name: Option<String>,

    #[arg(long, value_name = "ADDR")]
    pub http_addr: Option<String>,

    #[arg(long)]
    pub exit_after_replay: bool,
}
