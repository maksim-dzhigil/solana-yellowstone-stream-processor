use assert_cmd::Command;
use predicates::prelude::*;

const BIN_NAME: &str = "solana-yellowstone-stream-processor";

#[test]
fn exits_with_config_error_for_empty_cli_replay_override() {
    command()
        .arg("--replay")
        .arg(" ")
        .arg("--exit-after-replay")
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "configuration error: --replay must not be empty",
        ));
}

#[test]
fn exits_with_replay_error_for_missing_replay_file() {
    command()
        .arg("--replay")
        .arg("fixtures/does-not-exist.jsonl")
        .arg("--exit-after-replay")
        .assert()
        .code(3)
        .stdout(predicate::str::contains("application failed"))
        .stdout(predicate::str::contains("replay error"))
        .stdout(predicate::str::contains("fixtures/does-not-exist.jsonl"));
}

fn command() -> Command {
    let mut command = Command::cargo_bin(BIN_NAME).expect("binary should build");
    command.env(
        "DATABASE_URL",
        "postgres://postgres:postgres@localhost:5433/solana_stream",
    );
    command
}
