#![allow(clippy::unwrap_used, clippy::expect_used)]

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

const BIN_NAME: &str = "solana-yellowstone-stream-processor";

static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

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

#[test]
fn exits_with_config_error_for_yellowstone_mode_without_endpoint() {
    command()
        .arg("--mode")
        .arg("yellowstone")
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "configuration error: YELLOWSTONE_ENDPOINT is required when RUN_MODE=yellowstone",
        ));
}

#[test]
fn exits_with_config_error_for_invalid_yellowstone_reconnect_delays() {
    command()
        .arg("--mode")
        .arg("yellowstone")
        .arg("--yellowstone-endpoint")
        .arg("https://provider.example")
        .arg("--yellowstone-reconnect-initial-delay-ms")
        .arg("5000")
        .arg("--yellowstone-reconnect-max-delay-ms")
        .arg("1000")
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "configuration error: --yellowstone-reconnect-max-delay-ms must be greater than or equal to --yellowstone-reconnect-initial-delay-ms",
        ));
}

#[test]
fn exits_with_config_error_for_invalid_yellowstone_subscriptions() {
    command()
        .arg("--mode")
        .arg("yellowstone")
        .arg("--yellowstone-endpoint")
        .arg("https://provider.example")
        .arg("--yellowstone-subscriptions")
        .arg("accounts")
        .assert()
        .code(2)
        .stderr(predicate::str::contains(
            "configuration error: --yellowstone-subscriptions must be a comma-separated list containing slots, transactions, blocks, or entries",
        ));
}

#[cfg(not(feature = "yellowstone-live"))]
#[test]
fn exits_with_yellowstone_runtime_placeholder_after_valid_config() {
    command()
        .arg("--mode")
        .arg("yellowstone")
        .arg("--yellowstone-endpoint")
        .arg("https://provider.example")
        .assert()
        .code(7)
        .stdout(predicate::str::contains("yellowstone live mode selected"))
        .stdout(predicate::str::contains(
            "yellowstone live runtime is not implemented yet",
        ));
}

#[test]
#[ignore = "requires local postgres; run `make test-postgres`"]
fn exits_successfully_after_one_shot_replay() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL must be set for postgres integration tests");
    let fixture = write_replay_fixture();
    let stream_name = unique_name("binary-success");

    command_with_database_url(&database_url)
        .arg("--replay")
        .arg(&fixture)
        .arg("--stream-name")
        .arg(stream_name)
        .arg("--exit-after-replay")
        .assert()
        .success()
        .stdout(predicate::str::contains("replay pipeline completed"))
        .stdout(predicate::str::contains("exit after replay requested"));

    fs::remove_file(fixture).expect("remove fixture");
}

fn command() -> Command {
    command_with_database_url("postgres://postgres:postgres@localhost:5433/solana_stream")
}

fn command_with_database_url(database_url: &str) -> Command {
    let mut command = Command::cargo_bin(BIN_NAME).expect("binary should build");
    command.env("DATABASE_URL", database_url);
    command
}

fn write_replay_fixture() -> PathBuf {
    let unique_prefix = unique_name("binary-replay");
    let path = std::env::temp_dir().join(format!("{unique_prefix}.jsonl"));
    let first = format!(
        r#"{{"identity":{{"kind":"transaction","cluster":"localnet","slot":1,"signature":"{unique_prefix}-sig-1","index":0}},"payload":{{"index":1}}}}"#
    );
    let second = format!(
        r#"{{"identity":{{"kind":"transaction","cluster":"localnet","slot":2,"signature":"{unique_prefix}-sig-2","index":0}},"payload":{{"index":2}}}}"#
    );

    fs::write(&path, format!("{first}\n{second}\n{second}\n")).expect("write replay fixture");
    path
}

fn unique_name(prefix: &str) -> String {
    let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{id}", std::process::id())
}
