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
        r#"{{"slot":1,"signature":"{unique_prefix}-sig-1","program_id":"program-1","account":null,"event_type":"transaction","payload":{{"index":1}}}}"#
    );
    let second = format!(
        r#"{{"slot":2,"signature":"{unique_prefix}-sig-2","program_id":"program-1","account":null,"event_type":"transaction","payload":{{"index":2}}}}"#
    );

    fs::write(&path, format!("{first}\n{second}\n{second}\n")).expect("write replay fixture");
    path
}

fn unique_name(prefix: &str) -> String {
    let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{id}", std::process::id())
}
