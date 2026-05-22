SHELL := /bin/sh

TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5433/solana_stream

.PHONY: fmt fmt-check test test-postgres clippy check verify build run compose-up compose-down

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

test:
	cargo test --workspace

test-postgres:
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' cargo test --workspace -- --ignored

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

check: fmt-check test clippy

verify: check test-postgres

build:
	cargo build --workspace

run:
	cargo run -p solana-yellowstone-stream-processor

compose-up:
	docker compose up postgres

compose-down:
	docker compose down
