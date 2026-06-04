SHELL := /bin/sh

TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5433/solana_stream

.PHONY: fmt fmt-check test test-postgres clippy check verify build run compose-up compose-down bench-generate bench-replay

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

FIXTURE_OUTPUT ?= fixtures/bench_1M.jsonl
FIXTURE_COUNT ?= 1000000
BENCH_DB_URL ?= postgres://postgres:postgres@localhost:5433/solana_stream

bench-generate:
	cargo run -p solana-yellowstone-stream-processor --bin generate_fixture -- \
		--count $(FIXTURE_COUNT) \
		--output $(FIXTURE_OUTPUT)

bench-replay:
	DATABASE_URL='$(BENCH_DB_URL)' cargo run -p solana-yellowstone-stream-processor --release -- \
		--mode replay --source $(FIXTURE_OUTPUT)

bench: bench-generate bench-replay
