SHELL := /bin/sh

.PHONY: fmt fmt-check test clippy check build run compose-up compose-down

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

test:
	cargo test --workspace

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

check: fmt-check test clippy

build:
	cargo build --workspace

run:
	cargo run -p solana-yellowstone-stream-processor

compose-up:
	docker compose up postgres

compose-down:
	docker compose down
