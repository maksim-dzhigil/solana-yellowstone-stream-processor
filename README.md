# Solana Yellowstone Stream Processor

Reliability-first Rust service for ingesting Solana stream events from replay fixtures or feature-gated Yellowstone gRPC into durable PostgreSQL storage.

Current status: replay MVP is the stable default path. Live Yellowstone ingestion exists behind the `yellowstone-live` feature with conservative slots-only defaults, opt-in transaction/block/entry subscriptions, transaction account filters, concurrent HTTP status endpoints, coordinated shutdown, reconnect backoff, visible reconnect status, live event staleness telemetry, and observed-to-persisted slot lag; provider replay and gap recovery policy are still future work.

## Architecture

Replay path:

```mermaid
flowchart LR
    R[Replay JSONL] --> C[Bounded Channel]
    C --> B[Batcher]
    B --> P[(PostgreSQL Events)]
    B --> CUR[(Cursor State)]
    B --> H[Health / Status / Metrics]
```

Feature-gated live path:

```mermaid
flowchart LR
    Y[Yellowstone gRPC] --> N[Proto Normalizer]
    N --> C[Bounded Channel]
    C --> B[Batcher]
    B --> P[(PostgreSQL Events)]
    B --> CUR[(Cursor State)]
    B --> H[Health / Status / Metrics]
```

## What Works

- JSONL replay ingestion with one-shot and HTTP-serving modes.
- Feature-gated Yellowstone gRPC producer with `x-token` metadata support, configurable coarse subscription filters, and transaction account filters.
- Normalized event model with versioned, source-oriented event identities.
- Bounded producer-to-pipeline channel, batching, PostgreSQL writes, deduplication, and cursor persistence.
- Idempotent writes through stable `event_id` values and `ON CONFLICT DO NOTHING`.
- Replay resume from persisted stream cursor.
- HTTP `/healthz`, `/readyz`, `/status`, and `/metrics` after replay completion and concurrently during live Yellowstone ingestion.
- Coordinated live shutdown for HTTP and Yellowstone ingest tasks on shutdown signal.
- Yellowstone reconnect with bounded exponential backoff for transient connect/subscribe/receive failures.
- Live `/status` and `/metrics` expose producer state, recovery state, reconnect attempts, last reconnect delay/from-slot, last safe error summary, last observed event time, last observed slot, last persisted batch time, seconds-since staleness gauges, observed-to-persisted slot lag, and local gap-risk telemetry.
- Structured logs with redacted config/debug/error output for database URLs, Yellowstone endpoints, and tokens.
- Unit, binary, HTTP contract, integration, and PostgreSQL-backed tests.

## Event Identity

Current `event_id` values are derived from typed source identity, not payload contents:

- `transaction`: `cluster`, `slot`, `signature`, `index`
- `account`: `cluster`, `slot`, `account`, `write_version`, optional `txn_signature`, `is_startup`
- `instruction`: `cluster`, `slot`, `signature`, `transaction_index`, `instruction_index`, optional `inner_instruction_index`, `program_id`
- `slot`: `cluster`, `slot`, `status`
- `block`: `cluster`, `slot`, `blockhash`
- `entry`: `cluster`, `slot`, `index`

## Guarantees

- At-least-once processing inside the local pipeline.
- Idempotent persistence through stable event IDs and a database unique constraint.
- Cursor updates only after successful batch persistence.
- PostgreSQL is the durable source of truth for events and cursor state.
- Replay mode is covered by the full default `make verify` gate.

## Limitations

- Live Yellowstone mode is available only with `--features yellowstone-live`.
- Live Yellowstone defaults to slots-only subscription; broader transaction/block/entry subscriptions are opt-in.
- Live reconnect uses configurable bounded backoff and defaults to unlimited retries.
- Provider-specific replay behavior is not validated yet; see [docs/live-recovery.md](docs/live-recovery.md).
- Cursor progress is currently based on the maximum slot in each successful batch; this is not a gap-free live recovery guarantee.
- Replay currently loads the configured JSONL file before entering the bounded channel.
- Exactly-once upstream delivery is not claimed.
- Redis, ClickHouse, Kafka, and program-specific decoders are not part of the current MVP.

## Configuration

Default local configuration is shown in [.env.example](.env.example). The local compose database is exposed on host port `5433`:

```text
postgres://postgres:postgres@localhost:5433/solana_stream
```

Important variables:

- `DATABASE_URL`: PostgreSQL connection string.
- `RUN_MODE`: `replay` or `yellowstone`; default is `replay`.
- `REPLAY_PATH`: JSONL replay fixture path.
- `STREAM_NAME`: cursor namespace and metric label.
- `STREAM_BATCH_SIZE`: batch size for writes.
- `STREAM_CHANNEL_CAPACITY`: bounded channel capacity.
- `YELLOWSTONE_ENDPOINT`: required for `RUN_MODE=yellowstone`.
- `YELLOWSTONE_X_TOKEN`: optional Yellowstone provider token sent as `x-token` metadata.
- `YELLOWSTONE_CLUSTER`: cluster label used in event identity, default `mainnet-beta`.
- `YELLOWSTONE_SUBSCRIPTIONS`: comma-separated live subscription set; allowed values are `slots`, `transactions`, `blocks`, and `entries`; default is `slots`.
- `YELLOWSTONE_RECONNECT_INITIAL_DELAY_MS`: initial retry backoff delay in milliseconds; default is `1000`.
- `YELLOWSTONE_RECONNECT_MAX_DELAY_MS`: maximum retry backoff delay in milliseconds; default is `30000` and must be at least the initial delay.
- `YELLOWSTONE_RECONNECT_MAX_ATTEMPTS`: maximum reconnect attempts; default is unset/unlimited, and `0` also means unlimited.
- `YELLOWSTONE_TRANSACTION_ACCOUNT_INCLUDE`: optional comma-separated transaction `account_include` filters.
- `YELLOWSTONE_TRANSACTION_ACCOUNT_EXCLUDE`: optional comma-separated transaction `account_exclude` filters.
- `YELLOWSTONE_TRANSACTION_ACCOUNT_REQUIRED`: optional comma-separated transaction `account_required` filters.

## Local Run

Start PostgreSQL:

```bash
make compose-up
```

Run replay mode with the default fixture and serve HTTP endpoints after replay completes:

```bash
make run
```

Run one-shot replay and exit after persistence:

```bash
cargo run -p solana-yellowstone-stream-processor -- --replay fixtures/sample_stream.jsonl --exit-after-replay
```

Run replay with explicit CLI overrides:

```bash
cargo run -p solana-yellowstone-stream-processor -- --replay fixtures/sample_stream.jsonl --stream-name replay --http-addr 127.0.0.1:8080
```

Run feature-gated Yellowstone live mode:

```bash
RUN_MODE=yellowstone \
YELLOWSTONE_ENDPOINT=https://provider.example \
YELLOWSTONE_CLUSTER=mainnet-beta \
YELLOWSTONE_SUBSCRIPTIONS=slots,transactions \
YELLOWSTONE_RECONNECT_INITIAL_DELAY_MS=1000 \
YELLOWSTONE_RECONNECT_MAX_DELAY_MS=30000 \
YELLOWSTONE_TRANSACTION_ACCOUNT_INCLUDE=TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA \
cargo run -p solana-yellowstone-stream-processor --features yellowstone-live
```

Equivalent CLI mode selection:

```bash
cargo run -p solana-yellowstone-stream-processor --features yellowstone-live -- --mode yellowstone --yellowstone-endpoint https://provider.example --yellowstone-cluster mainnet-beta --yellowstone-subscriptions slots,transactions --yellowstone-reconnect-initial-delay-ms 1000 --yellowstone-reconnect-max-delay-ms 30000 --yellowstone-transaction-account-include TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
```

HTTP endpoints:

```text
GET /healthz
GET /readyz
GET /status
GET /metrics
```

## Verification

Full local quality gate:

```bash
make verify
```

It runs formatting, workspace tests, clippy, and PostgreSQL-backed ignored tests.

Useful focused commands:

```bash
make check
make test-postgres
cargo test -p solana-yellowstone-stream-processor --test cli
cargo test -p solana-yellowstone-stream
cargo test -p solana-yellowstone-stream --features yellowstone-live
cargo test -p solana-yellowstone-stream-processor --features yellowstone-live
cargo clippy -p solana-yellowstone-stream --features yellowstone-live --all-targets -- -D warnings
cargo clippy -p solana-yellowstone-stream-processor --features yellowstone-live --all-targets -- -D warnings
```

## Documentation

- [docs/live-recovery.md](docs/live-recovery.md) - current live reconnect and recovery policy.
- [LOGBOOK.md](LOGBOOK.md) - high-level project progress log.
