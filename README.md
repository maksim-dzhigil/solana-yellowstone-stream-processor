# Solana Yellowstone Stream Processor

Reliability-first Rust service for building an observable Solana ingestion pipeline from replay or future Yellowstone gRPC/Geyser streams to durable storage.

Current status: local replay MVP bootstrap. The project currently proves the storage, cursor, deduplication, batching, and observability path without depending on a live Yellowstone provider.

## Architecture

Current MVP path:

```mermaid
flowchart LR
    R[Replay JSONL] --> C[Bounded Channel]
    C --> B[Batcher]
    B --> P[(PostgreSQL Events)]
    B --> CUR[(Cursor State)]
    B --> H[Health / Status / Metrics]
```

Future live path:

```mermaid
flowchart LR
    Y[Yellowstone gRPC] --> S[Subscription Filters]
    S --> C[Stream Client]
    C --> N[Normalizer]
    N --> B[Batcher]
    B --> P[(PostgreSQL / ClickHouse)]
    B --> M[Prometheus Metrics]
```

## What Works Now

- JSONL replay source.
- Normalized internal event model.
- Bounded channel pipeline.
- PostgreSQL migrations and batch inserts.
- Idempotent writes via stable `event_id` and `ON CONFLICT DO NOTHING`.
- Cursor read, resume, and update after successful batch persistence.
- `/healthz`, `/readyz`, `/status`, and `/metrics`.
- Structured logs and graceful shutdown.
- One-shot replay mode for CI, smoke tests, and imports.
- Unit, integration, HTTP contract, binary, and PostgreSQL-backed tests.

## Guarantees And Limitations

Current guarantees:

- At-least-once processing inside the local replay pipeline.
- Idempotent persistence through stable event IDs and a database unique constraint.
- Cursor updates only after successful batch persistence.
- Bounded channel between replay producer and pipeline consumer.
- PostgreSQL is the durable source of truth for events and cursor state.

Current limitations:

- Live Yellowstone gRPC integration is not implemented yet.
- Replay currently loads the configured JSONL file before entering the bounded channel.
- Cursor progress is based on the maximum slot in each successful batch; this is not a gap-free live recovery guarantee.
- Current `event_id` identity is minimal: `slot`, `signature`, `program_id`, `account`, and `event_type`. It ignores payload, instruction index, write version, and source-specific indexes, so it must be revisited before live Yellowstone ingestion.
- Exactly-once upstream delivery is not claimed.
- Provider-dependent replay, start-slot, reconnect, and gap semantics are future work.
- Redis, ClickHouse, Kafka, and domain-specific decoders are not part of the MVP path.

## Local Run

Start PostgreSQL:

```bash
make compose-up
```

Run the service with the default replay fixture and keep HTTP endpoints available after replay completes:

```bash
make run
```

Run a one-shot replay and exit after persistence:

```bash
cargo run -p solana-yellowstone-stream-processor -- --replay fixtures/sample_stream.jsonl --exit-after-replay
```

Run with explicit replay-local CLI overrides:

```bash
cargo run -p solana-yellowstone-stream-processor -- --replay fixtures/sample_stream.jsonl --stream-name replay --http-addr 127.0.0.1:8080
```

`DATABASE_URL` is intentionally configured through the environment instead of CLI arguments. The local compose database is exposed on host port `5433`:

```text
postgres://postgres:postgres@localhost:5433/solana_stream
```

Expected local endpoints after replay completes without `--exit-after-replay`:

```text
GET /healthz
GET /readyz
GET /status
GET /metrics
```

## Verification

Use the full local quality gate before committing CI-bound changes:

```bash
make verify
```

It runs:

- `cargo fmt --all -- --check`
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`
- PostgreSQL-backed ignored tests through `make test-postgres`

Useful focused commands:

```bash
make check
make test-postgres
cargo test -p solana-yellowstone-stream-processor --test cli
cargo test -p solana-yellowstone-stream
```

## Documentation

- [LOGBOOK.md](LOGBOOK.md) - high-level project progress log.
