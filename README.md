# Solana Yellowstone Stream Processor

Rust service for building a reliable Solana ingestion pipeline from Yellowstone gRPC/Geyser streams to durable storage.

Current status: MVP bootstrap. The first implementation target is a local replay pipeline before live Yellowstone integration.

## Architecture

MVP architecture:

```mermaid
flowchart LR
    R[Replay JSONL] --> C[Bounded Channel]
    C --> N[Normalizer]
    N --> B[Batcher]
    B --> P[(PostgreSQL)]
    B --> CUR[(Cursor State)]
    B --> H[Health / Status / Metrics]
```

Future live architecture:

```mermaid
flowchart LR
    Y[Yellowstone gRPC] --> S[Subscription Filters]
    S --> C[Stream Client]
    C --> N[Normalizer]
    N --> B[Batcher]
    B --> P[(PostgreSQL)]
    B --> M[Prometheus Metrics]
```

## MVP Scope

- JSONL replay mode.
- Normalized internal event model.
- PostgreSQL batch inserts.
- Idempotent writes via stable `event_id`.
- Cursor resume and update after successful persistence.
- Bounded channels and batching.
- `/healthz`, `/readyz`, `/status`, and `/metrics`.
- Tests without a live Yellowstone endpoint.

## Local Run

Current local workflow:

```bash
make check
make verify
make compose-up
make run
make test-postgres
```

Use `make verify` before opening a pull request or pushing CI-bound changes. It runs formatting checks, workspace tests, clippy, and Docker-backed PostgreSQL tests.

Equivalent direct commands:

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
docker compose up postgres
cargo run -p solana-yellowstone-stream-processor
```

The app currently reads `REPLAY_PATH`, defaulting to `fixtures/sample_stream.jsonl`, resumes after the persisted cursor for `STREAM_NAME`, and writes cursor progress under the same stream name. `STREAM_NAME` defaults to `replay`. Override the replay path with:

```bash
REPLAY_PATH=fixtures/sample_stream.jsonl cargo run -p solana-yellowstone-stream-processor
```

Target CLI workflow after argument parsing lands:

```bash
cargo run -p solana-yellowstone-stream-processor -- --replay fixtures/sample_stream.jsonl
```

PostgreSQL can also be started directly with:

```bash
docker compose up postgres
```

The local compose database is exposed on host port `5433`:

```text
postgres://postgres:postgres@localhost:5433/solana_stream
```

Expected local endpoints:

```text
GET /healthz
GET /readyz
GET /status
GET /metrics
```

After replay completes, `make run` keeps the process alive so these endpoints remain available.

Note: the current binary reads the configured JSONL replay file, applies database migrations, reads the persisted stream cursor, skips replay events at or before the cursor slot, persists new events to PostgreSQL with `ON CONFLICT DO NOTHING`, updates the cursor after successful batch persistence, and then serves HTTP status and metrics endpoints on `HTTP_ADDR`.

## Documentation

- [MOTIVATION.md](MOTIVATION.md) - project motivation and value.
- [LOGBOOK.md](LOGBOOK.md) - project logs.
