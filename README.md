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
- Cursor update after successful persistence.
- Bounded channels and batching.
- `/healthz`, `/readyz`, `/status`, `/metrics`.
- Tests without a live Yellowstone endpoint.

## Local Run

Current local workflow:

```bash
make check
make compose-up
make run
make test-postgres
```

Equivalent direct commands:

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
docker compose up postgres
cargo run -p solana-yellowstone-stream-processor
```

The app currently reads `REPLAY_PATH`, defaulting to `fixtures/sample_stream.jsonl`. Override it with:

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

Note: the current binary reads the configured JSONL replay file, runs events through the replay batcher, applies database migrations, and persists events to PostgreSQL with `ON CONFLICT DO NOTHING`. Cursor persistence and HTTP endpoints are not implemented yet.

## Commit Style

Use Conventional Commits for readable project history:

```text
feat: add replay reader
fix: reject invalid batch size config
docs: update local run instructions
test: add duplicate replay test
chore: add baseline developer workflow
refactor: split cursor storage module
```

Prefer one logical change per commit.

## Documentation

- [MOTIVATION.md](MOTIVATION.md) - project motivation and value.
- [LOGBOOK.md](LOGBOOK.md) - project logs.
