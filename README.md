# Solana Yellowstone Stream Processor

[![CI](https://github.com/maksim-dzhigil/solana-yellowstone-stream-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/maksim-dzhigil/solana-yellowstone-stream-processor/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)

**Reliability-first Rust pipeline for ingesting Solana Yellowstone gRPC streams into durable PostgreSQL storage.**

Replayable, observable, idempotent. Built for teams that need a controlled, testable ingestion layer between a Yellowstone-compatible provider and their own database.

---

## What this is

- A **stream ingestion runtime** that normalizes Yellowstone gRPC events and writes them to PostgreSQL in batches.
- A **replay-first development path** — test the entire pipeline locally with JSONL fixtures, without a live provider.
- A **reliability layer** with bounded channels, idempotent persistence via stable event IDs, cursor tracking, reconnect backoff with jitter, and Prometheus-compatible metrics.

## What this is not

- Not a universal indexer of the entire Solana chain.
- Not a trading bot, MEV framework, or low-latency execution engine.
- Not a replacement for your RPC provider — it runs *after* the stream.
- Not a full semantic decoder for all Solana programs (program-specific decoding is future work).

## Why this exists

Yellowstone gRPC gives you the stream. Downstream, teams still need to solve:

- **Reconnects and duplicates** — provider drops, restarts, and redelivered messages.
- **Cursor safety** — tracking what is actually persisted vs. what was merely received.
- **Backpressure** — slow storage must not cause unbounded memory growth or silent data loss.
- **Local reproducibility** — testing failure cases without paying for a live endpoint.
- **Observability** — knowing whether the pipeline is healthy, lagging, or dropping events.

This project treats those problems as first-class concerns rather than afterthoughts.

## Quick start

Run the full pipeline locally in under 5 minutes:

```bash
# 1. Start PostgreSQL
docker compose up -d postgres

# 2. Run replay ingestion
cargo run -p solana-yellowstone-stream-processor -- \
  --replay fixtures/sample_stream.jsonl \
  --exit-after-replay

# 3. Verify events are persisted
psql $DATABASE_URL -c "SELECT COUNT(*) FROM events;"

# 4. Check metrics and status
curl http://localhost:8080/status
curl http://localhost:8080/metrics
```

Run the full local quality gate:

```bash
make verify
```

## Live mode

Live Yellowstone ingestion is available behind `--features yellowstone-live`:

```bash
RUN_MODE=yellowstone \
YELLOWSTONE_ENDPOINT=https://provider.example \
YELLOWSTONE_CLUSTER=mainnet-beta \
YELLOWSTONE_SUBSCRIPTIONS=slots,transactions \
cargo run -p solana-yellowstone-stream-processor --features yellowstone-live
```

Live mode includes:
- Conservative subscriptions with configurable program/account filters.
- Exponential reconnect backoff with jitter.
- Backoff reset after a healthy streaming grace period.
- Malformed update skipping with `decode_errors_total` metric.
- Recovery telemetry: reconnect attempts, slot lag, gap-risk signals.

Before relying on live recovery, validate your concrete provider profile. Provider replay behavior varies; the project tracks recovery state honestly rather than assuming gap-free delivery.

## Architecture

```text
Replay / Yellowstone gRPC
  -> NormalizedEvent
  -> Bounded channel
  -> Batcher
  -> PostgreSQL (idempotent batch insert, ON CONFLICT DO NOTHING)
  -> Cursor update (after successful batch)
  -> /healthz /readyz /status /metrics
```

- `crates/app` — config, HTTP endpoints, telemetry, shutdown orchestration.
- `crates/stream` — replay source, Yellowstone gRPC producer, batcher, pipeline.
- `crates/storage` — PostgreSQL writer, cursor store, slot-state store, migrations.
- `crates/domain` — normalized event model and stable event identity.

For a detailed architecture overview, see [docs/architecture.md](docs/architecture.md).

## Implemented and roadmap

| Capability | Status |
|---|---|
| JSONL replay ingestion | **Implemented** |
| PostgreSQL persistence with deduplication | **Implemented** |
| Stable source-oriented event IDs (`event_id`) | **Implemented** |
| Bounded channel pipeline with backpressure | **Implemented** |
| Cursor persistence after successful batch | **Implemented** |
| HTTP health, readiness, status, metrics | **Implemented** |
| Feature-gated Yellowstone gRPC ingest | **Implemented** |
| Live reconnect with backoff and jitter | **Implemented** |
| Backoff reset after healthy streaming | **Implemented** |
| Contiguous finalized slot watermark for reconnect | **Implemented** |
| Malformed update skipping and decode error metrics | **Implemented** |
| Provider compatibility tracking | Documented, community-verified |
| Synthetic replay generator + benchmarks | **Planned** |
| Infra-grade metrics (batch latency, channel pressure, slot lag) | **Planned** |
| Gap-free live recovery with fork handling | Designed, future milestone |
| Token balance delta extraction and DEX swap inference | Future milestone |
| ClickHouse sink for high-throughput analytics | Future milestone |
| REST API for recent events and swaps | Future milestone |

## Honest limitations

- **No gap-free live recovery yet.** Finalized slot watermark advances contiguously, but forks and provider-specific replay gaps are not fully reconciled. Document your provider's replay semantics before relying on live resume.
- **No program-specific decoders yet.** Events are stored as normalized raw envelopes. Domain decoding (swaps, token transfers, etc.) is on the roadmap.
- **No Kafka/ClickHouse/Redis sinks yet.** PostgreSQL is the only storage target today.
- **Replay materializes the full fixture.** Large fixtures load entirely into memory before entering the bounded channel. Streaming JSONL is planned.
- **Event write and cursor update are not atomic.** Recovery relies on idempotency (`event_id` + `ON CONFLICT DO NOTHING`), not a database transaction.

## Reliability model

- **At-least-once ingestion** — events may be redelivered, but duplicates are invisible due to database-level deduplication.
- **Idempotent persistence** — stable `event_id` guarantees safe replay and reconnect boundaries.
- **Cursor after persistence** — cursor advances only after a batch is successfully written. Crash between write and cursor update is safe because redelivery is deduplicated.
- **Bounded memory** — channels have fixed capacity. Slow storage creates backpressure, not silent OOM.

For full details, see [docs/reliability.md](docs/reliability.md).

## Benchmarks

Initial targets for the PostgreSQL replay path:

| Metric | Target | Measured |
|---|---|---|
| Replay ingest | 5k–20k events/sec | **~16.8k events/sec** (1M events, release build) |
| Avg batch write latency | < 500 ms | **~28 ms** |
| Max batch write latency | < 500 ms | **~141 ms** |
| Visible duplicates | 0 | **0** |
| Deduplication overhead (10% duplicates) | minimal | **~1 ms avg latency increase** |
| Restart/resume correctness | 100 % | Verified via duplicate replay tests |

Benchmarks will be added as part of the next milestone. See [docs/benchmarks.md](docs/benchmarks.md) for the benchmark plan.

## Provider compatibility

The project includes a compatibility checklist and a status matrix for Yellowstone-compatible providers. Not all providers support the same replay, start-slot, or reconnect semantics. Validate your provider before running live mode.

- [docs/provider-compatibility.md](docs/provider-compatibility.md) — validation checklist.
- [docs/provider-matrix.md](docs/provider-matrix.md) — compatibility status.

## Documentation

- [docs/configuration.md](docs/configuration.md) — configuration reference and run commands.
- [docs/event-identity.md](docs/event-identity.md) — event ID guarantees and limitations.
- [docs/live-recovery.md](docs/live-recovery.md) — live reconnect and recovery policy.
- [docs/finalized-reconciliation.md](docs/finalized-reconciliation.md) — gap-aware finalized recovery design.
- [LOGBOOK.md](LOGBOOK.md) — project progress log.

## Development

```bash
make check          # fmt, tests, clippy
make test-postgres  # include PostgreSQL-backed integration tests
make verify         # full quality gate (check + postgres tests)
```

## License

Licensed under either of [MIT](LICENSE) or [Apache-2.0](LICENSE) at your option.
