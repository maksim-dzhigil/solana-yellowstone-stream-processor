# Solana Yellowstone Stream Processor

Reliability-first Rust pipeline for replaying and feature-gated live ingestion of Solana Yellowstone gRPC stream events into durable PostgreSQL storage.

The stable path is replay ingestion. Live Yellowstone ingestion is available behind `--features yellowstone-live` and currently focuses on conservative subscriptions, idempotent persistence, reconnect behavior, status/metrics, and honest recovery observability. Gap-free live recovery is not claimed yet.

## Implemented

| Area | Status |
|---|---|
| JSONL replay ingestion | implemented |
| PostgreSQL persistence, deduplication, cursor updates | implemented |
| Stable source-oriented event IDs | implemented |
| HTTP `/healthz`, `/readyz`, `/status`, `/metrics` | implemented |
| Feature-gated Yellowstone gRPC ingest | implemented |
| Live reconnect/backoff and recovery state telemetry | implemented |
| Provider compatibility tracking | documented, unverified |
| Gap-free finalized recovery | designed, not implemented |
| Program-specific DeFi decoding | not implemented |
| Kafka/ClickHouse/Redis sinks | not implemented |

## Quick Start

Start local PostgreSQL and run replay mode:

```bash
make compose-up
make run
```

Run one-shot replay and exit after persistence:

```bash
cargo run -p solana-yellowstone-stream-processor -- --replay fixtures/sample_stream.jsonl --exit-after-replay
```

Run the full local quality gate:

```bash
make verify
```

## Live Mode

Yellowstone live mode is feature-gated and requires a provider endpoint:

```bash
RUN_MODE=yellowstone \
YELLOWSTONE_ENDPOINT=https://provider.example \
YELLOWSTONE_CLUSTER=mainnet-beta \
YELLOWSTONE_SUBSCRIPTIONS=slots,transactions \
cargo run -p solana-yellowstone-stream-processor --features yellowstone-live
```

Before relying on live recovery, validate the concrete provider profile. The project tracks recovery state and gap-risk signals, but provider replay behavior is not assumed.

## Documentation

Start with [docs/README.md](docs/README.md). Key docs:

- [docs/configuration.md](docs/configuration.md) - configuration, run commands, verification.
- [docs/event-identity.md](docs/event-identity.md) - event IDs, guarantees, and current limitations.
- [docs/live-recovery.md](docs/live-recovery.md) - current live reconnect and recovery policy.
- [docs/finalized-reconciliation.md](docs/finalized-reconciliation.md) - design for gap-aware finalized recovery.
- [docs/provider-compatibility.md](docs/provider-compatibility.md) - provider validation checklist.
- [docs/provider-matrix.md](docs/provider-matrix.md) - provider compatibility status matrix.
- [LOGBOOK.md](LOGBOOK.md) - high-level project progress log.

## Development

Useful focused commands:

```bash
make check
make test-postgres
cargo test -p solana-yellowstone-stream --features yellowstone-live
cargo test -p solana-yellowstone-stream-processor --features yellowstone-live
cargo clippy -p solana-yellowstone-stream --features yellowstone-live --all-targets -- -D warnings
cargo clippy -p solana-yellowstone-stream-processor --features yellowstone-live --all-targets -- -D warnings
```
