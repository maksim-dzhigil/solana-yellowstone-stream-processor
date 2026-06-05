# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-06-04

### Added

- Core replay pipeline: JSONL fixture ingestion, bounded channel batching, and idempotent PostgreSQL persistence via `ON CONFLICT DO NOTHING`.
- Stable source-oriented `event_id` identities with versioned canonical hashing.
- Cursor-based resume for replay mode — skips already-processed slots on restart.
- Feature-gated Yellowstone gRPC live ingestion (`--features yellowstone-live`).
- Exponential reconnect backoff with capped jitter (`delay * 2 + rand(0, delay)`).
- Backoff reset after healthy streaming grace period (default 30s, configurable via `YELLOWSTONE_RECONNECT_RESET_AFTER_MS`).
- Malformed protobuf update skipping with `decode_errors_total` Prometheus counter.
- Server-side stream close handling — treated as retryable reconnect instead of silent exit.
- Contiguous finalized slot watermark via `PostgresSlotStateStore` (recursive SQL walk) for safe live reconnect boundaries.
- HTTP health, readiness, status, and Prometheus-compatible metrics endpoints.
- Synthetic replay generator (`generate_fixture` binary) with configurable count, duplicate ratio, and events-per-slot.
- Batch write latency tracking in `PipelineSummary`.
- Makefile targets for quality gate (`make verify`) and benchmarks (`make bench`).
- Comprehensive documentation: configuration reference, event identity, live recovery, finalized reconciliation, provider compatibility, benchmarks.
- MIT OR Apache-2.0 dual license.

### Benchmarks

- **~16.8k events/sec** sustained ingest (1M events, release build).
- **~28 ms** average batch write latency.
- **~141 ms** maximum batch write latency.
- **0 visible duplicates**.
- **~1 ms** deduplication overhead at 10% duplicate ratio.

[Unreleased]: https://github.com/maksim-dzhigil/solana-yellowstone-stream-processor/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/maksim-dzhigil/solana-yellowstone-stream-processor/releases/tag/v0.1.0
