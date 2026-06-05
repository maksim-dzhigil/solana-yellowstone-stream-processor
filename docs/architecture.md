# Architecture

This document describes the high-level structure of the pipeline, crate boundaries, and data flow. For implementation details, see the source code and crate-level documentation.

## Overview

```text
Replay / Yellowstone gRPC
  -> NormalizedEvent
  -> Bounded channel
  -> Batcher
  -> PostgreSQL (idempotent batch insert, ON CONFLICT DO NOTHING)
  -> Cursor update (after successful batch)
  -> /healthz /readyz /status /metrics
```

The pipeline is intentionally simple: normalize, batch, write, track cursor. Complexity is pushed into resilience (reconnect, backoff, deduplication) rather than into the core flow.

## Crates

### `crates/domain`

**Responsibility:** normalized event model and stable event identity.

- `NormalizedEvent` — the canonical event envelope carried through the entire pipeline.
- `EventIdentity` — typed source-oriented identity (transaction, account, slot, block, entry).
- Stable `event_id` generation via canonical key + versioned hash.
- Identity validation (rejects empty fields, unsupported slot statuses).

This crate has no external dependencies beyond `serde` and `sha2`. It is the innermost layer and must remain lightweight.

### `crates/storage`

**Responsibility:** PostgreSQL persistence, cursor tracking, and slot-state tracking.

- `EventWriter` / `PostgresEventWriter` — batch inserts with deduplication.
- `CursorStore` / `PostgresCursorStore` — stream cursor persistence.
- `SlotStateStore` / `PostgresSlotStateStore` — finalized slot watermark with recursive contiguity walk.
- Migrations — schema versioning via SQLx.

Storage traits (`EventWriter`, `CursorStore`, `SlotStateStore`) are async and generic so that future sinks (ClickHouse, Redis) can implement the same interfaces without changing the pipeline.

### `crates/stream`

**Responsibility:** event sources, batching, and pipeline orchestration.

- **Replay source** — JSONL fixture reader.
- **Yellowstone producer** — feature-gated gRPC client (`yellowstone-live`).
- **Batcher** — accumulates events into bounded batches by size or timeout.
- **Pipeline** — bounded channel receiver, batch writer, cursor updater, and finalized watermark tracker.
- **Slot state mapper** — converts slot events into `SlotStateUpdate` for the store.

The pipeline is runtime-agnostic (Tokio) and storage-agnostic (trait-based).

### `crates/app`

**Responsibility:** config, HTTP endpoints, telemetry, and shutdown orchestration.

- Config loading from environment with validation and redaction.
- CLI (`clap`) for replay path, mode selection, and overrides.
- HTTP server (`axum`) with health, readiness, status, and Prometheus metrics.
- Application runner — wires storage, stream, and HTTP into a coordinated shutdown loop.
- Feature-gated Yellowstone mode (`--features yellowstone-live`).

## Data Flow

### Replay Path

1. `app::run_replay` reads the JSONL fixture into memory (streaming is planned).
2. Events are fed into a bounded `mpsc` channel.
3. `stream::pipeline::run_receiver_pipeline` consumes the channel, batches events, and writes batches via `storage::EventWriter`.
4. After each successful write, the cursor is updated via `storage::CursorStore`.
5. On completion, metrics and summary are logged and exposed via HTTP.

### Live Path

1. `app::run_yellowstone` starts the HTTP server and the reconnect loop.
2. `stream::yellowstone_live::run_yellowstone_reconnect_loop` attempts to connect with exponential backoff and jitter.
3. Each successful connection starts `stream::yellowstone_live::run_yellowstone_grpc_producer`, which normalizes `SubscribeUpdate` messages into `NormalizedEvent`.
4. Events are sent into the same bounded channel and pipeline as replay.
5. Finalized slot events are also sent to `storage::SlotStateStore` to advance the contiguous watermark.
6. On disconnect or stream close, the loop backs off and reconnects from `last_contiguous_finalized_slot`.

## Design Decisions

### Bounded Channels

The channel between producer and pipeline has a fixed capacity. If the writer is slow, the producer blocks instead of allocating unbounded memory. This is a deliberate trade-off: backpressure is preferable to silent OOM.

### Idempotency over Transactions

Event write and cursor update are separate database operations. Recovery relies on:
- Stable `event_id` so redelivery is harmless.
- `ON CONFLICT DO NOTHING` so duplicates are invisible.
- Cursor advancing only after successful write.

A crash between write and cursor update is safe because the next run will redeliver and deduplicate.

### Feature-Gated Live Mode

Yellowstone gRPC dependencies (tonic, protobuf) are heavy and require a live endpoint. They are behind `--features yellowstone-live` so that replay-only users do not compile them.

### Crate Boundaries

`domain` is dependency-free. `storage` depends on `domain`. `stream` depends on `domain` and `storage`. `app` depends on all three. This layering prevents circular dependencies and keeps the core model independent of storage and runtime choices.
