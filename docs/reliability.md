# Reliability Model

This document describes the guarantees, trade-offs, and non-guarantees of the pipeline. For configuration and run commands, see [configuration.md](configuration.md). For live recovery specifics, see [live-recovery.md](live-recovery.md).

## Guarantees

### At-Least-Once Ingestion

Events may be redelivered due to:
- Reconnects in live mode.
- Restarting replay from an earlier cursor.
- Partial batch writes (write succeeds, cursor update fails).

Duplicates are invisible because of database-level deduplication via `event_id` + `ON CONFLICT DO NOTHING`.

### Idempotent Persistence

Every event carries a stable `event_id` computed from its `EventIdentity` (source, slot, signature, index, etc.). The ID is deterministic and portable across restarts, replays, and reconnects. As long as the identity fields do not change, the same logical event always maps to the same `event_id`.

### Cursor After Persistence

The stream cursor advances only after a batch is successfully written to PostgreSQL. If the process crashes between write and cursor update:
- The next run starts from the old cursor.
- Redelivered events are deduplicated by `event_id`.
- No duplicate rows are visible.

The worst case is slightly more work on restart, not data loss or duplication.

### Bounded Memory

Channels between producer and consumer have a fixed capacity (configurable, default 10_000). If the writer is slower than the producer, the producer blocks on `send().await` instead of allocating unbounded memory. This creates explicit backpressure rather than silent OOM.

## Trade-Offs

### Event Write and Cursor Update Are Not Atomic

The pipeline performs:
1. `writer.write_batch(batch)`
2. `cursor_store.update_after_batch(slot)`

These are separate database calls, not wrapped in a transaction. This is intentional:
- A transaction would couple write latency to cursor latency.
- Recovery does not require atomicity because idempotency handles redelivery.
- If you need stricter semantics, wrap both calls in your own transaction at the storage trait level.

### Replay Materializes the Full Fixture

`read_jsonl_events` loads the entire JSONL file into a `Vec<NormalizedEvent>` before entering the bounded channel. For large fixtures this is O(file size) memory. Streaming JSONL line-by-line is planned but not yet implemented.

### Live Recovery Is Best-Effort

The contiguous finalized slot watermark (`PostgresSlotStateStore`) ensures that reconnect starts from a safe, fully-persisted slot. However:
- Forks are not fully reconciled.
- Provider-specific replay gaps are not automatically backfilled.
- Provider behavior varies; validate your concrete provider before relying on live resume.

See [provider-compatibility.md](provider-compatibility.md) and [finalized-reconciliation.md](finalized-reconciliation.md) for details.

## Non-Guarantees

Do not rely on the following:

- **Exactly-once delivery.** The pipeline is at-least-once. Duplicates happen; they are just invisible.
- **Gap-free live recovery.** Gaps and forks are tracked but not automatically healed.
- **Ordered delivery within a slot.** Events from the same slot may be persisted in different batches due to batching boundaries.
- **Real-time latency guarantees.** Batch size, channel depth, and PostgreSQL performance all affect end-to-end latency. Measure for your workload.
- **Universal Solana program decoding.** Events are normalized raw envelopes. Program-specific decoding is future work.

## Failure Modes and Behavior

| Failure | Behavior | Recovery |
|---|---|---|
| Producer disconnect (live) | Reconnect with exponential backoff + jitter | Automatic |
| Producer stream closed | Treated as retryable reconnect | Automatic |
| Malformed protobuf update | Logged, counted, skipped | Automatic |
| Writer slow | Channel backpressure (producer blocks) | Automatic |
| Writer error | Pipeline returns error, app shuts down | Manual restart |
| Cursor store error | Pipeline returns error, app shuts down | Manual restart |
| PostgreSQL unavailable | Connection pool error, app exits | Manual restart after DB restore |

## Observability

The pipeline exposes enough telemetry to detect degradation:

- `/healthz` — HTTP server is responsive.
- `/readyz` — pipeline has written at least one batch.
- `/status` — last persisted slot, contiguous finalized slot, live producer state, reconnect attempts.
- `/metrics` — Prometheus counters for events, batches, latency, deduplication, decode errors, reconnects.

If you need alerting, scrape `/metrics` and set thresholds on:
- `solana_stream_live_producer_staleness_seconds` — live producer has not produced events recently.
- `solana_stream_slot_lag` — observed slot is far behind persisted slot.
- `solana_stream_yellowstone_reconnects_total` — frequent reconnects indicate provider or network issues.
