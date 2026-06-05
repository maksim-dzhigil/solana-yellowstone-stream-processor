# Live Recovery Policy

This document defines the current recovery contract for feature-gated Yellowstone live ingestion.

## Current Implementation

- The app loads the persisted stream cursor and contiguous finalized frontier before starting Yellowstone live mode.
- The Yellowstone subscribe request uses `from_slot` derived from `last_contiguous_finalized_slot` when a local frontier exists.
- On each reconnect attempt, the producer refreshes `from_slot` from the latest contiguous finalized slot, not from the max persisted slot.
- Live mode does **not** skip events by slot. It relies on storage-level `event_id` deduplication (`ON CONFLICT DO NOTHING`) to handle duplicates. This prevents data loss when multiple events share a slot and only some were persisted before a crash.
- PostgreSQL remains the durable source of truth; duplicate events are handled through stable `event_id` values and idempotent inserts.
- The pipeline advances a `last_contiguous_finalized_slot` watermark from finalized slot status updates when `advance_finalized_watermark` is enabled. This watermark only moves across a proven parent-linked chain of finalized slots.
- `/status` and `/metrics` expose reconnect state, last reconnect delay, last reconnect `from_slot`, observed-to-persisted slot lag, contiguous finalized cursor, finalized head, and local gap-risk telemetry.

## Recovery State

Live `/status` includes `live.recovery_state`:

- `steady`: the producer is not currently reconnecting and no local gap-risk flag is active.
- `reconnecting`: the producer is reconnecting with a local `from_slot` available.
- `gap_risk`: at least one reconnect happened without a local `from_slot`; the service cannot prove it can replay missed updates from the provider.

Prometheus metrics include:

- `solana_stream_recovery_gap_risk`: `1` when the current recovery state is `gap_risk`, otherwise `0`.
- `solana_stream_recovery_gap_risk_total`: reconnect attempts made without a local `from_slot`.
- `solana_stream_last_reconnect_from_slot`: local cursor slot used by the last reconnect attempt when available.
- `solana_stream_last_contiguous_finalized_slot`: highest finalized slot with proven contiguous coverage.
- `solana_stream_last_finalized_slot`: latest finalized slot observed from the provider (lag/operational only).

## Guarantees

The current implementation provides these local guarantees:

- Reconnect attempts use bounded backoff.
- Cursor progress is updated only after successful batch persistence.
- Reconnect attempts refresh `from_slot` from the latest contiguous finalized cursor.
- Same-slot events are never dropped on restart; idempotent `event_id` dedup handles duplicates.
- Reconnect orchestration is covered by a fake-attempt unit test without a real Yellowstone provider.
- Replayed duplicate events are deduplicated at storage.
- Local observability surfaces when recovery cannot prove a cursor-backed replay point.

## Non-Guarantees

The current implementation does not yet prove gap-free live recovery:

- Provider-specific `from_slot` behavior is not validated by this repository.
- A successful reconnect does not prove that all slots between the previous stream position and new stream position were replayed.
- There is no provider backfill workflow yet; gaps older than provider retention cannot be automatically closed.

## Policy v0

- Use `last_contiguous_finalized_slot` as the reconnect `from_slot` when available.
- Fall back to `last_persisted_slot` only when no contiguous frontier has been established yet (fresh stream).
- Treat reconnect without a local `from_slot` as `gap_risk`.
- Treat provider replay support as an operational requirement, not an assumed guarantee.
- Validate provider behavior with [provider-compatibility.md](provider-compatibility.md) before relying on live recovery.
- Do not claim gap-free recovery until provider replay behavior and contiguous finalized tracking are tested together against a real provider.

## Next Implementation Steps

- Fill a provider compatibility profile for the first real Yellowstone provider.
- Add provider-specific integration tests once a concrete Yellowstone provider profile is selected.
- Add an automated backfill/reconciliation job for gaps that fall within provider retention.
- Add fork/dead-slot replay tests to verify watermark behavior across reorgs.
