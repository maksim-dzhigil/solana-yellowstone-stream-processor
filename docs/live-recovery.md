# Live Recovery Policy

This document defines the current recovery contract for feature-gated Yellowstone live ingestion.

## Current Implementation

- The app loads the persisted stream cursor before starting Yellowstone live mode.
- The Yellowstone subscribe request uses `from_slot` when a local persisted cursor exists.
- On each reconnect attempt, the producer refreshes `from_slot` from the latest successfully persisted slot, not only from the startup cursor.
- PostgreSQL remains the durable source of truth; duplicate events are handled through stable `event_id` values and idempotent inserts.
- `/status` and `/metrics` expose reconnect state, last reconnect delay, last reconnect `from_slot`, observed-to-persisted slot lag, and local gap-risk telemetry.

## Recovery State

Live `/status` includes `live.recovery_state`:

- `steady`: the producer is not currently reconnecting and no local gap-risk flag is active.
- `reconnecting`: the producer is reconnecting with a local `from_slot` available.
- `gap_risk`: at least one reconnect happened without a local `from_slot`; the service cannot prove it can replay missed updates from the provider.

Prometheus metrics include:

- `solana_stream_recovery_gap_risk`: `1` when the current recovery state is `gap_risk`, otherwise `0`.
- `solana_stream_recovery_gap_risk_total`: reconnect attempts made without a local `from_slot`.
- `solana_stream_last_reconnect_from_slot`: local cursor slot used by the last reconnect attempt when available.

## Guarantees

The current implementation provides these local guarantees:

- Reconnect attempts use bounded backoff.
- Cursor progress is updated only after successful batch persistence.
- Reconnect attempts refresh `from_slot` from the latest persisted cursor.
- Reconnect orchestration is covered by a fake-attempt unit test without a real Yellowstone provider.
- Replayed duplicate events are deduplicated at storage.
- Local observability surfaces when recovery cannot prove a cursor-backed replay point.

## Non-Guarantees

The current implementation does not yet prove gap-free live recovery:

- Provider-specific `from_slot` behavior is not validated by this repository.
- The cursor stores the maximum persisted slot, not a contiguous finalized slot frontier.
- A successful reconnect does not prove that all slots between the previous stream position and new stream position were replayed.
- There is no finalized-slot reconciliation job or provider backfill workflow yet.

## Policy v0

- Use `from_slot` whenever a local persisted cursor exists.
- Treat reconnect without a local `from_slot` as `gap_risk`.
- Treat provider replay support as an operational requirement, not an assumed guarantee.
- Validate provider behavior with [provider-compatibility.md](provider-compatibility.md) before relying on live recovery.
- Keep the current max-slot cursor contract documented until contiguous/finality-aware tracking is implemented.
- Do not claim gap-free recovery until provider replay, finalized slot tracking, and reconciliation are implemented and tested together.

## Next Implementation Steps

- Fill a provider compatibility profile for the first real Yellowstone provider.
- Add a finalized-slot reconciliation design before changing cursor semantics.
- Add provider-specific integration tests once a concrete Yellowstone provider profile is selected.
