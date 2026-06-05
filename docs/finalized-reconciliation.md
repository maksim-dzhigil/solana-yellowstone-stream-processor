# Finalized Slot Reconciliation Design

This document describes the path from max-slot cursoring to gap-aware finalized recovery. Core storage (`stream_slots`, `stream_cursors` with contiguous watermark), pipeline instrumentation, and controlled replay gap tests are implemented. Provider-specific live validation remains future work.

## Problem

The current cursor records the maximum slot in each successfully persisted batch. That is enough for replay MVP resume and duplicate-safe persistence, but it does not prove contiguous live progress.

A max-slot cursor can move past a missing slot if later slots were persisted first. For live recovery, that means `from_slot=max_persisted_slot` may skip unresolved earlier gaps. Gap-free claims require a different cursor frontier.

## Terms

| Term | Meaning |
|---|---|
| `last_persisted_slot` | Maximum slot seen in successfully persisted batches. Current cursor behavior. |
| `last_observed_slot` | Latest slot observed from live stream before persistence. Useful for lag/backpressure. |
| `last_finalized_slot` | Latest finalized slot signal observed from the provider. |
| `last_contiguous_finalized_slot` | Highest finalized slot for which all required data up to that slot is known complete. Future recovery cursor. |
| `gap` | Missing or incomplete slot/range below the finalized frontier. |

## Desired Model

Keep raw event persistence separate from slot reconciliation. Event writes stay idempotent and batch-oriented; reconciliation maintains slot-level completeness state.

Implemented tables:

| Model | Purpose | Status |
|---|---|---|
| `stream_slots` | Track observed/finalized/persisted state per stream and slot. | ✅ Implemented |
| `stream_cursors` | Extended with `last_contiguous_finalized_slot` alongside `last_persisted_slot`. | ✅ Implemented |

Future tables:

| Model | Purpose | Status |
|---|---|---|
| `stream_slot_gaps` | Track missing slot ranges, first detected time, last checked time, and resolution state. | Planned |
| `stream_reconciliation_runs` | Track provider backfill/recheck attempts and outcomes. | Planned |

## Slot State

A future slot record should distinguish these states:

- `observed`: slot appeared in live stream.
- `persisted`: at least one event for the slot was persisted.
- `finalized`: provider reported finalized status for the slot.
- `complete`: slot has all required data for the selected subscription/profile.
- `missing`: slot is below finalized frontier but not complete.
- `reconciled`: a prior missing slot/range was recovered or explicitly marked empty by a trusted source.

Exact completeness depends on subscription type and provider guarantees. A slots-only stream can prove less than a transactions or blocks stream.

## Cursor Policy

Current replay policy:

- `last_contiguous_finalized_slot` is computed from finalized slot parent chains in `stream_slots`.
- The watermark advances only across proven contiguous finalized ancestry.
- Gap-injected replay tests verify that a missing slot (e.g., 102 with slots 100, 101, 103) holds the contiguous cursor at the gap boundary (101).
- `last_persisted_slot` continues to track the maximum slot written for operational metrics.

Current live policy:

- Reconnect uses `last_contiguous_finalized_slot` as `from_slot` when available.
- Gap-risk telemetry is exposed when recovery cannot prove a cursor-backed replay point.
- Live gap behavior depends on the concrete provider; validate with [provider-compatibility.md](provider-compatibility.md).

Future policy:

- Use `last_contiguous_finalized_slot` for gap-free recovery claims once provider behavior is validated.
- Advance `last_contiguous_finalized_slot` only when every slot up to that point is finalized and complete or explicitly reconciled.
- Add automated backfill for gaps that fall within provider retention.

## Metrics

Future Prometheus metrics:

| Metric | Meaning |
|---|---|
| `solana_stream_last_finalized_slot` | Latest finalized slot observed. |
| `solana_stream_last_contiguous_finalized_slot` | Highest finalized slot with complete contiguous coverage. |
| `solana_stream_finalized_cursor_lag` | Difference between finalized head and contiguous finalized cursor. |
| `solana_stream_gap_detected_total` | Total detected missing slot/range events. |
| `solana_stream_gap_oldest_unresolved_slot` | Oldest unresolved missing slot, when one exists. |
| `solana_stream_reconciliation_runs_total` | Reconciliation attempts by outcome. |

## Provider Dependency

Provider behavior controls whether reconciliation can be automated:

- `from_slot` must be documented and tested.
- Retention window must be known.
- Behavior outside retention must be known.
- A backfill source or replay path is required for gaps older than provider retention.

Track provider status in [provider-matrix.md](provider-matrix.md). Validate provider details with [provider-compatibility.md](provider-compatibility.md).

## Implementation Sequence

1. ✅ Add storage model for slot observations and gaps. (`stream_slots`, `stream_cursors` extended)
2. ✅ Persist slot lifecycle events separately from raw event persistence. (`slot_state_from_event` + `PostgresSlotStateStore`)
3. ✅ Compute `last_finalized_slot` and unresolved gaps. (`advance_contiguous_finalized` recursive SQL)
4. ✅ Add status and metrics for finalized cursor lag and unresolved gaps. (`last_contiguous_finalized_slot`, `last_finalized_slot`, `slot_lag`)
5. ⏳ Add provider-specific reconciliation/backfill adapters.
6. ✅ Promote recovery claims only after controlled replay/reconnect tests prove contiguous finalized coverage. (Gap-injected replay tests implemented; live provider validation pending.)

## Non-Goals For The First Implementation

- Do not replace existing replay cursor behavior immediately.
- Do not claim exactly-once upstream delivery.
- Do not require program-specific decoders.
- Do not add Kafka/ClickHouse solely to solve cursor semantics.
