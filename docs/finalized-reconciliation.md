# Finalized Slot Reconciliation Design

This document describes the planned path from max-slot cursoring to gap-aware finalized recovery. It is a design document, not implemented behavior.

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

Keep raw event persistence separate from slot reconciliation. Event writes should stay idempotent and batch-oriented; reconciliation should maintain slot-level completeness state.

Planned tables or logical models:

| Model | Purpose |
|---|---|
| `stream_slots` | Track observed/finalized/persisted state per stream and slot. |
| `stream_slot_gaps` | Track missing slot ranges, first detected time, last checked time, and resolution state. |
| `stream_reconciliation_runs` | Track provider backfill/recheck attempts and outcomes. |
| `stream_cursors` extension | Add `last_contiguous_finalized_slot` without removing current max-slot cursor. |

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

Current policy:

- Keep using max persisted slot for replay MVP and basic live resume.
- Continue exposing gap-risk telemetry when local recovery cannot prove a cursor-backed replay point.

Future policy:

- Use `last_contiguous_finalized_slot` for gap-free recovery claims.
- Advance `last_contiguous_finalized_slot` only when every slot up to that point is finalized and complete or explicitly reconciled.
- Keep `last_persisted_slot` as an operational metric, not as a proof of contiguous recovery.

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

1. Add storage model for slot observations and gaps.
2. Persist slot lifecycle events separately from raw event persistence.
3. Compute `last_finalized_slot` and unresolved gaps.
4. Add status and metrics for finalized cursor lag and unresolved gaps.
5. Add provider-specific reconciliation/backfill adapters.
6. Promote recovery claims only after controlled replay/reconnect tests prove contiguous finalized coverage.

## Non-Goals For The First Implementation

- Do not replace existing replay cursor behavior immediately.
- Do not claim exactly-once upstream delivery.
- Do not require program-specific decoders.
- Do not add Kafka/ClickHouse solely to solve cursor semantics.
