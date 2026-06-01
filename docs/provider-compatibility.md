# Yellowstone Provider Compatibility Checklist

This checklist records what must be verified before treating a Yellowstone provider as safe for live recovery. It is intentionally provider-neutral: fill one profile per provider and environment. Track candidate providers in [provider-matrix.md](provider-matrix.md).

## Provider Profile

| Field | Value | Notes |
|---|---|---|
| Provider name | TBD | Commercial/provider label. |
| Environment | TBD | mainnet-beta, devnet, staging, or local proxy. |
| Endpoint URL shape | TBD | Do not record secrets. |
| Auth mode | TBD | `x-token`, bearer token, mTLS, IP allowlist, or custom metadata. |
| Supported commitment levels | TBD | processed, confirmed, finalized. |
| Supports `from_slot` | TBD | yes/no/partial/unknown. |
| Replay retention window | TBD | Slots, time, or provider-specific policy. |
| `from_slot` outside retention | TBD | Error code, silent clamp, latest-only, disconnect, or unknown. |
| Max subscription filters | TBD | Per request and per account list. |
| Keepalive requirement | TBD | Client ping interval, idle timeout, server limits. |
| Compression support | TBD | gzip/zstd/none/unknown. |
| Rate limits | TBD | Connect, subscribe, message, and reconnect limits. |
| Expected transient statuses | TBD | Usually `Unavailable`, `ResourceExhausted`, `Internal`, etc. |
| Expected fatal statuses | TBD | Usually `InvalidArgument`, `Unauthenticated`, `PermissionDenied`. |
| Provider docs link | TBD | Public docs or internal runbook. |
| Last verified at | TBD | Date and commit/config used. |

## Replay And Recovery

Verify these before relying on reconnect recovery:

- Provider accepts a valid recent `from_slot` and replays from that slot or a documented boundary.
- Provider behavior is known when `from_slot` is older than retention.
- Provider behavior is known when `from_slot` is in the future.
- Provider behavior is known when `from_slot` is omitted.
- Reconnect after a forced disconnect uses the local persisted cursor as `from_slot`.
- Duplicate replayed events are accepted by storage and deduplicated through stable `event_id` values.
- The provider documents whether replay is best-effort or guaranteed.

Record any uncertainty as an operational gap. Do not upgrade recovery claims to gap-free until provider replay behavior, finalized slot tracking, and reconciliation are tested together.

## Auth And Metadata

Verify:

- Required metadata key names, for example `x-token` versus `authorization`.
- Whether token value requires a raw token or `Bearer <token>`.
- Whether metadata values must be ASCII.
- Token rotation behavior during long-lived streams.
- Error code for missing, expired, or invalid credentials.
- Whether auth failures are terminal and should not be retried indefinitely.

## Subscription Filters

Verify limits and behavior for each enabled filter type:

- slots: commitment behavior and interslot update support.
- transactions: `account_include`, `account_exclude`, and `account_required` max lengths and combination rules.
- blocks: account include support, transaction/account/entry include flags, and payload size limits.
- entries: availability and expected volume.
- Mixed subscriptions: whether slots plus transactions plus blocks are supported in one request.
- Invalid filters: exact gRPC status and message shape.

## Transport

Verify:

- TLS requirements and certificate validation expectations.
- Keepalive settings needed to avoid idle disconnects.
- Server behavior under slow consumers or full client channel.
- Compression support and any CPU/latency tradeoff.
- Max inbound message size if exposed by provider docs.
- Reconnect rate limits and recommended backoff profile.

## Manual Smoke Test

Use a non-secret config profile and record the result:

1. Start live mode with slots-only subscription and a fresh stream name.
2. Confirm `/status` has `mode=yellowstone` and live producer state.
3. Confirm `/metrics` exposes reconnect, staleness, slot lag, and recovery gauges.
4. Stop the network path or provider connection long enough to trigger reconnect.
5. Confirm `last_reconnect_from_slot` is set once a local cursor exists.
6. Restore connection and confirm ingestion continues.
7. Inspect PostgreSQL for duplicate-safe persistence and cursor movement.
8. Record any gap-risk state or provider-specific ambiguity.

## Current Repository Defaults

- Auth metadata currently supports optional `x-token`.
- Live mode defaults to slots-only.
- Reconnect backoff defaults to 1s initial delay, 30s max delay, and unlimited attempts.
- Reconnect attempts refresh `from_slot` from the latest persisted local cursor.
- Gap-free recovery is not claimed.
