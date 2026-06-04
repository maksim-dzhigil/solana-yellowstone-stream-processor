## 2026-06-04

**Live mode correctness and reliability hardening.**

- **Contiguous finalized cursor for safe reconnect.** Previously the live reconnect used `last_persisted_slot` — the maximum slot seen in any successfully written batch. This was dangerous because a slot contains many events (transactions, instructions, account updates, different commitment levels). If the process crashed after persisting only some events from slot N, the cursor already pointed to N, and on restart all remaining events from the same slot were skipped, causing guaranteed data loss at every restart boundary. Changed the live pipeline to use `last_contiguous_finalized_slot` computed by `PostgresSlotStateStore` via a recursive SQL walk over finalized slot ancestry. Reconnect now requests replay starting from the last fully contiguous finalized slot, and deduplication is handled exclusively by stable `event_id` with `ON CONFLICT DO NOTHING` at the storage layer. Replay mode keeps slot-based resume; live mode disables it.

- **Server-side stream close no longer exits silently.** When the Yellowstone provider closed the gRPC stream (idle timeout, load balancer reset, graceful shutdown), the producer returned `Ok(())`, which the reconnect loop treated as successful completion. The entire pipeline stopped, the process exited with code 0, and no reconnect was attempted. Added `YellowstoneGrpcError::StreamClosed`, returned when the message stream ends. This error is classified as retryable, so the reconnect loop properly backs off and attempts reconnect instead of shutting down.

- **Malformed protobuf updates no longer kill ingestion.** A single bad protobuf update from the provider caused `normalize_yellowstone_proto_update` to fail with a `Normalize` error, which was non-retryable and propagated up, tearing down the entire ingestion loop permanently. Changed the producer to catch normalization errors, log a `warn`, increment a `decode_errors_total` counter, skip the bad update, and continue reading the stream. The counter is exposed in `LiveProducerStatus` and rendered as Prometheus metric `solana_stream_decode_errors_total`.

- **Jitter added to exponential reconnect backoff.** The backoff used pure `delay * 2` without randomization. During a provider-wide outage all clients with the same reconnect profile would retry in lockstep, creating a thundering herd. Added capped jitter: `delay * 2 + rand(0, delay)`, bounded by `max_delay`. Load is now spread across a randomized window.

- **Backoff resets after a healthy streaming period.** `retry_attempt` and `delay` accumulated forever across the reconnect loop lifetime. A connection that flapped (streamed for a while, dropped, reconnected, streamed again) eventually exhausted `max_retries` or stuck at `max_delay`, aborting a basically-working stream. Added a configurable grace period `reset_after` (default 30s, env `YELLOWSTONE_RECONNECT_RESET_AFTER_MS`). If an attempt streams for longer than the grace period before failing, `retry_attempt` resets to 0 and `delay` resets to `initial_delay` for the next reconnect. Normal exponential backoff with jitter still applies for quick failures.

## 2026-06-01

- Advanced Yellowstone live mode from configurable ingestion to an operationally observable runtime: provider-side filters, concurrent HTTP status endpoints, coordinated shutdown, reconnect backoff, retry tuning, and safe reconnect error summaries.
- Added live recovery telemetry for producer/recovery state, staleness, observed-to-persisted slot lag, reconnect `from_slot`, and local gap-risk metrics.
- Refreshed reconnect behavior to use the latest persisted cursor as `from_slot` and covered the reconnect loop with a focused unit test.
- Documented live recovery limits, provider compatibility requirements, candidate provider matrix, and finalized-slot reconciliation design without claiming gap-free recovery.
- Reorganized project documentation into a compact README plus focused docs for configuration, event identity, provider validation, and recovery design.

## 2026-05-31

- Hardened secret redaction for config/debug/error output so database URLs, Yellowstone endpoints, and tokens are not logged accidentally.
- Wired app Yellowstone mode to the feature-gated live gRPC producer and existing bounded producer pipeline.
- Added feature-gated Yellowstone gRPC producer support with conservative slots-only defaults, x-token metadata, and proto normalization into `NormalizedEvent`.
- Added reusable async producer-to-pipeline boundary and verified the full local quality gate, including PostgreSQL-backed tests.

## 2026-05-25

- Added optional Yellowstone protobuf mapping from real `SubscribeUpdate` messages into normalized event identities.
- Added the first Yellowstone normalization boundary for mapping source-like events into `NormalizedEvent` without a live gRPC client.
- Replaced the old minimal `event_id` contract with typed source-oriented event identities and versioned canonical hash IDs.
- Added identity storage to the initial PostgreSQL schema and updated replay, pipeline, storage, fixtures, tests, and README around the new contract.
- Verified the clean schema by recreating local PostgreSQL and running the full `make verify` gate.

## 2026-05-22

- Verified README local workflow commands and HTTP endpoint smoke behavior.
- Consolidated project motivation and MVP guarantees into README.
- Added a DB-backed binary success test for one-shot replay mode.
- Added binary-level tests for CLI config and replay failure exit behavior.
- Added HTTP endpoint contract tests for health, readiness, status, and metrics routes.
- Added config redaction and replay parser edge-case tests.
- Updated internal strategy and testing notes to match the current replay MVP state.
- Added a PostgreSQL-backed replay-to-storage integration test for persistence, deduplication, and cursor progress.
- Expanded bounded channel pipeline tests across receiver, producer, flush, resume, and error paths.
- Routed replay processing through a bounded event channel and exposed a receiver-based pipeline entry point.
- Introduced the first event source boundary and kept JSONL replay as its initial implementation.
- Added one-shot replay mode for CLI-driven replay runs without starting HTTP.
- Added CLI overrides for replay path, stream name, and HTTP bind address.
- Moved application orchestration out of `main.rs` into a dedicated app runner.
- Added GitHub Actions CI for formatting, tests, clippy, and PostgreSQL integration checks.
- Added graceful shutdown handling for the HTTP server on explicit shutdown signals.
- Added structured tracing logs for config, replay, cursor, pipeline, and HTTP lifecycle events.
- Added Prometheus-compatible replay metrics for the HTTP status layer.
- Added basic HTTP health, readiness, and replay status endpoints after replay completion.
- Added replay resume from the persisted stream cursor so already processed slots are skipped on restart.

## 2026-05-21

- Set up the baseline developer workflow and documented `make check` as the local quality gate.
- Standardized commit naming around Conventional Commits.
- Tightened environment configuration loading with explicit validation errors and focused unit tests.
- Introduced the normalized event model for replay fixtures, including JSON payload support and `EventType` validation.
- Added the first JSONL replay reader so the app can load events from `REPLAY_PATH` and report the loaded event count.
- Wired replay batching into the app and moved the pipeline toward the real storage path.
- Added PostgreSQL persistence for replay events with migrations, batch inserts, database-level deduplication, and Docker-backed integration tests.
- Added replay cursor persistence after successful PostgreSQL batch writes.

## 2026-05-20

- Created the initial Rust workspace structure.
- Added baseline crates: `app`, `domain`, `storage`, and `stream`.
- Added baseline project files: root `Cargo.toml`, `.gitignore`, `.env.example`, PostgreSQL migration, replay fixture, Docker Compose, and integration test placeholder.
