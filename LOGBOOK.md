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
