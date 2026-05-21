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
