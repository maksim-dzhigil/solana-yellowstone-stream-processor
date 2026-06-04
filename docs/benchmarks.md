# Benchmarks

## Environment

| Parameter | Value |
|---|---|
| CPU | 8 vCPU (measured on development machine) |
| RAM | 16 GB |
| Disk | SSD |
| PostgreSQL | 16-alpine (Docker) |
| Batch size | 500 |
| Channel capacity | 10,000 |
| Indexes | All indexes from `migrations/0001_init.sql` enabled |
| Rust profile | `--release` |

## PostgreSQL Replay Path

### 1M events, 0% duplicates

```bash
cargo run --bin generate-fixture --release -- --count 1000000 --output /tmp/bench_1m.jsonl --duplicate-ratio 0.0
cargo run --bin solana-yellowstone-stream-processor --release -- --replay /tmp/bench_1m.jsonl --exit-after-replay
```

| Metric | Value |
|---|---|
| Events | 1,000,000 |
| Duplicates | 0 |
| End-to-end duration | ~59.5 s |
| Ingest rate | ~16,800 events/sec |
| Batches written | 2,000 |
| Avg batch write latency | ~28 ms |
| Max batch write latency | ~141 ms |
| Visible duplicates | 0 |
| Memory RSS (peak) | ~900 MB |

### 1M events, 10% duplicates

```bash
cargo run --bin generate-fixture --release -- --count 1000000 --output /tmp/bench_1m_10pct.jsonl --duplicate-ratio 0.10
cargo run --bin solana-yellowstone-stream-processor --release -- --replay /tmp/bench_1m_10pct.jsonl --exit-after-replay
```

| Metric | Value |
|---|---|
| Events | 1,000,000 |
| Duplicates (intentional) | 100,000 (10%) |
| Events inserted | 909,094 |
| Events deduplicated | 90,906 |
| End-to-end duration | ~61.2 s |
| Ingest rate | ~16,300 events/sec |
| Avg batch write latency | ~29 ms |
| Max batch write latency | ~120 ms |

### 100K events, 0% duplicates (quick check)

| Metric | Value |
|---|---|
| Events | 100,000 |
| End-to-end duration | ~5.7 s |
| Ingest rate | ~17,500 events/sec |

## Observations

- **Deduplication overhead is negligible.** `ON CONFLICT DO NOTHING` adds ~1 ms to average batch write latency at 10% duplicate ratio.
- **Throughput is CPU-bound on serialization and JSON parsing**, not on PostgreSQL writes. The bounded channel and batcher keep memory stable.
- **Memory RSS is high because replay materializes the entire fixture** before entering the pipeline. Streaming JSONL replay is planned to reduce memory usage for large fixtures.
- **Batch write latency is well under the 500 ms target**, even with all indexes enabled.

## How to reproduce

```bash
# 1. Start PostgreSQL
docker compose up -d postgres

# 2. Generate fixture
cargo run --bin generate-fixture --release -- \
  --count 1000000 --output /tmp/bench.jsonl --duplicate-ratio 0.0

# 3. Truncate tables (for clean run)
psql $DATABASE_URL -c "TRUNCATE events, stream_cursors, stream_slots CASCADE;"

# 4. Run benchmark
time cargo run --bin solana-yellowstone-stream-processor --release -- \
  --replay /tmp/bench.jsonl --exit-after-replay
```
