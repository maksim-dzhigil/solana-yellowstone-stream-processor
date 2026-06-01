# Configuration And Local Run

Default local configuration is shown in [.env.example](../.env.example). The local compose database is exposed on host port `5433`:

```text
postgres://postgres:postgres@localhost:5433/solana_stream
```

## Environment Variables

| Variable | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string. | local compose database |
| `RUN_MODE` | `replay` or `yellowstone`. | `replay` |
| `REPLAY_PATH` | JSONL replay fixture path. | `fixtures/sample_stream.jsonl` |
| `STREAM_NAME` | Cursor namespace and metric label. | `replay` |
| `STREAM_BATCH_SIZE` | Batch size for writes. | `500` |
| `STREAM_CHANNEL_CAPACITY` | Bounded channel capacity. | `10000` |
| `YELLOWSTONE_ENDPOINT` | Required for `RUN_MODE=yellowstone`. | empty |
| `YELLOWSTONE_X_TOKEN` | Optional Yellowstone provider token sent as `x-token` metadata. | empty |
| `YELLOWSTONE_CLUSTER` | Cluster label used in event identity. | `mainnet-beta` |
| `YELLOWSTONE_SUBSCRIPTIONS` | Comma-separated `slots`, `transactions`, `blocks`, `entries`. | `slots` |
| `YELLOWSTONE_RECONNECT_INITIAL_DELAY_MS` | Initial retry backoff delay. | `1000` |
| `YELLOWSTONE_RECONNECT_MAX_DELAY_MS` | Maximum retry backoff delay. | `30000` |
| `YELLOWSTONE_RECONNECT_MAX_ATTEMPTS` | Max reconnect attempts; `0` or unset means unlimited. | unlimited |
| `YELLOWSTONE_TRANSACTION_ACCOUNT_INCLUDE` | Optional transaction `account_include` filters. | empty |
| `YELLOWSTONE_TRANSACTION_ACCOUNT_EXCLUDE` | Optional transaction `account_exclude` filters. | empty |
| `YELLOWSTONE_TRANSACTION_ACCOUNT_REQUIRED` | Optional transaction `account_required` filters. | empty |

## Replay

Start PostgreSQL:

```bash
make compose-up
```

Run replay mode and serve HTTP endpoints after replay completes:

```bash
make run
```

Run one-shot replay and exit:

```bash
cargo run -p solana-yellowstone-stream-processor -- --replay fixtures/sample_stream.jsonl --exit-after-replay
```

Run replay with explicit CLI overrides:

```bash
cargo run -p solana-yellowstone-stream-processor -- --replay fixtures/sample_stream.jsonl --stream-name replay --http-addr 127.0.0.1:8080
```

## Yellowstone Live

Run feature-gated Yellowstone live mode:

```bash
RUN_MODE=yellowstone \
YELLOWSTONE_ENDPOINT=https://provider.example \
YELLOWSTONE_CLUSTER=mainnet-beta \
YELLOWSTONE_SUBSCRIPTIONS=slots,transactions \
YELLOWSTONE_RECONNECT_INITIAL_DELAY_MS=1000 \
YELLOWSTONE_RECONNECT_MAX_DELAY_MS=30000 \
YELLOWSTONE_TRANSACTION_ACCOUNT_INCLUDE=TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA \
cargo run -p solana-yellowstone-stream-processor --features yellowstone-live
```

Equivalent CLI mode selection:

```bash
cargo run -p solana-yellowstone-stream-processor --features yellowstone-live -- --mode yellowstone --yellowstone-endpoint https://provider.example --yellowstone-cluster mainnet-beta --yellowstone-subscriptions slots,transactions --yellowstone-reconnect-initial-delay-ms 1000 --yellowstone-reconnect-max-delay-ms 30000 --yellowstone-transaction-account-include TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA
```

HTTP endpoints:

```text
GET /healthz
GET /readyz
GET /status
GET /metrics
```

## Verification

Full local quality gate:

```bash
make verify
```

It runs formatting, workspace tests, clippy, and PostgreSQL-backed ignored tests.

Useful focused commands:

```bash
make check
make test-postgres
cargo test -p solana-yellowstone-stream-processor --test cli
cargo test -p solana-yellowstone-stream
cargo test -p solana-yellowstone-stream --features yellowstone-live
cargo test -p solana-yellowstone-stream-processor --features yellowstone-live
cargo clippy -p solana-yellowstone-stream --features yellowstone-live --all-targets -- -D warnings
cargo clippy -p solana-yellowstone-stream-processor --features yellowstone-live --all-targets -- -D warnings
```
