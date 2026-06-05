# Consumer API

Minimal product-facing REST API on top of the ingestion pipeline. This is a
demo-level API, not a low-latency trading API.

## Endpoints

### `GET /v1/events/recent`

Query recent normalized events.

**Query parameters:**
- `event_type` (optional) — filter by event type, e.g. `transaction`, `slot`, `account`
- `limit` (optional, default 100, max 1000) — number of events to return

**Example:**
```bash
curl "http://localhost:8080/v1/events/recent?event_type=transaction&limit=10"
```

**Response:**
```json
[
  {
    "event_id": "...",
    "slot": 123456789,
    "event_type": "transaction",
    "signature": "...",
    "program_id": null,
    "payload": { ... },
    "inserted_at": "2026-06-05T08:44:11Z"
  }
]
```

Ordering: `slot DESC`, `signature DESC NULLS LAST`, `event_id DESC`.

---

### `GET /v1/swaps/recent`

Query recent inferred swaps.

**Query parameters:**
- `program_id` (optional) — filter by program ID
- `limit` (optional, default 100, max 1000) — number of swaps to return

**Example:**
```bash
curl "http://localhost:8080/v1/swaps/recent?program_id=program-raydium&limit=10"
```

**Response:**
```json
[
  {
    "slot": 123456789,
    "signature": "...",
    "program_id": "program-raydium",
    "token_in": "mint-a",
    "token_in_amount": 1000,
    "token_out": "mint-b",
    "token_out_amount": 2500,
    "inferred_at": "2026-06-05T08:44:11Z"
  }
]
```

Ordering: `slot DESC`, `signature DESC`.

---

### `GET /v1/streams/{stream_name}/lag`

Return cursor progress for a stream.

**Example:**
```bash
curl "http://localhost:8080/v1/streams/mainnet-swaps/lag"
```

**Response:**
```json
{
  "stream_name": "mainnet-swaps",
  "last_persisted_slot": 123456780,
  "last_contiguous_finalized_slot": 123456770,
  "last_finalized_slot": 123456780
}
```

## Limitations

- API latency depends on PostgreSQL query performance. No caching layer yet.
- No pagination (cursor-based or offset). Use `limit` and filter by slot range.
- WebSocket streaming is not implemented yet.
