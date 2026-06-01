# Event Identity And Guarantees

Current `event_id` values are derived from typed source identity, not payload contents.

## Identity Fields

| Kind | Identity fields |
|---|---|
| `transaction` | `cluster`, `slot`, `signature`, `index` |
| `account` | `cluster`, `slot`, `account`, `write_version`, optional `txn_signature`, `is_startup` |
| `instruction` | `cluster`, `slot`, `signature`, `transaction_index`, `instruction_index`, optional `inner_instruction_index`, `program_id` |
| `slot` | `cluster`, `slot`, `status` |
| `block` | `cluster`, `slot`, `blockhash` |
| `entry` | `cluster`, `slot`, `index` |

## Current Guarantees

- At-least-once processing inside the local pipeline.
- Idempotent persistence through stable event IDs and a database unique constraint.
- Cursor updates only after successful batch persistence.
- PostgreSQL is the durable source of truth for events and cursor state.
- Replay mode is covered by the full default `make verify` gate.

## Current Limitations

- Live Yellowstone mode is available only with `--features yellowstone-live`.
- Live Yellowstone defaults to slots-only subscription; broader transaction/block/entry subscriptions are opt-in.
- Provider-specific replay behavior is not validated yet.
- Cursor progress is currently based on the maximum slot in each successful batch; this is not a gap-free live recovery guarantee.
- Replay currently loads the configured JSONL file before entering the bounded channel.
- Exactly-once upstream delivery is not claimed.
- Redis, ClickHouse, Kafka, and program-specific decoders are not part of the current MVP.
