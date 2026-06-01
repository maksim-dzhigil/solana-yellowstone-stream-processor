# Yellowstone Provider Matrix

This matrix tracks provider compatibility work. A row in this file is not a support claim. Treat every provider as `unverified` until the checklist in [provider-compatibility.md](provider-compatibility.md) is completed for a concrete endpoint, environment, and date.

## Status Levels

| Status | Meaning | Required Evidence |
|---|---|---|
| `unverified` | Provider is a candidate, but this repository has not validated it. | Public mention, user request, or team interest. |
| `docs-reviewed` | Public or private provider docs were reviewed. | Link to docs and notes for auth, filters, limits, and replay behavior. |
| `smoke-tested` | Basic live connection was manually tested. | Config profile, date, commit, endpoint region, and `/status`/`/metrics` evidence. |
| `recovery-tested` | Reconnect and cursor-backed `from_slot` behavior were tested. | Forced reconnect notes, `last_reconnect_from_slot`, replay behavior, and duplicate-safe persistence evidence. |
| `unsupported` | Known limitation breaks the current live ingestion or recovery contract. | Clear reason and workaround, if any. |

## Candidate Providers

| Provider | Status | Docs | Yellowstone/gRPC Surface | Auth Mode | `from_slot` | Retention | Filters | Last Verified | Notes |
|---|---|---|---|---|---|---|---|---|---|
| Helius LaserStream | `unverified` | [docs](https://www.helius.dev/docs/laserstream/grpc) | TBD | TBD | TBD | TBD | TBD | TBD | Candidate because Helius documents managed Solana gRPC/LaserStream and Yellowstone-client compatibility. |
| QuickNode Yellowstone gRPC | `unverified` | [docs](https://www.quicknode.com/docs/solana/yellowstone-grpc/overview/) | TBD | TBD | TBD | TBD | TBD | TBD | Candidate because QuickNode documents a Yellowstone gRPC add-on for Solana. |
| Triton One / Yellowstone | `unverified` | [repo](https://github.com/rpcpool/yellowstone-grpc) | TBD | TBD | TBD | TBD | TBD | TBD | Candidate because Triton/Yellowstone is a common Solana gRPC reference point. |
| Alchemy Solana gRPC | `unverified` | [product](https://www.alchemy.com/solana-grpc) | TBD | TBD | TBD | TBD | TBD | TBD | Candidate because Alchemy documents managed Yellowstone-compatible Solana gRPC. |
| Chainstack | `unverified` | [announcement](https://chainstack.com/real-time-solana-streaming-yellowstone-grpc-geyser) | TBD | TBD | TBD | TBD | TBD | TBD | Candidate because Chainstack documents Yellowstone gRPC Geyser for Solana. |
| Shyft | `unverified` | [docs](https://docs.shyft.to/solana-yellowstone-grpc/docs) | TBD | TBD | TBD | TBD | TBD | TBD | Candidate because Shyft documents Solana Yellowstone gRPC service and regional endpoints. |
| Subglow | `unverified` | [docs](https://subglow.io/docs) | TBD | TBD | TBD | TBD | TBD | TBD | Candidate because it advertises Yellowstone-compatible Solana gRPC. |
| Solana Tracker Yellowstone gRPC | `unverified` | [product](https://www.solanatracker.io/yellowstone-grpc) | TBD | TBD | TBD | TBD | TBD | TBD | Candidate because it advertises Yellowstone gRPC streaming. |

## Promotion Rules

Move a provider from `unverified` to `docs-reviewed` only after recording:

- endpoint shape without secrets;
- auth metadata requirements;
- supported subscription filters;
- filter size limits;
- reconnect and rate-limit guidance;
- documented `from_slot` behavior or a clear note that docs do not specify it.

Move from `docs-reviewed` to `smoke-tested` only after recording:

- date, commit, feature flags, region, and config profile;
- successful slots-only connection;
- `/status` and `/metrics` output shape;
- observed slot movement and persisted cursor movement;
- clean shutdown and reconnect behavior under a simple forced disconnect.

Move from `smoke-tested` to `recovery-tested` only after recording:

- reconnect with a local persisted cursor;
- `solana_stream_last_reconnect_from_slot` showing the expected slot;
- provider behavior for recent `from_slot`;
- provider behavior for too-old, future, and omitted `from_slot`;
- duplicate-safe replay behavior in PostgreSQL;
- any remaining gap-risk caveat.

## Notes

- Keep provider secrets out of this repository.
- Keep exact endpoint URLs out unless they are public examples.
- Prefer one completed profile per provider-region-environment combination.
- Do not label a provider as generally supported without a dated verification record.
