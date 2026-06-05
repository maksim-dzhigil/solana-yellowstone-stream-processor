# Domain Event Decoding

The stream normalizes raw Yellowstone gRPC updates into `NormalizedEvent` envelopes.
Domain decoding sits on top of normalization and extracts business-level signals from the raw payload.

> **Scope:** The current decoder is intentionally simple and serves as a
> **demonstration** of the domain layer. It is *not* a full Solana program
> decoder (Orca, Raydium, Meteora, Pump.fun, etc.).

## Supported Decodings

### Token Balance Deltas

For `Transaction` events, the decoder looks for a `token_balances` array inside the payload:

```json
{
  "token_balances": [
    {"account": "...", "mint": "...", "pre": 1000, "post": 900},
    {"account": "...", "mint": "...", "pre": 500, "post": 600}
  ]
}
```

Each entry yields a `TokenBalanceDelta`:
- `account` — the token account address
- `mint` — the SPL mint
- `pre_amount` / `post_amount` — balances before and after
- `delta = pre - post` (positive = outflow, negative = inflow)

### Swap Inference

A simple two-legged swap is inferred when **exactly two** accounts have non-zero deltas:
- One account loses tokens (`delta > 0`)
- One account gains tokens (`delta < 0`)
- The mints are different

The inferred `DexSwap` is written to the `swaps` table.

**Limitations:**
- Does not account for fees, routing, or multi-hop swaps
- Does not handle wrapped SOL or mint decimals
- Does not verify pool state or program instructions
- Known program IDs are not validated (any payload shape can trigger inference)

## Adding a New Decoder

1. Define a new variant in `DecodedEvent`
2. Add extraction logic in `crates/domain/src/decoded.rs`
3. Add storage writer (if persisted) in `crates/storage/src/swaps.rs`
4. Add integration test in `crates/stream/tests/`
5. Update this document

## Future Work

- Full SPL-token instruction parsing (Transfer, TransferChecked, MintTo, Burn)
- Program-specific decoders (Orca Whirlpool, Raydium AMM, Meteora DLMM)
- Decimals-aware amount normalization
- CPI-level instruction decoding
