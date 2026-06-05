CREATE TABLE IF NOT EXISTS swaps (
    id BIGSERIAL PRIMARY KEY,
    slot BIGINT NOT NULL,
    signature TEXT NOT NULL,
    program_id TEXT NOT NULL,
    token_in TEXT NOT NULL,
    token_in_amount BIGINT NOT NULL,
    token_out TEXT NOT NULL,
    token_out_amount BIGINT NOT NULL,
    inferred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_swaps_slot ON swaps(slot);
CREATE INDEX IF NOT EXISTS idx_swaps_signature ON swaps(signature);
CREATE INDEX IF NOT EXISTS idx_swaps_program_slot ON swaps(program_id, slot);
