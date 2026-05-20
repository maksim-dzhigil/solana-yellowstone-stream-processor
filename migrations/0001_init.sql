CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    slot BIGINT NOT NULL,
    signature TEXT,
    program_id TEXT,
    account TEXT,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_slot ON events(slot);
CREATE INDEX IF NOT EXISTS idx_events_signature ON events(signature);
CREATE INDEX IF NOT EXISTS idx_events_program_slot ON events(program_id, slot);
CREATE INDEX IF NOT EXISTS idx_events_account_slot ON events(account, slot);
CREATE INDEX IF NOT EXISTS idx_events_type_slot ON events(event_type, slot);

CREATE TABLE IF NOT EXISTS stream_cursors (
    stream_name TEXT PRIMARY KEY,
    last_persisted_slot BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    stopped_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    git_sha TEXT,
    config_hash TEXT
);
