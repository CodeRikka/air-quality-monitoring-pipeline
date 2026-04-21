CREATE TABLE IF NOT EXISTS core.fact_observation_history (
    observation_history_id BIGSERIAL PRIMARY KEY,
    natural_key TEXT NOT NULL,
    location_id TEXT NOT NULL REFERENCES core.dim_location(location_id),
    pollutant_code TEXT NOT NULL REFERENCES core.dim_pollutant(pollutant_code),
    metric_type TEXT NOT NULL DEFAULT 'concentration',
    value_numeric DOUBLE PRECISION NOT NULL,
    unit TEXT NOT NULL,
    data_granularity TEXT NOT NULL,
    timestamp_start_utc TIMESTAMPTZ NOT NULL,
    timestamp_end_utc TIMESTAMPTZ,
    timestamp_local TIMESTAMPTZ,
    source_system TEXT NOT NULL CHECK (source_system IN ('AQS', 'AIRNOW')),
    source_dataset TEXT NOT NULL,
    source_priority INTEGER NOT NULL,
    data_status TEXT NOT NULL CHECK (data_status IN ('final', 'provisional')),
    method_code TEXT,
    method_name TEXT,
    qualifier_raw TEXT,
    source_last_modified_date DATE,
    is_current_version BOOLEAN NOT NULL DEFAULT FALSE,
    record_hash TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_history_key
    ON core.fact_observation_history(natural_key, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_fact_history_observation_time
    ON core.fact_observation_history(timestamp_start_utc DESC);

CREATE TABLE IF NOT EXISTS core.fact_observation_current (
    natural_key TEXT PRIMARY KEY,
    location_id TEXT NOT NULL REFERENCES core.dim_location(location_id),
    pollutant_code TEXT NOT NULL REFERENCES core.dim_pollutant(pollutant_code),
    metric_type TEXT NOT NULL DEFAULT 'concentration',
    value_numeric DOUBLE PRECISION NOT NULL,
    unit TEXT NOT NULL,
    data_granularity TEXT NOT NULL,
    timestamp_start_utc TIMESTAMPTZ NOT NULL,
    timestamp_end_utc TIMESTAMPTZ,
    timestamp_local TIMESTAMPTZ,
    source_system TEXT NOT NULL CHECK (source_system IN ('AQS', 'AIRNOW')),
    source_dataset TEXT NOT NULL,
    source_priority INTEGER NOT NULL,
    data_status TEXT NOT NULL CHECK (data_status IN ('final', 'provisional')),
    method_code TEXT,
    method_name TEXT,
    qualifier_raw TEXT,
    source_last_modified_date DATE,
    record_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_current_time
    ON core.fact_observation_current(timestamp_start_utc DESC);

CREATE INDEX IF NOT EXISTS idx_fact_current_location_pollutant_time
    ON core.fact_observation_current(location_id, pollutant_code, timestamp_start_utc DESC);

CREATE INDEX IF NOT EXISTS idx_fact_current_pollutant_time
    ON core.fact_observation_current(pollutant_code, timestamp_start_utc DESC);
