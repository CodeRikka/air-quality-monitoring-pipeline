CREATE TABLE IF NOT EXISTS staging.aqs_sample_observation (
    object_key TEXT NOT NULL,
    source_row_hash TEXT NOT NULL,
    payload JSONB NOT NULL,
    source_last_modified_date DATE,
    observed_at_utc TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (object_key, source_row_hash)
);

CREATE INDEX IF NOT EXISTS idx_staging_aqs_sample_observed_at
    ON staging.aqs_sample_observation(observed_at_utc DESC);

CREATE TABLE IF NOT EXISTS staging.aqs_daily_observation (
    object_key TEXT NOT NULL,
    source_row_hash TEXT NOT NULL,
    payload JSONB NOT NULL,
    source_last_modified_date DATE,
    observed_date DATE,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (object_key, source_row_hash)
);

CREATE INDEX IF NOT EXISTS idx_staging_aqs_daily_observed_date
    ON staging.aqs_daily_observation(observed_date DESC);

CREATE TABLE IF NOT EXISTS staging.aqs_monitor_metadata (
    object_key TEXT NOT NULL,
    source_row_hash TEXT NOT NULL,
    payload JSONB NOT NULL,
    monitor_id TEXT,
    source_last_modified_date DATE,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (object_key, source_row_hash)
);

CREATE INDEX IF NOT EXISTS idx_staging_aqs_monitor_id
    ON staging.aqs_monitor_metadata(monitor_id);

CREATE TABLE IF NOT EXISTS staging.airnow_hourly_observation (
    object_key TEXT NOT NULL,
    source_row_hash TEXT NOT NULL,
    payload JSONB NOT NULL,
    aqsid TEXT,
    hour_start_utc TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (object_key, source_row_hash)
);

CREATE INDEX IF NOT EXISTS idx_staging_airnow_hourly_time
    ON staging.airnow_hourly_observation(hour_start_utc DESC);

CREATE TABLE IF NOT EXISTS staging.airnow_gapfill_observation (
    object_key TEXT NOT NULL,
    source_row_hash TEXT NOT NULL,
    payload JSONB NOT NULL,
    aqsid TEXT,
    observed_at_utc TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (object_key, source_row_hash)
);

CREATE INDEX IF NOT EXISTS idx_staging_airnow_gapfill_time
    ON staging.airnow_gapfill_observation(observed_at_utc DESC);
