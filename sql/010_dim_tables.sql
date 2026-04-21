CREATE TABLE IF NOT EXISTS core.dim_pollutant (
    pollutant_code TEXT PRIMARY KEY,
    pollutant_name TEXT NOT NULL,
    default_unit TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS core.dim_location (
    location_id TEXT PRIMARY KEY,
    location_type TEXT NOT NULL,
    country_code TEXT,
    state_code TEXT,
    county_code TEXT,
    site_number TEXT,
    poc TEXT,
    aqsid TEXT,
    site_name TEXT,
    city_name TEXT,
    timezone_name TEXT,
    timezone_offset_hours DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS core.dim_exceedance_threshold (
    pollutant_code TEXT NOT NULL REFERENCES core.dim_pollutant(pollutant_code),
    threshold_type TEXT NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    unit TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pollutant_code, threshold_type)
);

CREATE INDEX IF NOT EXISTS idx_dim_location_state_county
    ON core.dim_location(state_code, county_code, location_id);

CREATE INDEX IF NOT EXISTS idx_dim_threshold_active
    ON core.dim_exceedance_threshold(pollutant_code)
    WHERE is_active = TRUE;
