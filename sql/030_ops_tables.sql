CREATE TABLE IF NOT EXISTS raw.aqs_sample_json_manifest (
    object_key TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS raw.aqs_daily_json_manifest (
    object_key TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS raw.aqs_metadata_json_manifest (
    object_key TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS raw.aqs_monitor_metadata_json_manifest (
    object_key TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS raw.airnow_hourly_file_manifest (
    object_key TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS raw.airnow_gapfill_manifest (
    object_key TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ops.pipeline_run (
    run_id BIGSERIAL PRIMARY KEY,
    dag_id TEXT NOT NULL,
    task_id TEXT,
    run_type TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    message TEXT
);

CREATE TABLE IF NOT EXISTS ops.source_watermark (
    source_name TEXT NOT NULL,
    watermark_key TEXT NOT NULL,
    watermark_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_name, watermark_key)
);

CREATE TABLE IF NOT EXISTS ops.airnow_revision_audit (
    revision_id BIGSERIAL PRIMARY KEY,
    natural_key TEXT NOT NULL,
    old_value DOUBLE PRECISION,
    new_value DOUBLE PRECISION NOT NULL,
    old_hash TEXT,
    new_hash TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.data_quality_issue (
    issue_id BIGSERIAL PRIMARY KEY,
    issue_type TEXT NOT NULL,
    natural_key TEXT,
    issue_detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
