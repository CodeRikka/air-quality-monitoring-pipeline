import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch

from aq_pipeline.utils.logging import get_logger
from aq_pipeline.utils.regions import airnow_aqsid_in_regions, load_regions, normalize_aqsid


logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
POLLUTANTS_CONFIG = REPO_ROOT / "configs" / "pollutants.yaml"
REGIONS_CONFIG = REPO_ROOT / "configs" / "regions.yaml"
THRESHOLDS_CONFIG = REPO_ROOT / "configs" / "thresholds.yaml"
REQUIRED_FIELDS = {
    "natural_key",
    "location_id",
    "pollutant_code",
    "metric_type",
    "value_numeric",
    "unit",
    "data_granularity",
    "timestamp_start_utc",
    "source_system",
    "source_dataset",
    "source_priority",
    "data_status",
    "record_hash",
}


def upsert_observation_tables() -> None:
    pg_hook = _get_pg_hook()
    started_at = datetime.now(timezone.utc)
    run_id = _start_pipeline_run(pg_hook=pg_hook, started_at=started_at)
    try:
        payloads = _fetch_staging_payloads(pg_hook=pg_hook)
        regions = load_regions()
        valid_rows, rejected, region_filtered = _validate_and_prepare_rows(payloads, regions=regions)

        if rejected > 0:
            _record_data_quality_issue(
                pg_hook=pg_hook,
                issue_type="normalize_payload_invalid",
                issue_detail={"rejected_rows": rejected},
            )
        if region_filtered > 0:
            _record_data_quality_issue(
                pg_hook=pg_hook,
                issue_type="airnow_region_filtered",
                issue_detail={"filtered_rows": region_filtered},
            )

        pollutant_defaults = _load_pollutant_defaults()
        threshold_defaults = _load_threshold_defaults()
        _upsert_dim_pollutant(pg_hook=pg_hook, rows=valid_rows, pollutant_defaults=pollutant_defaults)
        _upsert_dim_exceedance_threshold(pg_hook=pg_hook, threshold_defaults=threshold_defaults)

        if not valid_rows:
            _finish_pipeline_run(
                pg_hook=pg_hook,
                run_id=run_id,
                status="success",
                message="No valid staging rows to load after validation and region filtering. Dimension defaults synced.",
            )
            logger.info("No valid staged rows to load into core tables. Synced dimension defaults only.")
            return

        _upsert_dim_location(pg_hook=pg_hook, rows=valid_rows)
        inserted_history = _upsert_core_tables(pg_hook=pg_hook, rows=valid_rows)
        _update_source_watermarks(pg_hook=pg_hook, rows=valid_rows, pollutant_defaults=pollutant_defaults)

        _finish_pipeline_run(
            pg_hook=pg_hook,
            run_id=run_id,
            status="success",
            message=(
                f"Loaded rows={len(valid_rows)} inserted_history={inserted_history} "
                f"rejected={rejected} region_filtered={region_filtered}"
            ),
        )
        logger.info(
            "Load complete: valid_rows=%s inserted_history=%s rejected=%s region_filtered=%s",
            len(valid_rows),
            inserted_history,
            rejected,
            region_filtered,
        )
    except Exception as exc:
        _finish_pipeline_run(
            pg_hook=pg_hook,
            run_id=run_id,
            status="failed",
            message=str(exc),
        )
        raise


def _fetch_staging_payloads(*, pg_hook: PostgresHook) -> list[dict[str, Any]]:
    query = """
        WITH src AS (
            SELECT payload, ingested_at
            FROM staging.aqs_sample_observation
            UNION ALL
            SELECT payload, ingested_at
            FROM staging.aqs_daily_observation
            UNION ALL
            SELECT payload, ingested_at
            FROM staging.airnow_hourly_observation
            UNION ALL
            SELECT payload, ingested_at
            FROM staging.airnow_gapfill_observation
        ),
        dedup AS (
            SELECT
                payload,
                ROW_NUMBER() OVER (
                    PARTITION BY payload ->> 'natural_key', payload ->> 'record_hash'
                    ORDER BY ingested_at DESC
                ) AS rn
            FROM src
        )
        SELECT payload
        FROM dedup
        WHERE rn = 1
    """
    records = pg_hook.get_records(sql=query)
    payloads: list[dict[str, Any]] = []
    for record in records:
        payload = record[0]
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def _validate_and_prepare_rows(
    payloads: list[dict[str, Any]],
    *,
    regions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    valid: list[dict[str, Any]] = []
    rejected = 0
    region_filtered = 0
    for payload in payloads:
        if not _payload_has_required_fields(payload):
            rejected += 1
            continue
        try:
            prepared = {
                "natural_key": str(payload["natural_key"]),
                "location_id": str(payload["location_id"]),
                "pollutant_code": str(payload["pollutant_code"]),
                "metric_type": str(payload["metric_type"]),
                "value_numeric": float(payload["value_numeric"]),
                "unit": str(payload["unit"]),
                "data_granularity": str(payload["data_granularity"]),
                "timestamp_start_utc": _to_datetime(payload["timestamp_start_utc"]),
                "timestamp_end_utc": _to_datetime(payload.get("timestamp_end_utc")),
                "timestamp_local": _to_datetime(payload.get("timestamp_local")),
                "source_system": str(payload["source_system"]),
                "source_dataset": str(payload["source_dataset"]),
                "source_priority": int(payload["source_priority"]),
                "data_status": str(payload["data_status"]),
                "method_code": _optional_str(payload.get("method_code")),
                "method_name": _optional_str(payload.get("method_name")),
                "qualifier_raw": _optional_str(payload.get("qualifier_raw")),
                "source_last_modified_date": _to_date(payload.get("source_last_modified_date")),
                "record_hash": str(payload["record_hash"]),
                "raw_fields": payload.get("raw_fields", {}),
                "aqsid": _optional_str(payload.get("aqsid")),
            }
        except (TypeError, ValueError):
            rejected += 1
            continue
        if prepared["timestamp_start_utc"] is None:
            rejected += 1
            continue
        if prepared["source_system"] not in {"AQS", "AIRNOW"}:
            rejected += 1
            continue
        if prepared["data_status"] not in {"final", "provisional"}:
            rejected += 1
            continue
        if prepared["source_system"] == "AIRNOW" and not airnow_aqsid_in_regions(
            aqsid=prepared["aqsid"],
            regions=regions,
        ):
            region_filtered += 1
            continue
        valid.append(prepared)
    return valid, rejected, region_filtered


def _upsert_dim_pollutant(
    *,
    pg_hook: PostgresHook,
    rows: list[dict[str, Any]],
    pollutant_defaults: dict[str, dict[str, str]],
) -> None:
    unique_rows: dict[str, tuple[str, str, str, str]] = {}
    for code, defaults in pollutant_defaults.items():
        pollutant_name = defaults.get("name") or code
        default_unit = defaults.get("unit") or "unknown"
        metadata = json.dumps(
            {
                "source": "pollutants.yaml",
                "last_seen_source_system": "CONFIG",
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        unique_rows[code] = (code, pollutant_name, default_unit, metadata)

    for row in rows:
        code = row["pollutant_code"]
        defaults = pollutant_defaults.get(code, {})
        pollutant_name = defaults.get("name") or code
        default_unit = defaults.get("unit") or row["unit"]
        metadata = json.dumps(
            {
                "source": "normalized_observation",
                "last_seen_source_system": row["source_system"],
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        unique_rows[code] = (code, pollutant_name, default_unit, metadata)

    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    UPDATE core.dim_exceedance_threshold
                    SET is_active = FALSE,
                        updated_at = NOW()
                """
            )
            execute_batch(
                cursor,
                """
                    INSERT INTO core.dim_pollutant (
                        pollutant_code,
                        pollutant_name,
                        default_unit,
                        metadata
                    )
                    VALUES (%s, %s, %s, %s::jsonb)
                    ON CONFLICT (pollutant_code) DO UPDATE
                    SET pollutant_name = EXCLUDED.pollutant_name,
                        default_unit = EXCLUDED.default_unit,
                        metadata = EXCLUDED.metadata
                """,
                list(unique_rows.values()),
                page_size=200,
            )
        conn.commit()
    finally:
        conn.close()


def _upsert_dim_exceedance_threshold(
    *,
    pg_hook: PostgresHook,
    threshold_defaults: list[dict[str, Any]],
) -> None:
    unique_rows: dict[tuple[str, str], tuple[str, str, float, str, str]] = {}
    for threshold in threshold_defaults:
        pollutant_code = str(threshold.get("pollutant_code", "")).strip()
        threshold_type = str(threshold.get("threshold_type", "")).strip()
        unit = str(threshold.get("unit", "")).strip()
        threshold_value = threshold.get("threshold_value")
        if not pollutant_code or not threshold_type or not unit:
            continue
        try:
            numeric_value = float(threshold_value)
        except (TypeError, ValueError):
            continue
        metadata = json.dumps({"source": "thresholds.yaml"}, ensure_ascii=True, sort_keys=True)
        unique_rows[(pollutant_code, threshold_type)] = (
            pollutant_code,
            threshold_type,
            numeric_value,
            unit,
            metadata,
        )

    if not unique_rows:
        return

    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            execute_batch(
                cursor,
                """
                    INSERT INTO core.dim_exceedance_threshold (
                        pollutant_code,
                        threshold_type,
                        threshold_value,
                        unit,
                        metadata,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (pollutant_code, threshold_type) DO UPDATE
                    SET threshold_value = EXCLUDED.threshold_value,
                        unit = EXCLUDED.unit,
                        metadata = EXCLUDED.metadata,
                        is_active = TRUE,
                        updated_at = NOW()
                """,
                list(unique_rows.values()),
                page_size=100,
            )
        conn.commit()
    finally:
        conn.close()


def _upsert_dim_location(*, pg_hook: PostgresHook, rows: list[dict[str, Any]]) -> None:
    unique_rows: dict[str, tuple[Any, ...]] = {}
    for row in rows:
        location_id = row["location_id"]
        raw_fields = row.get("raw_fields", {}) if isinstance(row.get("raw_fields"), dict) else {}
        parsed = _parse_location_fields(location_id=location_id, raw_fields=raw_fields)
        metadata = json.dumps(
            {
                "source": "normalized_observation",
                "last_seen_source_system": row["source_system"],
                "raw_location_fields": parsed["raw_location_fields"],
            },
            ensure_ascii=True,
            sort_keys=True,
        )
        unique_rows[location_id] = (
            location_id,
            parsed["location_type"],
            parsed["country_code"],
            parsed["state_code"],
            parsed["county_code"],
            parsed["site_number"],
            parsed["poc"],
            parsed["aqsid"],
            parsed["site_name"],
            parsed["city_name"],
            parsed["timezone_name"],
            parsed["timezone_offset_hours"],
            parsed["latitude"],
            parsed["longitude"],
            metadata,
        )

    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            execute_batch(
                cursor,
                """
                    INSERT INTO core.dim_location (
                        location_id,
                        location_type,
                        country_code,
                        state_code,
                        county_code,
                        site_number,
                        poc,
                        aqsid,
                        site_name,
                        city_name,
                        timezone_name,
                        timezone_offset_hours,
                        latitude,
                        longitude,
                        metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (location_id) DO UPDATE
                    SET location_type = EXCLUDED.location_type,
                        country_code = EXCLUDED.country_code,
                        state_code = EXCLUDED.state_code,
                        county_code = EXCLUDED.county_code,
                        site_number = EXCLUDED.site_number,
                        poc = EXCLUDED.poc,
                        aqsid = EXCLUDED.aqsid,
                        site_name = EXCLUDED.site_name,
                        city_name = EXCLUDED.city_name,
                        timezone_name = EXCLUDED.timezone_name,
                        timezone_offset_hours = EXCLUDED.timezone_offset_hours,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        metadata = EXCLUDED.metadata
                """,
                list(unique_rows.values()),
                page_size=200,
            )
        conn.commit()
    finally:
        conn.close()


def _upsert_core_tables(*, pg_hook: PostgresHook, rows: list[dict[str, Any]]) -> int:
    conn = pg_hook.get_conn()
    inserted_history = 0
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    CREATE TEMP TABLE tmp_normalized_observation (
                        natural_key TEXT NOT NULL,
                        location_id TEXT NOT NULL,
                        pollutant_code TEXT NOT NULL,
                        metric_type TEXT NOT NULL,
                        value_numeric DOUBLE PRECISION NOT NULL,
                        unit TEXT NOT NULL,
                        data_granularity TEXT NOT NULL,
                        timestamp_start_utc TIMESTAMPTZ NOT NULL,
                        timestamp_end_utc TIMESTAMPTZ,
                        timestamp_local TIMESTAMPTZ,
                        source_system TEXT NOT NULL,
                        source_dataset TEXT NOT NULL,
                        source_priority INTEGER NOT NULL,
                        data_status TEXT NOT NULL,
                        method_code TEXT,
                        method_name TEXT,
                        qualifier_raw TEXT,
                        source_last_modified_date DATE,
                        record_hash TEXT NOT NULL
                    ) ON COMMIT DROP
                """
            )
            execute_batch(
                cursor,
                """
                    INSERT INTO tmp_normalized_observation (
                        natural_key,
                        location_id,
                        pollutant_code,
                        metric_type,
                        value_numeric,
                        unit,
                        data_granularity,
                        timestamp_start_utc,
                        timestamp_end_utc,
                        timestamp_local,
                        source_system,
                        source_dataset,
                        source_priority,
                        data_status,
                        method_code,
                        method_name,
                        qualifier_raw,
                        source_last_modified_date,
                        record_hash
                    )
                    VALUES (
                        %(natural_key)s,
                        %(location_id)s,
                        %(pollutant_code)s,
                        %(metric_type)s,
                        %(value_numeric)s,
                        %(unit)s,
                        %(data_granularity)s,
                        %(timestamp_start_utc)s,
                        %(timestamp_end_utc)s,
                        %(timestamp_local)s,
                        %(source_system)s,
                        %(source_dataset)s,
                        %(source_priority)s,
                        %(data_status)s,
                        %(method_code)s,
                        %(method_name)s,
                        %(qualifier_raw)s,
                        %(source_last_modified_date)s,
                        %(record_hash)s
                    )
                """,
                rows,
                page_size=500,
            )

            cursor.execute(
                """
                    INSERT INTO core.fact_observation_history (
                        natural_key,
                        location_id,
                        pollutant_code,
                        metric_type,
                        value_numeric,
                        unit,
                        data_granularity,
                        timestamp_start_utc,
                        timestamp_end_utc,
                        timestamp_local,
                        source_system,
                        source_dataset,
                        source_priority,
                        data_status,
                        method_code,
                        method_name,
                        qualifier_raw,
                        source_last_modified_date,
                        is_current_version,
                        record_hash
                    )
                    SELECT
                        t.natural_key,
                        t.location_id,
                        t.pollutant_code,
                        t.metric_type,
                        t.value_numeric,
                        t.unit,
                        t.data_granularity,
                        t.timestamp_start_utc,
                        t.timestamp_end_utc,
                        t.timestamp_local,
                        t.source_system,
                        t.source_dataset,
                        t.source_priority,
                        t.data_status,
                        t.method_code,
                        t.method_name,
                        t.qualifier_raw,
                        t.source_last_modified_date,
                        FALSE,
                        t.record_hash
                    FROM tmp_normalized_observation t
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM core.fact_observation_history h
                        WHERE h.natural_key = t.natural_key
                          AND h.record_hash = t.record_hash
                          AND h.source_system = t.source_system
                    )
                """
            )
            inserted_history = cursor.rowcount

            cursor.execute(
                """
                    INSERT INTO ops.airnow_revision_audit (
                        natural_key,
                        old_value,
                        new_value,
                        old_hash,
                        new_hash
                    )
                    SELECT
                        c.natural_key,
                        c.value_numeric,
                        t.value_numeric,
                        c.record_hash,
                        t.record_hash
                    FROM tmp_normalized_observation t
                    JOIN core.fact_observation_current c
                      ON c.natural_key = t.natural_key
                    WHERE c.source_system = 'AIRNOW'
                      AND t.source_system = 'AIRNOW'
                      AND c.source_priority = t.source_priority
                      AND c.record_hash <> t.record_hash
                """
            )

            cursor.execute(
                """
                    INSERT INTO core.fact_observation_current (
                        natural_key,
                        location_id,
                        pollutant_code,
                        metric_type,
                        value_numeric,
                        unit,
                        data_granularity,
                        timestamp_start_utc,
                        timestamp_end_utc,
                        timestamp_local,
                        source_system,
                        source_dataset,
                        source_priority,
                        data_status,
                        method_code,
                        method_name,
                        qualifier_raw,
                        source_last_modified_date,
                        record_hash,
                        updated_at
                    )
                    SELECT
                        latest.natural_key,
                        latest.location_id,
                        latest.pollutant_code,
                        latest.metric_type,
                        latest.value_numeric,
                        latest.unit,
                        latest.data_granularity,
                        latest.timestamp_start_utc,
                        latest.timestamp_end_utc,
                        latest.timestamp_local,
                        latest.source_system,
                        latest.source_dataset,
                        latest.source_priority,
                        latest.data_status,
                        latest.method_code,
                        latest.method_name,
                        latest.qualifier_raw,
                        latest.source_last_modified_date,
                        latest.record_hash,
                        NOW()
                    FROM (
                        SELECT DISTINCT ON (natural_key)
                            natural_key,
                            location_id,
                            pollutant_code,
                            metric_type,
                            value_numeric,
                            unit,
                            data_granularity,
                            timestamp_start_utc,
                            timestamp_end_utc,
                            timestamp_local,
                            source_system,
                            source_dataset,
                            source_priority,
                            data_status,
                            method_code,
                            method_name,
                            qualifier_raw,
                            source_last_modified_date,
                            record_hash
                        FROM tmp_normalized_observation
                        ORDER BY
                            natural_key,
                            source_priority DESC,
                            timestamp_start_utc DESC NULLS LAST,
                            source_last_modified_date DESC NULLS LAST,
                            record_hash DESC
                    ) latest
                    ON CONFLICT (natural_key) DO UPDATE
                    SET location_id = EXCLUDED.location_id,
                        pollutant_code = EXCLUDED.pollutant_code,
                        metric_type = EXCLUDED.metric_type,
                        value_numeric = EXCLUDED.value_numeric,
                        unit = EXCLUDED.unit,
                        data_granularity = EXCLUDED.data_granularity,
                        timestamp_start_utc = EXCLUDED.timestamp_start_utc,
                        timestamp_end_utc = EXCLUDED.timestamp_end_utc,
                        timestamp_local = EXCLUDED.timestamp_local,
                        source_system = EXCLUDED.source_system,
                        source_dataset = EXCLUDED.source_dataset,
                        source_priority = EXCLUDED.source_priority,
                        data_status = EXCLUDED.data_status,
                        method_code = EXCLUDED.method_code,
                        method_name = EXCLUDED.method_name,
                        qualifier_raw = EXCLUDED.qualifier_raw,
                        source_last_modified_date = EXCLUDED.source_last_modified_date,
                        record_hash = EXCLUDED.record_hash,
                        updated_at = NOW()
                    WHERE EXCLUDED.source_priority > core.fact_observation_current.source_priority
                       OR (
                            EXCLUDED.source_priority = core.fact_observation_current.source_priority
                            AND EXCLUDED.record_hash <> core.fact_observation_current.record_hash
                       )
                """
            )

            cursor.execute(
                """
                    UPDATE core.fact_observation_history h
                    SET is_current_version = FALSE
                    WHERE h.natural_key IN (SELECT DISTINCT natural_key FROM tmp_normalized_observation)
                """
            )

            cursor.execute(
                """
                    UPDATE core.fact_observation_history h
                    SET is_current_version = TRUE
                    FROM core.fact_observation_current c
                    WHERE h.natural_key = c.natural_key
                      AND h.record_hash = c.record_hash
                      AND h.source_system = c.source_system
                      AND h.natural_key IN (SELECT DISTINCT natural_key FROM tmp_normalized_observation)
                """
            )
        conn.commit()
    finally:
        conn.close()
    return inserted_history


def _start_pipeline_run(*, pg_hook: PostgresHook, started_at: datetime) -> int:
    dag_id = os.getenv("AIRFLOW_CTX_DAG_ID", "manual")
    task_id = os.getenv("AIRFLOW_CTX_TASK_ID")
    run_type = os.getenv("AIRFLOW_CTX_DAG_ID", "load_postgres")
    row = pg_hook.get_first(
        sql="""
            INSERT INTO ops.pipeline_run (dag_id, task_id, run_type, started_at, status, message)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING run_id
        """,
        parameters=(dag_id, task_id, run_type, started_at, "running", "load_postgres started"),
    )
    if not row:
        raise RuntimeError("Failed to create ops.pipeline_run row.")
    return int(row[0])


def _update_source_watermarks(
    *,
    pg_hook: PostgresHook,
    rows: list[dict[str, Any]],
    pollutant_defaults: dict[str, dict[str, str]],
) -> None:
    aqs_rows = [row for row in rows if row.get("source_system") == "AQS"]
    airnow_rows = [row for row in rows if row.get("source_system") == "AIRNOW"]

    if airnow_rows:
        latest_airnow_ts = max(
            (
                row.get("timestamp_start_utc")
                for row in airnow_rows
                if isinstance(row.get("timestamp_start_utc"), datetime)
            ),
            default=None,
        )
        if latest_airnow_ts:
            _upsert_source_watermark(
                pg_hook=pg_hook,
                source_name="AIRNOW",
                watermark_key="latest_completed_hour_utc",
                watermark_value=latest_airnow_ts.isoformat(),
            )

    if not aqs_rows:
        return

    latest_change_date = max(
        (
            row.get("source_last_modified_date")
            for row in aqs_rows
            if isinstance(row.get("source_last_modified_date"), date)
        ),
        default=None,
    )
    if latest_change_date:
        _upsert_source_watermark(
            pg_hook=pg_hook,
            source_name="AQS",
            watermark_key="latest_change_date",
            watermark_value=latest_change_date.strftime("%Y%m%d"),
        )

    regions = _load_regions()
    pollutant_codes = sorted(pollutant_defaults.keys())
    _update_aqs_tail_watermarks(
        pg_hook=pg_hook,
        regions=regions,
        pollutant_codes=pollutant_codes,
    )


def _update_aqs_tail_watermarks(
    *,
    pg_hook: PostgresHook,
    regions: list[dict[str, Any]],
    pollutant_codes: list[str],
) -> None:
    max_global: datetime | None = None
    for region in regions:
        region_id = str(region.get("id", "unknown")).strip() or "unknown"
        state_codes = _iter_region_state_codes(region)
        if not state_codes:
            continue
        for pollutant_code in pollutant_codes:
            row = pg_hook.get_first(
                sql="""
                    SELECT MAX(timestamp_start_utc)
                    FROM core.fact_observation_current
                    WHERE source_system = 'AQS'
                      AND pollutant_code = %s
                      AND split_part(location_id, ':', 2) = ANY(%s)
                """,
                parameters=(pollutant_code, state_codes),
            )
            if not row or not isinstance(row[0], datetime):
                continue
            value = row[0] if row[0].tzinfo else row[0].replace(tzinfo=timezone.utc)
            key = f"tail_max_ts_utc:{region_id}:{pollutant_code}"
            _upsert_source_watermark(
                pg_hook=pg_hook,
                source_name="AQS",
                watermark_key=key,
                watermark_value=value.isoformat(),
            )
            if max_global is None or value > max_global:
                max_global = value

    if max_global is not None:
        _upsert_source_watermark(
            pg_hook=pg_hook,
            source_name="AQS",
            watermark_key="tail_max_ts_utc:global",
            watermark_value=max_global.isoformat(),
        )


def _iter_region_state_codes(region: dict[str, Any]) -> list[str]:
    states = [str(code).strip().zfill(2) for code in region.get("state_codes", []) if str(code).strip()]
    explicit_pairs = region.get("state_county_pairs")
    if isinstance(explicit_pairs, list):
        for pair in explicit_pairs:
            if isinstance(pair, dict):
                state = str(pair.get("state", "")).strip()
            elif isinstance(pair, str):
                state = pair.split(":", 1)[0].strip()
            else:
                state = ""
            if state:
                states.append(state.zfill(2))
    return list(dict.fromkeys(states))


def _finish_pipeline_run(
    *,
    pg_hook: PostgresHook,
    run_id: int,
    status: str,
    message: str,
) -> None:
    pg_hook.run(
        sql="""
            UPDATE ops.pipeline_run
            SET ended_at = NOW(),
                status = %s,
                message = %s
            WHERE run_id = %s
        """,
        parameters=(status, message[:2000], run_id),
    )


def _record_data_quality_issue(
    *,
    pg_hook: PostgresHook,
    issue_type: str,
    issue_detail: dict[str, Any],
    natural_key: str | None = None,
) -> None:
    pg_hook.run(
        sql="""
            INSERT INTO ops.data_quality_issue (issue_type, natural_key, issue_detail)
            VALUES (%s, %s, %s::jsonb)
        """,
        parameters=(
            issue_type,
            natural_key,
            json.dumps(issue_detail, ensure_ascii=True, sort_keys=True),
        ),
    )


def _upsert_source_watermark(
    *,
    pg_hook: PostgresHook,
    source_name: str,
    watermark_key: str,
    watermark_value: str,
) -> None:
    pg_hook.run(
        sql="""
            INSERT INTO ops.source_watermark (source_name, watermark_key, watermark_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (source_name, watermark_key) DO UPDATE
            SET watermark_value = EXCLUDED.watermark_value,
                updated_at = NOW()
        """,
        parameters=(source_name, watermark_key, watermark_value),
    )


def _parse_location_fields(*, location_id: str, raw_fields: dict[str, Any]) -> dict[str, Any]:
    parts = location_id.split(":")
    location_type = parts[0] if parts else "unknown"

    if location_type == "aqs":
        return {
            "location_type": "site_monitor",
            "country_code": "US",
            "state_code": _none_if_na(parts[1] if len(parts) > 1 else None),
            "county_code": _none_if_na(parts[2] if len(parts) > 2 else None),
            "site_number": _none_if_na(parts[3] if len(parts) > 3 else None),
            "poc": _none_if_na(parts[4] if len(parts) > 4 else None),
            "aqsid": _optional_str(raw_fields.get("aqs_site_id")),
            "site_name": _optional_str(raw_fields.get("site_name")),
            "city_name": _optional_str(raw_fields.get("city_name")),
            "timezone_name": _optional_str(raw_fields.get("local_site_name")),
            "timezone_offset_hours": None,
            "latitude": _to_float(raw_fields.get("latitude")),
            "longitude": _to_float(raw_fields.get("longitude")),
            "raw_location_fields": raw_fields,
        }

    if location_type == "airnow":
        normalized_aqsid = normalize_aqsid(
            parts[1] if len(parts) > 1 else _optional_str(raw_fields.get("AQSID") or raw_fields.get("aqsid"))
        )
        return {
            "location_type": "site",
            "country_code": "US",
            "state_code": normalized_aqsid[0:2] if normalized_aqsid else None,
            "county_code": normalized_aqsid[2:5] if normalized_aqsid else None,
            "site_number": normalized_aqsid[5:9] if normalized_aqsid else None,
            "poc": None,
            "aqsid": normalized_aqsid,
            "site_name": _optional_str(raw_fields.get("SiteName") or raw_fields.get("site name")),
            "city_name": _optional_str(raw_fields.get("ReportingArea") or raw_fields.get("reporting area")),
            "timezone_name": None,
            "timezone_offset_hours": _to_float(raw_fields.get("GMTOffset") or raw_fields.get("gmt offset")),
            "latitude": _to_float(raw_fields.get("Latitude") or raw_fields.get("latitude")),
            "longitude": _to_float(raw_fields.get("Longitude") or raw_fields.get("longitude")),
            "raw_location_fields": raw_fields,
        }

    return {
        "location_type": "unknown",
        "country_code": None,
        "state_code": None,
        "county_code": None,
        "site_number": None,
        "poc": None,
        "aqsid": None,
        "site_name": None,
        "city_name": None,
        "timezone_name": None,
        "timezone_offset_hours": None,
        "latitude": None,
        "longitude": None,
        "raw_location_fields": raw_fields,
    }


def _load_pollutant_defaults() -> dict[str, dict[str, str]]:
    with POLLUTANTS_CONFIG.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    result: dict[str, dict[str, str]] = {}
    for pollutant in payload.get("pollutants", []):
        if not isinstance(pollutant, dict):
            continue
        code = str(pollutant.get("code", "")).strip()
        if not code:
            continue
        result[code] = {
            "name": str(pollutant.get("name", "")).strip(),
            "unit": str(pollutant.get("unit", "")).strip(),
        }
    return result


def _load_regions() -> list[dict[str, Any]]:
    with REGIONS_CONFIG.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    return [region for region in payload.get("regions", []) if isinstance(region, dict)]


def _load_threshold_defaults() -> list[dict[str, Any]]:
    with THRESHOLDS_CONFIG.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    return [threshold for threshold in payload.get("thresholds", []) if isinstance(threshold, dict)]


def _payload_has_required_fields(payload: dict[str, Any]) -> bool:
    missing = [field for field in REQUIRED_FIELDS if field not in payload or payload.get(field) in (None, "")]
    return len(missing) == 0


def _to_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _to_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _none_if_na(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned.lower() == "na":
        return None
    return cleaned


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_pg_hook() -> PostgresHook:
    pg_conn_id = os.getenv("AIRFLOW_CONN_POSTGRES_ID", "aq_postgres")
    return PostgresHook(postgres_conn_id=pg_conn_id)
