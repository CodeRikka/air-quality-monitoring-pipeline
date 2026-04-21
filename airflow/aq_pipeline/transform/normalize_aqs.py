import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch

from aq_pipeline.utils.keys import build_natural_key, build_record_hash, build_site_natural_location_id
from aq_pipeline.utils.logging import get_logger


logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
POLLUTANTS_CONFIG = REPO_ROOT / "configs" / "pollutants.yaml"


def normalize_aqs_payload() -> None:
    logger.info("Normalizing AQS payload into staging tables")
    pg_hook = _get_pg_hook()
    s3_hook = _get_s3_hook()
    pollutant_defaults = _load_pollutant_defaults()

    sample_written = _normalize_manifest_dataset(
        pg_hook=pg_hook,
        s3_hook=s3_hook,
        manifest_table="raw.aqs_sample_json_manifest",
        staging_table="staging.aqs_sample_observation",
        dataset="aqs_sample",
        pollutant_defaults=pollutant_defaults,
    )
    daily_written = _normalize_manifest_dataset(
        pg_hook=pg_hook,
        s3_hook=s3_hook,
        manifest_table="raw.aqs_daily_json_manifest",
        staging_table="staging.aqs_daily_observation",
        dataset="aqs_daily",
        pollutant_defaults=pollutant_defaults,
    )
    logger.info(
        "AQS normalization complete: staging_sample_rows=%s staging_daily_rows=%s",
        sample_written,
        daily_written,
    )


def _normalize_manifest_dataset(
    *,
    pg_hook: PostgresHook,
    s3_hook: S3Hook,
    manifest_table: str,
    staging_table: str,
    dataset: str,
    pollutant_defaults: dict[str, dict[str, str]],
) -> int:
    limit = int(os.getenv("AQS_NORMALIZE_MANIFEST_LIMIT", "500"))
    manifests = pg_hook.get_records(
        sql=f"""
            SELECT object_key, metadata
            FROM {manifest_table}
            ORDER BY extracted_at DESC
            LIMIT %s
        """,
        parameters=(limit,),
    )
    if not manifests:
        return 0

    rows_to_insert: list[tuple[str, str, str, Any]] = []
    for object_key, metadata in manifests:
        payload = _read_json_object(s3_hook=s3_hook, bucket="aqs-raw", object_key=object_key)
        source_rows = _extract_aqs_rows(payload)
        for source_row in source_rows:
            normalized = _normalize_aqs_row(
                source_row=source_row,
                metadata=metadata if isinstance(metadata, dict) else {},
                dataset=dataset,
                pollutant_defaults=pollutant_defaults,
            )
            if not normalized:
                continue
            source_row_hash = build_record_hash(
                {
                    "object_key": object_key,
                    "dataset": dataset,
                    "source_row": source_row,
                }
            )
            rows_to_insert.append(
                (
                    object_key,
                    source_row_hash,
                    json.dumps(normalized["payload"], ensure_ascii=True, sort_keys=True),
                    normalized["source_last_modified_date"],
                    normalized["observed_value"],
                )
            )

    if not rows_to_insert:
        return 0

    observed_column = "observed_at_utc" if dataset == "aqs_sample" else "observed_date"
    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            execute_batch(
                cursor,
                f"""
                    INSERT INTO {staging_table} (
                        object_key,
                        source_row_hash,
                        payload,
                        source_last_modified_date,
                        {observed_column}
                    )
                    VALUES (%s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (object_key, source_row_hash) DO NOTHING
                """,
                rows_to_insert,
                page_size=500,
            )
        conn.commit()
    finally:
        conn.close()
    return len(rows_to_insert)


def _normalize_aqs_row(
    *,
    source_row: dict[str, Any],
    metadata: dict[str, Any],
    dataset: str,
    pollutant_defaults: dict[str, dict[str, str]],
) -> dict[str, Any] | None:
    pollutant_code = str(
        source_row.get("parameter_code")
        or source_row.get("param")
        or metadata.get("pollutant_code")
        or ""
    ).strip()
    if not pollutant_code:
        return None

    value_numeric = _coerce_float(
        source_row.get("sample_measurement")
        if dataset == "aqs_sample"
        else source_row.get("arithmetic_mean")
        or source_row.get("first_max_value")
        or source_row.get("aqi")
    )
    if value_numeric is None:
        return None

    state = _clean_code(source_row.get("state_code") or metadata.get("state"), width=2)
    county = _clean_code(source_row.get("county_code") or metadata.get("county"), width=3)
    site = _clean_code(source_row.get("site_number"), width=4)
    poc = str(source_row.get("poc", "")).strip()
    location_id = f"aqs:{state}:{county}:{site}:{poc or 'na'}"
    site_natural_location_id = build_site_natural_location_id(
        state_code=state,
        county_code=county,
        site_number=site,
        fallback_location_id=location_id,
    )

    timestamp_start_utc, timestamp_local, timestamp_end_utc = _resolve_aqs_timestamps(
        source_row=source_row,
        dataset=dataset,
    )
    if timestamp_start_utc is None:
        return None

    data_granularity = "sample" if dataset == "aqs_sample" else "daily"
    metric_type = "concentration"
    timestamp_start_iso = timestamp_start_utc.isoformat()
    natural_key = build_natural_key(
        location_id=site_natural_location_id,
        pollutant_code=pollutant_code,
        metric_type=metric_type,
        data_granularity=data_granularity,
        timestamp_start_utc=timestamp_start_iso,
    )

    default_unit = pollutant_defaults.get(pollutant_code, {}).get("unit", "")
    unit = str(source_row.get("units_of_measure") or source_row.get("unit_of_measure") or default_unit).strip()
    source_last_modified_date = _parse_date(source_row.get("date_of_last_change"))

    normalized_record = {
        "natural_key": natural_key,
        "location_id": location_id,
        "site_natural_location_id": site_natural_location_id,
        "aqsid": f"{state}{county}{site}",
        "pollutant_code": pollutant_code,
        "metric_type": metric_type,
        "value_numeric": value_numeric,
        "unit": unit,
        "data_granularity": data_granularity,
        "timestamp_start_utc": timestamp_start_iso,
        "timestamp_end_utc": timestamp_end_utc.isoformat() if timestamp_end_utc else None,
        "timestamp_local": timestamp_local.isoformat() if timestamp_local else None,
        "source_system": "AQS",
        "source_dataset": dataset,
        "source_priority": 100,
        "data_status": "final",
        "method_code": str(source_row.get("method_code", "")).strip() or None,
        "method_name": str(source_row.get("method_name", "")).strip() or None,
        "qualifier_raw": str(source_row.get("qualifier") or source_row.get("qualifiers") or "").strip() or None,
        "source_last_modified_date": source_last_modified_date.isoformat() if source_last_modified_date else None,
        "raw_fields": source_row,
    }
    normalized_record["record_hash"] = build_record_hash(
        {
            "natural_key": normalized_record["natural_key"],
            "value_numeric": normalized_record["value_numeric"],
            "unit": normalized_record["unit"],
            "source_last_modified_date": normalized_record["source_last_modified_date"],
            "method_code": normalized_record["method_code"],
            "qualifier_raw": normalized_record["qualifier_raw"],
        }
    )
    return {
        "payload": normalized_record,
        "source_last_modified_date": source_last_modified_date,
        "observed_value": (
            timestamp_start_utc if dataset == "aqs_sample" else timestamp_start_utc.date()
        ),
    }


def _resolve_aqs_timestamps(
    *,
    source_row: dict[str, Any],
    dataset: str,
) -> tuple[datetime | None, datetime | None, datetime | None]:
    ts_utc = _parse_datetime_pair(source_row.get("date_gmt"), source_row.get("time_gmt"), tz=timezone.utc)
    ts_local = _parse_datetime_pair(source_row.get("date_local"), source_row.get("time_local"), tz=timezone.utc)

    if ts_utc is None:
        local_date = _parse_date(source_row.get("date_local"))
        if local_date:
            ts_utc = datetime(
                year=local_date.year,
                month=local_date.month,
                day=local_date.day,
                tzinfo=timezone.utc,
            )
    if ts_utc is None:
        return None, None, None

    if ts_local is None:
        ts_local = ts_utc

    if dataset == "aqs_daily":
        ts_end_utc = ts_utc + timedelta(days=1)
    else:
        ts_end_utc = None
    return ts_utc, ts_local, ts_end_utc


def _extract_aqs_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("Data", "Body", "data", "body"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return []


def _read_json_object(*, s3_hook: S3Hook, bucket: str, object_key: str) -> Any:
    obj = s3_hook.get_key(key=object_key, bucket_name=bucket)
    if obj is None:
        raise RuntimeError(f"Missing object in bucket '{bucket}': {object_key}")
    body = obj.get()["Body"].read()
    return json.loads(body.decode("utf-8"))


def _get_pg_hook() -> PostgresHook:
    pg_conn_id = os.getenv("AIRFLOW_CONN_POSTGRES_ID", "aq_postgres")
    return PostgresHook(postgres_conn_id=pg_conn_id)


def _get_s3_hook() -> S3Hook:
    s3_conn_id = os.getenv("AIRFLOW_CONN_MINIO_ID", "aq_minio")
    return S3Hook(aws_conn_id=s3_conn_id)


def _parse_datetime_pair(date_value: Any, time_value: Any, *, tz: timezone) -> datetime | None:
    date_str = str(date_value or "").strip()
    if not date_str:
        return None
    time_str = str(time_value or "").strip() or "00:00"
    try:
        return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M").replace(tzinfo=tz)
    except ValueError:
        return None


def _parse_date(value: Any) -> date | None:
    value_str = str(value or "").strip()
    if not value_str:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(value_str, fmt).date()
        except ValueError:
            continue
    return None


def _clean_code(value: Any, *, width: int) -> str:
    value_str = str(value or "").strip()
    if not value_str:
        return "na".zfill(width)
    return value_str.zfill(width)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
