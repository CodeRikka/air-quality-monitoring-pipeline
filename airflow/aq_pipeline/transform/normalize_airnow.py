import csv
import io
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_batch

from aq_pipeline.utils.keys import build_natural_key, build_record_hash, build_site_natural_location_id
from aq_pipeline.utils.logging import get_logger
from aq_pipeline.utils.regions import airnow_aqsid_in_regions, load_regions, normalize_aqsid


logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
POLLUTANTS_CONFIG = REPO_ROOT / "configs" / "pollutants.yaml"
HOURLY_FALLBACK_COLUMNS = [
    "ValidDate",
    "ValidTime",
    "AQSID",
    "SiteName",
    "GMTOffset",
    "ParameterName",
    "ReportingUnits",
    "Value",
    "DataSource",
]


def normalize_airnow_payload() -> None:
    logger.info("Normalizing AirNow payload into staging tables")
    pg_hook = _get_pg_hook()
    s3_hook = _get_s3_hook()
    pollutant_map = _build_airnow_pollutant_map()
    regions = load_regions()

    hourly_written = _normalize_hourly_files(
        pg_hook=pg_hook,
        s3_hook=s3_hook,
        pollutant_map=pollutant_map,
        regions=regions,
    )
    gapfill_written = _normalize_gapfill_json(
        pg_hook=pg_hook,
        s3_hook=s3_hook,
        pollutant_map=pollutant_map,
        regions=regions,
    )
    logger.info(
        "AirNow normalization complete: staging_hourly_rows=%s staging_gapfill_rows=%s",
        hourly_written,
        gapfill_written,
    )


def _normalize_hourly_files(
    *,
    pg_hook: PostgresHook,
    s3_hook: S3Hook,
    pollutant_map: dict[str, str],
    regions: list[dict[str, Any]],
) -> int:
    limit = int(os.getenv("AIRNOW_NORMALIZE_HOURLY_MANIFEST_LIMIT", "500"))
    manifests = pg_hook.get_records(
        sql="""
            SELECT object_key, metadata
            FROM raw.airnow_hourly_file_manifest
            ORDER BY extracted_at DESC
            LIMIT %s
        """,
        parameters=(limit,),
    )
    if not manifests:
        return 0

    rows_to_insert: list[tuple[str, str, str, str | None, datetime | None]] = []
    for object_key, metadata in manifests:
        metadata_dict = metadata if isinstance(metadata, dict) else {}
        content = _read_object_bytes(s3_hook=s3_hook, bucket="airnow-raw", object_key=object_key)
        source_rows = _parse_hourly_dat(content)
        for source_row in source_rows:
            normalized = _normalize_airnow_row(
                source_row=source_row,
                metadata=metadata_dict,
                pollutant_map=pollutant_map,
                source_dataset="airnow_hourly_file",
                default_granularity="hourly",
                regions=regions,
            )
            if not normalized:
                continue
            source_row_hash = build_record_hash(
                {
                    "object_key": object_key,
                    "dataset": "airnow_hourly_file",
                    "source_row": source_row,
                }
            )
            rows_to_insert.append(
                (
                    object_key,
                    source_row_hash,
                    json.dumps(normalized, ensure_ascii=True, sort_keys=True),
                    normalized.get("aqsid"),
                    _to_datetime(normalized.get("timestamp_start_utc")),
                )
            )

    if not rows_to_insert:
        return 0

    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            execute_batch(
                cursor,
                """
                    INSERT INTO staging.airnow_hourly_observation (
                        object_key,
                        source_row_hash,
                        payload,
                        aqsid,
                        hour_start_utc
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


def _normalize_gapfill_json(
    *,
    pg_hook: PostgresHook,
    s3_hook: S3Hook,
    pollutant_map: dict[str, str],
    regions: list[dict[str, Any]],
) -> int:
    limit = int(os.getenv("AIRNOW_NORMALIZE_GAP_MANIFEST_LIMIT", "500"))
    manifests = pg_hook.get_records(
        sql="""
            SELECT object_key, metadata
            FROM raw.airnow_gapfill_manifest
            ORDER BY extracted_at DESC
            LIMIT %s
        """,
        parameters=(limit,),
    )
    if not manifests:
        return 0

    rows_to_insert: list[tuple[str, str, str, str | None, datetime | None]] = []
    for object_key, metadata in manifests:
        metadata_dict = metadata if isinstance(metadata, dict) else {}
        payload = _read_json_object(s3_hook=s3_hook, bucket="airnow-raw", object_key=object_key)
        source_rows = payload if isinstance(payload, list) else []
        for source_row in source_rows:
            if not isinstance(source_row, dict):
                continue
            normalized = _normalize_airnow_row(
                source_row=source_row,
                metadata=metadata_dict,
                pollutant_map=pollutant_map,
                source_dataset="airnow_gapfill_json",
                default_granularity="hourly",
                regions=regions,
            )
            if not normalized:
                continue
            source_row_hash = build_record_hash(
                {
                    "object_key": object_key,
                    "dataset": "airnow_gapfill_json",
                    "source_row": source_row,
                }
            )
            rows_to_insert.append(
                (
                    object_key,
                    source_row_hash,
                    json.dumps(normalized, ensure_ascii=True, sort_keys=True),
                    normalized.get("aqsid"),
                    _to_datetime(normalized.get("timestamp_start_utc")),
                )
            )

    if not rows_to_insert:
        return 0

    conn = pg_hook.get_conn()
    try:
        with conn.cursor() as cursor:
            execute_batch(
                cursor,
                """
                    INSERT INTO staging.airnow_gapfill_observation (
                        object_key,
                        source_row_hash,
                        payload,
                        aqsid,
                        observed_at_utc
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


def _normalize_airnow_row(
    *,
    source_row: dict[str, Any],
    metadata: dict[str, Any],
    pollutant_map: dict[str, str],
    source_dataset: str,
    default_granularity: str,
    regions: list[dict[str, Any]],
) -> dict[str, Any] | None:
    parameter_name = _first_non_empty(
        source_row.get("ParameterName"),
        source_row.get("parameter name"),
        source_row.get("Parameter"),
        source_row.get("parameter"),
    )
    pollutant_code = pollutant_map.get(_normalize_pollutant_key(parameter_name))
    if not pollutant_code:
        return None

    value_numeric = _coerce_float(
        _first_non_empty(
            source_row.get("Value"),
            source_row.get("value"),
            source_row.get("RawConcentration"),
            source_row.get("raw concentration"),
        )
    )
    if value_numeric is None:
        return None

    aqsid = _extract_aqsid(source_row=source_row)
    if not aqsid:
        return None
    if not airnow_aqsid_in_regions(aqsid=aqsid, regions=regions):
        return None
    location_id = f"airnow:{aqsid}"
    site_natural_location_id = build_site_natural_location_id(
        aqsid=aqsid,
        fallback_location_id=location_id,
    )

    ts_utc = _parse_utc_timestamp(_first_non_empty(source_row.get("UTC"), source_row.get("utc")))
    if ts_utc is not None:
        ts_local = ts_utc
    else:
        date_text = _first_non_empty(source_row.get("ValidDate"), source_row.get("DateObserved"), source_row.get("valid date"))
        time_text = _first_non_empty(source_row.get("ValidTime"), source_row.get("HourObserved"), source_row.get("valid time"))
        gmt_offset = _first_non_empty(source_row.get("GMTOffset"), source_row.get("gmt offset"), source_row.get("UTCOffset"))
        ts_utc, ts_local = _resolve_airnow_timestamps(date_text=date_text, time_text=time_text, gmt_offset=gmt_offset)
    if ts_utc is None:
        return None

    data_granularity = default_granularity
    metric_type = "concentration"
    natural_key = build_natural_key(
        location_id=site_natural_location_id,
        pollutant_code=pollutant_code,
        metric_type=metric_type,
        data_granularity=data_granularity,
        timestamp_start_utc=ts_utc.isoformat(),
    )

    unit = _first_non_empty(
        source_row.get("ReportingUnits"),
        source_row.get("reporting units"),
        source_row.get("Unit"),
        source_row.get("unit"),
        "unknown",
    )
    source_last_modified_date = _parse_date(
        _first_non_empty(
            source_row.get("DateObserved"),
            source_row.get("ValidDate"),
        )
    )

    normalized_record = {
        "natural_key": natural_key,
        "location_id": location_id,
        "site_natural_location_id": site_natural_location_id,
        "aqsid": aqsid,
        "pollutant_code": pollutant_code,
        "metric_type": metric_type,
        "value_numeric": value_numeric,
        "unit": unit,
        "data_granularity": data_granularity,
        "timestamp_start_utc": ts_utc.isoformat(),
        "timestamp_end_utc": (ts_utc + timedelta(hours=1)).isoformat(),
        "timestamp_local": ts_local.isoformat() if ts_local else None,
        "source_system": "AIRNOW",
        "source_dataset": source_dataset,
        "source_priority": 10,
        "data_status": "provisional",
        "method_code": None,
        "method_name": None,
        "qualifier_raw": _first_non_empty(
            source_row.get("DataSource"),
            source_row.get("data source"),
            source_row.get("AgencyName"),
            source_row.get("agency name"),
        ),
        "source_last_modified_date": source_last_modified_date.isoformat() if source_last_modified_date else None,
        "raw_fields": source_row,
        "manifest_metadata": metadata,
    }
    normalized_record["record_hash"] = build_record_hash(
        {
            "natural_key": normalized_record["natural_key"],
            "value_numeric": normalized_record["value_numeric"],
            "unit": normalized_record["unit"],
            "source_last_modified_date": normalized_record["source_last_modified_date"],
            "qualifier_raw": normalized_record["qualifier_raw"],
        }
    )
    return normalized_record


def _parse_hourly_dat(content: bytes) -> list[dict[str, Any]]:
    text = content.decode("utf-8", errors="replace")
    first_non_empty = ""
    for line in text.splitlines():
        if line.strip():
            first_non_empty = line
            break
    if not first_non_empty:
        return []

    delimiter = "|" if "|" in first_non_empty else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    parsed_rows = [row for row in reader if row and any(str(col).strip() for col in row)]
    if not parsed_rows:
        return []

    header_is_present = _looks_like_header(parsed_rows[0])
    rows: list[dict[str, Any]] = []
    if header_is_present:
        headers = [str(col).strip() for col in parsed_rows[0]]
        data_rows = parsed_rows[1:]
    else:
        headers = HOURLY_FALLBACK_COLUMNS
        data_rows = parsed_rows

    for row in data_rows:
        row_dict: dict[str, Any] = {}
        for idx, value in enumerate(row):
            key = headers[idx] if idx < len(headers) else f"extra_{idx}"
            row_dict[key] = str(value).strip()
        rows.append(row_dict)
    return rows


def _looks_like_header(row: list[Any]) -> bool:
    header_tokens = {"valid date", "valid time", "aqsid", "parameter name", "value", "gmt offset"}
    lowered = " ".join(str(item).strip().lower() for item in row)
    return any(token in lowered for token in header_tokens)


def _build_airnow_pollutant_map() -> dict[str, str]:
    with POLLUTANTS_CONFIG.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    mapping: dict[str, str] = {}
    for pollutant in payload.get("pollutants", []):
        if not isinstance(pollutant, dict):
            continue
        code = str(pollutant.get("code", "")).strip()
        name = str(pollutant.get("name", "")).strip()
        if code and name:
            mapping[_normalize_pollutant_key(name)] = code

    aliases = {
        "pm25": "88101",
        "pm2.5": "88101",
        "pm10": "81102",
        "ozone": "44201",
        "o3": "44201",
        "sulfurdioxide": "42401",
        "so2": "42401",
        "nitrogendioxide": "42602",
        "no2": "42602",
        "carbonmonoxide": "42101",
        "co": "42101",
    }
    for alias, code in aliases.items():
        if code:
            mapping[_normalize_pollutant_key(alias)] = code
    return mapping


def _normalize_pollutant_key(value: str | None) -> str:
    text = str(value or "").lower().strip()
    return re.sub(r"[^a-z0-9.]+", "", text)


def _extract_aqsid(*, source_row: dict[str, Any]) -> str | None:
    raw = _first_non_empty(
        source_row.get("AQSID"),
        source_row.get("FullAQSCode"),
        source_row.get("IntlAQSCode"),
        source_row.get("full AQSID"),
        source_row.get("aqsid"),
    )
    return normalize_aqsid(raw)


def _parse_utc_timestamp(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _resolve_airnow_timestamps(
    *,
    date_text: str | None,
    time_text: str | None,
    gmt_offset: str | None,
) -> tuple[datetime | None, datetime | None]:
    local_date = _parse_date(date_text)
    hour = _parse_hour(time_text)
    if not local_date or hour is None:
        return None, None

    local_ts = datetime(
        year=local_date.year,
        month=local_date.month,
        day=local_date.day,
        hour=hour,
        tzinfo=timezone.utc,
    )
    offset_value = _coerce_float(gmt_offset)
    if offset_value is None:
        return local_ts, local_ts

    # GMT offset is local_time = UTC + offset.
    utc_ts = local_ts - timedelta(hours=offset_value)
    return utc_ts, local_ts


def _parse_hour(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    if ":" in text:
        text = text.split(":", maxsplit=1)[0].strip()
    try:
        hour = int(float(text))
    except (TypeError, ValueError):
        return None
    if 0 <= hour <= 23:
        return hour
    return None


def _read_object_bytes(*, s3_hook: S3Hook, bucket: str, object_key: str) -> bytes:
    obj = s3_hook.get_key(key=object_key, bucket_name=bucket)
    if obj is None:
        raise RuntimeError(f"Missing object in bucket '{bucket}': {object_key}")
    return obj.get()["Body"].read()


def _read_json_object(*, s3_hook: S3Hook, bucket: str, object_key: str) -> Any:
    return json.loads(_read_object_bytes(s3_hook=s3_hook, bucket=bucket, object_key=object_key).decode("utf-8"))


def _get_pg_hook() -> PostgresHook:
    pg_conn_id = os.getenv("AIRFLOW_CONN_POSTGRES_ID", "aq_postgres")
    return PostgresHook(postgres_conn_id=pg_conn_id)


def _get_s3_hook() -> S3Hook:
    s3_conn_id = os.getenv("AIRFLOW_CONN_MINIO_ID", "aq_minio")
    return S3Hook(aws_conn_id=s3_conn_id)


def _parse_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_non_empty(*values: Any) -> str | None:
    for value in values:
        text = str(value).strip() if value is not None else ""
        if text:
            return text
    return None


def _to_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
