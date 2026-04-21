import json
import os
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from aq_pipeline.utils.logging import get_logger


logger = get_logger(__name__)


DATASET_CONFIG: dict[str, dict[str, str]] = {
    "aqs_sample_json": {
        "bucket": "aqs-raw",
        "manifest_table": "raw.aqs_sample_json_manifest",
        "prefix": "aqs/sample",
        "extension": "json",
    },
    "aqs_daily_json": {
        "bucket": "aqs-raw",
        "manifest_table": "raw.aqs_daily_json_manifest",
        "prefix": "aqs/daily",
        "extension": "json",
    },
    "aqs_metadata_json": {
        "bucket": "aqs-raw",
        "manifest_table": "raw.aqs_metadata_json_manifest",
        "prefix": "aqs/metadata",
        "extension": "json",
    },
    "aqs_monitor_metadata_json": {
        "bucket": "aqs-raw",
        "manifest_table": "raw.aqs_monitor_metadata_json_manifest",
        "prefix": "aqs/monitor-metadata",
        "extension": "json",
    },
    "airnow_hourly_file": {
        "bucket": "airnow-raw",
        "manifest_table": "raw.airnow_hourly_file_manifest",
        "prefix": "airnow/hourly",
        "extension": "dat",
    },
    "airnow_gapfill_json": {
        "bucket": "airnow-raw",
        "manifest_table": "raw.airnow_gapfill_manifest",
        "prefix": "airnow/gapfill",
        "extension": "json",
    },
}


def _serialize_payload(payload: Any) -> bytes:
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _build_object_key(dataset: str, extracted_at: datetime, content_sha256: str, extension: str) -> str:
    cfg = DATASET_CONFIG[dataset]
    ts = extracted_at.strftime("%Y%m%dT%H%M%SZ")
    return (
        f"{cfg['prefix']}/year={extracted_at:%Y}/month={extracted_at:%m}/day={extracted_at:%d}/"
        f"{ts}_{content_sha256[:12]}.{extension}"
    )


def write_raw_object(
    dataset: str,
    payload: Any,
    source_url: str,
    metadata: dict[str, Any] | None = None,
    object_key: str | None = None,
) -> dict[str, str]:
    if dataset not in DATASET_CONFIG:
        raise ValueError(f"Unsupported dataset '{dataset}'. Supported: {sorted(DATASET_CONFIG.keys())}")

    cfg = DATASET_CONFIG[dataset]
    extracted_at = datetime.now(timezone.utc)
    body = _serialize_payload(payload)
    content_sha256 = sha256(body).hexdigest()

    final_object_key = object_key or _build_object_key(dataset, extracted_at, content_sha256, cfg["extension"])
    metadata_json = json.dumps(metadata or {}, ensure_ascii=True, sort_keys=True)

    s3_conn_id = os.getenv("AIRFLOW_CONN_MINIO_ID", "aq_minio")
    pg_conn_id = os.getenv("AIRFLOW_CONN_POSTGRES_ID", "aq_postgres")

    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3_hook.load_bytes(
        bytes_data=body,
        key=final_object_key,
        bucket_name=cfg["bucket"],
        replace=True,
    )

    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    pg_hook.run(
        sql=f"""
            INSERT INTO {cfg["manifest_table"]} (object_key, source_url, content_sha256, metadata)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (object_key) DO UPDATE
            SET source_url = EXCLUDED.source_url,
                content_sha256 = EXCLUDED.content_sha256,
                extracted_at = NOW(),
                metadata = EXCLUDED.metadata
        """,
        parameters=(final_object_key, source_url, content_sha256, metadata_json),
    )

    logger.info(
        "Raw object written: dataset=%s bucket=%s key=%s sha256=%s",
        dataset,
        cfg["bucket"],
        final_object_key,
        content_sha256,
    )
    return {
        "dataset": dataset,
        "bucket": cfg["bucket"],
        "object_key": final_object_key,
        "content_sha256": content_sha256,
        "manifest_table": cfg["manifest_table"],
    }
