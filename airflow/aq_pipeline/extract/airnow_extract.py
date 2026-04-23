import json
import os
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from urllib.parse import urlencode

from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests import HTTPError

from aq_pipeline.clients.airnow_client import AirNowClient
from aq_pipeline.load.load_minio import write_raw_object
from aq_pipeline.utils.logging import get_logger
from aq_pipeline.utils.time_windows import hot_window_bounds


logger = get_logger(__name__)


def run_airnow_gap_bootstrap_extract() -> None:
    logger.info("Running AirNow gap bootstrap extraction")
    client = AirNowClient()
    pg_hook = _get_pg_hook()
    gap_chunk_hours = max(1, int(os.getenv("AIRNOW_GAP_CHUNK_HOURS", "24")))
    start_dt, end_dt, aqs_tail_utc = _resolve_gap_bootstrap_window(pg_hook=pg_hook)

    if start_dt > end_dt:
        detail = {
            "start_utc": start_dt.isoformat(),
            "end_utc": end_dt.isoformat(),
            "aqs_tail_utc": aqs_tail_utc.isoformat() if aqs_tail_utc else None,
        }
        _record_data_quality_issue(
            pg_hook=pg_hook,
            issue_type="airnow_gap_no_work_window",
            issue_detail=detail,
        )
        logger.info("Skip gap bootstrap because computed window is empty: %s", detail)
        return

    request_count = 0
    request_failures = 0
    successful_chunks = 0
    written = 0
    empty = 0

    chunk_start = start_dt
    while chunk_start <= end_dt:
        chunk_end = min(chunk_start + timedelta(hours=gap_chunk_hours - 1), end_dt)
        subchunks = _extract_gap_window_with_split(
            client=client,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
        )
        for subchunk in subchunks:
            request_count += subchunk["requests"]
            if subchunk["error"]:
                request_failures += 1
                _record_data_quality_issue(
                    pg_hook=pg_hook,
                    issue_type="airnow_gap_request_failed",
                    issue_detail={"params": subchunk["params"], "error": subchunk["error"]},
                )
                logger.error(
                    "Gap bootstrap request failed after retries: start=%s end=%s params=%s error=%s",
                    subchunk["chunk_start"].isoformat(),
                    subchunk["chunk_end"].isoformat(),
                    subchunk["params"],
                    subchunk["error"],
                )
                continue

            payload = subchunk["payload"]
            successful_chunks += 1
            row_count = len(payload) if isinstance(payload, list) else 0
            if row_count == 0:
                empty += 1
                _record_data_quality_issue(
                    pg_hook=pg_hook,
                    issue_type="airnow_gap_empty_response",
                    issue_detail={"params": subchunk["params"]},
                )

            payload_bytes = json.dumps(
                payload,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            payload_sha = sha256(payload_bytes).hexdigest()
            manifest = write_raw_object(
                dataset="airnow_gapfill_json",
                payload=payload,
                source_url=_build_airnow_source_url(client, subchunk["params"]),
                object_key=_build_gapfill_object_key(
                    chunk_start=subchunk["chunk_start"],
                    chunk_end=subchunk["chunk_end"],
                    content_sha256=payload_sha,
                ),
                metadata={
                    "query_params": subchunk["params"],
                    "run_type": "gap_bootstrap",
                    "aqs_tail_utc": aqs_tail_utc.isoformat() if aqs_tail_utc else None,
                    "chunk_start_utc": subchunk["chunk_start"].isoformat(),
                    "chunk_end_utc": subchunk["chunk_end"].isoformat(),
                    "row_count": row_count,
                    "is_empty": row_count == 0,
                },
            )
            written += 1
            logger.info(
                "AirNow gap chunk complete: start=%s end=%s rows=%s object_key=%s",
                subchunk["chunk_start"].isoformat(),
                subchunk["chunk_end"].isoformat(),
                row_count,
                manifest["object_key"],
            )
        chunk_start = chunk_end + timedelta(hours=1)

    if request_count > 0 and successful_chunks == 0:
        raise RuntimeError("AirNow gap bootstrap extraction failed for all chunks.")

    logger.info(
        "AirNow gap bootstrap extract complete: requests=%s written=%s empty=%s failed=%s",
        request_count,
        written,
        empty,
        request_failures,
    )


def run_airnow_hourly_hot_extract() -> None:
    logger.info("Running AirNow hourly hot-window extraction")
    client = AirNowClient()
    pg_hook = _get_pg_hook()
    start_utc, end_utc = hot_window_bounds(hours=48)
    base_url = os.getenv("AIRNOW_HOURLY_FILE_BASE_URL", "https://files.airnowtech.org/airnow")

    current = start_utc
    total = 0
    written = 0
    missing = 0
    empty = 0
    no_update = 0
    failed = 0
    latest_completed_hour: datetime | None = None

    while current <= end_utc:
        total += 1
        filename = f"HourlyData_{current:%Y%m%d%H}.dat"
        source_url = f"{base_url.rstrip('/')}/{filename}"
        try:
            response = client.get_hourly_file(source_url)
        except Exception as exc:
            failed += 1
            _record_data_quality_issue(
                pg_hook=pg_hook,
                issue_type="airnow_hourly_request_failed",
                issue_detail={
                    "file_name": filename,
                    "hour_start_utc": current.isoformat(),
                    "source_url": source_url,
                    "error": str(exc),
                },
            )
            logger.exception("AirNow hourly file request failed: url=%s", source_url)
            current = current + timedelta(hours=1)
            continue

        if response.status_code == 404:
            missing += 1
            latest_completed_hour = current
            _record_data_quality_issue(
                pg_hook=pg_hook,
                issue_type="airnow_hourly_missing_file",
                issue_detail={
                    "file_name": filename,
                    "hour_start_utc": current.isoformat(),
                    "source_url": source_url,
                },
            )
        elif response.status_code != 200:
            failed += 1
            _record_data_quality_issue(
                pg_hook=pg_hook,
                issue_type="airnow_hourly_http_error",
                issue_detail={
                    "file_name": filename,
                    "hour_start_utc": current.isoformat(),
                    "source_url": source_url,
                    "http_status": response.status_code,
                },
            )
            logger.warning(
                "Skip AirNow hourly file due to HTTP status: url=%s status=%s",
                source_url,
                response.status_code,
            )
        elif not response.content:
            empty += 1
            latest_completed_hour = current
            _record_data_quality_issue(
                pg_hook=pg_hook,
                issue_type="airnow_hourly_empty_file",
                issue_detail={
                    "file_name": filename,
                    "hour_start_utc": current.isoformat(),
                    "source_url": source_url,
                    "http_status": response.status_code,
                },
            )
        else:
            content_sha256 = sha256(response.content).hexdigest()
            previous_sha = _latest_airnow_hourly_sha(pg_hook=pg_hook, file_name=filename)
            if previous_sha == content_sha256:
                no_update += 1
                latest_completed_hour = current
                _record_data_quality_issue(
                    pg_hook=pg_hook,
                    issue_type="airnow_hourly_no_update",
                    issue_detail={
                        "file_name": filename,
                        "hour_start_utc": current.isoformat(),
                        "source_url": source_url,
                        "content_sha256": content_sha256,
                    },
                )
            else:
                write_raw_object(
                    dataset="airnow_hourly_file",
                    payload=response.content,
                    source_url=source_url,
                    object_key=_build_hourly_file_object_key(
                        hour_start_utc=current,
                        file_name=filename,
                        content_sha256=content_sha256,
                    ),
                    metadata={
                        "file_name": filename,
                        "hour_start_utc": current.isoformat(),
                        "run_type": "hourly_hot_sync",
                        "http_status": response.status_code,
                        "content_sha256": content_sha256,
                    },
                )
                written += 1
                latest_completed_hour = current

        current = current + timedelta(hours=1)

    if latest_completed_hour:
        _upsert_source_watermark(
            pg_hook=pg_hook,
            source_name="AIRNOW",
            watermark_key="latest_completed_hour_utc",
            watermark_value=latest_completed_hour.isoformat(),
        )

    if total > 0 and failed == total:
        raise RuntimeError("AirNow hourly hot-window extraction failed for all files.")

    logger.info(
        "AirNow hourly hot extract complete: total=%s written=%s no_update=%s missing=%s empty=%s failed=%s",
        total,
        written,
        no_update,
        missing,
        empty,
        failed,
    )


def _resolve_gap_bootstrap_window(*, pg_hook: PostgresHook) -> tuple[datetime, datetime, datetime | None]:
    now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_lag_hours = max(0, int(os.getenv("AIRNOW_GAP_END_LAG_HOURS", "1")))
    end_dt = now_hour - timedelta(hours=end_lag_hours)

    manual_start = os.getenv("AIRNOW_GAP_START_UTC")
    manual_end = os.getenv("AIRNOW_GAP_END_UTC")
    if manual_start and manual_end:
        return _parse_airnow_datetime(manual_start), _parse_airnow_datetime(manual_end), None

    aqs_tail_utc = _query_max_aqs_tail_utc(pg_hook=pg_hook)
    if aqs_tail_utc:
        start_dt = aqs_tail_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return start_dt, end_dt, aqs_tail_utc

    fallback_hours = max(1, int(os.getenv("AIRNOW_GAP_FALLBACK_HOURS", "24")))
    return end_dt - timedelta(hours=fallback_hours - 1), end_dt, None


def _query_max_aqs_tail_utc(*, pg_hook: PostgresHook) -> datetime | None:
    watermark_row = pg_hook.get_first(
        sql="""
            SELECT MAX(watermark_value::timestamptz)
            FROM ops.source_watermark
            WHERE source_name = 'AQS'
              AND watermark_key LIKE 'tail_max_ts_utc:%'
        """
    )
    if watermark_row and isinstance(watermark_row[0], datetime):
        value = watermark_row[0]
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    row = pg_hook.get_first(
        sql="""
            SELECT MAX(timestamp_start_utc) AS max_ts
            FROM (
                SELECT timestamp_start_utc
                FROM core.fact_observation_current
                WHERE source_system = 'AQS'
                UNION ALL
                SELECT timestamp_start_utc
                FROM core.fact_observation_history
                WHERE source_system = 'AQS'
            ) tail
        """
    )
    if not row:
        return None
    value = row[0]
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return None


def _latest_airnow_hourly_sha(*, pg_hook: PostgresHook, file_name: str) -> str | None:
    row = pg_hook.get_first(
        sql="""
            SELECT content_sha256
            FROM raw.airnow_hourly_file_manifest
            WHERE metadata ->> 'file_name' = %s
            ORDER BY extracted_at DESC
            LIMIT 1
        """,
        parameters=(file_name,),
    )
    if not row:
        return None
    value = row[0]
    return str(value) if value else None


def _record_data_quality_issue(
    *,
    pg_hook: PostgresHook,
    issue_type: str,
    issue_detail: dict[str, object],
    natural_key: str | None = None,
) -> None:
    issue_detail_json = json.dumps(issue_detail, ensure_ascii=True, sort_keys=True)
    pg_hook.run(
        sql="""
            INSERT INTO ops.data_quality_issue (issue_type, natural_key, issue_detail)
            VALUES (%s, %s, %s::jsonb)
        """,
        parameters=(issue_type, natural_key, issue_detail_json),
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


def _build_hourly_file_object_key(*, hour_start_utc: datetime, file_name: str, content_sha256: str) -> str:
    return (
        "airnow/hourly/"
        f"year={hour_start_utc:%Y}/month={hour_start_utc:%m}/day={hour_start_utc:%d}/"
        f"{file_name.rsplit('.', 1)[0]}_{content_sha256[:12]}.dat"
    )


def _build_gapfill_object_key(*, chunk_start: datetime, chunk_end: datetime, content_sha256: str) -> str:
    return (
        "airnow/gapfill/"
        f"year={chunk_start:%Y}/month={chunk_start:%m}/day={chunk_start:%d}/"
        f"gap_{chunk_start:%Y%m%dT%H}_{chunk_end:%Y%m%dT%H}_{content_sha256[:12]}.json"
    )


def _get_pg_hook() -> PostgresHook:
    pg_conn_id = os.getenv("AIRFLOW_CONN_POSTGRES_ID", "aq_postgres")
    return PostgresHook(postgres_conn_id=pg_conn_id)


def _parse_airnow_datetime(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def _format_airnow_hour(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H")


def _build_gap_params(chunk_start: datetime, chunk_end: datetime) -> dict[str, str]:
    return {
        "startDate": _format_airnow_hour(chunk_start),
        "endDate": _format_airnow_hour(chunk_end),
        "parameters": os.getenv("AIRNOW_GAP_PARAMETERS", "PM25"),
        "BBOX": os.getenv("AIRNOW_GAP_BBOX", "-125,24,-66,49"),
        "dataType": os.getenv("AIRNOW_GAP_DATATYPE", "B"),
        "format": "application/json",
        "monitorType": os.getenv("AIRNOW_GAP_MONITOR_TYPE", "0"),
        "includerawconcentrations": os.getenv("AIRNOW_GAP_INCLUDE_RAW", "1"),
        "verbose": os.getenv("AIRNOW_GAP_VERBOSE", "1"),
    }


def _extract_gap_window_with_split(
    *,
    client: AirNowClient,
    chunk_start: datetime,
    chunk_end: datetime,
) -> list[dict[str, object]]:
    params = _build_gap_params(chunk_start, chunk_end)
    try:
        payload = client.get_observations(params)
        return [
            {
                "chunk_start": chunk_start,
                "chunk_end": chunk_end,
                "params": params,
                "payload": payload,
                "error": None,
                "requests": 1,
            }
        ]
    except HTTPError as exc:
        if _is_airnow_record_limit_error(exc) and chunk_start < chunk_end:
            split_point = chunk_start + timedelta(hours=_window_hour_count(chunk_start, chunk_end) // 2 - 1)
            logger.warning(
                "AirNow gap window exceeded record limit. Splitting request: start=%s end=%s split_end=%s",
                chunk_start.isoformat(),
                chunk_end.isoformat(),
                split_point.isoformat(),
            )
            left = _extract_gap_window_with_split(
                client=client,
                chunk_start=chunk_start,
                chunk_end=split_point,
            )
            right = _extract_gap_window_with_split(
                client=client,
                chunk_start=split_point + timedelta(hours=1),
                chunk_end=chunk_end,
            )
            left_requests = sum(int(item["requests"]) for item in left)
            right_requests = sum(int(item["requests"]) for item in right)
            if left:
                left[0]["requests"] = int(left[0]["requests"]) + 1
            elif right:
                right[0]["requests"] = int(right[0]["requests"]) + 1
            else:
                return [
                    {
                        "chunk_start": chunk_start,
                        "chunk_end": chunk_end,
                        "params": params,
                        "payload": None,
                        "error": _format_airnow_exception(exc),
                        "requests": 1 + left_requests + right_requests,
                    }
                ]
            return left + right
        return [
            {
                "chunk_start": chunk_start,
                "chunk_end": chunk_end,
                "params": params,
                "payload": None,
                "error": _format_airnow_exception(exc),
                "requests": 1,
            }
        ]
    except Exception as exc:
        return [
            {
                "chunk_start": chunk_start,
                "chunk_end": chunk_end,
                "params": params,
                "payload": None,
                "error": _format_airnow_exception(exc),
                "requests": 1,
            }
        ]


def _window_hour_count(chunk_start: datetime, chunk_end: datetime) -> int:
    return int((chunk_end - chunk_start).total_seconds() // 3600) + 1


def _is_airnow_record_limit_error(exc: HTTPError) -> bool:
    response = exc.response
    if response is None:
        return False
    return "exceeds the record query limit" in (response.text or "").lower()


def _format_airnow_exception(exc: Exception) -> str:
    if isinstance(exc, HTTPError) and exc.response is not None:
        response_text = (exc.response.text or "").strip()
        if response_text:
            return f"{exc} response={response_text}"
    return str(exc)


def _build_airnow_source_url(client: AirNowClient, params: dict[str, str]) -> str:
    return f"{client.BASE_URL}?{urlencode(params)}"
