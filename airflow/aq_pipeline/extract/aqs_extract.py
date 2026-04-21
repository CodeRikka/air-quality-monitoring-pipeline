import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
import yaml
from airflow.providers.postgres.hooks.postgres import PostgresHook

from aq_pipeline.clients.aqs_client import AQSClient
from aq_pipeline.load.load_minio import write_raw_object
from aq_pipeline.utils.logging import get_logger


logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
POLLUTANTS_CONFIG = REPO_ROOT / "configs" / "pollutants.yaml"
REGIONS_CONFIG = REPO_ROOT / "configs" / "regions.yaml"


def run_aqs_bootstrap_extract() -> None:
    logger.info("Running AQS bootstrap extraction")
    client = AQSClient()
    now_utc = datetime.now(timezone.utc)
    regions = _load_regions()
    pollutant_codes = _load_pollutant_codes()
    year_windows = _resolve_bootstrap_windows(now_utc=now_utc)
    state_codes_total = sum(len(_iter_region_state_codes(region)) for region in regions)
    batch_size = _resolve_shard_param_batch_size(max_allowed=client.MAX_PARAMS_PER_REQUEST)
    pollutant_batches = _chunk_pollutant_codes(pollutant_codes, max_batch_size=batch_size)
    expected_requests = state_codes_total * len(year_windows) * len(pollutant_batches) * 2

    logger.info(
        "AQS bootstrap plan: regions=%s states=%s pollutants=%s pollutant_batches=%s windows=%s expected_requests=%s geography=byState",
        len(regions),
        state_codes_total,
        len(pollutant_codes),
        len(pollutant_batches),
        len(year_windows),
        expected_requests,
    )
    if _is_enabled("AQS_ENABLE_STATE_COUNTY_COVERAGE_LOG", default=False):
        _log_state_county_coverage(client=client, regions=regions, run_type="bootstrap")
    else:
        logger.info("Skip state-county coverage log for bootstrap (AQS_ENABLE_STATE_COUNTY_COVERAGE_LOG=false)")
    _run_aqs_metadata_extract(client=client, regions=regions, run_type="bootstrap")
    _run_aqs_monitor_metadata_extract(
        client=client,
        regions=regions,
        pollutant_codes=pollutant_codes,
        run_type="bootstrap",
        extracted_at=now_utc,
    )

    shard_result = _extract_aqs_data_shards(
        client=client,
        regions=regions,
        pollutant_codes=pollutant_codes,
        run_type="bootstrap",
        year_windows=year_windows,
        cbdate=None,
        cedate=None,
    )

    logger.info(
        "AQS bootstrap extract complete: requests=%s sample_written=%s daily_written=%s failed=%s",
        shard_result["requests_total"],
        shard_result["sample_written"],
        shard_result["daily_written"],
        shard_result["failed"],
    )


def run_aqs_daily_reconcile_extract() -> None:
    logger.info("Running AQS daily reconcile extraction")
    client = AQSClient()
    pg_hook = _get_pg_hook()
    now_utc = datetime.now(timezone.utc)
    regions = _load_regions()
    pollutant_codes = _load_pollutant_codes()
    cbdate, cedate = _resolve_reconcile_change_window(now_utc=now_utc, pg_hook=pg_hook)
    year_windows = _split_change_window_by_year(cbdate=cbdate, cedate=cedate)
    state_codes_total = sum(len(_iter_region_state_codes(region)) for region in regions)
    batch_size = _resolve_shard_param_batch_size(max_allowed=client.MAX_PARAMS_PER_REQUEST)
    pollutant_batches = _chunk_pollutant_codes(pollutant_codes, max_batch_size=batch_size)
    expected_requests = state_codes_total * len(year_windows) * len(pollutant_batches) * 2

    logger.info(
        "AQS reconcile plan: cbdate=%s cedate=%s regions=%s states=%s pollutants=%s pollutant_batches=%s year_windows=%s expected_requests=%s geography=byState",
        cbdate,
        cedate,
        len(regions),
        state_codes_total,
        len(pollutant_codes),
        len(pollutant_batches),
        len(year_windows),
        expected_requests,
    )
    if _is_enabled("AQS_ENABLE_STATE_COUNTY_COVERAGE_LOG", default=False):
        _log_state_county_coverage(client=client, regions=regions, run_type="daily_reconcile")
    else:
        logger.info(
            "Skip state-county coverage log for daily reconcile (AQS_ENABLE_STATE_COUNTY_COVERAGE_LOG=false)"
        )

    shard_result = _extract_aqs_data_shards(
        client=client,
        regions=regions,
        pollutant_codes=pollutant_codes,
        run_type="daily_reconcile",
        year_windows=year_windows,
        cbdate=cbdate,
        cedate=cedate,
    )

    logger.info(
        "AQS reconcile extract complete: cbdate=%s cedate=%s requests=%s sample_written=%s daily_written=%s failed=%s",
        cbdate,
        cedate,
        shard_result["requests_total"],
        shard_result["sample_written"],
        shard_result["daily_written"],
        shard_result["failed"],
    )
    _upsert_source_watermark(
        pg_hook=pg_hook,
        source_name="AQS",
        watermark_key="reconcile_last_cedate",
        watermark_value=cedate,
    )


def _extract_aqs_data_shards(
    *,
    client: AQSClient,
    regions: list[dict[str, Any]],
    pollutant_codes: list[str],
    run_type: str,
    year_windows: list[dict[str, str]],
    cbdate: str | None,
    cedate: str | None,
) -> dict[str, int]:
    requests_total = 0
    sample_written = 0
    daily_written = 0
    failed = 0
    batch_size = _resolve_shard_param_batch_size(max_allowed=client.MAX_PARAMS_PER_REQUEST)
    pollutant_batches = _chunk_pollutant_codes(pollutant_codes, max_batch_size=batch_size)
    geography_mode = _resolve_aqs_geography_mode()
    state_count = sum(len(_iter_region_state_codes(region)) for region in regions)
    total_shards = state_count * len(pollutant_batches) * len(year_windows)
    completed_shards = 0
    counties_by_state: dict[str, list[str]] = {}

    logger.info(
        "AQS shard phase started: run_type=%s total_shards=%s pollutant_batches=%s year_windows=%s est_planned_requests=%s geography=%s",
        run_type,
        total_shards,
        len(pollutant_batches),
        len(year_windows),
        total_shards * 2,
        geography_mode,
    )

    for region in regions:
        region_id = str(region.get("id", "unknown"))
        region_states = _iter_region_state_codes(region)
        if not region_states:
            logger.warning("Region has no state codes. Skipping region_id=%s", region_id)
            continue
        logger.info(
            "AQS shard region start: run_type=%s region_id=%s states=%s pollutant_batches=%s year_windows=%s geography=%s",
            run_type,
            region_id,
            len(region_states),
            len(pollutant_batches),
            len(year_windows),
            geography_mode,
        )
        for state in region_states:
            state_counties = _resolve_state_counties(
                client=client,
                regions=regions,
                state=state,
                geography_mode=geography_mode,
                cache=counties_by_state,
            )
            for pollutant_batch in pollutant_batches:
                param_csv = ",".join(pollutant_batch)
                for window in year_windows:
                    shard_geography = _resolve_shard_geography(
                        base_mode=geography_mode,
                        state=state,
                        counties=state_counties,
                        pollutant_batch=pollutant_batch,
                        window=window,
                    )
                    try:
                        if shard_geography == "county":
                            shard_result = _extract_aqs_data_by_counties(
                                client=client,
                                run_type=run_type,
                                region_id=region_id,
                                state=state,
                                counties=state_counties,
                                pollutant_batch=pollutant_batch,
                                window=window,
                                cbdate=cbdate,
                                cedate=cedate,
                                geography_mode=shard_geography,
                            )
                        else:
                            shard_result = _extract_aqs_data_for_batch_with_timeout_split(
                                client=client,
                                run_type=run_type,
                                region_id=region_id,
                                state=state,
                                pollutant_batch=pollutant_batch,
                                window=window,
                                cbdate=cbdate,
                                cedate=cedate,
                                state_counties=state_counties,
                            )
                        requests_total += shard_result["requests_total"]
                        sample_written += shard_result["sample_written"]
                        daily_written += shard_result["daily_written"]
                        failed += shard_result["failed"]
                    except Exception:
                        failed += 1
                        logger.exception(
                            "AQS shard extract failed: run_type=%s region_id=%s state=%s param=%s bdate=%s edate=%s geography=%s",
                            run_type,
                            region_id,
                            state,
                            param_csv,
                            window["bdate"],
                            window["edate"],
                            shard_geography,
                        )
                    finally:
                        completed_shards += 1
                        logger.info(
                            "%s run_type=%s region_id=%s state=%s param=%s bdate=%s edate=%s requests_so_far=%s sample_written=%s daily_written=%s failed=%s",
                            _progress_bar("shards", completed_shards, total_shards),
                            run_type,
                            region_id,
                            state,
                            param_csv,
                            window["bdate"],
                            window["edate"],
                            requests_total,
                            sample_written,
                            daily_written,
                            failed,
                        )
        logger.info(
            "AQS shard region complete: run_type=%s region_id=%s requests_so_far=%s sample_written=%s daily_written=%s failed=%s",
            run_type,
            region_id,
            requests_total,
            sample_written,
            daily_written,
            failed,
        )

    if requests_total > 0 and (sample_written + daily_written) == 0:
        raise RuntimeError("AQS extraction failed for all shard requests.")

    return {
        "requests_total": requests_total,
        "sample_written": sample_written,
        "daily_written": daily_written,
        "failed": failed,
    }


def _extract_aqs_data_for_batch_with_timeout_split(
    *,
    client: AQSClient,
    run_type: str,
    region_id: str,
    state: str,
    pollutant_batch: list[str],
    window: dict[str, str],
    cbdate: str | None,
    cedate: str | None,
    state_counties: list[str] | None = None,
    allow_timeout_split: bool = True,
    allow_county_fallback: bool = True,
    allow_window_split: bool = True,
) -> dict[str, int]:
    result = {
        "requests_total": 0,
        "sample_written": 0,
        "daily_written": 0,
        "failed": 0,
    }
    param_csv = ",".join(pollutant_batch)
    shard_params: dict[str, str] = {
        "state": state,
        "param": param_csv,
        "bdate": window["bdate"],
        "edate": window["edate"],
    }
    if cbdate and cedate:
        shard_params["cbdate"] = cbdate
        shard_params["cedate"] = cedate
    shard_metadata = {
        "run_type": run_type,
        "region_id": region_id,
        "state": state,
        "county": None,
        "pollutant_codes": pollutant_batch,
        "pollutant_code": pollutant_batch[0] if len(pollutant_batch) == 1 else None,
        "year": window["year"],
        "query_params": shard_params,
    }

    try:
        sample_endpoint = "sampleData/byState"
        result["requests_total"] += 1
        sample_payload = client.get_sample_data_by_state(
            param=pollutant_batch,
            bdate=window["bdate"],
            edate=window["edate"],
            state=state,
            cbdate=cbdate,
            cedate=cedate,
        )
        daily_endpoint = "dailyData/byState"
        result["requests_total"] += 1
        daily_payload = client.get_daily_data_by_state(
            param=pollutant_batch,
            bdate=window["bdate"],
            edate=window["edate"],
            state=state,
            cbdate=cbdate,
            cedate=cedate,
        )

        write_raw_object(
            dataset="aqs_sample_json",
            payload=sample_payload,
            source_url=_build_aqs_source_url(client, sample_endpoint, shard_params),
            metadata={**shard_metadata, "endpoint": sample_endpoint},
        )
        result["sample_written"] += 1
        write_raw_object(
            dataset="aqs_daily_json",
            payload=daily_payload,
            source_url=_build_aqs_source_url(client, daily_endpoint, shard_params),
            metadata={**shard_metadata, "endpoint": daily_endpoint},
        )
        result["daily_written"] += 1
        return result
    except Exception as exc:
        if allow_window_split and _is_request_timeout_error(exc):
            split_windows = _split_window_on_timeout(window)
            if split_windows:
                logger.warning(
                    "AQS state shard timed out, splitting window into smaller ranges: run_type=%s region_id=%s state=%s param=%s bdate=%s edate=%s split_windows=%s",
                    run_type,
                    region_id,
                    state,
                    param_csv,
                    window["bdate"],
                    window["edate"],
                    len(split_windows),
                )
                split_successes = 0
                split_errors: list[Exception] = []
                for split_window in split_windows:
                    try:
                        split_result = _extract_aqs_data_for_batch_with_timeout_split(
                            client=client,
                            run_type=run_type,
                            region_id=region_id,
                            state=state,
                            pollutant_batch=pollutant_batch,
                            window=split_window,
                            cbdate=cbdate,
                            cedate=cedate,
                            state_counties=state_counties,
                            allow_timeout_split=allow_timeout_split,
                            allow_county_fallback=allow_county_fallback,
                            allow_window_split=True,
                        )
                        result["requests_total"] += split_result["requests_total"]
                        result["sample_written"] += split_result["sample_written"]
                        result["daily_written"] += split_result["daily_written"]
                        result["failed"] += split_result["failed"]
                        split_successes += 1
                    except Exception as split_exc:
                        split_errors.append(split_exc)
                        result["failed"] += 1
                        logger.exception(
                            "AQS state window split retry failed: run_type=%s region_id=%s state=%s param=%s bdate=%s edate=%s",
                            run_type,
                            region_id,
                            state,
                            param_csv,
                            split_window["bdate"],
                            split_window["edate"],
                        )
                if split_successes == 0 and split_errors:
                    raise split_errors[-1]
                return result
        if allow_county_fallback and state_counties and _is_request_timeout_error(exc):
            logger.warning(
                "AQS state shard timed out, falling back to county requests: run_type=%s region_id=%s state=%s counties=%s param=%s bdate=%s edate=%s",
                run_type,
                region_id,
                state,
                len(state_counties),
                param_csv,
                window["bdate"],
                window["edate"],
            )
            return _extract_aqs_data_by_counties(
                client=client,
                run_type=run_type,
                region_id=region_id,
                state=state,
                counties=state_counties,
                pollutant_batch=pollutant_batch,
                window=window,
                cbdate=cbdate,
                cedate=cedate,
                geography_mode="county_fallback",
            )
        if allow_timeout_split and len(pollutant_batch) > 1 and _is_request_timeout_error(exc):
            logger.warning(
                "AQS batch timed out, splitting to single pollutant retries: run_type=%s region_id=%s state=%s param=%s bdate=%s edate=%s",
                run_type,
                region_id,
                state,
                param_csv,
                window["bdate"],
                window["edate"],
            )
            split_successes = 0
            split_errors: list[Exception] = []
            for pollutant_code in pollutant_batch:
                try:
                    split_result = _extract_aqs_data_for_batch_with_timeout_split(
                        client=client,
                        run_type=run_type,
                        region_id=region_id,
                        state=state,
                        pollutant_batch=[pollutant_code],
                        window=window,
                        cbdate=cbdate,
                        cedate=cedate,
                        state_counties=state_counties,
                        allow_timeout_split=False,
                        allow_county_fallback=allow_county_fallback,
                        allow_window_split=allow_window_split,
                    )
                    result["requests_total"] += split_result["requests_total"]
                    result["sample_written"] += split_result["sample_written"]
                    result["daily_written"] += split_result["daily_written"]
                    result["failed"] += split_result["failed"]
                    split_successes += 1
                except Exception as split_exc:
                    split_errors.append(split_exc)
                    result["failed"] += 1
                    logger.exception(
                        "AQS single-pollutant retry failed after timeout split: run_type=%s region_id=%s state=%s param=%s bdate=%s edate=%s",
                        run_type,
                        region_id,
                        state,
                        pollutant_code,
                        window["bdate"],
                        window["edate"],
                    )
            if split_successes == 0 and split_errors:
                raise split_errors[-1]
            return result
        raise


def _extract_aqs_data_by_counties(
    *,
    client: AQSClient,
    run_type: str,
    region_id: str,
    state: str,
    counties: list[str],
    pollutant_batch: list[str],
    window: dict[str, str],
    cbdate: str | None,
    cedate: str | None,
    geography_mode: str,
) -> dict[str, int]:
    result = {
        "requests_total": 0,
        "sample_written": 0,
        "daily_written": 0,
        "failed": 0,
    }
    for county in counties:
        county_result = _extract_aqs_data_for_county_with_timeout_split(
            client=client,
            run_type=run_type,
            region_id=region_id,
            state=state,
            county=county,
            pollutant_batch=pollutant_batch,
            window=window,
            cbdate=cbdate,
            cedate=cedate,
            geography_mode=geography_mode,
        )
        result["requests_total"] += county_result["requests_total"]
        result["sample_written"] += county_result["sample_written"]
        result["daily_written"] += county_result["daily_written"]
        result["failed"] += county_result["failed"]
    return result


def _extract_aqs_data_for_county_with_timeout_split(
    *,
    client: AQSClient,
    run_type: str,
    region_id: str,
    state: str,
    county: str,
    pollutant_batch: list[str],
    window: dict[str, str],
    cbdate: str | None,
    cedate: str | None,
    geography_mode: str,
    allow_timeout_split: bool = True,
    allow_window_split: bool = True,
) -> dict[str, int]:
    result = {
        "requests_total": 0,
        "sample_written": 0,
        "daily_written": 0,
        "failed": 0,
    }
    param_csv = ",".join(pollutant_batch)
    shard_params: dict[str, str] = {
        "state": state,
        "county": county,
        "param": param_csv,
        "bdate": window["bdate"],
        "edate": window["edate"],
    }
    if cbdate and cedate:
        shard_params["cbdate"] = cbdate
        shard_params["cedate"] = cedate
    shard_metadata = {
        "run_type": run_type,
        "region_id": region_id,
        "state": state,
        "county": county,
        "pollutant_codes": pollutant_batch,
        "pollutant_code": pollutant_batch[0] if len(pollutant_batch) == 1 else None,
        "year": window["year"],
        "query_params": shard_params,
    }

    try:
        sample_endpoint = "sampleData/byCounty"
        result["requests_total"] += 1
        sample_payload = client.get_sample_data_by_county(
            param=pollutant_batch,
            bdate=window["bdate"],
            edate=window["edate"],
            state=state,
            county=county,
            cbdate=cbdate,
            cedate=cedate,
        )
        daily_endpoint = "dailyData/byCounty"
        result["requests_total"] += 1
        daily_payload = client.get_daily_data_by_county(
            param=pollutant_batch,
            bdate=window["bdate"],
            edate=window["edate"],
            state=state,
            county=county,
            cbdate=cbdate,
            cedate=cedate,
        )

        write_raw_object(
            dataset="aqs_sample_json",
            payload=sample_payload,
            source_url=_build_aqs_source_url(client, sample_endpoint, shard_params),
            metadata={**shard_metadata, "endpoint": sample_endpoint},
        )
        result["sample_written"] += 1
        write_raw_object(
            dataset="aqs_daily_json",
            payload=daily_payload,
            source_url=_build_aqs_source_url(client, daily_endpoint, shard_params),
            metadata={**shard_metadata, "endpoint": daily_endpoint},
        )
        result["daily_written"] += 1
        return result
    except Exception as exc:
        if allow_window_split and _is_request_timeout_error(exc):
            split_windows = _split_window_on_timeout(window)
            if split_windows:
                logger.warning(
                    "AQS county shard timed out, splitting window into smaller ranges: run_type=%s region_id=%s state=%s county=%s param=%s bdate=%s edate=%s geography=%s split_windows=%s",
                    run_type,
                    region_id,
                    state,
                    county,
                    param_csv,
                    window["bdate"],
                    window["edate"],
                    geography_mode,
                    len(split_windows),
                )
                split_successes = 0
                split_errors: list[Exception] = []
                for split_window in split_windows:
                    try:
                        split_result = _extract_aqs_data_for_county_with_timeout_split(
                            client=client,
                            run_type=run_type,
                            region_id=region_id,
                            state=state,
                            county=county,
                            pollutant_batch=pollutant_batch,
                            window=split_window,
                            cbdate=cbdate,
                            cedate=cedate,
                            geography_mode=geography_mode,
                            allow_timeout_split=allow_timeout_split,
                            allow_window_split=True,
                        )
                        result["requests_total"] += split_result["requests_total"]
                        result["sample_written"] += split_result["sample_written"]
                        result["daily_written"] += split_result["daily_written"]
                        result["failed"] += split_result["failed"]
                        split_successes += 1
                    except Exception as split_exc:
                        split_errors.append(split_exc)
                        result["failed"] += 1
                        logger.exception(
                            "AQS county window split retry failed: run_type=%s region_id=%s state=%s county=%s param=%s bdate=%s edate=%s geography=%s",
                            run_type,
                            region_id,
                            state,
                            county,
                            param_csv,
                            split_window["bdate"],
                            split_window["edate"],
                            geography_mode,
                        )
                if split_successes == 0 and split_errors:
                    raise split_errors[-1]
                return result
        if allow_timeout_split and len(pollutant_batch) > 1 and _is_request_timeout_error(exc):
            logger.warning(
                "AQS county batch timed out, splitting to single pollutant retries: run_type=%s region_id=%s state=%s county=%s param=%s bdate=%s edate=%s geography=%s",
                run_type,
                region_id,
                state,
                county,
                param_csv,
                window["bdate"],
                window["edate"],
                geography_mode,
            )
            split_successes = 0
            split_errors: list[Exception] = []
            for pollutant_code in pollutant_batch:
                try:
                    split_result = _extract_aqs_data_for_county_with_timeout_split(
                        client=client,
                        run_type=run_type,
                        region_id=region_id,
                        state=state,
                        county=county,
                        pollutant_batch=[pollutant_code],
                        window=window,
                        cbdate=cbdate,
                        cedate=cedate,
                        geography_mode=geography_mode,
                        allow_timeout_split=False,
                        allow_window_split=allow_window_split,
                    )
                    result["requests_total"] += split_result["requests_total"]
                    result["sample_written"] += split_result["sample_written"]
                    result["daily_written"] += split_result["daily_written"]
                    result["failed"] += split_result["failed"]
                    split_successes += 1
                except Exception as split_exc:
                    split_errors.append(split_exc)
                    result["failed"] += 1
                    logger.exception(
                        "AQS county single-pollutant retry failed after timeout split: run_type=%s region_id=%s state=%s county=%s param=%s bdate=%s edate=%s",
                        run_type,
                        region_id,
                        state,
                        county,
                        pollutant_code,
                        window["bdate"],
                        window["edate"],
                    )
            if split_successes == 0 and split_errors:
                raise split_errors[-1]
            return result
        raise


def _is_request_timeout_error(exc: Exception) -> bool:
    if isinstance(exc, requests.exceptions.Timeout):
        return True
    if isinstance(exc, requests.exceptions.ConnectionError):
        lowered = str(exc).strip().lower()
        return "timed out" in lowered
    return False


def _run_aqs_metadata_extract(
    *,
    client: AQSClient,
    regions: list[dict[str, Any]],
    run_type: str,
) -> None:
    logger.info("AQS metadata phase started: run_type=%s", run_type)
    parameter_classes_payload = client.get_parameter_classes()
    write_raw_object(
        dataset="aqs_metadata_json",
        payload=parameter_classes_payload,
        source_url=_build_aqs_source_url(client, "list/classes", {}),
        metadata={"run_type": run_type, "endpoint": "list/classes"},
    )
    logger.info("%s endpoint=list/classes", _progress_bar("metadata:classes", 1, 1))

    class_rows = parameter_classes_payload.get("Data", [])
    class_codes = []
    for row in class_rows:
        if not isinstance(row, dict):
            continue
        class_code = str(
            row.get("code")
            or row.get("class")
            or row.get("value_represented")
            or ""
        ).strip()
        if class_code:
            class_codes.append(class_code)

    unique_class_codes = sorted(set(class_codes))
    for idx, class_code in enumerate(unique_class_codes, start=1):
        endpoint = "list/parametersByClass"
        params = {"pc": class_code}
        payload = client.get_parameters_by_class(class_code)
        write_raw_object(
            dataset="aqs_metadata_json",
            payload=payload,
            source_url=_build_aqs_source_url(client, endpoint, params),
            metadata={
                "run_type": run_type,
                "endpoint": endpoint,
                "parameter_class": class_code,
                "query_params": params,
            },
        )
        logger.info(
            "%s endpoint=%s class=%s",
            _progress_bar("metadata:parametersByClass", idx, len(unique_class_codes)),
            endpoint,
            class_code,
        )

    state_codes = sorted(
        {
            state
            for region in regions
            for state in _iter_region_state_codes(region)
        }
    )
    if not state_codes:
        states_payload = client.get_states()
        write_raw_object(
            dataset="aqs_metadata_json",
            payload=states_payload,
            source_url=_build_aqs_source_url(client, "list/states", {}),
            metadata={"run_type": run_type, "endpoint": "list/states"},
        )
        state_rows = states_payload.get("Data", [])
        state_codes = [str(row.get("code", "")).strip() for row in state_rows if isinstance(row, dict)]
        state_codes = [code for code in state_codes if code]
        logger.info("%s endpoint=list/states states=%s", _progress_bar("metadata:states", 1, 1), len(state_codes))

    configured_counties_by_state = _configured_counties_by_state(regions)
    total_site_requests = 0
    for idx, state in enumerate(state_codes, start=1):
        counties_endpoint = "list/countiesByState"
        counties_params = {"state": state}
        counties_payload = client.get_counties_by_state(state)
        write_raw_object(
            dataset="aqs_metadata_json",
            payload=counties_payload,
            source_url=_build_aqs_source_url(client, counties_endpoint, counties_params),
            metadata={
                "run_type": run_type,
                "endpoint": counties_endpoint,
                "state": state,
                "query_params": counties_params,
            },
        )

        county_rows = counties_payload.get("Data", [])
        county_codes = [str(row.get("code", "")).strip() for row in county_rows if isinstance(row, dict)]
        county_codes = [code for code in county_codes if code]
        configured_for_state = configured_counties_by_state.get(state, set())
        if configured_for_state:
            target_counties = [code for code in county_codes if code in configured_for_state]
        else:
            target_counties = county_codes
        total_site_requests += len(target_counties)
        logger.info(
            "%s endpoint=%s state=%s counties_total=%s counties_selected=%s",
            _progress_bar("metadata:countiesByState", idx, len(state_codes)),
            counties_endpoint,
            state,
            len(county_codes),
            len(target_counties),
        )
        for county in target_counties:
            sites_endpoint = "list/sitesByCounty"
            sites_params = {"state": state, "county": county}
            sites_payload = client.get_sites_by_county(state=state, county=county)
            write_raw_object(
                dataset="aqs_metadata_json",
                payload=sites_payload,
                source_url=_build_aqs_source_url(client, sites_endpoint, sites_params),
                metadata={
                    "run_type": run_type,
                    "endpoint": sites_endpoint,
                    "state": state,
                    "county": county,
                    "query_params": sites_params,
                },
            )
            logger.info(
                "metadata sites progress: state=%s county=%s",
                state,
                county,
            )

    logger.info(
        "AQS metadata extraction complete for run_type=%s classes=%s states=%s planned_sites=%s",
        run_type,
        len(unique_class_codes),
        len(state_codes),
        total_site_requests,
    )


def _run_aqs_monitor_metadata_extract(
    *,
    client: AQSClient,
    regions: list[dict[str, Any]],
    pollutant_codes: list[str],
    run_type: str,
    extracted_at: datetime,
) -> None:
    bdate = extracted_at.replace(month=1, day=1).strftime("%Y%m%d")
    edate = extracted_at.strftime("%Y%m%d")

    total_written = 0
    batch_size = _resolve_shard_param_batch_size(max_allowed=client.MAX_PARAMS_PER_REQUEST)
    pollutant_batches = _chunk_pollutant_codes(pollutant_codes, max_batch_size=batch_size)
    total_targets = sum(len(_iter_region_state_codes(region)) for region in regions) * len(pollutant_batches)
    completed_targets = 0
    geography_mode = _resolve_aqs_geography_mode()
    counties_by_state: dict[str, list[str]] = {}
    logger.info(
        "AQS monitor metadata phase started: run_type=%s total_targets=%s pollutant_batches=%s geography=%s",
        run_type,
        total_targets,
        len(pollutant_batches),
        geography_mode,
    )
    for region in regions:
        region_id = str(region.get("id", "unknown"))
        region_states = _iter_region_state_codes(region)
        if not region_states:
            logger.warning("Region has no state codes for monitor metadata. Skipping region_id=%s", region_id)
            continue
        for state in region_states:
            state_counties = _resolve_state_counties(
                client=client,
                regions=regions,
                state=state,
                geography_mode=geography_mode,
                cache=counties_by_state,
            )
            for pollutant_batch in pollutant_batches:
                param_csv = ",".join(pollutant_batch)
                shard_geography = _resolve_monitor_metadata_geography(
                    base_mode=geography_mode,
                    state=state,
                    counties=state_counties,
                    pollutant_batch=pollutant_batch,
                    bdate=bdate,
                    edate=edate,
                )
                if shard_geography == "county":
                    for county in state_counties:
                        params = {
                            "state": state,
                            "county": county,
                            "param": param_csv,
                            "bdate": bdate,
                            "edate": edate,
                        }
                        endpoint = "monitors/byCounty"
                        payload = client.get_monitors_by_county(
                            state=state,
                            county=county,
                            bdate=bdate,
                            edate=edate,
                            param=pollutant_batch,
                        )
                        write_raw_object(
                            dataset="aqs_monitor_metadata_json",
                            payload=payload,
                            source_url=_build_aqs_source_url(client, endpoint, params),
                            metadata={
                                "run_type": run_type,
                                "endpoint": endpoint,
                                "region_id": region_id,
                                "state": state,
                                "county": county,
                                "pollutant_codes": pollutant_batch,
                                "pollutant_code": pollutant_batch[0] if len(pollutant_batch) == 1 else None,
                                "query_params": params,
                            },
                        )
                        total_written += 1
                else:
                    params = {
                        "state": state,
                        "param": param_csv,
                        "bdate": bdate,
                        "edate": edate,
                    }
                    endpoint = "monitors/byState"
                    payload = client.get_monitors_by_state(
                        state=state,
                        bdate=bdate,
                        edate=edate,
                        param=pollutant_batch,
                    )
                    write_raw_object(
                        dataset="aqs_monitor_metadata_json",
                        payload=payload,
                        source_url=_build_aqs_source_url(client, endpoint, params),
                        metadata={
                            "run_type": run_type,
                            "endpoint": endpoint,
                            "region_id": region_id,
                            "state": state,
                            "county": None,
                            "pollutant_codes": pollutant_batch,
                            "pollutant_code": pollutant_batch[0] if len(pollutant_batch) == 1 else None,
                            "query_params": params,
                        },
                    )
                    total_written += 1
                completed_targets += 1
                logger.info(
                    "%s run_type=%s region_id=%s state=%s param=%s written=%s geography=%s",
                    _progress_bar("metadata:monitorsByState", completed_targets, total_targets),
                    run_type,
                    region_id,
                    state,
                    param_csv,
                    total_written,
                    shard_geography,
                )

    logger.info("AQS monitor metadata extraction complete: run_type=%s written=%s", run_type, total_written)


def _load_pollutant_codes() -> list[str]:
    override = os.getenv("AQS_BOOTSTRAP_PARAMETERS")
    if override:
        values = [value.strip() for value in override.split(",")]
        return [value for value in values if value]

    with POLLUTANTS_CONFIG.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    pollutants = payload.get("pollutants", [])
    codes = [str(row.get("code", "")).strip() for row in pollutants if isinstance(row, dict)]
    return [code for code in codes if code]


def _load_regions() -> list[dict[str, Any]]:
    with REGIONS_CONFIG.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    regions = payload.get("regions", [])
    requested_region_ids = {
        region_id.strip()
        for region_id in os.getenv("AQS_BOOTSTRAP_REGION_IDS", "").split(",")
        if region_id.strip()
    }
    if not requested_region_ids:
        return [region for region in regions if isinstance(region, dict)]

    selected = [
        region
        for region in regions
        if isinstance(region, dict) and str(region.get("id", "")).strip() in requested_region_ids
    ]
    if selected:
        return selected
    raise ValueError("No regions matched AQS_BOOTSTRAP_REGION_IDS.")


def _iter_region_state_codes(region: dict[str, Any]) -> list[str]:
    states = [str(value).strip().zfill(2) for value in region.get("state_codes", []) if str(value).strip()]
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


def _chunk_pollutant_codes(pollutant_codes: list[str], *, max_batch_size: int) -> list[list[str]]:
    if max_batch_size <= 0:
        raise ValueError("max_batch_size must be greater than 0.")
    codes = [str(code).strip() for code in pollutant_codes if str(code).strip()]
    if not codes:
        raise ValueError("No AQS pollutant codes configured.")
    return [codes[i : i + max_batch_size] for i in range(0, len(codes), max_batch_size)]


def _resolve_shard_param_batch_size(*, max_allowed: int) -> int:
    raw = os.getenv("AQS_SHARD_PARAMS_PER_REQUEST", "").strip()
    if not raw:
        return max_allowed

    try:
        configured = int(raw)
    except ValueError:
        logger.warning(
            "Invalid AQS_SHARD_PARAMS_PER_REQUEST=%s, using max_allowed=%s",
            raw,
            max_allowed,
        )
        return max_allowed

    if configured < 1:
        logger.warning(
            "AQS_SHARD_PARAMS_PER_REQUEST=%s is less than 1, forcing to 1",
            configured,
        )
        return 1
    if configured > max_allowed:
        logger.warning(
            "AQS_SHARD_PARAMS_PER_REQUEST=%s exceeds AQS API max=%s, capping to max",
            configured,
            max_allowed,
        )
        return max_allowed
    return configured


def _resolve_aqs_geography_mode() -> str:
    raw = os.getenv("AQS_GEOGRAPHY_MODE", "auto").strip().lower()
    if raw in {"auto", "state", "county"}:
        return raw
    logger.warning("Invalid AQS_GEOGRAPHY_MODE=%s, using auto", raw)
    return "auto"


def _resolve_state_counties(
    *,
    client: AQSClient,
    regions: list[dict[str, Any]],
    state: str,
    geography_mode: str,
    cache: dict[str, list[str]] | None = None,
) -> list[str]:
    if geography_mode == "state":
        return []
    if cache is not None and state in cache:
        return cache[state]

    configured_for_state = _configured_counties_by_state(regions).get(state, set())
    if configured_for_state:
        counties = sorted(configured_for_state)
        if cache is not None:
            cache[state] = counties
        return counties

    payload = client.get_counties_by_state(state)
    rows = payload.get("Data", [])
    counties = [
        str(row.get("code", "")).strip().zfill(3)
        for row in rows
        if isinstance(row, dict) and str(row.get("code", "")).strip()
    ]
    resolved = sorted(dict.fromkeys(counties))
    if cache is not None:
        cache[state] = resolved
    return resolved


def _resolve_shard_geography(
    *,
    base_mode: str,
    state: str,
    counties: list[str],
    pollutant_batch: list[str],
    window: dict[str, str],
) -> str:
    if base_mode in {"state", "county"}:
        return base_mode
    score = _estimate_shard_size_score(counties=counties, pollutant_batch=pollutant_batch, window=window)
    threshold = _resolve_state_to_county_score_threshold()
    if score >= threshold and counties:
        logger.info(
            "AQS shard geography auto-selected county shard: state=%s counties=%s pollutants=%s bdate=%s edate=%s score=%s threshold=%s",
            state,
            len(counties),
            len(pollutant_batch),
            window["bdate"],
            window["edate"],
            score,
            threshold,
        )
        return "county"
    logger.info(
        "AQS shard geography auto-selected state: state=%s counties=%s pollutants=%s bdate=%s edate=%s score=%s threshold=%s",
        state,
        len(counties),
        len(pollutant_batch),
        window["bdate"],
        window["edate"],
        score,
        threshold,
    )
    return "state"


def _resolve_monitor_metadata_geography(
    *,
    base_mode: str,
    state: str,
    counties: list[str],
    pollutant_batch: list[str],
    bdate: str,
    edate: str,
) -> str:
    if base_mode in {"state", "county"}:
        return base_mode
    window = {"year": bdate[:4], "bdate": bdate, "edate": edate}
    score = _estimate_shard_size_score(counties=counties, pollutant_batch=pollutant_batch, window=window)
    threshold = _resolve_state_to_county_score_threshold()
    if score >= threshold and counties:
        logger.info(
            "AQS monitor geography auto-selected county shard: state=%s counties=%s pollutants=%s bdate=%s edate=%s score=%s threshold=%s",
            state,
            len(counties),
            len(pollutant_batch),
            bdate,
            edate,
            score,
            threshold,
        )
        return "county"
    logger.info(
        "AQS monitor geography auto-selected state: state=%s counties=%s pollutants=%s bdate=%s edate=%s score=%s threshold=%s",
        state,
        len(counties),
        len(pollutant_batch),
        bdate,
        edate,
        score,
        threshold,
    )
    return "state"


def _estimate_shard_size_score(*, counties: list[str], pollutant_batch: list[str], window: dict[str, str]) -> int:
    return max(len(counties), 1) * max(len(pollutant_batch), 1) * _window_span_days(window)


def _window_span_days(window: dict[str, str]) -> int:
    start = datetime.strptime(window["bdate"], "%Y%m%d").date()
    end = datetime.strptime(window["edate"], "%Y%m%d").date()
    return (end - start).days + 1


def _resolve_state_to_county_score_threshold() -> int:
    raw = os.getenv("AQS_STATE_TO_COUNTY_SCORE_THRESHOLD", "8000").strip()
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Invalid AQS_STATE_TO_COUNTY_SCORE_THRESHOLD=%s, using 8000", raw)
        return 8000
    if value < 1:
        logger.warning("AQS_STATE_TO_COUNTY_SCORE_THRESHOLD=%s is less than 1, forcing to 1", value)
        return 1
    return value


def _configured_counties_by_state(regions: list[dict[str, Any]]) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    for region in regions:
        state_codes = [str(value).strip().zfill(2) for value in region.get("state_codes", []) if str(value).strip()]
        county_codes = [str(value).strip().zfill(3) for value in region.get("county_codes", []) if str(value).strip()]
        for state in state_codes:
            if county_codes:
                mapping.setdefault(state, set()).update(county_codes)

        explicit_pairs = region.get("state_county_pairs")
        if isinstance(explicit_pairs, list):
            for pair in explicit_pairs:
                if isinstance(pair, dict):
                    state = str(pair.get("state", "")).strip().zfill(2)
                    county = str(pair.get("county", "")).strip().zfill(3)
                elif isinstance(pair, str):
                    raw_state, _, raw_county = pair.partition(":")
                    state = raw_state.strip().zfill(2)
                    county = raw_county.strip().zfill(3)
                else:
                    continue
                if state and county:
                    mapping.setdefault(state, set()).add(county)
    return mapping


def _log_state_county_coverage(*, client: AQSClient, regions: list[dict[str, Any]], run_type: str) -> None:
    states = sorted(
        {
            state
            for region in regions
            for state in _iter_region_state_codes(region)
        }
    )
    if not states:
        logger.warning("Skip AQS state-county coverage log: no states configured for run_type=%s", run_type)
        return

    for state in states:
        try:
            payload = client.get_counties_by_state(state)
            rows = payload.get("Data", [])
            counties = sorted(
                {
                    str(row.get("code", "")).strip().zfill(3)
                    for row in rows
                    if isinstance(row, dict) and str(row.get("code", "")).strip()
                }
            )
            logger.info(
                "AQS state county coverage: run_type=%s state=%s county_count=%s counties=%s",
                run_type,
                state,
                len(counties),
                ",".join(counties),
            )
        except Exception:
            logger.exception(
                "Failed to fetch countiesByState for coverage log: run_type=%s state=%s",
                run_type,
                state,
            )


def _build_year_windows(*, start_year: int, end_date: date) -> list[dict[str, str]]:
    windows: list[dict[str, str]] = []
    for year in range(start_year, end_date.year + 1):
        year_start = date(year=year, month=1, day=1)
        year_end = date(year=year, month=12, day=31)
        if year == end_date.year:
            year_end = end_date
        windows.append(
            {
                "year": str(year),
                "bdate": year_start.strftime("%Y%m%d"),
                "edate": year_end.strftime("%Y%m%d"),
            }
        )
    return windows


def _resolve_bootstrap_windows(*, now_utc: datetime) -> list[dict[str, str]]:
    bdate_override = os.getenv("AQS_BOOTSTRAP_BDATE", "").strip()
    edate_override = os.getenv("AQS_BOOTSTRAP_EDATE", "").strip()
    granularity = os.getenv("AQS_BOOTSTRAP_WINDOW_GRANULARITY", "year").strip().lower()
    if granularity not in {"year", "month"}:
        logger.warning("Unsupported AQS_BOOTSTRAP_WINDOW_GRANULARITY=%s, fallback to year", granularity)
        granularity = "year"

    if bool(bdate_override) ^ bool(edate_override):
        raise ValueError("AQS_BOOTSTRAP_BDATE and AQS_BOOTSTRAP_EDATE must both be set or both be empty.")

    if bdate_override and edate_override:
        start = datetime.strptime(bdate_override, "%Y%m%d").date()
        end = datetime.strptime(edate_override, "%Y%m%d").date()
        if start > end:
            raise ValueError(f"AQS bootstrap bdate {bdate_override} cannot be after edate {edate_override}")
        if granularity == "month":
            windows = _split_date_range_by_month(start=start, end=end)
        else:
            windows = _split_date_range_by_year(start=start, end=end)
        logger.info(
            "Bootstrap window override enabled: bdate=%s edate=%s granularity=%s windows=%s",
            bdate_override,
            edate_override,
            granularity,
            len(windows),
        )
        return windows

    start_year = int(os.getenv("AQS_BOOTSTRAP_START_YEAR", str(now_utc.year)))
    default_start = date(year=start_year, month=1, day=1)
    default_end = now_utc.date()
    if granularity == "month":
        return _split_date_range_by_month(start=default_start, end=default_end)
    return _build_year_windows(start_year=start_year, end_date=default_end)


def _split_date_range_by_year(*, start: date, end: date) -> list[dict[str, str]]:
    windows: list[dict[str, str]] = []
    for year in range(start.year, end.year + 1):
        year_start = date(year=year, month=1, day=1)
        year_end = date(year=year, month=12, day=31)
        if year == start.year:
            year_start = start
        if year == end.year:
            year_end = end
        windows.append(
            {
                "year": str(year),
                "bdate": year_start.strftime("%Y%m%d"),
                "edate": year_end.strftime("%Y%m%d"),
            }
        )
    return windows


def _split_date_range_by_month(*, start: date, end: date) -> list[dict[str, str]]:
    windows: list[dict[str, str]] = []
    current = date(year=start.year, month=start.month, day=1)
    while current <= end:
        month_start = current
        if month_start.year == start.year and month_start.month == start.month:
            month_start = start

        if current.month == 12:
            next_month = date(year=current.year + 1, month=1, day=1)
        else:
            next_month = date(year=current.year, month=current.month + 1, day=1)
        month_end = next_month - timedelta(days=1)
        if month_end > end:
            month_end = end

        windows.append(
            {
                "year": str(month_start.year),
                "month": f"{month_start.month:02d}",
                "bdate": month_start.strftime("%Y%m%d"),
                "edate": month_end.strftime("%Y%m%d"),
            }
        )
        current = next_month
    return windows


def _split_window_on_timeout(window: dict[str, str]) -> list[dict[str, str]]:
    start = datetime.strptime(window["bdate"], "%Y%m%d").date()
    end = datetime.strptime(window["edate"], "%Y%m%d").date()
    if start >= end:
        return []

    midpoint = start + timedelta(days=(end - start).days // 2)
    first = {
        "year": str(start.year),
        "bdate": start.strftime("%Y%m%d"),
        "edate": midpoint.strftime("%Y%m%d"),
    }
    second_start = midpoint + timedelta(days=1)
    second = {
        "year": str(second_start.year),
        "bdate": second_start.strftime("%Y%m%d"),
        "edate": end.strftime("%Y%m%d"),
    }
    return [first, second]


def _resolve_reconcile_change_window(*, now_utc: datetime, pg_hook: PostgresHook) -> tuple[str, str]:
    cbdate = os.getenv("AQS_RECONCILE_CBDATE")
    cedate = os.getenv("AQS_RECONCILE_CEDATE")
    if cbdate and cedate:
        return cbdate, cedate

    checkpoint = _get_source_watermark(
        pg_hook=pg_hook,
        source_name="AQS",
        watermark_key="reconcile_last_cedate",
    )
    overlap_days = max(0, int(os.getenv("AQS_RECONCILE_CHECKPOINT_OVERLAP_DAYS", "1")))
    if checkpoint:
        checkpoint_date = _parse_watermark_date(checkpoint)
        if checkpoint_date:
            resolved_cbdate = (checkpoint_date - timedelta(days=overlap_days)).strftime("%Y%m%d")
        else:
            default_lookback_days = max(1, int(os.getenv("AQS_RECONCILE_DEFAULT_LOOKBACK_DAYS", "2")))
            resolved_cbdate = (now_utc - timedelta(days=default_lookback_days)).strftime("%Y%m%d")
    else:
        default_lookback_days = max(1, int(os.getenv("AQS_RECONCILE_DEFAULT_LOOKBACK_DAYS", "2")))
        resolved_cbdate = (now_utc - timedelta(days=default_lookback_days)).strftime("%Y%m%d")
    resolved_cedate = now_utc.strftime("%Y%m%d")
    return resolved_cbdate, resolved_cedate


def _split_change_window_by_year(*, cbdate: str, cedate: str) -> list[dict[str, str]]:
    start = datetime.strptime(cbdate, "%Y%m%d").date()
    end = datetime.strptime(cedate, "%Y%m%d").date()
    if start > end:
        raise ValueError(f"cbdate {cbdate} cannot be after cedate {cedate}")
    windows: list[dict[str, str]] = []
    for year in range(start.year, end.year + 1):
        year_start = date(year=year, month=1, day=1)
        year_end = date(year=year, month=12, day=31)
        if year == start.year:
            year_start = start
        if year == end.year:
            year_end = end
        windows.append(
            {
                "year": str(year),
                "bdate": year_start.strftime("%Y%m%d"),
                "edate": year_end.strftime("%Y%m%d"),
            }
        )
    return windows


def _build_aqs_source_url(client: AQSClient, endpoint: str, params: dict[str, str]) -> str:
    query = {
        "email": client.email,
        "key": client.api_key,
        **params,
    }
    return f"{client.BASE_URL}/{endpoint}?{urlencode(query)}"


def _get_source_watermark(
    *,
    pg_hook: PostgresHook,
    source_name: str,
    watermark_key: str,
) -> str | None:
    row = pg_hook.get_first(
        sql="""
            SELECT watermark_value
            FROM ops.source_watermark
            WHERE source_name = %s
              AND watermark_key = %s
            LIMIT 1
        """,
        parameters=(source_name, watermark_key),
    )
    if not row:
        return None
    value = row[0]
    return str(value) if value is not None else None


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


def _get_pg_hook() -> PostgresHook:
    pg_conn_id = os.getenv("AIRFLOW_CONN_POSTGRES_ID", "aq_postgres")
    return PostgresHook(postgres_conn_id=pg_conn_id)


def _parse_watermark_date(value: str) -> date | None:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _is_enabled(env_name: str, *, default: bool) -> bool:
    raw = os.getenv(env_name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _progress_bar(stage: str, current: int, total: int, width: int = 20) -> str:
    safe_total = max(total, 1)
    safe_current = min(max(current, 0), safe_total)
    filled = int((safe_current / safe_total) * width)
    bar = "#" * filled + "-" * (width - filled)
    percent = (safe_current / safe_total) * 100
    return f"[{stage}] |{bar}| {safe_current}/{safe_total} ({percent:5.1f}%)"
