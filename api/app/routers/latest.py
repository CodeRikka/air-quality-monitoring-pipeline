from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request
from psycopg2 import Error as PsycopgError

from app.db import fetch_all
from app.schemas import LatestObservationPage


router = APIRouter(prefix="/latest", tags=["latest"])


@router.get("/health")
def health():
    return {"status": "ok"}


@router.get("", response_model=LatestObservationPage)
def list_latest(
    request: Request,
    pollutant_code: str | None = Query(default=None, min_length=5, max_length=10),
    state_code: str | None = Query(default=None, min_length=2, max_length=8),
    county_code: str | None = Query(default=None, min_length=1, max_length=8),
    updated_after_utc: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> LatestObservationPage:
    sql_query = """
        SELECT
            natural_key,
            location_id,
            state_code,
            county_code,
            site_name,
            city_name,
            latitude,
            longitude,
            pollutant_code,
            pollutant_name,
            value_numeric,
            unit,
            timestamp_start_utc,
            source_system,
            data_status,
            threshold_type,
            threshold_value,
            is_exceedance
        FROM mart.v_latest_station_observation
        WHERE (%s IS NULL OR pollutant_code = %s)
          AND (%s IS NULL OR state_code = %s)
          AND (%s IS NULL OR county_code = %s)
          AND (%s IS NULL OR timestamp_start_utc >= %s)
        ORDER BY timestamp_start_utc DESC, pollutant_code, location_id
        LIMIT %s OFFSET %s
    """
    params = (
        pollutant_code,
        pollutant_code,
        state_code,
        state_code,
        county_code,
        county_code,
        updated_after_utc,
        updated_after_utc,
        limit,
        offset,
    )
    try:
        items = fetch_all(request.app.state.db_pool, sql_query, params)
    except PsycopgError as exc:
        raise HTTPException(status_code=503, detail="Database query failed for /latest.") from exc
    return LatestObservationPage(items=items, limit=limit, offset=offset, count=len(items))
