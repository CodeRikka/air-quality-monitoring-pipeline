from fastapi import APIRouter, HTTPException, Query, Request
from psycopg2 import Error as PsycopgError

from app.db import fetch_all
from app.schemas import MapLatestPointPage


router = APIRouter(prefix="/map", tags=["map"])


@router.get("/latest", response_model=MapLatestPointPage)
def map_latest(
    request: Request,
    pollutant_code: str | None = Query(default=None, min_length=5, max_length=10),
    state_code: str | None = Query(default=None, min_length=2, max_length=8),
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> MapLatestPointPage:
    sql_query = """
        SELECT
            natural_key,
            location_id,
            latitude,
            longitude,
            pollutant_code,
            pollutant_name,
            value_numeric,
            unit,
            timestamp_start_utc,
            data_status,
            source_system,
            is_exceedance
        FROM mart.v_latest_station_observation
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND (%s IS NULL OR pollutant_code = %s)
          AND (%s IS NULL OR state_code = %s)
        ORDER BY timestamp_start_utc DESC, pollutant_code, location_id
        LIMIT %s OFFSET %s
    """
    params = (pollutant_code, pollutant_code, state_code, state_code, limit, offset)
    try:
        items = fetch_all(request.app.state.db_pool, sql_query, params)
    except PsycopgError as exc:
        raise HTTPException(status_code=503, detail="Database query failed for /map/latest.") from exc
    return MapLatestPointPage(items=items, limit=limit, offset=offset, count=len(items))
