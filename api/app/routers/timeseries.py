from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request
from psycopg2 import Error as PsycopgError

from app.db import fetch_all
from app.schemas import TimeSeriesPage


router = APIRouter(prefix="/timeseries", tags=["timeseries"])


@router.get("", response_model=TimeSeriesPage)
def list_timeseries(
    request: Request,
    location_id: str = Query(..., min_length=3, max_length=128),
    pollutant_code: str = Query(..., min_length=5, max_length=10),
    start_day_utc: datetime | None = Query(default=None),
    end_day_utc: datetime | None = Query(default=None),
    limit: int = Query(default=365, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
) -> TimeSeriesPage:
    if start_day_utc and end_day_utc and start_day_utc > end_day_utc:
        raise HTTPException(status_code=422, detail="start_day_utc must be earlier than or equal to end_day_utc.")

    sql_query = """
        SELECT
            location_id,
            pollutant_code,
            pollutant_name,
            day_utc,
            avg_value,
            min_value,
            max_value,
            observation_count,
            final_count,
            provisional_count
        FROM mart.v_daily_trend
        WHERE location_id = %s
          AND pollutant_code = %s
          AND (%s IS NULL OR day_utc >= %s)
          AND (%s IS NULL OR day_utc <= %s)
        ORDER BY day_utc ASC
        LIMIT %s OFFSET %s
    """
    params = (
        location_id,
        pollutant_code,
        start_day_utc,
        start_day_utc,
        end_day_utc,
        end_day_utc,
        limit,
        offset,
    )
    try:
        items = fetch_all(request.app.state.db_pool, sql_query, params)
    except PsycopgError as exc:
        raise HTTPException(status_code=503, detail="Database query failed for /timeseries.") from exc
    return TimeSeriesPage(items=items, limit=limit, offset=offset, count=len(items))
