from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request
from psycopg2 import Error as PsycopgError

from app.db import fetch_all
from app.schemas import CoveragePage


router = APIRouter(prefix="/quality", tags=["quality"])


@router.get("/coverage", response_model=CoveragePage)
def quality_coverage(
    request: Request,
    pollutant_code: str | None = Query(default=None, min_length=5, max_length=10),
    state_code: str | None = Query(default=None, min_length=2, max_length=8),
    county_code: str | None = Query(default=None, min_length=1, max_length=8),
    start_day_utc: datetime | None = Query(default=None),
    end_day_utc: datetime | None = Query(default=None),
    min_coverage_ratio: float | None = Query(default=None, ge=0.0, le=1.0),
    limit: int = Query(default=365, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
) -> CoveragePage:
    if start_day_utc and end_day_utc and start_day_utc > end_day_utc:
        raise HTTPException(status_code=422, detail="start_day_utc must be earlier than or equal to end_day_utc.")

    sql_query = """
        SELECT
            state_code,
            county_code,
            pollutant_code,
            pollutant_name,
            day_utc,
            observation_count,
            reporting_station_count,
            total_station_count,
            station_coverage_ratio
        FROM mart.v_region_coverage
        WHERE (%s IS NULL OR pollutant_code = %s)
          AND (%s IS NULL OR state_code = %s)
          AND (%s IS NULL OR county_code = %s)
          AND (%s IS NULL OR day_utc >= %s)
          AND (%s IS NULL OR day_utc <= %s)
          AND (%s IS NULL OR station_coverage_ratio >= %s)
        ORDER BY day_utc DESC, pollutant_code, state_code, county_code
        LIMIT %s OFFSET %s
    """
    params = (
        pollutant_code,
        pollutant_code,
        state_code,
        state_code,
        county_code,
        county_code,
        start_day_utc,
        start_day_utc,
        end_day_utc,
        end_day_utc,
        min_coverage_ratio,
        min_coverage_ratio,
        limit,
        offset,
    )
    try:
        items = fetch_all(request.app.state.db_pool, sql_query, params)
    except PsycopgError as exc:
        raise HTTPException(status_code=503, detail="Database query failed for /quality/coverage.") from exc
    return CoveragePage(items=items, limit=limit, offset=offset, count=len(items))
