from fastapi import APIRouter, HTTPException, Query, Request
from psycopg2 import Error as PsycopgError

from app.db import fetch_all
from app.schemas import MapLatestPointPage


router = APIRouter(prefix="/map", tags=["map"])
ALLOWED_SOURCE_SYSTEMS = {"AQS", "AIRNOW"}
ALLOWED_GRANULARITIES = {"daily", "hourly", "sample"}


@router.get("/latest", response_model=MapLatestPointPage)
def map_latest(
    request: Request,
    pollutant_code: str | None = Query(default=None, min_length=5, max_length=10),
    state_code: str | None = Query(default=None, min_length=2, max_length=8),
    source_systems: str | None = Query(default=None, description="Comma-separated source systems, e.g. AQS,AIRNOW"),
    data_granularities: str | None = Query(default=None, description="Comma-separated granularities, e.g. daily,hourly"),
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> MapLatestPointPage:
    parsed_source_systems = _parse_csv_filter(
        raw_value=source_systems,
        allowed_values=ALLOWED_SOURCE_SYSTEMS,
        param_name="source_systems",
        normalize=str.upper,
    )
    parsed_granularities = _parse_csv_filter(
        raw_value=data_granularities,
        allowed_values=ALLOWED_GRANULARITIES,
        param_name="data_granularities",
        normalize=str.lower,
    )
    sql_query = """
        WITH ranked AS (
            SELECT
                h.natural_key,
                h.location_id,
                l.state_code,
                l.county_code,
                l.site_name,
                l.city_name,
                l.latitude,
                l.longitude,
                h.pollutant_code,
                p.pollutant_name,
                h.value_numeric,
                h.unit,
                h.timestamp_start_utc,
                h.data_granularity,
                h.data_status,
                h.source_system,
                h.source_dataset,
                (h.value_numeric > t.threshold_value) AS is_exceedance,
                ROW_NUMBER() OVER (
                    PARTITION BY h.location_id, h.pollutant_code, h.source_system, h.data_granularity
                    ORDER BY h.timestamp_start_utc DESC, h.ingested_at DESC, h.observation_history_id DESC
                ) AS rn
            FROM core.fact_observation_history h
            JOIN core.dim_location l ON h.location_id = l.location_id
            JOIN core.dim_pollutant p ON h.pollutant_code = p.pollutant_code
            LEFT JOIN core.dim_exceedance_threshold t
              ON h.pollutant_code = t.pollutant_code
             AND h.unit = t.unit
             AND t.is_active = TRUE
            WHERE l.latitude IS NOT NULL
              AND l.longitude IS NOT NULL
              AND (%s IS NULL OR h.pollutant_code = %s)
              AND (%s IS NULL OR l.state_code = %s)
              AND (%s IS NULL OR h.source_system = ANY(%s))
              AND (%s IS NULL OR h.data_granularity = ANY(%s))
        )
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
            data_granularity,
            data_status,
            source_system,
            source_dataset,
            is_exceedance
        FROM ranked
        WHERE rn = 1
        ORDER BY timestamp_start_utc DESC, pollutant_code, location_id, source_system, data_granularity
        LIMIT %s OFFSET %s
    """
    params = (
        pollutant_code,
        pollutant_code,
        state_code,
        state_code,
        parsed_source_systems,
        parsed_source_systems,
        parsed_granularities,
        parsed_granularities,
        limit,
        offset,
    )
    try:
        items = fetch_all(request.app.state.db_pool, sql_query, params)
    except PsycopgError as exc:
        raise HTTPException(status_code=503, detail="Database query failed for /map/latest.") from exc
    return MapLatestPointPage(items=items, limit=limit, offset=offset, count=len(items))


def _parse_csv_filter(
    *,
    raw_value: str | None,
    allowed_values: set[str],
    param_name: str,
    normalize,
) -> list[str] | None:
    if raw_value is None or not raw_value.strip():
        return None
    parsed_values = [normalize(value.strip()) for value in raw_value.split(",") if value.strip()]
    invalid_values = [value for value in parsed_values if value not in allowed_values]
    if invalid_values:
        allowed_text = ", ".join(sorted(allowed_values))
        invalid_text = ", ".join(invalid_values)
        raise HTTPException(
            status_code=422,
            detail=f"Invalid {param_name}: {invalid_text}. Allowed values: {allowed_text}.",
        )
    return list(dict.fromkeys(parsed_values))
