from datetime import datetime

from pydantic import BaseModel


class LatestObservation(BaseModel):
    natural_key: str
    location_id: str
    state_code: str | None = None
    county_code: str | None = None
    site_name: str | None = None
    city_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    pollutant_code: str
    pollutant_name: str
    value_numeric: float
    unit: str
    timestamp_start_utc: datetime
    source_system: str
    data_status: str
    threshold_type: str | None = None
    threshold_value: float | None = None
    is_exceedance: bool | None = None


class LatestObservationPage(BaseModel):
    items: list[LatestObservation]
    limit: int
    offset: int
    count: int


class MapLatestPoint(BaseModel):
    natural_key: str
    location_id: str
    latitude: float
    longitude: float
    pollutant_code: str
    pollutant_name: str
    value_numeric: float
    unit: str
    timestamp_start_utc: datetime
    data_status: str
    source_system: str
    is_exceedance: bool | None = None


class MapLatestPointPage(BaseModel):
    items: list[MapLatestPoint]
    limit: int
    offset: int
    count: int


class TimeSeriesPoint(BaseModel):
    location_id: str
    pollutant_code: str
    pollutant_name: str
    day_utc: datetime
    avg_value: float
    min_value: float
    max_value: float
    observation_count: int
    final_count: int
    provisional_count: int


class TimeSeriesPage(BaseModel):
    items: list[TimeSeriesPoint]
    limit: int
    offset: int
    count: int


class CoveragePoint(BaseModel):
    state_code: str | None = None
    county_code: str | None = None
    pollutant_code: str
    pollutant_name: str
    day_utc: datetime
    observation_count: int
    reporting_station_count: int
    total_station_count: int
    station_coverage_ratio: float | None = None


class CoveragePage(BaseModel):
    items: list[CoveragePoint]
    limit: int
    offset: int
    count: int
