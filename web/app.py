import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
import streamlit as st


API_BASE = os.environ.get("AQ_API_BASE_URL", "http://aq-api.serving.svc.cluster.local:8080")
API_TIMEOUT_SECONDS = int(os.environ.get("AQ_API_TIMEOUT_SECONDS", "15"))

st.set_page_config(page_title="Air Quality Monitoring", layout="wide")
st.title("Air Quality Monitoring Dashboard")


def _to_iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


@st.cache_data(ttl=120)
def _api_get(path: str, params_items: tuple[tuple[str, Any], ...]) -> dict[str, Any]:
    params = {k: v for k, v in params_items if v is not None and v != ""}
    response = requests.get(
        f"{API_BASE}{path}",
        params=params,
        timeout=API_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        return payload
    return {"items": payload}


def fetch_page(path: str, params: dict[str, Any]) -> dict[str, Any]:
    try:
        return _api_get(path, tuple(sorted(params.items())))
    except requests.RequestException as exc:
        st.error(f"FastAPI request failed for `{path}`: {exc}")
        return {"items": [], "count": 0}


now_utc = datetime.now(timezone.utc)

with st.expander("API health checks", expanded=False):
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Check /healthz"):
            st.json(fetch_page("/healthz", {}))
    with col2:
        if st.button("Check /latest/health"):
            st.json(fetch_page("/latest/health", {}))

tab_map, tab_station, tab_region, tab_quality = st.tabs(
    [
        "Latest Source Map",
        "Station Daily Time Series",
        "Regional Trend",
        "Data Completeness",
    ]
)

with tab_map:
    st.subheader("Latest Source Map")
    st.caption("Shows the latest station observation by selected source system and granularity.")
    map_col1, map_col2 = st.columns(2)
    with map_col1:
        map_source_mode = st.selectbox(
            "Source view",
            options=["AQS", "AIRNOW", "AQS & AIRNOW"],
            index=2,
            key="map_source_mode",
        )
        map_state_code = st.text_input("Map state_code", value="", key="map_state_code").strip().upper()
    with map_col2:
        map_granularities = st.multiselect(
            "Granularity",
            options=["daily", "hourly", "sample"],
            default=["daily", "hourly"],
            key="map_granularities",
        )
        map_pollutant_code = st.text_input("Map pollutant code", value="", key="map_pollutant_code").strip()

    map_source_systems = {
        "AQS": "AQS",
        "AIRNOW": "AIRNOW",
        "AQS & AIRNOW": "AQS,AIRNOW",
    }[map_source_mode]
    map_payload = fetch_page(
        "/map/latest",
        {
            "state_code": map_state_code or None,
            "pollutant_code": map_pollutant_code or None,
            "source_systems": map_source_systems,
            "data_granularities": ",".join(map_granularities) if map_granularities else None,
            "limit": 2000,
            "offset": 0,
        },
    )
    map_items = map_payload.get("items", [])
    map_df = pd.DataFrame(map_items)
    st.caption(f"Records: {len(map_df)}")
    if not map_df.empty:
        metric_col1, metric_col2 = st.columns(2)
        with metric_col1:
            st.metric("Exceedance points", int(map_df["is_exceedance"].fillna(False).sum()))
        with metric_col2:
            st.metric(
                "Sources shown",
                ", ".join(sorted(map_df["source_system"].dropna().astype(str).unique())),
            )
        st.dataframe(
            map_df[
                [
                    "location_id",
                    "state_code",
                    "pollutant_code",
                    "value_numeric",
                    "unit",
                    "timestamp_start_utc",
                    "data_granularity",
                    "data_status",
                    "source_system",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
        st.map(map_df.rename(columns={"latitude": "lat", "longitude": "lon"})[["lat", "lon"]])
    else:
        st.info("No map points found for the current filters.")

with tab_station:
    st.subheader("Single-Station Daily Time Series")
    station_window_days = st.selectbox(
        "Time window (days)",
        options=[1, 3, 7, 14, 30, 90],
        index=4,
        key="station_window_days",
    )
    station_start_iso = _to_iso(now_utc - timedelta(days=int(station_window_days)))
    station_end_iso = _to_iso(now_utc)
    station_location_id = st.text_input(
        "Location ID",
        value="",
        placeholder="aqs:06:075:0010:1",
        key="station_location_id",
    )
    station_pollutant_code = st.text_input(
        "Station pollutant code",
        value="",
        placeholder="88101",
        key="station_pollutant_code",
    ).strip()
    if station_location_id and station_pollutant_code:
        ts_payload = fetch_page(
            "/timeseries",
            {
                "location_id": station_location_id.strip(),
                "pollutant_code": station_pollutant_code,
                "start_day_utc": station_start_iso,
                "end_day_utc": station_end_iso,
                "limit": 1000,
                "offset": 0,
            },
        )
        ts_df = pd.DataFrame(ts_payload.get("items", []))
        if not ts_df.empty:
            ts_df["day_utc"] = pd.to_datetime(ts_df["day_utc"], errors="coerce")
            ts_df = ts_df.sort_values("day_utc")
            st.line_chart(ts_df.set_index("day_utc")[["avg_value", "min_value", "max_value"]])
            st.dataframe(ts_df, use_container_width=True, hide_index=True)
        else:
            st.info("No time-series records found for the selected station and pollutant.")
    else:
        st.info("Input both `location_id` and `pollutant_code` to query station time series.")

with tab_region:
    st.subheader("Regional Trend")
    region_col1, region_col2 = st.columns(2)
    with region_col1:
        region_state_code = st.text_input("Region state_code", value="", key="region_state_code").strip().upper()
        region_pollutant_code = st.text_input("Region pollutant code", value="", key="region_pollutant_code").strip()
    with region_col2:
        region_county_code = st.text_input("Region county_code", value="", key="region_county_code").strip()
        region_window_days = st.selectbox(
            "Time window (days)",
            options=[1, 3, 7, 14, 30, 90],
            index=4,
            key="region_window_days",
        )
    region_start_iso = _to_iso(now_utc - timedelta(days=int(region_window_days)))
    region_end_iso = _to_iso(now_utc)
    region_payload = fetch_page(
        "/quality/coverage",
        {
            "state_code": region_state_code or None,
            "county_code": region_county_code or None,
            "pollutant_code": region_pollutant_code or None,
            "start_day_utc": region_start_iso,
            "end_day_utc": region_end_iso,
            "limit": 2000,
            "offset": 0,
        },
    )
    region_df = pd.DataFrame(region_payload.get("items", []))
    if not region_df.empty:
        region_df["day_utc"] = pd.to_datetime(region_df["day_utc"], errors="coerce")
        trend_df = (
            region_df.groupby("day_utc", as_index=False)
            .agg(
                observation_count=("observation_count", "sum"),
                avg_station_coverage_ratio=("station_coverage_ratio", "mean"),
            )
            .sort_values("day_utc")
        )
        st.line_chart(trend_df.set_index("day_utc")[["observation_count"]])
        st.line_chart(trend_df.set_index("day_utc")[["avg_station_coverage_ratio"]])
        st.dataframe(trend_df, use_container_width=True, hide_index=True)
    else:
        st.info("No regional trend records found for the current filters.")

with tab_quality:
    st.subheader("Data Completeness")
    quality_col1, quality_col2 = st.columns(2)
    with quality_col1:
        quality_state_code = st.text_input("Quality state_code", value="", key="quality_state_code").strip().upper()
        quality_pollutant_code = st.text_input("Quality pollutant code", value="", key="quality_pollutant_code").strip()
    with quality_col2:
        quality_county_code = st.text_input("Quality county_code", value="", key="quality_county_code").strip()
        quality_window_days = st.selectbox(
            "Time window (days)",
            options=[1, 3, 7, 14, 30, 90],
            index=4,
            key="quality_window_days",
        )
    quality_start_iso = _to_iso(now_utc - timedelta(days=int(quality_window_days)))
    quality_end_iso = _to_iso(now_utc)
    min_coverage_ratio = st.slider("Minimum coverage ratio", min_value=0.0, max_value=1.0, value=0.0, step=0.05)
    quality_payload = fetch_page(
        "/quality/coverage",
        {
            "state_code": quality_state_code or None,
            "county_code": quality_county_code or None,
            "pollutant_code": quality_pollutant_code or None,
            "start_day_utc": quality_start_iso,
            "end_day_utc": quality_end_iso,
            "min_coverage_ratio": min_coverage_ratio,
            "limit": 2000,
            "offset": 0,
        },
    )
    quality_df = pd.DataFrame(quality_payload.get("items", []))
    if not quality_df.empty:
        quality_df["day_utc"] = pd.to_datetime(quality_df["day_utc"], errors="coerce")
        latest_day = quality_df["day_utc"].max()
        latest_df = quality_df[quality_df["day_utc"] == latest_day]
        st.metric("Latest day avg coverage", f"{latest_df['station_coverage_ratio'].mean():.2%}")
        st.metric("Rows below 70% coverage", int((quality_df["station_coverage_ratio"] < 0.7).sum()))
        st.dataframe(
            quality_df.sort_values(["day_utc", "pollutant_code", "state_code"], ascending=[False, True, True]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No completeness records found for the current filters.")

st.caption("All dashboard queries are served through FastAPI endpoints only.")
