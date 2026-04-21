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


st.sidebar.header("Filters")
state_code = st.sidebar.text_input("Region: state_code", value="").strip().upper()
county_code = st.sidebar.text_input("Region: county_code", value="").strip()
pollutant_code = st.sidebar.text_input("Pollutant code", value="").strip()
time_window_days = st.sidebar.selectbox("Time window (days)", options=[1, 3, 7, 14, 30, 90], index=4)

now_utc = datetime.now(timezone.utc)
window_start = now_utc - timedelta(days=int(time_window_days))
start_iso = _to_iso(window_start)
end_iso = _to_iso(now_utc)

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
        "Latest Map",
        "Station Time Series",
        "Regional Trend",
        "Data Completeness",
    ]
)

with tab_map:
    st.subheader("Latest Map")
    map_payload = fetch_page(
        "/map/latest",
        {
            "state_code": state_code or None,
            "pollutant_code": pollutant_code or None,
            "limit": 2000,
            "offset": 0,
        },
    )
    map_items = map_payload.get("items", [])
    map_df = pd.DataFrame(map_items)
    st.caption(f"Records: {len(map_df)}")
    if not map_df.empty:
        if "is_exceedance" in map_df.columns:
            st.metric("Exceedance points", int(map_df["is_exceedance"].fillna(False).sum()))
        st.dataframe(
            map_df[
                [
                    "location_id",
                    "pollutant_code",
                    "value_numeric",
                    "unit",
                    "timestamp_start_utc",
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
    st.subheader("Single-Station Time Series")
    station_location_id = st.text_input("Location ID", value="", placeholder="aqs:06:075:0010:1")
    station_pollutant_code = st.text_input(
        "Station pollutant code",
        value=pollutant_code,
        placeholder="88101",
    ).strip()
    if station_location_id and station_pollutant_code:
        ts_payload = fetch_page(
            "/timeseries",
            {
                "location_id": station_location_id.strip(),
                "pollutant_code": station_pollutant_code,
                "start_day_utc": start_iso,
                "end_day_utc": end_iso,
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
    region_payload = fetch_page(
        "/quality/coverage",
        {
            "state_code": state_code or None,
            "county_code": county_code or None,
            "pollutant_code": pollutant_code or None,
            "start_day_utc": start_iso,
            "end_day_utc": end_iso,
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
    min_coverage_ratio = st.slider("Minimum coverage ratio", min_value=0.0, max_value=1.0, value=0.0, step=0.05)
    quality_payload = fetch_page(
        "/quality/coverage",
        {
            "state_code": state_code or None,
            "county_code": county_code or None,
            "pollutant_code": pollutant_code or None,
            "start_day_utc": start_iso,
            "end_day_utc": end_iso,
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
