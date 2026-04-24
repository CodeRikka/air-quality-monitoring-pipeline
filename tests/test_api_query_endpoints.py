import sys
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover
    TestClient = None  # type: ignore


REPO_ROOT = Path(__file__).resolve().parents[1]
API_ROOT = REPO_ROOT / "api"
if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))

if TestClient is not None:
    from app.main import app  # noqa: E402
else:  # pragma: no cover
    app = None  # type: ignore


@unittest.skipIf(TestClient is None, "fastapi is not installed in this environment")
class ApiQueryEndpointTests(unittest.TestCase):
    def setUp(self):
        self.patches = [
            patch("app.main.init_db_pool", return_value=object()),
            patch("app.main.close_db_pool", return_value=None),
        ]
        for p in self.patches:
            p.start()
        self.client = TestClient(app)

    def tearDown(self):
        self.client.close()
        for p in reversed(self.patches):
            p.stop()

    def test_latest_query_returns_paginated_payload(self):
        mocked_rows = [
            {
                "natural_key": "k1",
                "location_id": "aqs:06:001:0001:1",
                "state_code": "06",
                "county_code": "001",
                "site_name": "Test Site",
                "city_name": "Test City",
                "latitude": 37.77,
                "longitude": -122.42,
                "pollutant_code": "88101",
                "pollutant_name": "PM2.5",
                "value_numeric": 12.0,
                "unit": "ug/m3",
                "timestamp_start_utc": "2026-04-17T00:00:00Z",
                "source_system": "AQS",
                "data_status": "final",
                "threshold_type": "pm25_24h_standard",
                "threshold_value": 35.0,
                "is_exceedance": False,
            }
        ]
        with patch("app.routers.latest.fetch_all", return_value=mocked_rows):
            response = self.client.get("/latest", params={"pollutant_code": "88101", "limit": 10, "offset": 0})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["items"][0]["pollutant_code"], "88101")

    def test_map_latest_query_returns_points(self):
        mocked_rows = [
            {
                "natural_key": "k2",
                "location_id": "airnow:123456789",
                "state_code": "06",
                "county_code": "075",
                "site_name": "Downtown",
                "city_name": "San Francisco",
                "latitude": 34.05,
                "longitude": -118.25,
                "pollutant_code": "44201",
                "pollutant_name": "O3",
                "value_numeric": 0.05,
                "unit": "ppm",
                "timestamp_start_utc": "2026-04-17T01:00:00Z",
                "data_granularity": "hourly",
                "data_status": "provisional",
                "source_system": "AIRNOW",
                "source_dataset": "airnow_hourly_file",
                "is_exceedance": False,
            }
        ]
        with patch("app.routers.map.fetch_all", return_value=mocked_rows):
            response = self.client.get("/map/latest", params={"pollutant_code": "44201"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["items"][0]["location_id"], "airnow:123456789")

    def test_timeseries_rejects_invalid_window(self):
        response = self.client.get(
            "/timeseries",
            params={
                "location_id": "aqs:06:001:0001:1",
                "pollutant_code": "88101",
                "start_day_utc": "2026-04-18T00:00:00Z",
                "end_day_utc": "2026-04-17T00:00:00Z",
            },
        )
        self.assertEqual(response.status_code, 422)

    def test_quality_coverage_returns_filtered_rows(self):
        mocked_rows = [
            {
                "state_code": "06",
                "county_code": "001",
                "pollutant_code": "88101",
                "pollutant_name": "PM2.5",
                "day_utc": "2026-04-17T00:00:00Z",
                "observation_count": 200,
                "reporting_station_count": 20,
                "total_station_count": 25,
                "station_coverage_ratio": 0.8,
            }
        ]
        with patch("app.routers.quality.fetch_all", return_value=mocked_rows):
            response = self.client.get("/quality/coverage", params={"min_coverage_ratio": 0.7})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertGreaterEqual(payload["items"][0]["station_coverage_ratio"], 0.7)


if __name__ == "__main__":
    unittest.main()
