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
class MinimalE2ERegressionTests(unittest.TestCase):
    def test_health_and_latest_smoke_path(self):
        with patch("app.main.init_db_pool", return_value=object()), patch("app.main.close_db_pool", return_value=None), patch(
            "app.routers.latest.fetch_all",
            return_value=[
                {
                    "natural_key": "smoke-key",
                    "location_id": "aqs:06:001:0001:1",
                    "state_code": "06",
                    "county_code": "001",
                    "site_name": "Smoke Site",
                    "city_name": "Smoke City",
                    "latitude": 1.0,
                    "longitude": 2.0,
                    "pollutant_code": "88101",
                    "pollutant_name": "PM2.5",
                    "value_numeric": 10.0,
                    "unit": "ug/m3",
                    "timestamp_start_utc": "2026-04-17T00:00:00Z",
                    "source_system": "AQS",
                    "data_status": "final",
                    "threshold_type": "pm25_24h_standard",
                    "threshold_value": 35.0,
                    "is_exceedance": False,
                }
            ],
        ):
            client = TestClient(app)
            try:
                healthz = client.get("/healthz")
                self.assertEqual(healthz.status_code, 200)
                latest_health = client.get("/latest/health")
                self.assertEqual(latest_health.status_code, 200)
                latest = client.get("/latest", params={"pollutant_code": "88101", "limit": 5})
                self.assertEqual(latest.status_code, 200)
                self.assertEqual(latest.json()["count"], 1)
            finally:
                client.close()


if __name__ == "__main__":
    unittest.main()
