import os
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_ROOT = REPO_ROOT / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))


def _stub_module(name: str, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


_stub_module("airflow")
_stub_module("airflow.providers")
_stub_module("airflow.providers.postgres")
_stub_module("airflow.providers.postgres.hooks")
_stub_module("airflow.providers.postgres.hooks.postgres", PostgresHook=object)
_stub_module("aq_pipeline.load.load_minio", write_raw_object=lambda **kwargs: None)

from aq_pipeline.extract import aqs_extract  # noqa: E402


class AqsExtractGeographyModeTests(unittest.TestCase):
    def test_resolve_aqs_geography_mode_defaults_to_auto(self):
        with patch.dict(os.environ, {}, clear=False):
            with patch("aq_pipeline.extract.aqs_extract.logger.warning") as warning:
                self.assertEqual(aqs_extract._resolve_aqs_geography_mode(), "auto")
        warning.assert_not_called()

    def test_resolve_aqs_geography_mode_accepts_county(self):
        with patch.dict(os.environ, {"AQS_GEOGRAPHY_MODE": "county"}, clear=False):
            self.assertEqual(aqs_extract._resolve_aqs_geography_mode(), "county")

    def test_resolve_bootstrap_windows_defaults_to_month(self):
        now_utc = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)
        with patch.dict(
            os.environ,
            {
                "AQS_BOOTSTRAP_START_YEAR": "2026",
                "AQS_BOOTSTRAP_WINDOW_GRANULARITY": "month",
                "AQS_BOOTSTRAP_BDATE": "",
                "AQS_BOOTSTRAP_EDATE": "",
            },
            clear=False,
        ):
            windows = aqs_extract._resolve_bootstrap_windows(now_utc=now_utc)

        self.assertEqual(len(windows), 4)
        self.assertEqual(windows[0]["bdate"], "20260101")
        self.assertEqual(windows[0]["edate"], "20260131")
        self.assertEqual(windows[-1]["bdate"], "20260401")
        self.assertEqual(windows[-1]["edate"], "20260419")

    def test_extract_data_shards_auto_selects_county_requests_for_large_shard(self):
        client = Mock()
        client.MAX_PARAMS_PER_REQUEST = 5
        client.get_counties_by_state.return_value = {
            "Data": [
                {"code": "001"},
                {"code": "013"},
            ]
        }
        regions = [{"id": "test_region", "state_codes": ["06"]}]
        window = {"year": "2026", "bdate": "20260101", "edate": "20260131"}

        with patch.dict(
            os.environ,
            {"AQS_GEOGRAPHY_MODE": "auto", "AQS_STATE_TO_COUNTY_SCORE_THRESHOLD": "10"},
            clear=False,
        ):
            with patch(
                "aq_pipeline.extract.aqs_extract._extract_aqs_data_for_county_with_timeout_split",
                side_effect=[
                    {"requests_total": 2, "sample_written": 1, "daily_written": 1, "failed": 0},
                    {"requests_total": 2, "sample_written": 1, "daily_written": 1, "failed": 0},
                ],
            ) as county_extract:
                result = aqs_extract._extract_aqs_data_shards(
                    client=client,
                    regions=regions,
                    pollutant_codes=["88101"],
                    run_type="bootstrap",
                    year_windows=[window],
                    cbdate=None,
                    cedate=None,
                )

        self.assertEqual(result["requests_total"], 4)
        self.assertEqual(result["sample_written"], 2)
        self.assertEqual(result["daily_written"], 2)
        self.assertEqual(client.get_counties_by_state.call_count, 1)
        self.assertEqual(county_extract.call_count, 2)
        counties = [call.kwargs["county"] for call in county_extract.call_args_list]
        self.assertEqual(counties, ["001", "013"])

    def test_state_timeout_falls_back_to_county_requests(self):
        client = Mock()
        client.get_sample_data_by_state.side_effect = requests.exceptions.Timeout("timed out")
        with patch(
            "aq_pipeline.extract.aqs_extract._extract_aqs_data_by_counties",
            return_value={"requests_total": 6, "sample_written": 3, "daily_written": 3, "failed": 0},
        ) as county_extract:
            result = aqs_extract._extract_aqs_data_for_batch_with_timeout_split(
                client=client,
                run_type="bootstrap",
                region_id="test_region",
                state="06",
                pollutant_batch=["88101"],
                window={"year": "2026", "bdate": "20260101", "edate": "20260131"},
                cbdate=None,
                cedate=None,
                state_counties=["001", "013", "075"],
            )

        self.assertEqual(result["requests_total"], 6)
        county_extract.assert_called_once()
        self.assertEqual(county_extract.call_args.kwargs["counties"], ["001", "013", "075"])
        self.assertEqual(county_extract.call_args.kwargs["geography_mode"], "county_fallback")


if __name__ == "__main__":
    unittest.main()
