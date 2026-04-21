import sys
import types
import unittest
from datetime import timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_SRC = REPO_ROOT / "airflow"
if str(AIRFLOW_SRC) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_SRC))


def _stub_module(name: str, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


class _DummyS3Hook:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class _DummyPostgresHook:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class NormalizationUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _stub_module("airflow")
        _stub_module("airflow.providers")
        _stub_module("airflow.providers.amazon")
        _stub_module("airflow.providers.amazon.aws")
        _stub_module("airflow.providers.amazon.aws.hooks")
        _stub_module("airflow.providers.postgres")
        _stub_module("airflow.providers.postgres.hooks")
        _stub_module("airflow.providers.amazon.aws.hooks.s3", S3Hook=_DummyS3Hook)
        _stub_module("airflow.providers.postgres.hooks.postgres", PostgresHook=_DummyPostgresHook)

        try:
            # Imported after stubs are in place.
            from aq_pipeline.transform import normalize_airnow, normalize_aqs
        except Exception as exc:  # pragma: no cover
            raise unittest.SkipTest(f"Normalization modules unavailable in this environment: {exc}") from exc

        cls.normalize_airnow = normalize_airnow
        cls.normalize_aqs = normalize_aqs

    def test_parse_datetime_pair_handles_valid_and_invalid(self):
        parsed = self.normalize_aqs._parse_datetime_pair("2026-04-17", "13:45", tz=timezone.utc)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.hour, 13)
        invalid = self.normalize_aqs._parse_datetime_pair("invalid", "13:45", tz=timezone.utc)
        self.assertIsNone(invalid)

    def test_clean_code_zero_fills_and_handles_empty(self):
        self.assertEqual(self.normalize_aqs._clean_code("7", width=3), "007")
        self.assertEqual(self.normalize_aqs._clean_code("", width=4), "00na")

    def test_parse_hourly_dat_with_header(self):
        content = b"ValidDate|ValidTime|AQSID|ParameterName|Value\n2026-04-17|13|060010001|PM2.5|12.3\n"
        rows = self.normalize_airnow._parse_hourly_dat(content)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["AQSID"], "060010001")
        self.assertEqual(rows[0]["ParameterName"], "PM2.5")

    def test_extract_aqsid_and_timestamp_resolution(self):
        aqsid = self.normalize_airnow._extract_aqsid(source_row={"AQSID": "06-001-0001"})
        self.assertEqual(aqsid, "060010001")
        utc_ts, local_ts = self.normalize_airnow._resolve_airnow_timestamps(
            date_text="2026-04-17",
            time_text="13:00",
            gmt_offset="-7",
        )
        self.assertIsNotNone(utc_ts)
        self.assertIsNotNone(local_ts)
        self.assertEqual((utc_ts - local_ts).total_seconds(), 7 * 3600)


if __name__ == "__main__":
    unittest.main()
