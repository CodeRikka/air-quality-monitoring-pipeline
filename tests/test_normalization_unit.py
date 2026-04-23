import sys
import types
import unittest
from datetime import timezone
from unittest.mock import patch
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
        _stub_module("psycopg2")
        _stub_module("psycopg2.extras", execute_batch=lambda *args, **kwargs: None)
        _stub_module("airflow.providers.amazon.aws.hooks.s3", S3Hook=_DummyS3Hook)
        _stub_module("airflow.providers.postgres.hooks.postgres", PostgresHook=_DummyPostgresHook)

        try:
            # Imported after stubs are in place.
            from aq_pipeline.load import load_postgres
            from aq_pipeline.transform import normalize_airnow, normalize_aqs
        except Exception as exc:  # pragma: no cover
            raise unittest.SkipTest(f"Normalization modules unavailable in this environment: {exc}") from exc

        cls.load_postgres = load_postgres
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

    def test_normalize_airnow_row_filters_to_configured_regions(self):
        pollutant_map = {"pm2.5": "88101"}
        base_row = {
            "ParameterName": "PM2.5",
            "Value": "12.3",
            "ValidDate": "2026-04-17",
            "ValidTime": "13",
            "GMTOffset": "-7",
        }

        allowed = self.normalize_airnow._normalize_airnow_row(
            source_row={**base_row, "AQSID": "06-001-0001"},
            metadata={},
            pollutant_map=pollutant_map,
            source_dataset="airnow_hourly_file",
            default_granularity="hourly",
            regions=[{"id": "ca_only", "state_codes": ["06"]}],
        )
        filtered = self.normalize_airnow._normalize_airnow_row(
            source_row={**base_row, "AQSID": "12-001-0001"},
            metadata={},
            pollutant_map=pollutant_map,
            source_dataset="airnow_hourly_file",
            default_granularity="hourly",
            regions=[{"id": "ca_only", "state_codes": ["06"]}],
        )

        self.assertIsNotNone(allowed)
        self.assertEqual(allowed["aqsid"], "060010001")
        self.assertIsNone(filtered)

    def test_validate_and_prepare_rows_filters_out_of_region_airnow(self):
        payloads = [
            {
                "natural_key": "airnow-allowed",
                "location_id": "airnow:060010001",
                "pollutant_code": "88101",
                "metric_type": "concentration",
                "value_numeric": 12.3,
                "unit": "ug/m3",
                "data_granularity": "hourly",
                "timestamp_start_utc": "2026-04-17T20:00:00+00:00",
                "timestamp_end_utc": "2026-04-17T21:00:00+00:00",
                "timestamp_local": "2026-04-17T13:00:00+00:00",
                "source_system": "AIRNOW",
                "source_dataset": "airnow_hourly_file",
                "source_priority": 10,
                "data_status": "provisional",
                "record_hash": "hash-1",
                "raw_fields": {},
                "aqsid": "060010001",
            },
            {
                "natural_key": "airnow-filtered",
                "location_id": "airnow:120010001",
                "pollutant_code": "88101",
                "metric_type": "concentration",
                "value_numeric": 10.1,
                "unit": "ug/m3",
                "data_granularity": "hourly",
                "timestamp_start_utc": "2026-04-17T20:00:00+00:00",
                "timestamp_end_utc": "2026-04-17T21:00:00+00:00",
                "timestamp_local": "2026-04-17T15:00:00+00:00",
                "source_system": "AIRNOW",
                "source_dataset": "airnow_hourly_file",
                "source_priority": 10,
                "data_status": "provisional",
                "record_hash": "hash-2",
                "raw_fields": {},
                "aqsid": "120010001",
            },
            {
                "natural_key": "aqs-kept",
                "location_id": "aqs:12:001:0001:1",
                "pollutant_code": "88101",
                "metric_type": "concentration",
                "value_numeric": 11.5,
                "unit": "ug/m3",
                "data_granularity": "hourly",
                "timestamp_start_utc": "2026-04-17T20:00:00+00:00",
                "timestamp_end_utc": "2026-04-17T21:00:00+00:00",
                "timestamp_local": "2026-04-17T20:00:00+00:00",
                "source_system": "AQS",
                "source_dataset": "aqs_sample_observation",
                "source_priority": 100,
                "data_status": "final",
                "record_hash": "hash-3",
                "raw_fields": {},
                "aqsid": "120010001",
            },
        ]

        valid, rejected, region_filtered = self.load_postgres._validate_and_prepare_rows(
            payloads,
            regions=[{"id": "ca_only", "state_codes": ["06"]}],
        )

        self.assertEqual(rejected, 0)
        self.assertEqual(region_filtered, 1)
        self.assertEqual(len(valid), 2)
        self.assertEqual([row["source_system"] for row in valid], ["AIRNOW", "AQS"])

    def test_parse_location_fields_derives_airnow_state_from_aqsid(self):
        parsed = self.load_postgres._parse_location_fields(
            location_id="airnow:060010001",
            raw_fields={"SiteName": "Test Site", "ReportingArea": "Test City"},
        )

        self.assertEqual(parsed["aqsid"], "060010001")
        self.assertEqual(parsed["state_code"], "06")
        self.assertEqual(parsed["county_code"], "001")
        self.assertEqual(parsed["site_number"], "0001")

    def test_resolve_staging_tables_uses_dag_specific_sources(self):
        with patch.dict("os.environ", {"AIRFLOW_CTX_DAG_ID": "airnow_gap_bootstrap_manual"}, clear=False):
            tables = self.load_postgres._resolve_staging_tables()

        self.assertEqual(tables, ("staging.airnow_gapfill_observation",))

    def test_chunk_rows_splits_into_configured_batches(self):
        rows = [{"natural_key": str(index)} for index in range(5)]

        chunks = self.load_postgres._chunk_rows(rows, 2)

        self.assertEqual([len(chunk) for chunk in chunks], [2, 2, 1])


if __name__ == "__main__":
    unittest.main()
