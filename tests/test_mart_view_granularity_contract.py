import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MART_VIEWS_SQL = REPO_ROOT / "sql" / "040_mart_views.sql"


class MartViewGranularityContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql_text = MART_VIEWS_SQL.read_text(encoding="utf-8")

    def _view_section(self, view_name: str) -> str:
        marker = f"CREATE OR REPLACE VIEW mart.{view_name} AS"
        start = self.sql_text.index(marker)
        next_start = self.sql_text.find("CREATE OR REPLACE VIEW mart.", start + len(marker))
        if next_start == -1:
            return self.sql_text[start:]
        return self.sql_text[start:next_start]

    def test_default_serving_views_are_daily_only(self):
        for view_name in (
            "v_latest_station_observation",
            "v_daily_trend",
            "v_monthly_trend",
            "v_seasonal_trend",
            "v_region_coverage",
            "v_exceedance_events",
            "v_anomaly_spikes",
        ):
            with self.subTest(view_name=view_name):
                self.assertIn(
                    "data_granularity = 'daily'",
                    self._view_section(view_name),
                )


if __name__ == "__main__":
    unittest.main()
