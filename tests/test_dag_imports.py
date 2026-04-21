import importlib.util
import sys
import types
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "airflow" / "dags"


class _DummyTask:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __rshift__(self, other):
        return other


class _DummyDag:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.dag_id = kwargs.get("dag_id")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _stub_module(name: str, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


STUBBED_MODULE_NAMES = [
    "pendulum",
    "airflow",
    "airflow.providers",
    "airflow.providers.standard",
    "airflow.providers.standard.operators",
    "airflow.providers.standard.operators.python",
    "aq_pipeline",
    "aq_pipeline.extract",
    "aq_pipeline.load",
    "aq_pipeline.transform",
    "aq_pipeline.utils",
    "aq_pipeline.extract.aqs_extract",
    "aq_pipeline.extract.airnow_extract",
    "aq_pipeline.transform.normalize_aqs",
    "aq_pipeline.transform.normalize_airnow",
    "aq_pipeline.transform.merge_current",
    "aq_pipeline.load.load_postgres",
    "aq_pipeline.load.load_serving",
    "aq_pipeline.utils.dag_runtime",
    "aq_pipeline.utils.dependency_guard",
]


def _install_runtime_stubs() -> None:
    _stub_module("pendulum", datetime=lambda *args, **kwargs: {"args": args, "kwargs": kwargs})
    _stub_module("airflow", DAG=_DummyDag)
    _stub_module("airflow.providers")
    _stub_module("airflow.providers.standard")
    _stub_module("airflow.providers.standard.operators")
    _stub_module("airflow.providers.standard.operators.python", PythonOperator=_DummyTask)
    _stub_module("aq_pipeline")
    _stub_module("aq_pipeline.extract")
    _stub_module("aq_pipeline.load")
    _stub_module("aq_pipeline.transform")
    _stub_module("aq_pipeline.utils")
    _stub_module("aq_pipeline.extract.aqs_extract", run_aqs_bootstrap_extract=lambda: None, run_aqs_daily_reconcile_extract=lambda: None)
    _stub_module("aq_pipeline.extract.airnow_extract", run_airnow_gap_bootstrap_extract=lambda: None, run_airnow_hourly_hot_extract=lambda: None)
    _stub_module("aq_pipeline.transform.normalize_aqs", normalize_aqs_payload=lambda: None)
    _stub_module("aq_pipeline.transform.normalize_airnow", normalize_airnow_payload=lambda: None)
    _stub_module("aq_pipeline.transform.merge_current", apply_priority_merge=lambda: None)
    _stub_module("aq_pipeline.load.load_postgres", upsert_observation_tables=lambda: None)
    _stub_module("aq_pipeline.load.load_serving", refresh_serving_rollups=lambda: None)
    _stub_module("aq_pipeline.utils.dag_runtime", build_default_args=lambda **kwargs: {}, task_timeout=lambda **kwargs: 0)
    _stub_module("aq_pipeline.utils.dependency_guard", require_aqs_bootstrap_completed=lambda: None, require_recent_airnow_hourly_sync=lambda: None, require_serving_upstreams_ready=lambda: None)


class DagImportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._original_modules = {name: sys.modules.get(name) for name in STUBBED_MODULE_NAMES}
        _install_runtime_stubs()

    @classmethod
    def tearDownClass(cls):
        for name, original in cls._original_modules.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original

    def _import_dag_file(self, file_name: str):
        module_name = f"test_import_{file_name.replace('.py', '')}"
        file_path = DAGS_DIR / file_name
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        self.assertIsNotNone(spec)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        return module

    def test_all_expected_dag_files_import(self):
        for dag_file in [
            "aqs_bootstrap_manual.py",
            "airnow_gap_bootstrap_manual.py",
            "airnow_hourly_hot_sync.py",
            "aqs_daily_reconcile.py",
            "serving_rollups_daily.py",
        ]:
            module = self._import_dag_file(dag_file)
            self.assertTrue(hasattr(module, "dag"))
            self.assertIsNotNone(module.dag.dag_id)


if __name__ == "__main__":
    unittest.main()
