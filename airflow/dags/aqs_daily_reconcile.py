from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from aq_pipeline.extract.aqs_extract import run_aqs_daily_reconcile_extract
from aq_pipeline.load.load_postgres import upsert_observation_tables
from aq_pipeline.transform.merge_current import apply_priority_merge
from aq_pipeline.transform.normalize_aqs import normalize_aqs_payload
from aq_pipeline.utils.dag_runtime import build_default_args, task_timeout
from aq_pipeline.utils.dependency_guard import require_recent_airnow_hourly_sync


with DAG(
    dag_id="aqs_daily_reconcile",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="30 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=build_default_args(),
    dagrun_timeout=timedelta(hours=3),
    tags=["daily", "aqs"],
) as dag:
    ensure_airnow_hourly_ready = PythonOperator(
        task_id="ensure_recent_airnow_hourly_sync",
        python_callable=require_recent_airnow_hourly_sync,
        execution_timeout=task_timeout(env_key="AQS_RECONCILE_PRECHECK_TIMEOUT_MINUTES", fallback_minutes=10),
    )
    extract = PythonOperator(
        task_id="extract_aqs_daily_reconcile",
        python_callable=run_aqs_daily_reconcile_extract,
        execution_timeout=task_timeout(env_key="AQS_RECONCILE_EXTRACT_TIMEOUT_MINUTES", fallback_minutes=90),
    )
    normalize = PythonOperator(
        task_id="normalize_aqs_payload",
        python_callable=normalize_aqs_payload,
        execution_timeout=task_timeout(env_key="AQS_RECONCILE_NORMALIZE_TIMEOUT_MINUTES", fallback_minutes=45),
    )
    load = PythonOperator(
        task_id="load_aqs_daily_reconcile",
        python_callable=upsert_observation_tables,
        execution_timeout=task_timeout(env_key="AQS_RECONCILE_LOAD_TIMEOUT_MINUTES", fallback_minutes=60),
    )
    merge = PythonOperator(
        task_id="merge_aqs_over_airnow",
        python_callable=apply_priority_merge,
        execution_timeout=task_timeout(env_key="AQS_RECONCILE_MERGE_TIMEOUT_MINUTES", fallback_minutes=30),
    )

    ensure_airnow_hourly_ready >> extract >> normalize >> load >> merge
