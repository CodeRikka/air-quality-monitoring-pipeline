from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from aq_pipeline.extract.airnow_extract import run_airnow_gap_bootstrap_extract
from aq_pipeline.load.load_postgres import upsert_observation_tables
from aq_pipeline.transform.normalize_airnow import normalize_airnow_payload
from aq_pipeline.utils.dag_runtime import build_default_args, task_timeout
from aq_pipeline.utils.dependency_guard import require_aqs_bootstrap_completed


with DAG(
    dag_id="airnow_gap_bootstrap_manual",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=build_default_args(),
    dagrun_timeout=timedelta(hours=4),
    tags=["bootstrap", "airnow"],
) as dag:
    ensure_aqs_bootstrap_completed = PythonOperator(
        task_id="ensure_aqs_bootstrap_completed",
        python_callable=require_aqs_bootstrap_completed,
        execution_timeout=task_timeout(env_key="AIRNOW_GAP_PRECHECK_TIMEOUT_MINUTES", fallback_minutes=10),
    )
    extract = PythonOperator(
        task_id="extract_airnow_gap_bootstrap",
        python_callable=run_airnow_gap_bootstrap_extract,
        execution_timeout=task_timeout(env_key="AIRNOW_GAP_EXTRACT_TIMEOUT_MINUTES", fallback_minutes=150),
    )
    normalize = PythonOperator(
        task_id="normalize_airnow_payload",
        python_callable=normalize_airnow_payload,
        execution_timeout=task_timeout(env_key="AIRNOW_GAP_NORMALIZE_TIMEOUT_MINUTES", fallback_minutes=60),
    )
    load = PythonOperator(
        task_id="load_airnow_gap_bootstrap",
        python_callable=upsert_observation_tables,
        execution_timeout=task_timeout(env_key="AIRNOW_GAP_LOAD_TIMEOUT_MINUTES", fallback_minutes=90),
    )

    ensure_aqs_bootstrap_completed >> extract >> normalize >> load
