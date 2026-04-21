from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from aq_pipeline.extract.airnow_extract import run_airnow_hourly_hot_extract
from aq_pipeline.load.load_postgres import upsert_observation_tables
from aq_pipeline.transform.normalize_airnow import normalize_airnow_payload
from aq_pipeline.utils.dag_runtime import build_default_args, task_timeout


with DAG(
    dag_id="airnow_hourly_hot_sync",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="58 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=build_default_args(),
    dagrun_timeout=timedelta(hours=2),
    tags=["hourly", "airnow"],
) as dag:
    extract = PythonOperator(
        task_id="extract_airnow_hourly_hot",
        python_callable=run_airnow_hourly_hot_extract,
        execution_timeout=task_timeout(env_key="AIRNOW_HOT_EXTRACT_TIMEOUT_MINUTES", fallback_minutes=45),
    )
    normalize = PythonOperator(
        task_id="normalize_airnow_payload",
        python_callable=normalize_airnow_payload,
        execution_timeout=task_timeout(env_key="AIRNOW_HOT_NORMALIZE_TIMEOUT_MINUTES", fallback_minutes=30),
    )
    load = PythonOperator(
        task_id="load_airnow_hourly_hot",
        python_callable=upsert_observation_tables,
        execution_timeout=task_timeout(env_key="AIRNOW_HOT_LOAD_TIMEOUT_MINUTES", fallback_minutes=45),
    )

    extract >> normalize >> load
