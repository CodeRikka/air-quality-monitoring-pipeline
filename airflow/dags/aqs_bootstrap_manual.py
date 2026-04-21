from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from aq_pipeline.extract.aqs_extract import run_aqs_bootstrap_extract
from aq_pipeline.load.load_postgres import upsert_observation_tables
from aq_pipeline.transform.normalize_aqs import normalize_aqs_payload
from aq_pipeline.utils.dag_runtime import build_default_args, task_timeout


with DAG(
    dag_id="aqs_bootstrap_manual",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=build_default_args(),
    dagrun_timeout=timedelta(hours=6),
    tags=["bootstrap", "aqs"],
) as dag:
    extract = PythonOperator(
        task_id="extract_aqs_bootstrap",
        python_callable=run_aqs_bootstrap_extract,
        execution_timeout=task_timeout(env_key="AQS_BOOTSTRAP_EXTRACT_TIMEOUT_MINUTES", fallback_minutes=180),
    )
    normalize = PythonOperator(
        task_id="normalize_aqs_payload",
        python_callable=normalize_aqs_payload,
        execution_timeout=task_timeout(env_key="AQS_BOOTSTRAP_NORMALIZE_TIMEOUT_MINUTES", fallback_minutes=60),
    )
    load = PythonOperator(
        task_id="load_aqs_bootstrap",
        python_callable=upsert_observation_tables,
        execution_timeout=task_timeout(env_key="AQS_BOOTSTRAP_LOAD_TIMEOUT_MINUTES", fallback_minutes=90),
    )

    extract >> normalize >> load
