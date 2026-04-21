from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from aq_pipeline.load.load_serving import refresh_serving_rollups
from aq_pipeline.utils.dag_runtime import build_default_args, task_timeout
from aq_pipeline.utils.dependency_guard import require_serving_upstreams_ready


with DAG(
    dag_id="serving_rollups_daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="45 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=build_default_args(),
    dagrun_timeout=timedelta(hours=2),
    tags=["daily", "serving"],
) as dag:
    ensure_upstreams_ready = PythonOperator(
        task_id="ensure_serving_upstreams_ready",
        python_callable=require_serving_upstreams_ready,
        execution_timeout=task_timeout(env_key="SERVING_PRECHECK_TIMEOUT_MINUTES", fallback_minutes=10),
    )
    refresh_rollups = PythonOperator(
        task_id="refresh_mart_rollups",
        python_callable=refresh_serving_rollups,
        execution_timeout=task_timeout(env_key="SERVING_REFRESH_TIMEOUT_MINUTES", fallback_minutes=45),
    )

    ensure_upstreams_ready >> refresh_rollups
