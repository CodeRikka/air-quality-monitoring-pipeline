import os

from airflow.providers.postgres.hooks.postgres import PostgresHook

from aq_pipeline.utils.logging import get_logger


logger = get_logger(__name__)


def require_successful_run(*, dag_id: str, within_hours: int | None = None) -> None:
    pg_hook = _get_pg_hook()
    parameters: list[object] = [dag_id]
    freshness_clause = ""
    if within_hours is not None:
        freshness_clause = "AND ended_at >= NOW() - (%s::int * INTERVAL '1 hour')"
        parameters.append(max(1, within_hours))

    row = pg_hook.get_first(
        sql=f"""
            SELECT run_id, ended_at
            FROM ops.pipeline_run
            WHERE dag_id = %s
              AND status = 'success'
              {freshness_clause}
            ORDER BY ended_at DESC
            LIMIT 1
        """,
        parameters=tuple(parameters),
    )
    if not row:
        freshness_text = f"within {within_hours} hours" if within_hours is not None else "at any time"
        raise RuntimeError(f"Dependency check failed: no successful run found for DAG '{dag_id}' {freshness_text}.")

    logger.info("Dependency check passed for DAG '%s' with run_id=%s ended_at=%s", dag_id, row[0], row[1])


def require_aqs_bootstrap_completed() -> None:
    require_successful_run(dag_id="aqs_bootstrap_manual")


def require_recent_airnow_hourly_sync() -> None:
    freshness = int(os.getenv("AIRNOW_HOURLY_DEPENDENCY_MAX_AGE_HOURS", "6"))
    require_successful_run(dag_id="airnow_hourly_hot_sync", within_hours=freshness)


def require_serving_upstreams_ready() -> None:
    reconcile_freshness = int(os.getenv("SERVING_RECONCILE_MAX_AGE_HOURS", "30"))
    hourly_freshness = int(os.getenv("SERVING_HOURLY_SYNC_MAX_AGE_HOURS", "6"))
    require_successful_run(dag_id="aqs_daily_reconcile", within_hours=reconcile_freshness)
    require_successful_run(dag_id="airnow_hourly_hot_sync", within_hours=hourly_freshness)


def _get_pg_hook() -> PostgresHook:
    pg_conn_id = os.getenv("AIRFLOW_CONN_POSTGRES_ID", "aq_postgres")
    return PostgresHook(postgres_conn_id=pg_conn_id)
