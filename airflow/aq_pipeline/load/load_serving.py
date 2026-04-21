import os
from datetime import datetime, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

from aq_pipeline.utils.logging import get_logger


logger = get_logger(__name__)


def refresh_serving_rollups() -> None:
    pg_hook = _get_pg_hook()
    started_at = datetime.now(timezone.utc)
    run_id = _start_pipeline_run(pg_hook=pg_hook, started_at=started_at)
    try:
        refreshed_matviews, validated_views = _refresh_and_validate_mart_objects(pg_hook=pg_hook)
        _finish_pipeline_run(
            pg_hook=pg_hook,
            run_id=run_id,
            status="success",
            message=f"serving refresh complete; matviews={refreshed_matviews} validated_views={validated_views}",
        )
        logger.info(
            "Serving refresh complete: refreshed_matviews=%s validated_views=%s",
            refreshed_matviews,
            validated_views,
        )
    except Exception as exc:
        _finish_pipeline_run(
            pg_hook=pg_hook,
            run_id=run_id,
            status="failed",
            message=str(exc),
        )
        raise


def _refresh_and_validate_mart_objects(*, pg_hook: PostgresHook) -> tuple[int, int]:
    conn = pg_hook.get_conn()
    refreshed_matviews = 0
    validated_views = 0
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    SELECT matviewname
                    FROM pg_matviews
                    WHERE schemaname = 'mart'
                    ORDER BY matviewname
                """
            )
            for (matview_name,) in cursor.fetchall():
                cursor.execute(
                    sql.SQL("REFRESH MATERIALIZED VIEW mart.{}").format(sql.Identifier(matview_name))
                )
                refreshed_matviews += 1

            cursor.execute(
                """
                    SELECT table_name
                    FROM information_schema.views
                    WHERE table_schema = 'mart'
                    ORDER BY table_name
                """
            )
            for (view_name,) in cursor.fetchall():
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM mart.{}").format(sql.Identifier(view_name))
                )
                cursor.fetchone()
                validated_views += 1

        conn.commit()
    finally:
        conn.close()
    return refreshed_matviews, validated_views


def _start_pipeline_run(*, pg_hook: PostgresHook, started_at: datetime) -> int:
    dag_id = os.getenv("AIRFLOW_CTX_DAG_ID", "manual")
    task_id = os.getenv("AIRFLOW_CTX_TASK_ID")
    run_type = "serving_rollup_refresh"
    row = pg_hook.get_first(
        sql="""
            INSERT INTO ops.pipeline_run (dag_id, task_id, run_type, started_at, status, message)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING run_id
        """,
        parameters=(dag_id, task_id, run_type, started_at, "running", "serving rollup refresh started"),
    )
    if not row:
        raise RuntimeError("Failed to create ops.pipeline_run row for serving refresh.")
    return int(row[0])


def _finish_pipeline_run(
    *,
    pg_hook: PostgresHook,
    run_id: int,
    status: str,
    message: str,
) -> None:
    pg_hook.run(
        sql="""
            UPDATE ops.pipeline_run
            SET ended_at = NOW(),
                status = %s,
                message = %s
            WHERE run_id = %s
        """,
        parameters=(status, message[:2000], run_id),
    )


def _get_pg_hook() -> PostgresHook:
    pg_conn_id = os.getenv("AIRFLOW_CONN_POSTGRES_ID", "aq_postgres")
    return PostgresHook(postgres_conn_id=pg_conn_id)
