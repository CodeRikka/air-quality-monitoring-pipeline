import os
from datetime import datetime, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook

from aq_pipeline.utils.logging import get_logger


logger = get_logger(__name__)


def apply_priority_merge() -> None:
    """
    Rebuild core.fact_observation_current from history using:
    - higher source_priority wins
    - for same priority, latest ingested version wins
    """
    pg_hook = _get_pg_hook()
    started_at = datetime.now(timezone.utc)
    run_id = _start_pipeline_run(pg_hook=pg_hook, started_at=started_at)
    try:
        conn = pg_hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                        WITH ranked AS (
                            SELECT
                                h.*,
                                ROW_NUMBER() OVER (
                                    PARTITION BY h.natural_key
                                    ORDER BY h.source_priority DESC, h.ingested_at DESC, h.observation_history_id DESC
                                ) AS rn
                            FROM core.fact_observation_history h
                        )
                        INSERT INTO core.fact_observation_current (
                            natural_key,
                            location_id,
                            pollutant_code,
                            metric_type,
                            value_numeric,
                            unit,
                            data_granularity,
                            timestamp_start_utc,
                            timestamp_end_utc,
                            timestamp_local,
                            source_system,
                            source_dataset,
                            source_priority,
                            data_status,
                            method_code,
                            method_name,
                            qualifier_raw,
                            source_last_modified_date,
                            record_hash,
                            updated_at
                        )
                        SELECT
                            natural_key,
                            location_id,
                            pollutant_code,
                            metric_type,
                            value_numeric,
                            unit,
                            data_granularity,
                            timestamp_start_utc,
                            timestamp_end_utc,
                            timestamp_local,
                            source_system,
                            source_dataset,
                            source_priority,
                            data_status,
                            method_code,
                            method_name,
                            qualifier_raw,
                            source_last_modified_date,
                            record_hash,
                            NOW()
                        FROM ranked
                        WHERE rn = 1
                        ON CONFLICT (natural_key) DO UPDATE
                        SET location_id = EXCLUDED.location_id,
                            pollutant_code = EXCLUDED.pollutant_code,
                            metric_type = EXCLUDED.metric_type,
                            value_numeric = EXCLUDED.value_numeric,
                            unit = EXCLUDED.unit,
                            data_granularity = EXCLUDED.data_granularity,
                            timestamp_start_utc = EXCLUDED.timestamp_start_utc,
                            timestamp_end_utc = EXCLUDED.timestamp_end_utc,
                            timestamp_local = EXCLUDED.timestamp_local,
                            source_system = EXCLUDED.source_system,
                            source_dataset = EXCLUDED.source_dataset,
                            source_priority = EXCLUDED.source_priority,
                            data_status = EXCLUDED.data_status,
                            method_code = EXCLUDED.method_code,
                            method_name = EXCLUDED.method_name,
                            qualifier_raw = EXCLUDED.qualifier_raw,
                            source_last_modified_date = EXCLUDED.source_last_modified_date,
                            record_hash = EXCLUDED.record_hash,
                            updated_at = NOW()
                    """
                )

                cursor.execute("UPDATE core.fact_observation_history SET is_current_version = FALSE")
                cursor.execute(
                    """
                        UPDATE core.fact_observation_history h
                        SET is_current_version = TRUE
                        FROM core.fact_observation_current c
                        WHERE h.natural_key = c.natural_key
                          AND h.record_hash = c.record_hash
                          AND h.source_system = c.source_system
                    """
                )
            conn.commit()
        finally:
            conn.close()

        _finish_pipeline_run(
            pg_hook=pg_hook,
            run_id=run_id,
            status="success",
            message="apply_priority_merge complete",
        )
        logger.info("Priority merge completed successfully.")
    except Exception as exc:
        _finish_pipeline_run(
            pg_hook=pg_hook,
            run_id=run_id,
            status="failed",
            message=str(exc),
        )
        raise


def _start_pipeline_run(*, pg_hook: PostgresHook, started_at: datetime) -> int:
    dag_id = os.getenv("AIRFLOW_CTX_DAG_ID", "manual")
    task_id = os.getenv("AIRFLOW_CTX_TASK_ID")
    run_type = "apply_priority_merge"
    row = pg_hook.get_first(
        sql="""
            INSERT INTO ops.pipeline_run (dag_id, task_id, run_type, started_at, status, message)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING run_id
        """,
        parameters=(dag_id, task_id, run_type, started_at, "running", "priority merge started"),
    )
    if not row:
        raise RuntimeError("Failed to create ops.pipeline_run row for merge.")
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
