import os
from datetime import timedelta
from typing import Any


def build_default_args(*, owner: str = "aq_pipeline") -> dict[str, Any]:
    retries = int(os.getenv("AIRFLOW_TASK_RETRIES", "2"))
    retry_delay_minutes = int(os.getenv("AIRFLOW_TASK_RETRY_DELAY_MINUTES", "5"))
    return {
        "owner": owner,
        "depends_on_past": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=max(1, retry_delay_minutes)),
    }


def task_timeout(*, env_key: str, fallback_minutes: int) -> timedelta:
    timeout_minutes = int(os.getenv(env_key, str(fallback_minutes)))
    return timedelta(minutes=max(1, timeout_minutes))
