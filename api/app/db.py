import os
from contextlib import contextmanager
from typing import Any, Generator

from psycopg2 import pool
from psycopg2.extras import RealDictCursor


def init_db_pool() -> pool.SimpleConnectionPool:
    min_conn = int(os.getenv("API_DB_POOL_MIN_CONN", "1"))
    max_conn = int(os.getenv("API_DB_POOL_MAX_CONN", "10"))
    return pool.SimpleConnectionPool(
        minconn=max(1, min_conn),
        maxconn=max(1, max_conn),
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB_AIRQUALITY", "airquality"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def close_db_pool(db_pool: pool.SimpleConnectionPool | None) -> None:
    if db_pool is not None:
        db_pool.closeall()


@contextmanager
def get_db_cursor(db_pool: pool.SimpleConnectionPool) -> Generator[RealDictCursor, None, None]:
    conn = db_pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)


def fetch_all(db_pool: pool.SimpleConnectionPool, sql_query: str, parameters: tuple[Any, ...]) -> list[dict[str, Any]]:
    with get_db_cursor(db_pool) as cursor:
        cursor.execute(sql_query, parameters)
        rows = cursor.fetchall()
    return [dict(row) for row in rows]
