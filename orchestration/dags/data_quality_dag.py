"""
Airflow DAG: Data Quality Checks (Phase 5)

Runs basic row-count and null-check validations on Postgres tables.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import os


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _log_pipeline_event(stage: str, status: str, message: str) -> None:
    try:
        import psycopg2
    except ImportError:
        return

    try:
        with psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB", "crypto_db"),
            user=os.getenv("POSTGRES_USER", "crypto_user"),
            password=os.getenv("POSTGRES_PASSWORD", "crypto_pass"),
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_logs (stage, status, message)
                    VALUES (%s, %s, %s)
                    """,
                    (stage, status, message),
                )
    except Exception:
        pass


def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "crypto_db"),
        user=os.getenv("POSTGRES_USER", "crypto_user"),
        password=os.getenv("POSTGRES_PASSWORD", "crypto_pass"),
    )


def check_row_counts():
    _log_pipeline_event("data_quality_row_counts", "started", "Checking coin_prices row count")
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM coin_prices;")
            count = cur.fetchone()[0]
            if count < 1:
                _log_pipeline_event("data_quality_row_counts", "failed", "coin_prices is empty")
                raise ValueError("coin_prices is empty")
    _log_pipeline_event("data_quality_row_counts", "success", "coin_prices row count passed")


def check_nulls():
    _log_pipeline_event("data_quality_nulls", "started", "Checking coin_prices null-critical columns")
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM coin_prices
                WHERE symbol IS NULL OR name IS NULL OR price_usd IS NULL OR recorded_at IS NULL;
                """
            )
            bad = cur.fetchone()[0]
            if bad > 0:
                _log_pipeline_event("data_quality_nulls", "failed", f"Found {bad} null-critical rows in coin_prices")
                raise ValueError(f"Found {bad} null-critical rows in coin_prices")
    _log_pipeline_event("data_quality_nulls", "success", "Null checks passed")


with DAG(
    dag_id="data_quality_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:
    row_counts = PythonOperator(task_id="check_row_counts", python_callable=check_row_counts)
    null_checks = PythonOperator(task_id="check_nulls", python_callable=check_nulls)

    row_counts >> null_checks
