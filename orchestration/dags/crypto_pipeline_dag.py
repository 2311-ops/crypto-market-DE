"""
Airflow DAG: Crypto Pipeline Orchestration (Phase 5)

- Runs every minute to publish one ingestion batch to Kafka.
- Triggers Spark streaming/batch job via spark-submit on the Spark master container.
- Kicks off data-quality DAG after Spark job.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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
        # Pipeline logging must not block the pipeline itself.
        pass


def ingest_once(**_):
    """Fetch one batch from CoinGecko and publish a single message to Kafka."""
    import json
    import logging
    from datetime import datetime, timezone
    from kafka import KafkaProducer
    import requests

    # Load environment variables (airflow sets them from .env via env_file)
    COINGECKO_API_BASE = os.getenv("COINGECKO_API_BASE", "https://api.coingecko.com/api/v3")
    COINGECKO_COINS = os.getenv("COINGECKO_COINS", "bitcoin,ethereum,solana").split(",")
    COINGECKO_VS_CURRENCY = os.getenv("COINGECKO_VS_CURRENCY", "usd")
    COINGECKO_ORDER = os.getenv("COINGECKO_ORDER", "market_cap_desc")
    COINGECKO_PER_PAGE = int(os.getenv("COINGECKO_PER_PAGE", "50"))
    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "raw_crypto_data")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("airflow.ingest")
    _log_pipeline_event("ingestion", "started", "Fetching CoinGecko market data")

    # Build Kafka producer with retries
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        retries=3,
    )

    # Fetch from CoinGecko
    params = {
        "vs_currency": COINGECKO_VS_CURRENCY,
        "ids": ",".join(COINGECKO_COINS),
        "order": COINGECKO_ORDER,
        "per_page": COINGECKO_PER_PAGE,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    try:
        resp = requests.get(
            f"{COINGECKO_API_BASE}/coins/markets",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        coins = resp.json()
    except Exception as e:
        _log_pipeline_event("ingestion", "failed", f"CoinGecko fetch failed: {e}")
        logger.error("Failed to fetch from CoinGecko: %s", e)
        raise

    # Publish to Kafka
    message = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "coins": coins,
    }
    producer.send(KAFKA_TOPIC, value=message)
    producer.flush()
    producer.close()
    _log_pipeline_event("ingestion", "success", f"Published {len(coins)} coins to Kafka")
    logger.info("Published %d coins to Kafka topic %s", len(coins), KAFKA_TOPIC)


def run_spark_job(**_):
    """
    Submit the Spark streaming job by exec'ing into the spark-master container.
    Includes Hadoop AWS JAR for MinIO S3A connectivity and Kafka/PostgreSQL connectors.
    """
    project = os.getenv("PROJECT_NAME", "crypto-pipeline")
    _log_pipeline_event("spark_job", "started", "Submitting spark streaming job")
    cmd = [
        "docker", "exec", f"{project}_spark_master",
        "/opt/spark/bin/spark-submit",
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4",
        "/app/processing/spark_streaming_job.py",
    ]
    try:
        subprocess.run(cmd, check=True)
        _log_pipeline_event("spark_job", "success", "Spark job completed successfully")
    except subprocess.CalledProcessError as exc:
        _log_pipeline_event("spark_job", "failed", f"Spark job failed: {exc}")
        raise


with DAG(
    dag_id="crypto_pipeline_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval="* * * * *",  # every minute
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_batch = PythonOperator(
        task_id="ingest_once",
        python_callable=ingest_once,
    )

    spark_job = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
    )

    trigger_dq = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality_dag",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end")

    start >> ingest_batch >> spark_job >> trigger_dq >> end
