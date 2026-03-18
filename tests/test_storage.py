"""
Schema validation smoke tests for PostgreSQL storage.

Run (host with psycopg2 installed):
  python -m pytest tests/test_storage.py

The tests open a transaction and roll back, so no data is persisted.
"""

import os
import datetime

import psycopg
import pytest

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "crypto_db")
PG_USER = os.getenv("POSTGRES_USER", "crypto_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")


def get_conn():
    try:
        return psycopg.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            connect_timeout=3,
        )
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable at {PG_HOST}:{PG_PORT} ({exc})")


@pytest.fixture(scope="function")
def conn():
    connection = get_conn()
    connection.autocommit = False
    yield connection
    connection.rollback()
    connection.close()


def test_insert_coin_prices(conn):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO coin_prices (symbol, name, price_usd, market_cap, volume_24h, price_change_pct_24h)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        ("tbtc", "Test Bitcoin", 12345.67, 999999, 11111, 1.23),
    )
    inserted_id = cur.fetchone()[0]
    assert inserted_id is not None

    cur.execute("SELECT symbol, name, price_usd FROM coin_prices WHERE id = %s", (inserted_id,))
    row = cur.fetchone()
    assert row[0] == "tbtc"
    assert row[1] == "Test Bitcoin"
    assert float(row[2]) == 12345.67


def test_insert_market_stats(conn):
    cur = conn.cursor()
    now = datetime.datetime.now(datetime.UTC)
    cur.execute(
        """
        INSERT INTO market_stats (symbol, volatility_score, volume_spike, window_start, window_end)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """,
        ("tbtc", 12.34, True, now, now + datetime.timedelta(minutes=5)),
    )
    inserted_id = cur.fetchone()[0]
    assert inserted_id is not None


def test_insert_pipeline_logs(conn):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO pipeline_logs (stage, status, message)
        VALUES (%s, %s, %s)
        RETURNING run_id;
        """,
        ("storage_validation", "success", "storage smoke test"),
    )
    inserted_id = cur.fetchone()[0]
    assert inserted_id is not None
