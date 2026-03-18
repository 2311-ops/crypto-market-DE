"""Poll CoinGecko and publish raw market data to Kafka."""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

try:
    from .config import Settings
except ImportError:  # pragma: no cover - allows running as a standalone script
    from config import Settings


logger = logging.getLogger("ingestion.producer")
stop_requested = False


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _handle_stop(signum, frame):  # type: ignore[override]
    global stop_requested
    logger.info("Received signal %s, shutting down gracefully...", signum)
    stop_requested = True


def build_producer(bootstrap_servers: List[str]) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        retries=3,
        api_version_auto_timeout_ms=15000,
    )


def fetch_markets(session: requests.Session, settings: Settings) -> List[Dict[str, Any]]:
    params = {
        "vs_currency": settings.vs_currency,
        "ids": ",".join(settings.coins),
        "order": settings.order,
        "per_page": settings.per_page,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    response = session.get(
        f"{settings.coingecko_api_base}/coins/markets",
        params=params,
        headers=settings.coingecko_headers(),
        timeout=settings.request_timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def build_message(payload: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "coins": payload,
    }


def main() -> None:
    load_dotenv()
    settings = Settings.from_env()
    _setup_logging(settings.log_level)
    logger.info("Starting ingestion with settings: %s", settings)

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    producer: KafkaProducer | None = None
    for attempt in range(1, 6):
        try:
            producer = build_producer(settings.kafka_bootstrap_servers)
            logger.info("Connected to Kafka on attempt %d", attempt)
            break
        except NoBrokersAvailable as exc:
            wait = min(5 * attempt, 30)
            logger.warning("Kafka not ready (attempt %d): %s. Retrying in %ss", attempt, exc, wait)
            time.sleep(wait)
    if producer is None:
        logger.error("Failed to connect to Kafka after retries; exiting.")
        sys.exit(1)
    session = requests.Session()

    while not stop_requested:
        start = time.time()
        try:
            data = fetch_markets(session, settings)
            message = build_message(data)
            producer.send(settings.kafka_topic_raw, value=message)
            producer.flush()
            logger.info("Published %d coins to topic %s", len(data), settings.kafka_topic_raw)
        except Exception as exc:  # broad to keep loop alive
            logger.exception("Ingestion cycle failed: %s", exc)

        elapsed = time.time() - start
        sleep_for = max(settings.poll_interval_seconds - elapsed, 1)
        time.sleep(sleep_for)

    logger.info("Flushing producer before exit...")
    producer.flush()
    producer.close()
    session.close()
    logger.info("Ingestion stopped cleanly.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Fatal error in producer")
        sys.exit(1)
