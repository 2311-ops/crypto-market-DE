"""
Unit tests for ingestion.producer (Phase 7).
"""

import json
from unittest.mock import MagicMock, patch

import pytest

try:
    import kafka  # noqa: F401
except ImportError:
    pytest.skip("kafka-python not installed; install ingestion/requirements.txt", allow_module_level=True)

from ingestion import producer
from ingestion.config import Settings


def test_build_message_includes_ingested_at_and_coins():
    payload = [{"id": "btc"}, {"id": "eth"}]
    msg = producer.build_message(payload)
    assert "ingested_at" in msg
    assert msg["coins"] == payload


@patch("ingestion.producer.KafkaProducer")
def test_build_producer_configures_serializer(mock_kafka):
    bootstrap = ["kafka:9092"]
    producer.build_producer(bootstrap)
    assert mock_kafka.call_args.kwargs["bootstrap_servers"] == bootstrap
    # verify serializer works as expected
    serializer = mock_kafka.call_args.kwargs["value_serializer"]
    encoded = serializer({"a": 1})
    assert encoded == json.dumps({"a": 1}).encode("utf-8")


@patch("ingestion.producer.requests.Session")
def test_fetch_markets_calls_api(mock_session):
    fake_response = MagicMock()
    fake_response.json.return_value = [{"id": "btc"}]
    fake_response.raise_for_status.return_value = None
    mock_session.return_value.get.return_value = fake_response

    settings = Settings(
        coingecko_api_base="http://example.com",
        coingecko_api_key=None,
        kafka_bootstrap_servers=["kafka:9092"],
        kafka_topic_raw="raw",
        coins=["btc"],
        vs_currency="usd",
        order="market_cap_desc",
        per_page=10,
        poll_interval_seconds=1,
        request_timeout_seconds=5,
        log_level="INFO",
    )

    result = producer.fetch_markets(mock_session.return_value, settings)
    assert result == [{"id": "btc"}]
    mock_session.return_value.get.assert_called_once()
