"""
Lightweight unit tests for processing logic (Phase 7).
"""

import pytest

from processing.spark_streaming_job import coin_schema, is_volume_spike


def test_coin_schema_fields():
    fields = {f.name for f in coin_schema}
    assert {"id", "symbol", "name", "current_price", "total_volume", "price_change_percentage_24h"} <= fields


def test_volume_spike_flag():
    assert is_volume_spike(100.0, 200.0) is False
    assert is_volume_spike(150.0, 200.0) is False  # equal threshold not greater
    assert is_volume_spike(250.0, 200.0) is True
    assert is_volume_spike(None, 200.0) is False
