"""
PySpark schemas for crypto data pipeline.

Defines schemas for:
1. Raw Kafka JSON messages from CoinGecko API
2. Processed/enriched data for downstream storage
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    BooleanType,
)


# =============================================================================
# Raw Schema - Kafka JSON messages from CoinGecko API
# =============================================================================

raw_crypto_schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("symbol", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("current_price", DoubleType(), nullable=True),
    StructField("market_cap", LongType(), nullable=True),
    StructField("total_volume", LongType(), nullable=True),
    StructField("price_change_percentage_24h", DoubleType(), nullable=True),
    StructField("high_24h", DoubleType(), nullable=True),
    StructField("low_24h", DoubleType(), nullable=True),
    StructField("circulating_supply", DoubleType(), nullable=True),
    StructField("total_supply", DoubleType(), nullable=True),
    StructField("max_supply", DoubleType(), nullable=True),
    StructField("ath", DoubleType(), nullable=True),
    StructField("atl", DoubleType(), nullable=True),
    StructField("last_updated", TimestampType(), nullable=True),
])


# =============================================================================
# Processed Schema - Enriched data for PostgreSQL/MinIO storage
# =============================================================================

processed_crypto_schema = StructType([
    # Original fields
    StructField("id", StringType(), nullable=False),
    StructField("symbol", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("current_price", DoubleType(), nullable=True),
    StructField("market_cap", LongType(), nullable=True),
    StructField("total_volume", LongType(), nullable=True),
    StructField("price_change_percentage_24h", DoubleType(), nullable=True),

    # Derived/enriched fields
    StructField("price_category", StringType(), nullable=True),
    StructField("volume_category", StringType(), nullable=True),
    StructField("market_cap_category", StringType(), nullable=True),

    # Risk indicators
    StructField("is_near_ath", BooleanType(), nullable=True),
    StructField("is_near_atl", BooleanType(), nullable=True),
    StructField("volatility_24h", DoubleType(), nullable=True),

    # Timestamps
    StructField("processing_timestamp", TimestampType(), nullable=False),
    StructField("data_date", StringType(), nullable=False),

    # Metadata
    StructField("data_source", StringType(), nullable=False),
    StructField("pipeline_version", StringType(), nullable=False),
])


# =============================================================================
# Helper Functions
# =============================================================================

def get_raw_schema() -> StructType:
    """Return the schema for raw Kafka messages."""
    return raw_crypto_schema


def get_processed_schema() -> StructType:
    """Return the schema for processed/enriched data."""
    return processed_crypto_schema
