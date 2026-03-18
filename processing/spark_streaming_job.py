"""
Phase 3/4 Spark Streaming Job: Kafka -> Enrichment -> PostgreSQL + MinIO

Reads raw crypto messages from Kafka topic `raw_crypto_data`, computes rolling
metrics, writes:
  - Raw flattened data to MinIO bucket `raw-data`
  - Enriched data to MinIO bucket `processed-data`
  - Processed coin price records to PostgreSQL `coin_prices`
  - Windowed market statistics to PostgreSQL `market_stats`

Configuration is via environment variables (see .env).
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    avg,
    max as spark_max,
    min as spark_min,
    stddev,
    when,
    from_json,
    round as spark_round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    ArrayType,
)

# ---------------------------------------------------------------------------
# Environment + Defaults
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "raw_crypto_data")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw-data")
PROCESSED_BUCKET = os.getenv("MINIO_BUCKET_PROCESSED", "processed-data")

CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/tmp/checkpoints")

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

coin_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", LongType(), True),
    StructField("total_volume", LongType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("circulating_supply", DoubleType(), True),
    StructField("total_supply", DoubleType(), True),
    StructField("max_supply", DoubleType(), True),
    StructField("last_updated", StringType(), True),
])

outer_schema = StructType([
    StructField("ingested_at", StringType(), True),
    StructField("coins", ArrayType(coin_schema), True),
])

# ---------------------------------------------------------------------------
# Main streaming job
# ---------------------------------------------------------------------------

def is_volume_spike(total_volume, avg_volume_5min) -> bool:
    """Return True when the current volume exceeds the 5-minute average."""
    if total_volume is None or avg_volume_5min is None:
        return False
    return total_volume > avg_volume_5min


def run():
    spark = (
        SparkSession.builder
        .appName("CryptoDataStreaming")
        .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_BASE}/main")
        .config("spark.sql.streaming.stopOnDataLoss", "false")
        # MinIO / S3A config
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.driver.extraClassPath", "/tmp/.ivy2/jars/*")
        .config("spark.executor.extraClassPath", "/tmp/.ivy2/jars/*")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "100")
        .load()
    )

    raw_flat = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_body")
        .select(from_json(col("json_body"), outer_schema).alias("parsed"))
        .selectExpr("parsed.ingested_at", "explode(parsed.coins) AS coin")
        .select(
            col("ingested_at"),
            col("coin.id").alias("id"),
            col("coin.symbol").alias("symbol"),
            col("coin.name").alias("name"),
            col("coin.current_price").alias("current_price"),
            col("coin.market_cap").alias("market_cap"),
            col("coin.total_volume").alias("total_volume"),
            col("coin.price_change_percentage_24h").alias("price_change_pct_24h"),
            col("coin.high_24h").alias("high_24h"),
            col("coin.low_24h").alias("low_24h"),
            col("coin.circulating_supply").alias("circulating_supply"),
            col("coin.total_supply").alias("total_supply"),
            col("coin.max_supply").alias("max_supply"),
            col("coin.last_updated").alias("last_updated"),
        )
        .withColumn("processing_time", current_timestamp())
    )

    def process_batch(batch_df, batch_id):
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

        raw_out = batch_df.select(
            col("symbol"),
            col("name"),
            col("current_price").alias("price_usd"),
            col("market_cap"),
            col("total_volume").alias("volume_24h"),
            col("price_change_pct_24h"),
            col("processing_time").alias("recorded_at"),
            col("id"),
            col("high_24h"),
            col("low_24h"),
            col("circulating_supply"),
            col("total_supply"),
            col("max_supply"),
            col("last_updated"),
        )

        raw_out.write.format("parquet").mode("append").option(
            "path", f"s3a://{RAW_BUCKET}/stream/raw_crypto_data/"
        ).save()

        stats_base = (
            batch_df.groupBy("symbol")
            .agg(
                spark_round(avg("total_volume"), 6).alias("avg_volume_5min"),
                spark_round(stddev("current_price"), 6).alias("volatility_score"),
                spark_min("processing_time").alias("window_start"),
                spark_max("processing_time").alias("window_end"),
            )
        )

        spike_df = (
            batch_df.select("symbol", "total_volume")
            .join(stats_base.select("symbol", "avg_volume_5min"), on="symbol", how="left")
            .groupBy("symbol")
            .agg(
                spark_max(when(col("total_volume") > col("avg_volume_5min"), 1).otherwise(0)).alias(
                    "volume_spike_int"
                )
            )
        )

        stats_df = (
            stats_base.join(spike_df, on="symbol", how="left")
            .select(
                col("symbol"),
                col("volatility_score"),
                when(col("volume_spike_int") > 0, True).otherwise(False).alias("volume_spike"),
                col("window_start"),
                col("window_end"),
            )
        )

        processed_df = (
            raw_out.join(stats_df.select("symbol", "volatility_score", "volume_spike"), on="symbol", how="left")
            .select(
                col("symbol"),
                col("name"),
                col("price_usd"),
                col("market_cap"),
                col("volume_24h"),
                col("price_change_pct_24h"),
                col("recorded_at"),
                col("volatility_score"),
                col("volume_spike"),
            )
        )

        processed_df.write.format("parquet").mode("append").option(
            "path", f"s3a://{PROCESSED_BUCKET}/stream/enriched/"
        ).save()

        raw_out.select(
            col("symbol"),
            col("name"),
            col("price_usd"),
            col("market_cap"),
            col("volume_24h"),
            col("price_change_pct_24h"),
            col("recorded_at"),
        ).write.format("jdbc").option("url", jdbc_url).option("dbtable", "coin_prices").option(
            "user", POSTGRES_USER
        ).option("password", POSTGRES_PASSWORD).option("driver", "org.postgresql.Driver").mode("append").save()

        stats_df.write.format("jdbc").option("url", jdbc_url).option("dbtable", "market_stats").option(
            "user", POSTGRES_USER
        ).option("password", POSTGRES_PASSWORD).option("driver", "org.postgresql.Driver").mode("append").save()

    query = (
        raw_flat.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/main_pipeline")
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
