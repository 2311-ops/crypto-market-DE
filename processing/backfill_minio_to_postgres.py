"""
Backfill PostgreSQL tables from the parquet output already written to MinIO.

This is a practical recovery path when the Kafka -> Spark streaming pipeline
has written parquet files successfully, but the JDBC sink has not populated
PostgreSQL yet.
"""

from __future__ import annotations

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    max as spark_max,
    min as spark_min,
    stddev,
    when,
    round as spark_round,
)


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
RAW_BUCKET = os.getenv("MINIO_BUCKET_RAW", "raw-data")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("CryptoPostgresBackfill")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    source_path = f"s3a://{RAW_BUCKET}/stream/raw_crypto_data/"
    df = spark.read.parquet(source_path)

    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    props = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified",
    }

    coin_prices = df.select(
        col("symbol"),
        col("name"),
        col("current_price").alias("price_usd"),
        col("market_cap"),
        col("total_volume").alias("volume_24h"),
        col("price_change_pct_24h"),
        col("processing_time").alias("recorded_at"),
    )

    market_stats = (
        df.groupBy("symbol")
        .agg(
            spark_round(stddev("current_price"), 6).alias("volatility_score"),
            avg("total_volume").alias("avg_volume_5min"),
            spark_max("total_volume").alias("max_volume"),
            spark_min("processing_time").alias("window_start"),
            spark_max("processing_time").alias("window_end"),
        )
        .select(
            col("symbol"),
            col("volatility_score"),
            when(col("max_volume") > col("avg_volume_5min"), True).otherwise(False).alias("volume_spike"),
            col("window_start"),
            col("window_end"),
        )
    )

    coin_count = coin_prices.count()
    stats_count = market_stats.count()
    print(f"Backfilling {coin_count} coin_prices rows and {stats_count} market_stats rows from {source_path}")

    if coin_count:
        coin_prices.write.jdbc(url=jdbc_url, table="coin_prices", mode="append", properties=props)
    if stats_count:
        market_stats.write.jdbc(url=jdbc_url, table="market_stats", mode="append", properties=props)

    spark.stop()


if __name__ == "__main__":
    main()
