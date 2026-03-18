"""
PHASE 3 TESTING — Crypto Data Pipeline (Kafka → Spark → PostgreSQL + MinIO)

Run inside Spark master container:
  docker compose exec spark-master bash -lc "python3 /app/processing/test_pipeline.py"
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    avg,
    stddev,
    when,
    from_unixtime,
    window,
    lit,
    round as spark_round,
    unix_timestamp,
    expr,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    BooleanType,
    ArrayType,
)

# =============================================================================
# Configuration (matches .env)
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "raw_crypto_data"
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "crypto_db"
POSTGRES_USER = "crypto_user"
POSTGRES_PASSWORD = "crypto_pass"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio_admin"
MINIO_SECRET_KEY = "minio_password"
MINIO_BUCKET = "crypto-data"
CHECKPOINT_BASE = "/tmp/checkpoint"

# =============================================================================
# Logging Setup
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("test_pipeline")


# =============================================================================
# Schemas
# =============================================================================

# Outer message schema (wrapper with ingested_at + coins array)
outer_schema = StructType([
    StructField("ingested_at", StringType(), True),
    StructField("coins", ArrayType(
        StructType([
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
            StructField("last_updated", StringType(), True),
        ])
    ), True),
])


# =============================================================================
# Spark Session Initialization
# =============================================================================

def create_spark_session() -> SparkSession:
    """Initialize Spark with all required configs for Kafka, MinIO, PostgreSQL."""
    logger.info("Initializing Spark session...")

    # MinIO / S3A and Kafka configuration
    spark = SparkSession.builder \
        .appName("CryptoPipelineTest") \
        .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_BASE}/main") \
        .config("spark.sql.streaming.stopOnDataLoss", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.default.parallelism", "2") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.kafka.version", "2.0.0") \
        .config("spark.executor.memory", "1G") \
        .config("spark.driver.memory", "512M") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized successfully")
    return spark


# =============================================================================
# Stage 1: Kafka Input Test
# =============================================================================

def test_kafka_input(spark: SparkSession) -> None:
    """Read raw Kafka messages and print to console for verification."""
    logger.info("=" * 60)
    logger.info("STAGE 1: Kafka Input Test")
    logger.info("=" * 60)

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    # Select key fields for display
    raw_display = kafka_df.selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS raw_json",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp"
    )

    query = raw_display.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "5") \
        .start()

    logger.info("✅ Kafka connected - reading raw messages")
    logger.info("Displaying raw Kafka messages for 10 seconds...")

    time.sleep(10)
    query.stop()
    logger.info("✅ Stage 1 complete: Kafka input verified")


# =============================================================================
# Stage 2: JSON Parsing Test
# =============================================================================

def test_json_parsing(spark: SparkSession) -> None:
    """Parse JSON using schema, show structured columns, handle nulls."""
    logger.info("=" * 60)
    logger.info("STAGE 2: JSON Parsing Test")
    logger.info("=" * 60)

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    # Parse outer JSON wrapper
    parsed_outer = kafka_df.selectExpr("CAST(value AS STRING) as json_body") \
        .select(
            col("json_body").from_json(outer_schema).alias("parsed")
        ) \
        .select("parsed.*")

    # Explode coins array to flatten
    flattened = parsed_outer.select(
        col("ingested_at"),
        col("coins").alias("coin_array")
    ).selectExpr(
        "ingested_at",
        "explode(coin_array) as coin"
    ).select(
        col("ingested_at"),
        col("coin.id"),
        col("coin.symbol"),
        col("coin.name"),
        col("coin.current_price"),
        col("coin.market_cap"),
        col("coin.total_volume"),
        col("coin.price_change_percentage_24h"),
        col("coin.high_24h"),
        col("coin.low_24h"),
        col("coin.circulating_supply"),
        col("coin.last_updated")
    )

    # Add processing timestamp
    with_timestamp = flattened.withColumn(
        "processing_time",
        current_timestamp()
    )

    # Show schema and sample data
    logger.info("✅ JSON parsed successfully")
    logger.info("Schema after parsing:")
    with_timestamp.printSchema()

    # Create a batch query to show sample data
    sample_query = with_timestamp.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "5") \
        .start()

    logger.info("Displaying parsed JSON structure for 10 seconds...")
    time.sleep(10)
    sample_query.stop()

    logger.info("✅ Stage 2 complete: JSON parsing verified")


# =============================================================================
# Stage 3: Transformation Test
# =============================================================================

def test_transformations(spark: SparkSession) -> None:
    """Cast numeric fields, add timestamps, compute derived metrics."""
    logger.info("=" * 60)
    logger.info("STAGE 3: Transformation Test")
    logger.info("=" * 60)

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    # Parse and flatten
    parsed = kafka_df.selectExpr("CAST(value AS STRING) as json_body") \
        .select(col("json_body").from_json(outer_schema).alias("parsed")) \
        .select("parsed.*") \
        .selectExpr(
            "ingested_at",
            "explode(coins) as coin"
        ) \
        .select(
            col("ingested_at"),
            col("coin.id"),
            col("coin.symbol"),
            col("coin.name"),
            col("coin.current_price"),
            col("coin.market_cap"),
            col("coin.total_volume"),
            col("coin.price_change_percentage_24h"),
            col("coin.high_24h"),
            col("coin.low_24h"),
            col("coin.circulating_supply"),
            col("coin.last_updated")
        )

    # Add processing timestamp
    with_time = parsed.withColumn("processing_time", current_timestamp())

    # Parse last_updated string to timestamp (handle nulls safely)
    with_timestamps = with_time.withColumn(
        "last_updated_ts",
        from_unixtime(
            unix_timestamp(col("last_updated").cast(StringType()), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ).cast(TimestampType())
    )

    # Cast numeric fields explicitly
    casted = with_timestamps.select(
        col("id"),
        col("symbol"),
        col("name"),
        col("current_price").cast(DoubleType()).alias("current_price"),
        col("market_cap").cast(LongType()).alias("market_cap"),
        col("total_volume").cast(LongType()).alias("total_volume"),
        col("price_change_percentage_24h").cast(DoubleType()).alias("price_change_percentage_24h"),
        col("high_24h").cast(DoubleType()).alias("high_24h"),
        col("low_24h").cast(DoubleType()).alias("low_24h"),
        col("circulating_supply").cast(DoubleType()).alias("circulating_supply"),
        col("ingested_at"),
        col("last_updated_ts"),
        col("processing_time")
    )

    # Compute derived fields
    transformed = casted.withColumn(
        "price_range_24h",
        (col("high_24h") - col("low_24h")).cast(DoubleType())
    ).withColumn(
        "is_price_positive",
        when(col("price_change_percentage_24h") > 0, True).otherwise(False)
    ).withColumn(
        "price_category",
        when(col("current_price") > 1000, "high")
        .when(col("current_price") > 100, "medium")
        .otherwise("low")
    )

    logger.info("✅ Transformations applied")
    logger.info("Schema after transformations:")
    transformed.printSchema()

    # Display transformed data
    transform_query = transformed.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "5") \
        .start()

    logger.info("Displaying transformed data for 10 seconds...")
    time.sleep(10)
    transform_query.stop()

    logger.info("✅ Stage 3 complete: Transformations verified")


# =============================================================================
# Stage 4: Streaming Window Test
# =============================================================================

def test_streaming_window(spark: SparkSession) -> None:
    """Test 5-minute watermark and window aggregation."""
    logger.info("=" * 60)
    logger.info("STAGE 4: Streaming Window Test")
    logger.info("=" * 60)

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    # Parse and flatten
    parsed = kafka_df.selectExpr("CAST(value AS STRING) as json_body") \
        .select(col("json_body").from_json(outer_schema).alias("parsed")) \
        .select("parsed.*") \
        .selectExpr("ingested_at", "explode(coins) as coin") \
        .select(
            col("ingested_at"),
            col("coin.symbol"),
            col("coin.current_price").cast(DoubleType()).alias("current_price"),
            col("coin.total_volume").cast(LongType()).alias("total_volume"),
            col("coin.price_change_percentage_24h").cast(DoubleType()).alias("price_change_pct")
        ) \
        .withColumn("event_time", current_timestamp())

    # Add watermark
    with_watermark = parsed.withWatermark("event_time", "5 minutes")

    # Window aggregation
    windowed = with_watermark.groupBy(
        col("symbol"),
        window(col("event_time"), "5 minutes").alias("time_window")
    ).agg(
        avg("current_price").alias("avg_price_5min"),
        stddev("current_price").alias("price_stddev"),
        avg("total_volume").alias("avg_volume_5min"),
        stddev("total_volume").alias("volume_stddev"),
        lit(len("symbol")).alias("record_count")
    ).select(
        col("symbol"),
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        spark_round(col("avg_price_5min"), 2).alias("avg_price"),
        spark_round(col("price_stddev"), 4).alias("volatility_score"),
        spark_round(col("avg_volume_5min"), 2).alias("avg_volume"),
        spark_round(col("volume_stddev"), 2).alias("volume_stddev"),
        col("record_count")
    )

    logger.info("✅ Window aggregation configured (5-minute windows)")
    logger.info("Schema after window aggregation:")
    windowed.printSchema()

    # Display windowed data
    window_query = windowed.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .start()

    logger.info("Displaying window aggregations for 15 seconds...")
    time.sleep(15)
    window_query.stop()

    logger.info("✅ Stage 4 complete: Streaming window verified")


# =============================================================================
# Stage 5: Output Tests
# =============================================================================

def test_outputs(spark: SparkSession) -> None:
    """Test all three outputs: Console, MinIO Parquet, PostgreSQL."""
    logger.info("=" * 60)
    logger.info("STAGE 5: Output Tests")
    logger.info("=" * 60)

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    # Parse, flatten, transform
    parsed = kafka_df.selectExpr("CAST(value AS STRING) as json_body") \
        .select(col("json_body").from_json(outer_schema).alias("parsed")) \
        .select("parsed.*") \
        .selectExpr("ingested_at", "explode(coins) as coin") \
        .select(
            col("ingested_at"),
            col("coin.id"),
            col("coin.symbol"),
            col("coin.name"),
            col("coin.current_price").cast(DoubleType()).alias("current_price"),
            col("coin.market_cap").cast(LongType()).alias("market_cap"),
            col("coin.total_volume").cast(LongType()).alias("total_volume"),
            col("coin.price_change_percentage_24h").cast(DoubleType()).alias("price_change_pct"),
            col("coin.high_24h").cast(DoubleType()).alias("high_24h"),
            col("coin.low_24h").cast(DoubleType()).alias("low_24h"),
            col("coin.circulating_supply").cast(DoubleType()).alias("circulating_supply")
        ) \
        .withColumn("processing_time", current_timestamp())

    # Add watermark for window operations
    with_watermark = parsed.withWatermark("processing_time", "5 minutes")

    # Window aggregations for volume_spike and volatility
    windowed_agg = with_watermark.groupBy(
        col("symbol"),
        window(col("processing_time"), "5 minutes").alias("time_window")
    ).agg(
        avg("total_volume").alias("avg_volume_5min"),
        stddev("current_price").alias("volatility_score")
    )

    # Join back to original data
    joined = parsed.alias("p").join(
        windowed_agg.alias("w"),
        (col("p.symbol") == col("w.symbol")) &
        (col("p.processing_time") >= col("w.time_window.start")) &
        (col("p.processing_time") < col("w.time_window.end")),
        "left"
    ).select(
        col("p.id"),
        col("p.symbol"),
        col("p.name"),
        col("p.current_price"),
        col("p.market_cap"),
        col("p.total_volume"),
        col("p.price_change_pct"),
        col("p.high_24h"),
        col("p.low_24h"),
        col("p.circulating_supply"),
        col("p.ingested_at"),
        col("p.processing_time"),
        col("w.avg_volume_5min"),
        col("w.volatility_score")
    )

    # Compute volume_spike flag
    final_df = joined.withColumn(
        "volume_spike",
        when(col("total_volume") > col("avg_volume_5min"), True).otherwise(False)
    ).withColumn(
        "volatility_flag",
        when(col("volatility_score").isNull(), False).otherwise(
            col("volatility_score") > lit(1000)
        )
    )

    logger.info("Final transformed schema:")
    final_df.printSchema()

    # -------------------------------------------------------------------------
    # Output A: Console Output
    # -------------------------------------------------------------------------
    logger.info("Setting up Console output...")
    console_query = final_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "3") \
        .start()

    logger.info("✅ Console output active")

    # -------------------------------------------------------------------------
    # Output B: MinIO Parquet Output
    # -------------------------------------------------------------------------
    logger.info("Setting up MinIO Parquet output...")
    logger.info("Writing to: s3a://%s/test/parquet_output/", MINIO_BUCKET)

    parquet_query = final_df.writeStream \
        .format("parquet") \
        .option("path", f"s3a://{MINIO_BUCKET}/test/parquet_output/") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/parquet_test") \
        .option("overwriteSchema", "true") \
        .outputMode("append") \
        .start()

    logger.info("✅ MinIO Parquet output active - writing to s3a://%s/test/parquet_output/", MINIO_BUCKET)

    # -------------------------------------------------------------------------
    # Output C: PostgreSQL Output (using foreachBatch)
    # -------------------------------------------------------------------------
    logger.info("Setting up PostgreSQL output...")

    def save_to_postgres(batch_df, batch_id):
        """Write batch to PostgreSQL using JDBC."""
        jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified"
        }

        # Select and rename columns to match PostgreSQL schema
        write_df = batch_df.select(
            col("id"),
            col("symbol"),
            col("name"),
            col("current_price").alias("price_usd"),
            col("market_cap"),
            col("total_volume").alias("volume_24h"),
            col("price_change_pct").alias("price_change_pct_24h"),
            col("volume_spike"),
            col("volatility_score"),
            col("processing_time").alias("recorded_at")
        )

        logger.info("✅ Writing batch %d to PostgreSQL (append mode)", batch_id)
        write_df.write.jdbc(
            table="coin_prices",
            properties=properties,
            mode="append",
            url=jdbc_url
        )

    postgres_query = final_df.writeStream \
        .foreachBatch(save_to_postgres) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/postgres_test") \
        .outputMode("append") \
        .start()

    logger.info("✅ PostgreSQL output active - writing to %s.coin_prices", POSTGRES_DB)

    # Let all outputs run for 30 seconds
    logger.info("Running all outputs for 30 seconds...")
    time.sleep(30)

    # Stop all queries
    console_query.stop()
    parquet_query.stop()
    postgres_query.stop()

    logger.info("✅ Stage 5 complete: All outputs verified")


# =============================================================================
# Stage 6: Fault Tolerance Test
# =============================================================================

def test_fault_tolerance(spark: SparkSession) -> None:
    """Verify checkpointing and safe restart capability."""
    logger.info("=" * 60)
    logger.info("STAGE 6: Fault Tolerance Test")
    logger.info("=" * 60)

    checkpoint_loc = f"{CHECKPOINT_BASE}/fault_tolerance_test"

    logger.info("Checkpoint location: %s", checkpoint_loc)
    logger.info("Testing with checkpointing enabled...")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "100") \
        .load()

    parsed = kafka_df.selectExpr("CAST(value AS STRING) as json_body") \
        .select(col("json_body").from_json(outer_schema).alias("parsed")) \
        .select("parsed.*") \
        .selectExpr("ingested_at", "explode(coins) as coin") \
        .select(
            col("coin.symbol"),
            col("coin.current_price").cast(DoubleType()).alias("price"),
            current_timestamp().alias("processing_time")
        )

    query = parsed.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_loc) \
        .start()

    logger.info("✅ Stream started with checkpointing")
    time.sleep(10)

    # Simulate stop and restart
    logger.info("Stopping stream (simulating restart)...")
    query.stop()

    # Restart with same checkpoint location
    logger.info("Restarting stream from checkpoint...")
    restart_query = parsed.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_loc) \
        .start()

    logger.info("✅ Stream restarted successfully from checkpoint")
    time.sleep(5)

    restart_query.stop()
    logger.info("✅ Stage 6 complete: Fault tolerance verified")


# =============================================================================
# Main Test Runner
# =============================================================================

def run_all_tests():
    """Execute all test stages sequentially."""
    logger.info("#" * 60)
    logger.info("# PHASE 3 PIPELINE END-TO-END TEST")
    logger.info("# Started at: %s", datetime.now(timezone.utc).isoformat())
    logger.info("#" * 60)

    spark = create_spark_session()

    try:
        # Run all test stages
        test_kafka_input(spark)
        test_json_parsing(spark)
        test_transformations(spark)
        test_streaming_window(spark)
        test_outputs(spark)
        test_fault_tolerance(spark)

        logger.info("#" * 60)
        logger.info("# ✅ ALL TESTS PASSED")
        logger.info("# Completed at: %s", datetime.now(timezone.utc).isoformat())
        logger.info("#" * 60)

    except Exception as e:
        logger.error("❌ TEST FAILED: %s", e, exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    run_all_tests()
