# Real-Time Crypto Market Data Pipeline

This document is the detailed project overview for the crypto pipeline. It explains the repository structure, how data moves through the system, when data is updated, and how to visualize the results.

## What This Project Does

The project ingests live crypto market data from CoinGecko, publishes it to Kafka, processes it with Spark, stores it in PostgreSQL and MinIO, and exposes the results for dashboards in Metabase or Power BI.

The main business goal is simple:

- collect fresh crypto market snapshots
- turn them into structured analytical tables
- keep the data queryable for dashboards and reporting

Current runtime behavior:

- CoinGecko is polled every 60 seconds by the ingestion service.
- Airflow orchestrates one ingestion batch, then submits the Spark job, then triggers the data-quality DAG.
- Spark runs in `availableNow` mode for the current backlog and then exits.
- PostgreSQL is the dashboard source of truth.
- MinIO stores parquet outputs for durable raw and processed data.

## System Architecture

```text
CoinGecko API
    |
    v
Ingestion service  ---> Kafka topic: raw_crypto_data
    |
    v
Spark streaming job
    |---------------------> PostgreSQL (coin_prices, market_stats, pipeline_logs)
    |
    +---------------------> MinIO (raw-data, processed-data parquet)

PostgreSQL ---------------> Metabase / Power BI dashboards

Airflow orchestrates the ingestion, Spark job, and data quality checks.
```

## Repository Structure

```text
crypto-pipeline/
  .env
  docker-compose.yml
  README.md
  PROJECT_README.md
  RUNBOOK.md
  AI_CONTEXT.md
  scripts/
    health_check.py
  ingestion/
    config.py
    producer.py
    Dockerfile
    requirements.txt
    test_api.py
    test_api_no_key.py
    test_kafka.py
  processing/
    schemas.py
    spark_streaming_job.py
    backfill_minio_to_postgres.py
    test_pipeline.py
    Dockerfile
    requirements.txt
  storage/
    postgres/
      init.sql
    minio/
      setup.py
  orchestration/
    dags/
      crypto_pipeline_dag.py
      data_quality_dag.py
  visualization/
    dashboards/
      powerbi_queries.sql
      powerbi_setup.md
      metabase_config.json
  tests/
    test_producer.py
    test_spark_job.py
    test_storage.py
```

### What Each Area Does

- `ingestion/`: Fetches data from CoinGecko and publishes it to Kafka.
- `processing/`: Consumes Kafka data, enriches it, writes to PostgreSQL, and writes parquet outputs to MinIO.
- `storage/`: Creates database schema and MinIO buckets.
- `orchestration/`: Airflow DAGs that run the pipeline and data-quality checks.
- `visualization/`: SQL queries and setup guidance for dashboards.
- `scripts/`: Operational scripts such as service health checks.
- `tests/`: Local verification and regression tests.

## Runtime Services

The stack runs in Docker Compose:

- Zookeeper: coordinates Kafka
- Kafka: message bus for raw crypto events
- PostgreSQL: operational analytics database
- MinIO: S3-compatible data lake
- Airflow: orchestration layer
- Spark master/worker: stream processing engine
- Metabase: live dashboard tool

### Important Ports

- Kafka: `9092` and `29092`
- PostgreSQL: `5432`
- MinIO API: `9000`
- MinIO console: `9001`
- Spark UI: `8081`
- Airflow UI: `8088`
- Metabase UI: `3000`

## Data Flow

### 1) Ingestion

The ingestion service in [ingestion/producer.py](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/ingestion/producer.py) polls CoinGecko every 60 seconds and publishes a Kafka message for each batch.

Current coin set:

- Bitcoin
- Ethereum
- Solana

The ingestion message structure looks like this:

```json
{
  "ingested_at": "<ISO timestamp>",
  "coins": [
    {
      "id": "bitcoin",
      "symbol": "btc",
      "name": "Bitcoin",
      "current_price": 74288.0,
      "market_cap": 1340000000000,
      "total_volume": 44167880203,
      "price_change_percentage_24h": 2.5
    }
  ]
}
```

Each message is published to Kafka topic `raw_crypto_data`.

Implementation note:

- The Airflow DAG uses `kafka-python-ng` in the Airflow container because the repo currently runs on Python 3.12.
- The DAG task imports `KafkaProducer` at runtime and fails fast if the Kafka client or broker is unavailable.

### 2) Stream Processing

The Spark job in [processing/spark_streaming_job.py](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/processing/spark_streaming_job.py) consumes `raw_crypto_data`, flattens the coin array, and writes results to four places:

- PostgreSQL tables
- MinIO parquet files
- `coin_prices` and `market_stats` are the tables used by dashboards.
- `pipeline_logs` is populated by Airflow task helpers and is useful for operational tracing.

#### PostgreSQL outputs

- `coin_prices`
  - one row per coin snapshot
  - fields: `symbol`, `name`, `price_usd`, `market_cap`, `volume_24h`, `price_change_pct_24h`, `recorded_at`
- `market_stats`
  - derived per-symbol summary produced from the current Spark batch
  - fields: `symbol`, `volatility_score`, `volume_spike`, `window_start`, `window_end`
- `pipeline_logs`
  - operational log table populated by Airflow DAG tasks

#### MinIO outputs

- `raw-data/stream/raw_crypto_data/`
- `processed-data/stream/enriched/`

### 3) Orchestration

The main DAG in [orchestration/dags/crypto_pipeline_dag.py](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/orchestration/dags/crypto_pipeline_dag.py) is responsible for:

- running one ingestion batch
- submitting the Spark job on the Spark master container
- triggering the data-quality DAG

The data-quality DAG in [orchestration/dags/data_quality_dag.py](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/orchestration/dags/data_quality_dag.py) checks:

- row counts
- null values
- pipeline logging

### 4) Visualization

Metabase and Power BI both query PostgreSQL directly. They do not read Kafka.

The dashboard layer uses:

- [visualization/dashboards/powerbi_queries.sql](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/visualization/dashboards/powerbi_queries.sql)
- [visualization/dashboards/powerbi_setup.md](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/visualization/dashboards/powerbi_setup.md)

Metabase is the easiest option for live dashboards because it queries the database directly and refreshes against current rows.

Suggested dashboard tables:

- `coin_prices` for latest price, market cap, and volume views
- `market_stats` for volatility and volume-spike analysis

## When Data Is Updated

This is the most important operational behavior.

### Ingestion cadence

- The ingestion service runs every 60 seconds.
- Every batch currently contains 3 coins.
- Each run creates a new Kafka message with a fresh `ingested_at` timestamp.

### Processing cadence

- The Spark job is run as a job-per-batch process.
- It reads the Kafka backlog available when the job starts.
- The job uses `availableNow` semantics, so it processes the current data and exits.
- The first run can backfill the topic backlog because the Kafka source starts at the earliest offset by default.
- After the first run, the checkpoint preserves progress and future runs only process new data.
- The Spark job writes both the raw snapshot table and the derived market stats table in the same batch path.

### Dashboard freshness

- Dashboards query PostgreSQL live.
- When a new ingestion + Spark cycle completes, the tables update.
- Metabase will show the new data after the next card or dashboard refresh.

### Practical freshness expectation

This is near-realtime, not push-streaming to the dashboard.

- Kafka and Spark are the pipeline backbone.
- PostgreSQL is the dashboard source.
- The effective freshness is usually minute-scale, depending on how often the pipeline runs.
- If Airflow is healthy, the scheduled DAG run keeps the tables advancing roughly once per minute.

## Current Behavioral Notes

These notes describe how the project behaves in its current local setup.

- `coin_prices` is the main fact table for price snapshots and volume analytics.
- `market_stats` is the derived summary table for volatility and spike analysis.
- `pipeline_logs` is an ops table and will grow when Airflow tasks run successfully.
- MinIO is used as durable object storage for parquet outputs.
- Metabase is the most convenient live visualization layer in this repo.
- Power BI support is documented, but it is not a separate running service in Docker Compose.

## How To Run The Project

### Start everything

```powershell
docker compose up -d
```

### Check service health

```powershell
python scripts/health_check.py
```

### Verify PostgreSQL data

```sql
SELECT COUNT(*) FROM coin_prices;
SELECT COUNT(*) FROM market_stats;
SELECT COUNT(*) FROM pipeline_logs;
```

### Verify latest rows

```sql
SELECT symbol, name, price_usd, volume_24h, recorded_at
FROM coin_prices
ORDER BY recorded_at DESC
LIMIT 10;

SELECT symbol, volatility_score, volume_spike, window_start, window_end
FROM market_stats
ORDER BY window_end DESC
LIMIT 10;
```

## Dashboard Setup

### Metabase

1. Open `http://localhost:3000`
2. Finish the first-time Metabase setup
3. Add a PostgreSQL database connection
4. Use:
   - host: `postgres`
   - port: `5432`
   - database: `crypto_db`
   - user: `crypto_user`
   - password: `crypto_pass`
5. Create questions from the SQL in `powerbi_queries.sql`
6. Save the questions
7. Add them to a dashboard
8. Refresh the dashboard to see new rows as the pipeline updates Postgres

### Power BI

Power BI uses the same Postgres database.

- Read the setup guide in [visualization/dashboards/powerbi_setup.md](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/visualization/dashboards/powerbi_setup.md)
- Use the queries in [visualization/dashboards/powerbi_queries.sql](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/visualization/dashboards/powerbi_queries.sql)
- For Metabase, replace the `:symbol` parameter with `{{symbol}}`

## Operational Utilities

- [storage/minio/setup.py](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/storage/minio/setup.py)
  - creates `raw-data` and `processed-data`
- [processing/backfill_minio_to_postgres.py](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/processing/backfill_minio_to_postgres.py)
  - backfills PostgreSQL from the parquet files already in MinIO
- [scripts/health_check.py](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/scripts/health_check.py)
  - checks service reachability from the host

## Data Validation Strategy

The project is easiest to validate in this order:

1. Kafka receives messages from the ingestion container.
2. Spark writes parquet files to MinIO.
3. PostgreSQL tables contain rows.
4. Metabase cards return results.
5. Airflow logs pipeline activity in `pipeline_logs`.

## Troubleshooting

### If Kafka does not start

- Start Zookeeper first.
- If Kafka has a stale volume or cluster mismatch, use `docker compose down -v` and start again.

### If Airflow does not start

- Check `docker compose logs airflow`
- Confirm the Airflow webserver port is `8088`
- Confirm the database connection variables in `.env`

### If dashboards are empty

- Check whether `coin_prices` and `market_stats` have rows
- Refresh the dashboard
- Confirm the pipeline ran recently
- If needed, use the backfill utility to repopulate from MinIO

### If `pipeline_logs` is empty

- That usually means the Airflow DAGs have not executed successfully yet
- The core dashboard tables can still be used without it

## Summary

This project is a Dockerized crypto data pipeline with:

- live ingestion from CoinGecko
- Kafka-based event transport
- Spark-based transformation
- PostgreSQL for dashboard queries
- MinIO for parquet storage
- Metabase or Power BI for visualization

The key dashboard tables are `coin_prices` and `market_stats`. They update whenever the ingestion and processing steps run successfully, and the dashboards read them live from PostgreSQL.

If you need the shortest mental model:

1. Ingestion publishes a new CoinGecko snapshot every minute.
2. Airflow runs the Spark job against the latest Kafka backlog.
3. Spark writes new rows to PostgreSQL and parquet to MinIO.
4. Metabase reads PostgreSQL directly, so dashboard refreshes show the latest committed rows.
