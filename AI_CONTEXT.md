# AI Context File — Real-Time Crypto Market Data Engineering Pipeline

> This file is the single source of truth for AI assistants working on this project.
> Read this before writing any code, suggesting any architecture, or answering any questions.

---

## Project Identity

**Name:** Real-Time Crypto Market Data Engineering Pipeline
**Type:** Production-grade Data Engineering Portfolio Project
**Goal:** Ingest live crypto market data, stream it, process it, store it, and visualize it end-to-end.

---

## Tech Stack (Authoritative)

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| Data Ingestion | CoinGecko Public API |
| Event Streaming | Apache Kafka |
| Stream Processing | Apache Spark Streaming (PySpark) |
| Relational Storage | PostgreSQL |
| Data Lake | MinIO (S3-compatible) |
| Orchestration | Apache Airflow |
| Visualization | Metabase or Power BI |
| Infrastructure | Docker + Docker Compose |
| Version Control | Git + GitHub |

**Do not suggest alternative technologies unless the user explicitly asks.**

---

## Project Architecture (High Level)

```
CoinGecko API
     │
     ▼
[Ingestion Service] ──► Kafka Topic: raw_crypto_data
                                │
                                ▼
                    [Spark Streaming Job]
                      /              \
                     ▼                ▼
               PostgreSQL           MinIO
             (processed DB)       (data lake /
                  │                Parquet files)
                  ▼
            Metabase / Power BI
                (Dashboards)
                  │
             Airflow DAGs
          (Orchestrate pipeline)
```

---

## Directory Structure

```
crypto-pipeline/
├── docker-compose.yml
├── .env
├── README.md
├── AI_CONTEXT.md               ← this file
├── docs/
│   └── PROJECT_DOCUMENTATION.docx
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py             ← Polls CoinGecko, publishes to Kafka
│   └── config.py
├── processing/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── spark_streaming_job.py  ← Consumes Kafka, processes, writes to PG + MinIO
│   └── schemas.py
├── storage/
│   ├── postgres/
│   │   └── init.sql            ← DB schema initialization
│   └── minio/
│       └── setup.py            ← Bucket creation script
├── orchestration/
│   └── dags/
│       ├── crypto_pipeline_dag.py
│       └── data_quality_dag.py
├── visualization/
│   └── dashboards/
│       └── metabase_config.json
├── tests/
│   ├── test_producer.py
│   ├── test_spark_job.py
│   └── test_storage.py
└── scripts/
    └── health_check.py
```

---

## Task List (Ordered — Work Top to Bottom)

### PHASE 1 — Infrastructure Setup
- [x] **TASK-01** — Write `docker-compose.yml` with: Kafka + Zookeeper, PostgreSQL, MinIO, Airflow, Spark, Metabase
- [x] **TASK-02** — Write `.env` file template with all required environment variables
- [x] **TASK-03** — Write `storage/postgres/init.sql` with schema for `coin_prices`, `market_stats`, `pipeline_logs`

### PHASE 2 — Data Ingestion
- [x] **TASK-04** — Write `ingestion/config.py` with CoinGecko API settings and Kafka config
- [x] **TASK-05** — Write `ingestion/producer.py` that polls CoinGecko `/coins/markets` every 60s and publishes JSON to Kafka topic `raw_crypto_data`
- [x] **TASK-06** — Write `ingestion/Dockerfile` and `ingestion/requirements.txt`

### PHASE 3 — Stream Processing
- [x] **TASK-07** — Write `processing/schemas.py` with PySpark and Avro schemas for raw and processed data
- [x] **TASK-08** — Write `processing/spark_streaming_job.py` that:
  - Reads from Kafka topic `raw_crypto_data`
  - Parses and validates JSON
  - Computes: price change %, volume spike flag, volatility score (rolling 5-min window)
  - Writes enriched records to PostgreSQL
  - Writes raw + enriched Parquet files to MinIO
- [x] **TASK-09** — Write `processing/Dockerfile` and `processing/requirements.txt`

### PHASE 4 — Storage Layer
- [x] **TASK-10** — Write `storage/minio/setup.py` to create MinIO buckets (`raw-data`, `processed-data`)
- [x] **TASK-11** — Validate PostgreSQL schema by writing and running test INSERT/SELECT queries

### PHASE 5 — Orchestration
- [x] **TASK-12** — Write `orchestration/dags/crypto_pipeline_dag.py` Airflow DAG that:
  - Schedules ingestion every 60s
  - Triggers Spark job
  - Runs data quality checks
- [x] **TASK-13** — Write `orchestration/dags/data_quality_dag.py` with row count and null checks

### PHASE 6 — Visualization
- [x] **TASK-14** — Write SQL queries for powerbi dashboards: top coins by volume, price trends, volatility heatmap
- [x] **TASK-15** — Document Power BI dashboard setup steps in `visualization/dashboards/`

### PHASE 7 — Testing & Health Checks
- [x] **TASK-16** — Write `tests/test_producer.py` with mocked CoinGecko API tests
- [x] **TASK-17** — Write `tests/test_spark_job.py` with PySpark unit tests on transformations
- [x] **TASK-18** — Write `scripts/health_check.py` to ping all services and report status
- [x] **TASK-19** — Write `README.md` with setup instructions, architecture diagram, and usage guide

---

## Key Data Contracts

### CoinGecko API Response Fields Used
```json
{
  "id": "bitcoin",
  "symbol": "btc",
  "name": "Bitcoin",
  "current_price": 68000.0,
  "market_cap": 1340000000000,
  "total_volume": 25000000000,
  "price_change_percentage_24h": 2.5,
  "last_updated": "2024-01-01T00:00:00Z"
}
```

### Kafka Message Schema (raw_crypto_data)
```json
{
  "ingested_at": "<ISO timestamp>",
  "coins": [ /* array of CoinGecko coin objects */ ]
}
```

### PostgreSQL Tables
- `coin_prices(id, symbol, name, price_usd, market_cap, volume_24h, price_change_pct_24h, recorded_at)`
- `market_stats(id, symbol, volatility_score, volume_spike, window_start, window_end)`
- `pipeline_logs(run_id, stage, status, message, created_at)`

---

## Rules for AI Assistants

1. **Always use Python** — no other language unless asked.
2. **All services run in Docker** — never assume local installs.
3. **Use environment variables** from `.env` — never hardcode secrets.
4. **Kafka topic name is** `raw_crypto_data` — do not rename it.
5. **MinIO bucket names are** `raw-data` and `processed-data`.
6. **PostgreSQL database name is** `crypto_db`, user is `crypto_user`.
7. **Always add logging** using Python's `logging` module.
8. **Tasks are sequential** — complete PHASE 1 before PHASE 2, etc.
9. **When a task is complete**, mark it `[x]` in this file.
10. **Ask before deviating** from the architecture described above.

---

## Troubleshooting & Operational Notes

### Kafka Connection Issues

**Symptom**: `kafka.errors.NoBrokersAvailable: NoBrokersAvailable` or Kafka container fails to start.

**Root Causes & Solutions**:

1. **Zookeeper not ready**: Always start Zookeeper before Kafka.
   ```bash
   docker compose up -d zookeeper
   sleep 5
   docker compose up -d kafka
   ```

2. **Cluster ID mismatch** (Kafka fails with `InconsistentClusterIdException`):
   - Occurs when Kafka volume persists from a previous cluster
   - **Fix**: Remove all volumes and recreate:
     ```bash
     docker compose down -v
     docker compose up -d zookeeper kafka
     ```

3. **Kafka not healthy**: Check health status:
   ```bash
   docker compose ps
   ```
   Kafka should show `(healthy)` in STATUS. If not, check logs:
   ```bash
   docker compose logs kafka
   ```

### CoinGecko API Configuration

**Important**: The API key determines which endpoint to use:
- **Free tier (no API key or demo key)**: Use `https://api.coingecko.com/api/v3`
- **Pro tier**: Use `https://pro-api.coingecko.com/api/v3`

**Common Error Signatures**:
- `error_code: 10010` → Using pro endpoint with demo/free key → Switch to free endpoint
- `error_code: 10011` → Using free endpoint with pro key → Switch to pro endpoint

**Current Configuration (Working)**:
- Using free tier (no API key needed)
- `COINGECKO_API_BASE=https://api.coingecko.com/api/v3`
- `COINGECKO_API_KEY` is commented out in `.env`

### Network & Ports

All services communicate via Docker network `crypto_network`:
- **Zookeeper**: `172.19.0.2:2181` (port 2181 exposed to host)
- **Kafka**: `172.19.0.3:9092` (port 9092 exposed to host, `kafka:9092` internal)
- **Ingestion → Kafka**: Uses `kafka:9092` (service name resolution)
- **External access**: Kafka also listens on `localhost:29092` (EXTERNAL listener)

### Testing Connectivity

**Test Kafka connection from ingestion**:
```bash
docker compose exec ingestion python -c "
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
print('✅ Kafka connection OK')
"
```

**Test CoinGecko API**:
```bash
docker compose exec ingestion python ingestion/test_api_no_key.py
```

### Service Startup Order

1. `docker compose up -d zookeeper` (wait 5-10 seconds)
2. `docker compose up -d kafka` (wait until healthy)
3. `docker compose up -d ingestion`
4. Start other services (PostgreSQL, MinIO, Spark, Airflow) in any order

### Volume Management

If you encounter state-related errors (cluster ID mismatch, broker conflicts):
```bash
docker compose down -v  # Removes all volumes
docker compose up -d zookeeper kafka
```

### Health Checks

Kafka has a built-in healthcheck that runs every 15 seconds:
```yaml
healthcheck:
  test: ["CMD-SHELL", "kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1"]
  interval: 15s
  timeout: 10s
  retries: 8
  start_period: 20s
```

Monitor with: `docker compose ps`

### Running Phase 3 End-to-End Tests

- Ensure services are up (`docker compose up -d`) and ingestion is producing to Kafka topic `raw_crypto_data`.
- From project root, run the test suite inside Spark master using the bundled Python:
  ```bash
  docker compose exec spark-master bash -lc "python3 /opt/bitnami/spark/jobs/test_pipeline.py"
  ```
- Spark job code lives at `/opt/bitnami/spark/jobs/` (mounted from `./processing`).
- The test runner auto-downloads required connectors (Kafka + PostgreSQL) into `/tmp/.ivy2` inside the container.
- Preflight checks inside the test verify Kafka, PostgreSQL, and MinIO reachability and create the `raw-data` bucket if missing.
- Verification stage reads back sample rows from `coin_prices` (PostgreSQL) and Parquet files under `s3a://raw-data/test/parquet_output/`.

### Running Phase 4 Storage Checks

- Create MinIO buckets (idempotent):
  ```bash
  python storage/minio/setup.py
  ```
  (Requires `boto3`; install if needed: `pip install boto3`)
- Validate PostgreSQL schema with transactional smoke tests:
  ```bash
  pip install psycopg2-binary pytest
  docker compose up -d postgres
  pytest tests/test_storage.py
  ```
  Tests insert and roll back sample rows for `coin_prices`, `market_stats`, and `pipeline_logs`.
---
