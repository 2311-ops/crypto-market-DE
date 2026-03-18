# Runbook — Real-Time Crypto Market Data Pipeline

## Prereqs
- Docker & Docker Compose installed
- Ports available: 2181, 9092/29092, 5432, 9000/9001, 8080, 8088
- For local tests: Python 3.11+ with pip, Java (for PySpark)

## 1) Configure
Update `.env` if needed (defaults are safe for local).

## 2) Start the stack
```bash
docker compose up -d
```
Services: Zookeeper, Kafka, Postgres, MinIO, Airflow, Spark master/worker, ingestion.

## 3) Verify ingestion → Kafka
- Tail logs: `docker compose logs -f ingestion | head`
- Optional peek: `docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic raw_crypto_data --from-beginning --max-messages 1`

## 4) Run Spark streaming job
```bash
docker compose exec spark-master bash -lc "/opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.7.4,org.apache.kafka:kafka-clients:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/processing/spark_streaming_job.py"
```
Leave it running for a couple minutes to ingest/process.

## 5) Check outputs
- Postgres rows: `docker compose exec postgres psql -U crypto_user -d crypto_db -c "SELECT COUNT(*) FROM coin_prices;"`
- MinIO parquet:
  ```bash
  docker compose exec minio mc alias set local http://minio:9000 minio_admin minio_password
  docker compose exec minio mc ls --recursive local/processed-data/stream/enriched/ | head
  ```

## 6) UIs
- Spark UI: http://localhost:8080
- Airflow UI: http://localhost:8088 (admin / admin_password)
- Metabase UI: http://localhost:3000
- MinIO Console: http://localhost:9001 (minio_admin / minio_password)

## 7) Power BI / Metabase
- Postgres source: host `localhost`, db `crypto_db`, user `crypto_user`, pass `crypto_pass`.
- Queries and setup: `visualization/dashboards/powerbi_queries.sql`, `powerbi_setup.md`.
- For live dashboards, Metabase is the quickest path: connect it directly to the Postgres source above.

## 8) Tests (optional, local)
```bash
pip install --user -r processing/requirements.txt -r ingestion/requirements.txt
python -m pytest tests
```

## 9) Stop / clean
- Stop: `docker compose down`
- Reset volumes (if needed): `docker compose down -v`
