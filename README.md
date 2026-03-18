# Real-Time Crypto Market Data Engineering Pipeline

For a detailed overview of the architecture, data flow, update cadence, and dashboard behavior, see [PROJECT_README.md](/c:/Users/LOQ/Documents/Cypto_Market/crypto-pipeline/PROJECT_README.md).

Phases completed
- Phase 1-4: Infra, ingestion, streaming, storage operational.
- Phase 5: Airflow DAGs orchestrate ingestion, Spark, and data quality.
- Phase 6: Power BI queries and setup docs added.
- Phase 7: Tests and health checks added.

Quick start
1. `docker compose up -d`
2. Verify storage: `python -m pytest tests/test_storage.py`
3. Run streaming job: see `processing/spark_streaming_job.py` for the spark-submit command.
4. Airflow UI: http://localhost:8088 (admin/admin_password by default).
5. Metabase UI: http://localhost:3000 (first-time setup in browser).

Current runtime behavior
- Ingestion polls CoinGecko every 60 seconds.
- Airflow orchestrates ingestion, Spark processing, and data-quality checks.
- Spark processes the available Kafka backlog and writes to PostgreSQL and MinIO.
- Metabase reads PostgreSQL directly, so dashboard refreshes show the latest committed rows.

Run Spark streaming job
```powershell
docker compose exec spark-master bash -lc "/opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/processing/spark_streaming_job.py"
```

Tests
- Install local test deps: `pip install --user -r processing/requirements.txt -r ingestion/requirements.txt`
- Producer/unit: `python -m pytest tests/test_producer.py`
- Spark logic (local): `python -m pytest tests/test_spark_job.py`
- Storage: `python -m pytest tests/test_storage.py`

Health check
```powershell
python scripts/health_check.py
```

Power BI
- Queries: `visualization/dashboards/powerbi_queries.sql`
- Setup guide: `visualization/dashboards/powerbi_setup.md`

Metabase
- Connect Metabase to Postgres at `localhost:5432`, database `crypto_db`, user `crypto_user`, password `crypto_pass`.
- Use the same SQL from `visualization/dashboards/powerbi_queries.sql` or build cards directly in Metabase.
