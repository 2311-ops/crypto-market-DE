# Power BI and Metabase Setup for Crypto Pipeline (Phase 6)

Prereqs
- Running stack: Postgres, MinIO, Kafka, Spark, Airflow.
- Network access from Power BI Desktop to Postgres host/port (from your machine, use `localhost:5432` per docker-compose).
- Credentials: `user=crypto_user`, `password=crypto_pass`, `db=crypto_db`.

Metabase is the quickest live dashboard path if you want a browser-based UI without desktop tooling.

Metabase quick start
1) Start the stack: `docker compose up -d`
2) Open `http://localhost:3000`
3) Create the first admin account
4) Add a new database connection:
   - Type: PostgreSQL
   - Host: `postgres`
   - Port: `5432`
   - Database: `crypto_db`
   - User: `crypto_user`
   - Password: `crypto_pass`
5) Save, then create questions/cards from the SQL in `powerbi_queries.sql`
6) Build a dashboard and turn on auto-refresh if desired

Option A — Direct Postgres connection (recommended)
1) In Power BI Desktop: Home → Get Data → PostgreSQL.
2) Server: `localhost`, Database: `crypto_db`.
3) Advanced options → SQL statement: paste the query you need from `powerbi_queries.sql` (e.g., latest snapshot or top volumes).
4) Data Connectivity: choose Import (or DirectQuery if latency acceptable).
5) Load → build visuals:
   - Top by volume: use query #1.
   - Price trend: use query #2 with a parameterized `symbol`.
   - Volatility heatmap: query #3, visualize as matrix or heatmap.
   - Volume spike rate: query #4 for KPI/table.

Option B — Parquet from MinIO (S3)
1) In MinIO create an access key/secret (or reuse `minio_admin` / `minio_password`).
2) Use Power BI S3 connector (or generic S3-compatible) pointing to `http://localhost:9000`, bucket `processed-data`, path `stream/enriched/`.
3) If connector not available, sync parquet locally:
   - `docker compose exec minio mc cp --recursive local/processed-data/stream/enriched/ /tmp/enriched/`
   - Load folder in Power BI as parquet files.

Model tips
- Use `symbol` as primary dimension.
- Date hierarchy from `recorded_at`.
- Create measures:
  - `Volume Spike Rate = DIVIDE(SUM(volume_spike), COUNTROWS(...))`
  - `Avg Volatility = AVERAGE(volatility_score)`

Refresh guidance
- For Import mode, schedule refresh to match ingestion cadence (e.g., every 5–15 minutes) if your gateway allows.
- Ensure ingestion and Spark are running before refresh; otherwise tables may be empty.

Validation checklist
- `coin_prices` has rows (`SELECT count(*) FROM coin_prices;`).
- `market_stats` has rows (`SELECT count(*) FROM market_stats;`).
- `pipeline_logs` has rows (`SELECT count(*) FROM pipeline_logs;`).
- MinIO bucket `processed-data` contains parquet files under `stream/enriched/`.
- Queries from `powerbi_queries.sql` return data in psql before connecting from Power BI.
