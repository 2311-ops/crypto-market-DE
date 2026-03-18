# Crypto Market Data Pipeline

Start here:
- [PROJECT_README.md](./PROJECT_README.md) for the full architecture, runtime behavior, data flow, dashboards, and troubleshooting.

Quick links:
- [RUNBOOK.md](./RUNBOOK.md)
- [visualization/dashboards/powerbi_setup.md](./visualization/dashboards/powerbi_setup.md)
- [visualization/dashboards/powerbi_queries.sql](./visualization/dashboards/powerbi_queries.sql)

What this project does:
- ingests crypto market data from CoinGecko every 60 seconds
- publishes to Kafka
- processes the data with Spark
- stores analytical tables in PostgreSQL
- writes parquet outputs to MinIO
- powers Metabase and Power BI dashboards from PostgreSQL
