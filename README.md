# Crypto Market Data Pipeline
project Architecture  
<img width="963" height="888" alt="Screenshot 2026-03-18 075846" src="https://github.com/user-attachments/assets/6c70c35e-8772-4c8f-bb1d-a32d947903bb" />

Start here:
- [PROJECT_README.md](./PROJECT_README.md) for the full architecture, runtime behavior, data flow, dashboards, and troubleshooting.

Quick links:
- [visualization/dashboards/powerbi_setup.md](./visualization/dashboards/powerbi_setup.md)
- [visualization/dashboards/powerbi_queries.sql](./visualization/dashboards/powerbi_queries.sql)

What this project does:
- ingests crypto market data from CoinGecko every 60 seconds
- publishes to Kafka
- processes the data with Spark
- stores analytical tables in PostgreSQL
- writes parquet outputs to MinIO
- powers Metabase and Power BI dashboards from PostgreSQL
