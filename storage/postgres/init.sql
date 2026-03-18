\set ON_ERROR_STOP on

-- Idempotent setup for crypto_db schema, Airflow metadata DB, and Power BI reader

-- Create roles first (in default postgres database) using DO block
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
      CREATE ROLE airflow_user LOGIN PASSWORD 'airflow_pass';
   END IF;
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'powerbi_reader') THEN
      CREATE ROLE powerbi_reader LOGIN PASSWORD 'powerbi_pass';
   END IF;
END;
$$;

-- Create Airflow metadata database (allow error if exists)
\set ON_ERROR_STOP off
CREATE DATABASE airflow_db OWNER airflow_user;
\set ON_ERROR_STOP on

-- Switch into the target data warehouse DB
\connect crypto_db

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS coin_prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    price_usd NUMERIC(18,8) NOT NULL,
    market_cap NUMERIC(24,2),
    volume_24h NUMERIC(24,2),
    price_change_pct_24h NUMERIC(8,4),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_coin_prices_symbol_time
    ON coin_prices (symbol, recorded_at DESC);

CREATE TABLE IF NOT EXISTS market_stats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol TEXT NOT NULL,
    volatility_score NUMERIC(10,4),
    volume_spike BOOLEAN DEFAULT FALSE,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_market_stats_symbol_window
    ON market_stats (symbol, window_end DESC);

CREATE TABLE IF NOT EXISTS pipeline_logs (
    run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    stage TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('started','running','success','failed')),
    message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_stage_time
    ON pipeline_logs (stage, created_at DESC);

-- Grants for Power BI
GRANT CONNECT ON DATABASE crypto_db TO powerbi_reader;
GRANT USAGE ON SCHEMA public TO powerbi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO powerbi_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO powerbi_reader;