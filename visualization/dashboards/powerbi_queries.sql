-- Power BI / Metabase ready queries for crypto pipeline

-- 1) Top coins by 24h volume (latest snapshot)
WITH latest AS (
    SELECT DISTINCT ON (symbol)
        symbol, name, volume_24h, price_usd, market_cap, price_change_pct_24h, recorded_at
    FROM coin_prices
    ORDER BY symbol, recorded_at DESC
)
SELECT symbol, name, volume_24h, price_usd, market_cap, price_change_pct_24h, recorded_at
FROM latest
ORDER BY volume_24h DESC
LIMIT 20;

-- 2) Price trend (time series) for a selected symbol
-- Parameterize :symbol in your BI tool
SELECT
    recorded_at::date AS day,
    AVG(price_usd) AS avg_price,
    MAX(price_usd) AS max_price,
    MIN(price_usd) AS min_price
FROM coin_prices
WHERE symbol = :symbol
GROUP BY recorded_at::date
ORDER BY day;

-- 3) Volatility heatmap (5-min rolling stddev from market_stats)
SELECT
    symbol,
    window_start,
    window_end,
    volatility_score
FROM market_stats
ORDER BY window_end DESC, symbol;

-- 4) Volume spike rate per symbol (percentage of records flagged)
SELECT
    symbol,
    100.0 * SUM(CASE WHEN volume_spike THEN 1 ELSE 0 END)::decimal / COUNT(*) AS volume_spike_pct,
    COUNT(*) AS records
FROM market_stats
GROUP BY symbol
HAVING COUNT(*) > 0
ORDER BY volume_spike_pct DESC;

-- 5) Latest snapshot table for tool-friendly import
SELECT DISTINCT ON (symbol)
    symbol,
    name,
    price_usd,
    market_cap,
    volume_24h,
    price_change_pct_24h,
    recorded_at
FROM coin_prices
ORDER BY symbol, recorded_at DESC;
