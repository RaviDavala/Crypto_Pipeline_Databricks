CREATE OR REFRESH MATERIALIZED VIEW p1_crypto.gold_layer.gold_daily_snapshot
PARTITIONED BY (date)
AS
WITH daily_ranked AS (
    SELECT *,
           DATE(last_updated) AS date,
           ROW_NUMBER() OVER (
               PARTITION BY id, DATE(last_updated)
               ORDER BY last_updated DESC
           ) AS rn
    FROM p1_crypto.silver_layer.silver_crypto_streamtable
)

SELECT
    id,
    name,
    symbol,
    date,
    current_price AS close_price,
    high_24h,
    low_24h,
    total_volume,
    market_cap
FROM daily_ranked
WHERE rn = 1;