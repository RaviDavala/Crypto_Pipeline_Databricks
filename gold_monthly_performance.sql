CREATE OR REFRESH MATERIALIZED VIEW p1_crypto.gold_layer.gold_monthly_performance
PARTITIONED BY (month)
AS
WITH monthly_prices AS (
    SELECT
        id,
        name,
        DATE_TRUNC('month', last_updated) AS month,
        FIRST_VALUE(current_price) OVER (
            PARTITION BY id, DATE_TRUNC('month', last_updated)
            ORDER BY last_updated ASC
        ) AS first_price,
        FIRST_VALUE(current_price) OVER (
            PARTITION BY id, DATE_TRUNC('month', last_updated)
            ORDER BY last_updated DESC
        ) AS last_price
    FROM p1_crypto.silver_layer.silver_crypto_streamtable
)

SELECT DISTINCT
    id,
    name,
    month,
    first_price,
    last_price,
    round(((last_price - first_price) / first_price) * 100,2) AS monthly_return
FROM monthly_prices;