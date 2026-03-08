CREATE OR REFRESH MATERIALIZED VIEW p1_crypto.gold_layer.gold_hourly_cap_ranking
AS
WITH classified AS (
    SELECT *,
        CASE
            WHEN market_cap_rank <= 10 THEN 'mega cap'
            WHEN market_cap_rank <= 50 THEN 'large cap'
            WHEN market_cap_rank <= 200 THEN 'mid cap'
            ELSE 'small cap'
        END AS cap_segment
    FROM p1_crypto.silver_layer.silver_crypto_streamtable
    WHERE ingested_at = (select max(ingested_at) from p1_crypto.silver_layer.silver_crypto_streamtable)
)

SELECT *,
       RANK() OVER (PARTITION BY cap_segment
                    ORDER BY price_change_percentage_24h DESC) AS rank_within_segment
FROM classified;