CREATE OR REFRESH MATERIALIZED VIEW p1_crypto.gold_layer.gold_segment_summary
AS
WITH classified AS (
    SELECT *,
        CASE
            WHEN market_cap_rank <= 10 THEN 'mega cap'
            WHEN market_cap_rank <= 50 THEN 'large cap'
            WHEN market_cap_rank <= 200 THEN 'mid cap'
            ELSE 'small cap'
        END AS cap_segment
    FROM p1_crypto.gold_layer.gold_latest_market_snapshot
)

SELECT
    cap_segment,
    round(AVG(price_change_percentage_24h),2) AS avg_return_24h,
    SUM(market_cap) AS total_market_cap,
    COUNT(*) AS total_coins
FROM classified
GROUP BY cap_segment;