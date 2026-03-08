CREATE OR REFRESH MATERIALIZED VIEW p1_crypto.gold_layer.gold_market_snapshot
AS
SELECT
    CURRENT_TIMESTAMP() AS snapshot_time,
    SUM(market_cap) AS total_market_cap,
    SUM(total_volume) AS total_volume_24h,
    SUM(total_supply) AS total_supply
FROM p1_crypto.gold_layer.gold_latest_market_snapshot;