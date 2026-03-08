create or refresh materialized view p1_crypto.gold_layer.gold_latest_market_snapshot as
select date_format(last_updated, 'yyyy-MM-dd HH:mm:ss') as date_time,
  name,
  current_price,
  high_24h,
  market_cap,
  (market_cap - market_cap_change_24h) as previous_market_cap,
  low_24h,
  price_change_percentage_24h,
  market_cap_change_24h,
  total_supply,
  total_volume,
  market_cap_rank
from p1_crypto.silver_layer.silver_crypto_streamtable
where ingested_at = (select max(ingested_at) from p1_crypto.silver_layer.silver_crypto_streamtable);