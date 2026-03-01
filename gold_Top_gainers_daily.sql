create or refresh materialized view p1_crypto.gold_layer.gold_top_10_gainers_daily as
  select
  name as Asset_Name,
  current_price as Current_Price,
  high_24h as 24h_High,
  low_24h as 24h_Low,
  price_change_24h as Price_Change,
  price_change_percentage_24h as Price_Change_Percentage
  from p1_crypto.silver_layer.silver_crypto_streamtable
  where market_cap_rank <= 50 and ingested_at = (select max(ingested_at) from p1_crypto.silver_layer.silver_crypto_streamtable)
  order by price_change_percentage_24h desc
  limit 10;
 