create or refresh materialized view p1_crypto.gold_layer.market_leaders_24h_trend as
 select
   date_format(last_updated, 'yyyy-MM-dd HH:mm:ss') as date_time,
   name, id, current_price, high_24h, low_24h, price_change_percentage_24h, market_cap 
 from p1_crypto.silver_layer.silver_crypto_streamtable
 where market_cap_rank <= 5 and last_updated between date_sub(current_timestamp(), 1) and current_timestamp()
 order by market_cap_rank asc, id asc, date_time desc
 limit 120;